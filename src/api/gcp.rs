//! [`BqApi`] implementation over the Google Cloud REST APIs.
//!
//! Authentication uses Application Default Credentials via `gcp_auth`
//! (``GOOGLE_APPLICATION_CREDENTIALS``, ``gcloud auth application-default
//! login``, the metadata server, or the ``gcloud`` CLI).  Credentials are
//! resolved lazily on the first request.

use std::collections::HashMap;
use std::future::Future;
use std::path::Path;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use anyhow::{Context, anyhow};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use chrono::{DateTime, Utc};
use futures::{StreamExt, TryStreamExt, stream};
use gcp_auth::TokenProvider;
use regex::Regex;
use reqwest::{Method, RequestBuilder};
use serde_json::{Value, json};

use super::BqApi;
use super::tabular::{self, BqField};
use crate::error::{Error, Result};
use crate::resources::{
    DataFormat, EPOCH, ExternalTableInfo, RoutineArgument, RoutineInfo, SavedQueryInfo,
    SchemaField, TableInfo, TransferConfig, ViewInfo,
};

const SCOPES: &[&str] = &["https://www.googleapis.com/auth/cloud-platform"];
const BQ: &str = "https://bigquery.googleapis.com/bigquery/v2";
const BQ_UPLOAD: &str = "https://bigquery.googleapis.com/upload/bigquery/v2";
const DTS: &str = "https://bigquerydatatransfer.googleapis.com/v1";
const DATAFORM: &str = "https://dataform.googleapis.com/v1beta1";

/// Maximum concurrent per-resource metadata requests while listing.
const CONCURRENCY: usize = 8;
const QUERY_TIMEOUT_MS: &str = "10000";

/// Standard SQL type kinds accepted by the routines API.
const TYPE_KINDS: &[&str] = &[
    "TYPE_KIND_UNSPECIFIED",
    "INT64",
    "BOOL",
    "FLOAT64",
    "STRING",
    "BYTES",
    "TIMESTAMP",
    "DATE",
    "TIME",
    "DATETIME",
    "INTERVAL",
    "GEOGRAPHY",
    "NUMERIC",
    "BIGNUMERIC",
    "JSON",
    "ARRAY",
    "STRUCT",
    "RANGE",
];

static REPO_ID_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"[^a-z0-9-]").unwrap());

/// Production [`BqApi`] backed by the BigQuery, Data Transfer and
/// Dataform REST APIs.
pub struct GcpClient {
    rt: tokio::runtime::Runtime,
    http: reqwest::Client,
    auth: tokio::sync::OnceCell<Arc<dyn TokenProvider>>,
}

impl GcpClient {
    /// Create a client.  No network access happens until the first call.
    pub fn new() -> Result<Self> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("failed to start async runtime")?;
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(30))
            .user_agent(concat!("bq-sync/", env!("CARGO_PKG_VERSION")))
            .build()
            .context("failed to build HTTP client")?;
        Ok(GcpClient {
            rt,
            http,
            auth: tokio::sync::OnceCell::new(),
        })
    }

    fn block_on<F: Future>(&self, fut: F) -> F::Output {
        self.rt.block_on(fut)
    }

    async fn token(&self) -> Result<String> {
        let provider = self
            .auth
            .get_or_try_init(|| async {
                gcp_auth::provider().await.map_err(|e| {
                    anyhow!(
                        "no Google Cloud credentials found ({e}); run \
                         `gcloud auth application-default login` or set \
                         GOOGLE_APPLICATION_CREDENTIALS"
                    )
                })
            })
            .await?;
        let token = provider
            .token(SCOPES)
            .await
            .map_err(|e| anyhow!("failed to obtain access token: {e}"))?;
        Ok(token.as_str().to_owned())
    }

    async fn request(
        &self,
        method: Method,
        url: &str,
        quota_project: Option<&str>,
    ) -> Result<RequestBuilder> {
        let mut req = self
            .http
            .request(method, url)
            .bearer_auth(self.token().await?);
        if let Some(project) = quota_project {
            req = req.header("x-goog-user-project", project);
        }
        Ok(req)
    }

    /// Send *req* and decode the JSON response (``Null`` for empty bodies).
    async fn send(req: RequestBuilder) -> Result<Value> {
        let resp = req.send().await.context("HTTP request failed")?;
        let status = resp.status();
        let bytes = resp.bytes().await.context("failed to read response")?;
        if !status.is_success() {
            return Err(api_error(status.as_u16(), &bytes));
        }
        if bytes.is_empty() {
            return Ok(Value::Null);
        }
        Ok(serde_json::from_slice(&bytes).context("invalid JSON response")?)
    }

    async fn get(&self, url: &str, query: &[(&str, &str)], quota: Option<&str>) -> Result<Value> {
        Self::send(self.request(Method::GET, url, quota).await?.query(query)).await
    }

    async fn send_json(
        &self,
        method: Method,
        url: &str,
        query: &[(&str, &str)],
        body: &Value,
        quota: Option<&str>,
    ) -> Result<Value> {
        Self::send(
            self.request(method, url, quota)
                .await?
                .query(query)
                .json(body),
        )
        .await
    }

    async fn delete(&self, url: &str, query: &[(&str, &str)], quota: Option<&str>) -> Result<()> {
        Self::send(self.request(Method::DELETE, url, quota).await?.query(query)).await?;
        Ok(())
    }

    /// Follow ``nextPageToken`` and collect every element of *items_key*.
    async fn list_all(
        &self,
        url: &str,
        query: &[(&str, &str)],
        items_key: &str,
        quota: Option<&str>,
    ) -> Result<Vec<Value>> {
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut q: Vec<(&str, &str)> = query.to_vec();
            if let Some(t) = token.as_deref() {
                q.push(("pageToken", t));
            }
            let page = self.get(url, &q, quota).await?;
            if let Some(items) = page.get(items_key).and_then(Value::as_array) {
                out.extend(items.iter().cloned());
            }
            match page.get("nextPageToken").and_then(Value::as_str) {
                Some(t) if !t.is_empty() => token = Some(t.to_owned()),
                _ => return Ok(out),
            }
        }
    }

    // ------------------------------------------------------------------
    // BigQuery: tables & routines
    // ------------------------------------------------------------------

    fn table_url(project: &str, dataset: &str, table: &str) -> String {
        format!("{BQ}/projects/{project}/datasets/{dataset}/tables/{table}")
    }

    fn routine_url(project: &str, dataset: &str, routine: &str) -> String {
        format!("{BQ}/projects/{project}/datasets/{dataset}/routines/{routine}")
    }

    async fn get_table(&self, project: &str, dataset: &str, table: &str) -> Result<Value> {
        self.get(&Self::table_url(project, dataset, table), &[], None)
            .await
    }

    /// Full metadata of every table of *kind* (``VIEW``, ``TABLE``, …).
    async fn tables_of_type(&self, project: &str, dataset: &str, kind: &str) -> Result<Vec<Value>> {
        let url = format!("{BQ}/projects/{project}/datasets/{dataset}/tables");
        let items = self
            .list_all(&url, &[("maxResults", "1000")], "tables", None)
            .await?;
        let ids: Vec<String> = items
            .iter()
            .filter(|t| str_at(t, &["type"]) == Some(kind))
            .filter_map(|t| str_at(t, &["tableReference", "tableId"]).map(str::to_owned))
            .collect();
        stream::iter(ids)
            .map(|id| async move { self.get_table(project, dataset, &id).await })
            .buffered(CONCURRENCY)
            .try_collect()
            .await
    }

    async fn get_routine(&self, project: &str, dataset: &str, routine: &str) -> Result<Value> {
        self.get(&Self::routine_url(project, dataset, routine), &[], None)
            .await
    }

    async fn put_routine(
        &self,
        project: &str,
        dataset: &str,
        name: &str,
        body: &Value,
    ) -> Result<()> {
        self.send_json(
            Method::PUT,
            &Self::routine_url(project, dataset, name),
            &[],
            body,
            None,
        )
        .await?;
        Ok(())
    }

    async fn update_view(&self, project: &str, dataset: &str, name: &str, sql: &str) -> Result<()> {
        let table = self.get_table(project, dataset, name).await?;
        let mut view = table.get("view").cloned().unwrap_or_else(|| json!({}));
        view["query"] = json!(sql);
        if view.get("useLegacySql").is_none_or(Value::is_null) {
            view["useLegacySql"] = json!(false);
        }
        self.send_json(
            Method::PATCH,
            &Self::table_url(project, dataset, name),
            &[],
            &json!({ "view": view }),
            None,
        )
        .await?;
        log::info!("Updated view {project}.{dataset}.{name}");
        Ok(())
    }

    async fn create_view(&self, project: &str, dataset: &str, name: &str, sql: &str) -> Result<()> {
        let body = json!({
            "tableReference": {"projectId": project, "datasetId": dataset, "tableId": name},
            "view": {"query": sql, "useLegacySql": false},
        });
        let url = format!("{BQ}/projects/{project}/datasets/{dataset}/tables");
        self.send_json(Method::POST, &url, &[], &body, None).await?;
        log::info!("Created view {project}.{dataset}.{name}");
        Ok(())
    }

    async fn update_table_description(
        &self,
        project: &str,
        dataset: &str,
        name: &str,
        description: &str,
        field_descriptions: &HashMap<String, String>,
    ) -> Result<()> {
        let table = self.get_table(project, dataset, name).await?;
        let mut patch = json!({ "description": description });
        if !field_descriptions.is_empty() {
            let mut schema = table.get("schema").cloned().unwrap_or_else(|| json!({}));
            if let Some(fields) = schema.get_mut("fields").and_then(Value::as_array_mut) {
                for field in fields {
                    let fname = str_at(field, &["name"]).unwrap_or_default().to_owned();
                    if let Some(desc) = field_descriptions.get(&fname) {
                        field["description"] = json!(desc);
                    }
                }
            }
            patch["schema"] = schema;
        }
        self.send_json(
            Method::PATCH,
            &Self::table_url(project, dataset, name),
            &[],
            &patch,
            None,
        )
        .await?;
        log::info!("Updated description for {project}.{dataset}.{name}");
        Ok(())
    }

    async fn update_routine_field(
        &self,
        project: &str,
        dataset: &str,
        name: &str,
        key: &str,
        value: &str,
    ) -> Result<()> {
        // routines.update replaces the whole resource: read-modify-write.
        let mut routine = self.get_routine(project, dataset, name).await?;
        routine[key] = json!(value);
        self.put_routine(project, dataset, name, &routine).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn create_routine(
        &self,
        project: &str,
        dataset: &str,
        name: &str,
        body: &str,
        language: &str,
        arguments: Option<&[RoutineArgument]>,
        return_type: Option<&str>,
    ) -> Result<()> {
        let mut routine = json!({
            "routineReference": {"projectId": project, "datasetId": dataset, "routineId": name},
            "definitionBody": body,
            "language": language,
        });
        if let Some(args) = arguments.filter(|a| !a.is_empty()) {
            routine["arguments"] = Value::Array(
                args.iter()
                    .map(|a| json!({"name": a.name, "dataType": {"typeKind": type_kind(&a.arg_type)}}))
                    .collect(),
            );
        }
        if let Some(rt) = return_type.filter(|r| !r.is_empty()) {
            routine["returnType"] = json!({"typeKind": type_kind(rt)});
        }
        let url = format!("{BQ}/projects/{project}/datasets/{dataset}/routines");
        self.send_json(Method::POST, &url, &[], &routine, None)
            .await?;
        log::info!("Created routine {project}.{dataset}.{name}");
        Ok(())
    }

    // ------------------------------------------------------------------
    // BigQuery: jobs
    // ------------------------------------------------------------------

    /// Start a query and wait for it; returns the first result page
    /// (with ``schema``) plus the job id / location for paging.
    async fn start_query(
        &self,
        project: &str,
        sql: &str,
        location: Option<&str>,
    ) -> Result<(Value, String, Option<String>)> {
        let mut body = json!({
            "query": sql,
            "useLegacySql": false,
            "timeoutMs": 10_000,
            "formatOptions": {"useInt64Timestamp": true},
        });
        if let Some(loc) = location {
            body["location"] = json!(loc);
        }
        let url = format!("{BQ}/projects/{project}/queries");
        let mut page = self.send_json(Method::POST, &url, &[], &body, None).await?;
        let job_id = str_at(&page, &["jobReference", "jobId"])
            .ok_or_else(|| anyhow!("query response without job id"))?
            .to_owned();
        let job_location = str_at(&page, &["jobReference", "location"]).map(str::to_owned);

        while !page
            .get("jobComplete")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            page = self
                .query_results(project, &job_id, job_location.as_deref(), None)
                .await?;
        }
        self.check_job(project, &job_id, job_location.as_deref())
            .await?;
        Ok((page, job_id, job_location))
    }

    async fn query_results(
        &self,
        project: &str,
        job_id: &str,
        location: Option<&str>,
        page_token: Option<&str>,
    ) -> Result<Value> {
        let url = format!("{BQ}/projects/{project}/queries/{job_id}");
        let mut q = vec![
            ("timeoutMs", QUERY_TIMEOUT_MS),
            ("formatOptions.useInt64Timestamp", "true"),
        ];
        if let Some(loc) = location {
            q.push(("location", loc));
        }
        if let Some(t) = page_token {
            q.push(("pageToken", t));
        }
        self.get(&url, &q, None).await
    }

    /// Fail when the job finished with an ``errorResult``.
    async fn check_job(
        &self,
        project: &str,
        job_id: &str,
        location: Option<&str>,
    ) -> Result<Value> {
        let url = format!("{BQ}/projects/{project}/jobs/{job_id}");
        let q: Vec<(&str, &str)> = location.map(|l| ("location", l)).into_iter().collect();
        let job = self.get(&url, &q, None).await?;
        if let Some(err) = job.pointer("/status/errorResult") {
            let message = str_at(err, &["message"]).unwrap_or("job failed");
            return Err(Error::Api {
                status: 400,
                message: message.to_owned(),
            });
        }
        Ok(job)
    }

    async fn wait_job_done(&self, project: &str, job: &Value) -> Result<()> {
        let job_id = str_at(job, &["jobReference", "jobId"])
            .ok_or_else(|| anyhow!("job response without job id"))?
            .to_owned();
        let location = str_at(job, &["jobReference", "location"]).map(str::to_owned);
        loop {
            let current = self
                .check_job(project, &job_id, location.as_deref())
                .await?;
            if str_at(&current, &["status", "state"]) == Some("DONE") {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    async fn query_to_rows(
        &self,
        project: &str,
        sql: &str,
        location: Option<&str>,
    ) -> Result<(Vec<BqField>, Vec<Value>)> {
        let (mut page, job_id, job_location) = self.start_query(project, sql, location).await?;
        let schema = BqField::from_schema(page.get("schema"));
        let mut rows = Vec::new();
        loop {
            if let Some(r) = page.get("rows").and_then(Value::as_array) {
                rows.extend(r.iter().cloned());
            }
            let Some(token) = page
                .get("pageToken")
                .and_then(Value::as_str)
                .map(str::to_owned)
            else {
                break;
            };
            page = self
                .query_results(project, &job_id, job_location.as_deref(), Some(&token))
                .await?;
        }
        Ok((schema, rows))
    }

    async fn table_rows(&self, project: &str, dataset: &str, table: &str) -> Result<Vec<Value>> {
        let url = format!("{}/data", Self::table_url(project, dataset, table));
        let mut rows = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut q = vec![("formatOptions.useInt64Timestamp", "true")];
            if let Some(t) = token.as_deref() {
                q.push(("pageToken", t));
            }
            let page = self.get(&url, &q, None).await?;
            if let Some(r) = page.get("rows").and_then(Value::as_array) {
                rows.extend(r.iter().cloned());
            }
            match page.get("pageToken").and_then(Value::as_str) {
                Some(t) if !t.is_empty() => token = Some(t.to_owned()),
                _ => return Ok(rows),
            }
        }
    }

    // ------------------------------------------------------------------
    // Dataform (saved queries)
    // ------------------------------------------------------------------

    async fn list_repositories(&self, project: &str, region: &str) -> Result<Vec<Value>> {
        let url = format!("{DATAFORM}/projects/{project}/locations/{region}/repositories");
        self.list_all(&url, &[], "repositories", Some(project))
            .await
    }

    async fn find_repository(
        &self,
        project: &str,
        region: &str,
        name: &str,
    ) -> Result<Option<Value>> {
        Ok(self
            .list_repositories(project, region)
            .await?
            .into_iter()
            .find(|r| str_at(r, &["displayName"]) == Some(name)))
    }

    async fn first_workspace(&self, project: &str, repo: &str) -> Result<Option<String>> {
        let url = format!("{DATAFORM}/{repo}/workspaces");
        let page = self.get(&url, &[], Some(project)).await?;
        Ok(page
            .get("workspaces")
            .and_then(Value::as_array)
            .and_then(|ws| ws.first())
            .and_then(|w| str_at(w, &["name"]))
            .map(str::to_owned))
    }

    async fn read_saved_sql(&self, project: &str, workspace: &str) -> Result<String> {
        let url = format!("{DATAFORM}/{workspace}:readFile");
        let resp = self
            .get(&url, &[("path", "content.sql")], Some(project))
            .await?;
        let encoded = str_at(&resp, &["fileContents"]).unwrap_or_default();
        let bytes = B64
            .decode(encoded)
            .context("invalid base64 file contents")?;
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }

    async fn write_saved_sql(&self, project: &str, workspace: &str, sql: &str) -> Result<()> {
        let url = format!("{DATAFORM}/{workspace}:writeFile");
        let body = json!({"path": "content.sql", "contents": B64.encode(sql)});
        self.send_json(Method::POST, &url, &[], &body, Some(project))
            .await?;
        Ok(())
    }

    async fn list_saved_queries_inner(
        &self,
        project: &str,
        region: &str,
    ) -> Result<Vec<SavedQueryInfo>> {
        let mut saved = Vec::new();
        for repo in self.list_repositories(project, region).await? {
            let Some(display) = str_at(&repo, &["displayName"]).filter(|d| !d.is_empty()) else {
                continue;
            };
            let repo_name = str_at(&repo, &["name"]).unwrap_or_default();
            let ws = match self.first_workspace(project, repo_name).await {
                Ok(Some(ws)) => ws,
                Ok(None) => continue,
                Err(_) => {
                    log::debug!("Cannot list workspaces for repo '{display}'.");
                    continue;
                }
            };
            match self.read_saved_sql(project, &ws).await {
                Ok(sql) => saved.push(SavedQueryInfo {
                    name: display.to_owned(),
                    sql,
                    modified: EPOCH,
                }),
                Err(_) => log::debug!("Cannot read content.sql in repo '{display}'."),
            }
        }
        Ok(saved)
    }

    async fn update_saved_query(
        &self,
        project: &str,
        region: &str,
        name: &str,
        sql: &str,
    ) -> Result<()> {
        let mut workspace = None;
        if let Some(repo) = self.find_repository(project, region, name).await? {
            let repo_name = str_at(&repo, &["name"]).unwrap_or_default();
            workspace = self.first_workspace(project, repo_name).await?;
        }
        let Some(ws) = workspace else {
            return Err(Error::Invalid(format!(
                "Saved query '{name}' not found in project '{project}' region '{region}'."
            )));
        };
        self.write_saved_sql(project, &ws, sql).await?;
        log::info!("Updated saved query '{name}'");
        Ok(())
    }

    async fn create_saved_query(
        &self,
        project: &str,
        region: &str,
        name: &str,
        sql: &str,
    ) -> Result<()> {
        let repo_id = REPO_ID_RE
            .replace_all(&name.to_lowercase(), "-")
            .into_owned();
        let url = format!("{DATAFORM}/projects/{project}/locations/{region}/repositories");
        let repo = self
            .send_json(
                Method::POST,
                &url,
                &[("repositoryId", &repo_id)],
                &json!({"displayName": name}),
                Some(project),
            )
            .await?;
        let repo_name = str_at(&repo, &["name"])
            .ok_or_else(|| anyhow!("create repository response without name"))?;
        let ws = self
            .send_json(
                Method::POST,
                &format!("{DATAFORM}/{repo_name}/workspaces"),
                &[("workspaceId", "default")],
                &json!({}),
                Some(project),
            )
            .await?;
        let ws_name = str_at(&ws, &["name"])
            .ok_or_else(|| anyhow!("create workspace response without name"))?;
        self.write_saved_sql(project, ws_name, sql).await?;
        log::info!("Created saved query '{name}'");
        Ok(())
    }
}

impl BqApi for GcpClient {
    fn list_views(&self, project: &str, dataset: &str) -> Result<Vec<ViewInfo>> {
        let tables = self.block_on(self.tables_of_type(project, dataset, "VIEW"))?;
        Ok(tables
            .iter()
            .map(|t| ViewInfo {
                name: table_id(t),
                sql: str_at(t, &["view", "query"]).unwrap_or_default().to_owned(),
                modified: millis_at(t, "lastModifiedTime").unwrap_or(EPOCH),
                schema: parse_schema(t),
                description: str_at(t, &["description"]).unwrap_or_default().to_owned(),
                created: millis_at(t, "creationTime"),
                region: str_at(t, &["location"]).map(str::to_owned),
            })
            .collect())
    }

    fn list_routines(&self, project: &str, dataset: &str) -> Result<Vec<RoutineInfo>> {
        self.block_on(async {
            let url = format!("{BQ}/projects/{project}/datasets/{dataset}/routines");
            let items = self.list_all(&url, &[], "routines", None).await?;
            let ids: Vec<String> = items
                .iter()
                .filter_map(|r| str_at(r, &["routineReference", "routineId"]).map(str::to_owned))
                .collect();
            let routines: Vec<Value> = stream::iter(ids)
                .map(|id| async move { self.get_routine(project, dataset, &id).await })
                .buffered(CONCURRENCY)
                .try_collect()
                .await?;
            Ok(routines.iter().map(parse_routine).collect())
        })
    }

    fn list_tables(&self, project: &str, dataset: &str) -> Result<Vec<TableInfo>> {
        let tables = self.block_on(self.tables_of_type(project, dataset, "TABLE"))?;
        Ok(tables.iter().map(parse_table_info).collect())
    }

    fn list_external_tables(&self, project: &str, dataset: &str) -> Result<Vec<ExternalTableInfo>> {
        let tables = self.block_on(self.tables_of_type(project, dataset, "EXTERNAL"))?;
        Ok(tables
            .iter()
            .map(|t| {
                let info = parse_table_info(t);
                let ext = t.get("externalDataConfiguration");
                ExternalTableInfo {
                    name: info.name,
                    source_uris: ext.map(|e| str_list(e, "sourceUris")).unwrap_or_default(),
                    schema: info.schema,
                    source_format: ext
                        .and_then(|e| str_at(e, &["sourceFormat"]))
                        .unwrap_or_default()
                        .to_owned(),
                    modified: info.modified,
                    description: info.description,
                    created: info.created,
                    region: info.region,
                    total_logical_bytes: info.total_logical_bytes,
                    row_count: info.row_count,
                    partitioning: info.partitioning,
                    clustering: info.clustering,
                    primary_keys: info.primary_keys,
                }
            })
            .collect())
    }

    fn get_table_info(&self, project: &str, dataset: &str, name: &str) -> Result<TableInfo> {
        let table = self.block_on(self.get_table(project, dataset, name))?;
        Ok(parse_table_info(&table))
    }

    fn list_saved_queries(&self, project: &str, region: &str) -> Result<Vec<SavedQueryInfo>> {
        log::warn!(
            "Saved queries use the Dataform API which is an unstable feature. \
             Results may be incomplete or change without notice."
        );
        match self.block_on(self.list_saved_queries_inner(project, region)) {
            Ok(saved) => Ok(saved),
            Err(e) => {
                log::warn!(
                    "Failed to list saved queries via Dataform API. \
                     Dataform may not be enabled for project '{project}'. ({e})"
                );
                Ok(Vec::new())
            }
        }
    }

    fn list_transfer_configs(&self, project: &str, region: &str) -> Result<Vec<TransferConfig>> {
        let url = format!("{DTS}/projects/{project}/locations/{region}/transferConfigs");
        let items = self.block_on(self.list_all(
            &url,
            &[("dataSourceIds", "scheduled_query")],
            "transferConfigs",
            Some(project),
        ))?;
        Ok(items
            .iter()
            .map(|tc| TransferConfig {
                name: str_at(tc, &["name"]).unwrap_or_default().to_owned(),
                display_name: str_at(tc, &["displayName"]).unwrap_or_default().to_owned(),
                schedule: str_at(tc, &["schedule"]).unwrap_or_default().to_owned(),
                data_source_id: str_at(tc, &["dataSourceId"]).unwrap_or_default().to_owned(),
                query: str_at(tc, &["params", "query"])
                    .unwrap_or_default()
                    .to_owned(),
                update_time: str_at(tc, &["updateTime"])
                    .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                    .map(|d| d.with_timezone(&Utc)),
            })
            .filter(|tc| tc.data_source_id == "scheduled_query")
            .collect())
    }

    fn update_transfer_config_schedule(
        &self,
        project: &str,
        name: &str,
        schedule: &str,
    ) -> Result<()> {
        let url = format!("{DTS}/{name}");
        self.block_on(self.send_json(
            Method::PATCH,
            &url,
            &[("updateMask", "schedule")],
            &json!({"schedule": schedule}),
            Some(project),
        ))?;
        Ok(())
    }

    fn start_manual_transfer_run(&self, project: &str, name: &str) -> Result<()> {
        let url = format!("{DTS}/{name}:startManualRuns");
        let now = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true);
        self.block_on(self.send_json(
            Method::POST,
            &url,
            &[],
            &json!({"requestedRunTime": now}),
            Some(project),
        ))?;
        Ok(())
    }

    fn fetch_table_to_file(
        &self,
        project: &str,
        dataset: &str,
        table: &str,
        dest: &Path,
        fmt: DataFormat,
    ) -> Result<()> {
        let (schema, rows) = self.block_on(async {
            let meta = self.get_table(project, dataset, table).await?;
            match str_at(&meta, &["type"]) {
                // tabledata.list does not serve views: run them instead.
                Some("VIEW" | "MATERIALIZED_VIEW") => {
                    let sql = format!("SELECT * FROM `{project}.{dataset}.{table}`");
                    self.query_to_rows(project, &sql, str_at(&meta, &["location"]))
                        .await
                }
                _ => {
                    let schema = BqField::from_schema(meta.get("schema"));
                    let rows = self.table_rows(project, dataset, table).await?;
                    Ok((schema, rows))
                }
            }
        })?;
        tabular::write_rows(&schema, &rows, dest, fmt)
    }

    fn fetch_query_to_file(
        &self,
        project: &str,
        sql: &str,
        dest: &Path,
        fmt: DataFormat,
    ) -> Result<()> {
        let (schema, rows) = self.block_on(self.query_to_rows(project, sql, None))?;
        tabular::write_rows(&schema, &rows, dest, fmt)
    }

    fn load_table_from_file(
        &self,
        project: &str,
        dataset: &str,
        table: &str,
        source: &Path,
        fmt: DataFormat,
    ) -> Result<()> {
        if !source.is_file() {
            return Err(Error::Invalid(format!(
                "Source file not found: {}",
                source.display()
            )));
        }
        let data = std::fs::read(source).map_err(|e| Error::io(source, e))?;
        let mut load = json!({
            "destinationTable": {"projectId": project, "datasetId": dataset, "tableId": table},
            "sourceFormat": match fmt { DataFormat::Csv => "CSV", DataFormat::Parquet => "PARQUET" },
            "writeDisposition": "WRITE_TRUNCATE",
        });
        if fmt == DataFormat::Csv {
            load["autodetect"] = json!(true);
        }
        let config = json!({
            "jobReference": {"projectId": project},
            "configuration": {"load": load},
        });
        self.block_on(async {
            let url = format!("{BQ_UPLOAD}/projects/{project}/jobs");
            let resp = self
                .request(Method::POST, &url, None)
                .await?
                .query(&[("uploadType", "resumable")])
                .header("X-Upload-Content-Type", "application/octet-stream")
                .json(&config)
                .send()
                .await
                .context("HTTP request failed")?;
            let status = resp.status();
            if !status.is_success() {
                let bytes = resp.bytes().await.unwrap_or_default();
                return Err(api_error(status.as_u16(), &bytes));
            }
            let session = resp
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|v| v.to_str().ok())
                .ok_or_else(|| anyhow!("resumable upload without session URI"))?
                .to_owned();
            let job = Self::send(
                self.request(Method::PUT, &session, None)
                    .await?
                    .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
                    .body(data),
            )
            .await?;
            self.wait_job_done(project, &job).await
        })?;
        log::info!(
            "Loaded {} into {project}.{dataset}.{table} (WRITE_TRUNCATE)",
            source.display()
        );
        Ok(())
    }

    fn upsert_view(&self, project: &str, dataset: &str, name: &str, sql: &str) -> Result<()> {
        self.block_on(async {
            match self.update_view(project, dataset, name, sql).await {
                Err(e) if e.is_not_found() => {
                    log::info!("View {project}.{dataset}.{name} not found — creating.");
                    self.create_view(project, dataset, name, sql).await
                }
                other => other,
            }
        })
    }

    fn upsert_table_description(
        &self,
        project: &str,
        dataset: &str,
        name: &str,
        description: &str,
        field_descriptions: &HashMap<String, String>,
    ) -> Result<()> {
        match self.block_on(self.update_table_description(
            project,
            dataset,
            name,
            description,
            field_descriptions,
        )) {
            Err(e) if e.is_not_found() => {
                log::warn!(
                    "Table {project}.{dataset}.{name} not found; skipping description push. \
                     Create or materialize the table first, then push its model YAML."
                );
                Ok(())
            }
            other => other,
        }
    }

    fn upsert_routine(
        &self,
        project: &str,
        dataset: &str,
        name: &str,
        body: &str,
        language: &str,
        arguments: Option<&[RoutineArgument]>,
        return_type: Option<&str>,
    ) -> Result<()> {
        self.block_on(async {
            match self
                .update_routine_field(project, dataset, name, "definitionBody", body)
                .await
            {
                Ok(()) => {
                    log::info!("Updated routine {project}.{dataset}.{name}");
                    Ok(())
                }
                Err(e) if e.is_not_found() => {
                    log::info!("Routine {project}.{dataset}.{name} not found — creating.");
                    self.create_routine(
                        project,
                        dataset,
                        name,
                        body,
                        language,
                        arguments,
                        return_type,
                    )
                    .await
                }
                Err(e) => Err(e),
            }
        })
    }

    fn upsert_routine_description(
        &self,
        project: &str,
        dataset: &str,
        name: &str,
        description: &str,
    ) -> Result<()> {
        match self.block_on(self.update_routine_field(
            project,
            dataset,
            name,
            "description",
            description,
        )) {
            Ok(()) => {
                log::info!("Updated routine description {project}.{dataset}.{name}");
                Ok(())
            }
            Err(e) if e.is_not_found() => {
                log::warn!(
                    "Routine {project}.{dataset}.{name} not found; skipping description push."
                );
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    fn upsert_saved_query(&self, project: &str, region: &str, name: &str, sql: &str) -> Result<()> {
        self.block_on(async {
            match self.update_saved_query(project, region, name, sql).await {
                Err(Error::Invalid(_)) => {
                    log::info!("Saved query '{name}' not found — creating.");
                    self.create_saved_query(project, region, name, sql).await
                }
                other => other,
            }
        })
    }

    fn delete_view(&self, project: &str, dataset: &str, name: &str) -> Result<()> {
        self.block_on(self.delete(&Self::table_url(project, dataset, name), &[], None))?;
        log::info!("Deleted view {project}.{dataset}.{name}");
        Ok(())
    }

    fn delete_table(&self, project: &str, dataset: &str, name: &str) -> Result<()> {
        self.block_on(self.delete(&Self::table_url(project, dataset, name), &[], None))?;
        log::info!("Deleted table {project}.{dataset}.{name}");
        Ok(())
    }

    fn delete_routine(&self, project: &str, dataset: &str, name: &str) -> Result<()> {
        self.block_on(self.delete(&Self::routine_url(project, dataset, name), &[], None))?;
        log::info!("Deleted routine {project}.{dataset}.{name}");
        Ok(())
    }

    fn delete_saved_query(&self, project: &str, region: &str, name: &str) -> Result<()> {
        self.block_on(async {
            let repo = self
                .find_repository(project, region, name)
                .await?
                .ok_or_else(|| {
                    Error::Invalid(format!(
                        "Saved query '{name}' not found in project '{project}' region '{region}'."
                    ))
                })?;
            let repo_name = str_at(&repo, &["name"]).unwrap_or_default();
            self.delete(
                &format!("{DATAFORM}/{repo_name}"),
                &[("force", "true")],
                Some(project),
            )
            .await
        })?;
        log::info!("Deleted saved query '{name}'");
        Ok(())
    }

    fn run_query(&self, project: &str, sql: &str, location: Option<&str>) -> Result<()> {
        self.block_on(self.start_query(project, sql, location))?;
        log::info!("Executed query on project '{project}'.");
        Ok(())
    }

    fn dry_run_query(&self, project: &str, sql: &str) -> Result<()> {
        let body = json!({
            "query": sql,
            "useLegacySql": false,
            "dryRun": true,
            "useQueryCache": false,
        });
        let url = format!("{BQ}/projects/{project}/queries");
        self.block_on(self.send_json(Method::POST, &url, &[], &body, None))?;
        Ok(())
    }
}

// ----------------------------------------------------------------------
// JSON helpers
// ----------------------------------------------------------------------

fn api_error(status: u16, body: &[u8]) -> Error {
    let parsed: Option<Value> = serde_json::from_slice(body).ok();
    let message = parsed
        .as_ref()
        .and_then(|v| str_at(v, &["error", "message"]))
        .map(str::to_owned)
        .unwrap_or_else(|| String::from_utf8_lossy(body).trim().to_owned());
    if status == 404 {
        Error::NotFound(message)
    } else {
        Error::Api { status, message }
    }
}

fn str_at<'a>(v: &'a Value, path: &[&str]) -> Option<&'a str> {
    path.iter()
        .try_fold(v, |cur, key| cur.get(key))
        .and_then(Value::as_str)
}

fn str_list(v: &Value, key: &str) -> Vec<String> {
    v.get(key)
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn int_at(v: &Value, key: &str) -> Option<i64> {
    match v.get(key)? {
        Value::String(s) => s.parse().ok(),
        Value::Number(n) => n.as_i64(),
        _ => None,
    }
}

/// Parse a millisecond-epoch field (``creationTime``, ``lastModifiedTime``).
fn millis_at(v: &Value, key: &str) -> Option<DateTime<Utc>> {
    let ms = match v.get(key)? {
        Value::String(s) => s.parse::<f64>().ok()?,
        Value::Number(n) => n.as_f64()?,
        _ => return None,
    };
    DateTime::from_timestamp_micros((ms * 1000.0).round() as i64)
}

fn table_id(t: &Value) -> String {
    str_at(t, &["tableReference", "tableId"])
        .unwrap_or_default()
        .to_owned()
}

fn parse_schema(t: &Value) -> Vec<SchemaField> {
    t.pointer("/schema/fields")
        .and_then(Value::as_array)
        .map(|fields| {
            fields
                .iter()
                .map(|f| SchemaField {
                    name: str_at(f, &["name"]).unwrap_or_default().to_owned(),
                    field_type: str_at(f, &["type"]).unwrap_or_default().to_uppercase(),
                    mode: str_at(f, &["mode"]).unwrap_or("NULLABLE").to_uppercase(),
                    description: str_at(f, &["description"]).unwrap_or_default().to_owned(),
                })
                .collect()
        })
        .unwrap_or_default()
}

fn parse_table_info(t: &Value) -> TableInfo {
    let partitioning = t.get("timePartitioning").map(|tp| {
        str_at(tp, &["field"])
            .filter(|f| !f.is_empty())
            .unwrap_or("ingestion_time")
            .to_owned()
    });
    let clustering = t
        .get("clustering")
        .map(|c| str_list(c, "fields"))
        .filter(|c| !c.is_empty());
    let primary_keys = t
        .pointer("/tableConstraints/primaryKey")
        .map(|pk| str_list(pk, "columns"));
    TableInfo {
        name: table_id(t),
        schema: parse_schema(t),
        description: str_at(t, &["description"]).unwrap_or_default().to_owned(),
        row_count: int_at(t, "numRows").unwrap_or(0),
        modified: millis_at(t, "lastModifiedTime").unwrap_or(EPOCH),
        partitioning,
        clustering,
        created: millis_at(t, "creationTime"),
        region: str_at(t, &["location"]).map(str::to_owned),
        primary_keys,
        total_logical_bytes: int_at(t, "numBytes"),
    }
}

fn parse_routine(r: &Value) -> RoutineInfo {
    let arguments = r
        .get("arguments")
        .and_then(Value::as_array)
        .map(|args| {
            args.iter()
                .map(|a| RoutineArgument {
                    name: str_at(a, &["name"]).unwrap_or_default().to_owned(),
                    arg_type: a
                        .get("dataType")
                        .map(|dt| str_at(dt, &["typeKind"]).unwrap_or("TYPE_KIND_UNSPECIFIED"))
                        .unwrap_or("ANY")
                        .to_owned(),
                    mode: str_at(a, &["mode"])
                        .filter(|m| !m.is_empty())
                        .unwrap_or("IN")
                        .to_owned(),
                })
                .collect()
        })
        .unwrap_or_default();
    RoutineInfo {
        name: str_at(r, &["routineReference", "routineId"])
            .unwrap_or_default()
            .to_owned(),
        sql: str_at(r, &["definitionBody"])
            .unwrap_or_default()
            .to_owned(),
        language: str_at(r, &["language"])
            .filter(|l| !l.is_empty())
            .unwrap_or("SQL")
            .to_owned(),
        modified: millis_at(r, "lastModifiedTime").unwrap_or(EPOCH),
        description: str_at(r, &["description"]).unwrap_or_default().to_owned(),
        created: millis_at(r, "creationTime"),
        arguments,
        return_type: r
            .get("returnType")
            .and_then(|rt| str_at(rt, &["typeKind"]))
            .map(str::to_owned),
    }
}

/// Map a type name to a routines-API ``typeKind`` (unknown → unspecified).
fn type_kind(name: &str) -> &'static str {
    TYPE_KINDS
        .iter()
        .find(|k| **k == name)
        .copied()
        .unwrap_or("TYPE_KIND_UNSPECIFIED")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_table_metadata() {
        let t = json!({
            "tableReference": {"tableId": "events"},
            "type": "TABLE",
            "description": "Event log",
            "numRows": "1000",
            "numBytes": "2048",
            "creationTime": "1718452800000",
            "lastModifiedTime": "1735689600123",
            "location": "US",
            "timePartitioning": {"type": "DAY"},
            "clustering": {"fields": ["id"]},
            "tableConstraints": {"primaryKey": {"columns": ["id"]}},
            "schema": {"fields": [
                {"name": "id", "type": "INTEGER", "mode": "REQUIRED", "description": "PK"},
                {"name": "ts", "type": "TIMESTAMP"}
            ]}
        });
        let info = parse_table_info(&t);
        assert_eq!(info.name, "events");
        assert_eq!(info.row_count, 1000);
        assert_eq!(info.total_logical_bytes, Some(2048));
        assert_eq!(info.partitioning.as_deref(), Some("ingestion_time"));
        assert_eq!(info.clustering, Some(vec!["id".to_owned()]));
        assert_eq!(info.primary_keys, Some(vec!["id".to_owned()]));
        assert_eq!(
            crate::utils::isoformat(&info.modified),
            "2025-01-01T00:00:00.123000+00:00"
        );
        assert_eq!(info.schema[1].mode, "NULLABLE");
        assert_eq!(info.schema[1].description, "");
    }

    #[test]
    fn parses_routine() {
        let r = json!({
            "routineReference": {"routineId": "fn"},
            "definitionBody": "x + 1",
            "arguments": [
                {"name": "x", "dataType": {"typeKind": "INT64"}},
                {"name": "y", "argumentKind": "ANY_TYPE"}
            ],
            "returnType": {"typeKind": "INT64"},
            "lastModifiedTime": "0"
        });
        let info = parse_routine(&r);
        assert_eq!(info.language, "SQL");
        assert_eq!(info.arguments[0].arg_type, "INT64");
        assert_eq!(info.arguments[1].arg_type, "ANY");
        assert_eq!(info.arguments[1].mode, "IN");
        assert_eq!(info.return_type.as_deref(), Some("INT64"));
    }

    #[test]
    fn api_errors() {
        let body = br#"{"error": {"code": 404, "message": "Not found: Table p:d.t"}}"#;
        assert!(
            matches!(api_error(404, body), Error::NotFound(m) if m == "Not found: Table p:d.t")
        );
        assert!(matches!(
            api_error(500, b"boom"),
            Error::Api { status: 500, .. }
        ));
    }

    #[test]
    fn type_kinds_and_repo_ids() {
        assert_eq!(type_kind("INT64"), "INT64");
        assert_eq!(type_kind("ANY"), "TYPE_KIND_UNSPECIFIED");
        assert_eq!(
            REPO_ID_RE.replace_all(&"My Query_v2".to_lowercase(), "-"),
            "my-query-v2"
        );
    }
}
