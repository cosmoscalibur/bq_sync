//! [`BqApi`] implementation on the official Google Cloud Rust SDK
//! (`googleapis/google-cloud-rust`):
//!
//! - `google-cloud-bigquery-v2` — tables, routines and query jobs;
//! - `google-cloud-bigquery-datatransfer-v1` — scheduled queries;
//! - `google-cloud-dataform-v1` — BigQuery Studio saved queries;
//! - `google-cloud-auth` — Application Default Credentials.
//!
//! Two BigQuery endpoints are not exposed by the SDK and are called over
//! HTTP with the SDK's credentials: `tabledata.list` (free row reads) and
//! the resumable media upload that backs load jobs.

use std::collections::HashMap;
use std::future::Future;
use std::path::Path;
use std::sync::LazyLock;
use std::time::Duration;

use anyhow::{Context, anyhow};
use chrono::{DateTime, Utc};
use futures::{StreamExt, TryStreamExt, stream};
use google_cloud_auth::credentials::{
    AccessTokenCredentials, Builder as CredentialsBuilder, Credentials,
};
use google_cloud_bigquery_datatransfer_v1::client::DataTransferService;
use google_cloud_bigquery_datatransfer_v1::model as dts;
use google_cloud_bigquery_v2::client::{JobService, RoutineService, TableService};
use google_cloud_bigquery_v2::model as bq;
use google_cloud_dataform_v1::client::Dataform;
use google_cloud_dataform_v1::model as df;
use google_cloud_gax::paginator::{ItemPaginator, Paginator};
use regex::Regex;
use reqwest::Method;
use serde_json::{Value, json};
use tokio::sync::{Mutex, OnceCell};

use super::BqApi;
use super::tabular::{self, BqField};
use crate::error::{Error, Result};
use crate::resources::{
    DataFormat, EPOCH, ExternalTableInfo, RoutineArgument, RoutineInfo, SavedQueryInfo,
    SchemaField, TableInfo, TransferConfig, ViewInfo,
};

const BQ_REST: &str = "https://bigquery.googleapis.com/bigquery/v2";
const BQ_UPLOAD: &str = "https://bigquery.googleapis.com/upload/bigquery/v2";

/// Maximum concurrent per-resource metadata requests while listing.
const CONCURRENCY: usize = 8;
const QUERY_TIMEOUT_MS: u32 = 10_000;

/// Drain an SDK item paginator into a vector.
macro_rules! collect_items {
    ($paginator:expr) => {{
        let mut items = $paginator;
        let mut out = Vec::new();
        while let Some(item) = items.next().await {
            out.push(item.map_err(gax_error)?);
        }
        out
    }};
}

static REPO_ID_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"[^a-z0-9-]").unwrap());

/// BigQuery service clients (no quota project override).
struct BigQueryClients {
    tables: TableService,
    routines: RoutineService,
    jobs: JobService,
}

/// Production [`BqApi`] backed by the Google Cloud Rust SDK.
pub struct GcpClient {
    rt: tokio::runtime::Runtime,
    bigquery: OnceCell<BigQueryClients>,
    /// Data Transfer / Dataform clients bill quota to the target project,
    /// so one client per project is kept.
    transfer: Mutex<HashMap<String, DataTransferService>>,
    dataform: Mutex<HashMap<String, Dataform>>,
    token: OnceCell<AccessTokenCredentials>,
    http: reqwest::Client,
}

impl GcpClient {
    /// Create a client.  Credentials are resolved on first use.
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
            bigquery: OnceCell::new(),
            transfer: Mutex::new(HashMap::new()),
            dataform: Mutex::new(HashMap::new()),
            token: OnceCell::new(),
            http,
        })
    }

    fn block_on<F: Future>(&self, fut: F) -> F::Output {
        self.rt.block_on(fut)
    }

    fn credentials(quota_project: Option<&str>) -> Result<Credentials> {
        let mut builder = CredentialsBuilder::default();
        if let Some(project) = quota_project {
            builder = builder.with_quota_project_id(project);
        }
        builder.build().map_err(credentials_error)
    }

    async fn bq(&self) -> Result<&BigQueryClients> {
        self.bigquery
            .get_or_try_init(|| async {
                let creds = Self::credentials(None)?;
                Ok::<_, Error>(BigQueryClients {
                    tables: TableService::builder()
                        .with_credentials(creds.clone())
                        .build()
                        .await
                        .map_err(build_error)?,
                    routines: RoutineService::builder()
                        .with_credentials(creds.clone())
                        .build()
                        .await
                        .map_err(build_error)?,
                    jobs: JobService::builder()
                        .with_credentials(creds)
                        .build()
                        .await
                        .map_err(build_error)?,
                })
            })
            .await
    }

    async fn transfer(&self, project: &str) -> Result<DataTransferService> {
        let mut cache = self.transfer.lock().await;
        if let Some(c) = cache.get(project) {
            return Ok(c.clone());
        }
        let client = DataTransferService::builder()
            .with_credentials(Self::credentials(Some(project))?)
            .build()
            .await
            .map_err(build_error)?;
        cache.insert(project.to_owned(), client.clone());
        Ok(client)
    }

    async fn dataform(&self, project: &str) -> Result<Dataform> {
        let mut cache = self.dataform.lock().await;
        if let Some(c) = cache.get(project) {
            return Ok(c.clone());
        }
        let client = Dataform::builder()
            .with_credentials(Self::credentials(Some(project))?)
            .build()
            .await
            .map_err(build_error)?;
        cache.insert(project.to_owned(), client.clone());
        Ok(client)
    }

    /// Bearer token for the endpoints the SDK does not cover.
    async fn access_token(&self) -> Result<String> {
        let creds = self
            .token
            .get_or_try_init(|| async {
                CredentialsBuilder::default()
                    .build_access_token_credentials()
                    .map_err(credentials_error)
            })
            .await?;
        let token = creds
            .access_token()
            .await
            .map_err(|e| anyhow!("failed to obtain access token: {e}"))?;
        Ok(token.token)
    }

    // ------------------------------------------------------------------
    // BigQuery: tables & routines
    // ------------------------------------------------------------------

    async fn get_table(&self, project: &str, dataset: &str, table: &str) -> Result<bq::Table> {
        self.bq()
            .await?
            .tables
            .get_table()
            .set_project_id(project)
            .set_dataset_id(dataset)
            .set_table_id(table)
            .send()
            .await
            .map_err(gax_error)
    }

    /// Full metadata of every table of *kind* (``VIEW``, ``TABLE``, …).
    async fn tables_of_type(
        &self,
        project: &str,
        dataset: &str,
        kind: &str,
    ) -> Result<Vec<bq::Table>> {
        let tables = &self.bq().await?.tables;
        let listed: Vec<bq::ListFormatTable> = collect_items!(
            tables
                .list_tables()
                .set_project_id(project)
                .set_dataset_id(dataset)
                .by_item()
        );
        let ids: Vec<String> = listed
            .into_iter()
            .filter(|t| t.r#type == kind)
            .filter_map(|t| t.table_reference.map(|r| r.table_id))
            .collect();
        stream::iter(ids)
            .map(|id: String| async move { self.get_table(project, dataset, &id).await })
            .buffered(CONCURRENCY)
            .try_collect()
            .await
    }

    async fn get_routine(
        &self,
        project: &str,
        dataset: &str,
        routine: &str,
    ) -> Result<bq::Routine> {
        self.bq()
            .await?
            .routines
            .get_routine()
            .set_project_id(project)
            .set_dataset_id(dataset)
            .set_routine_id(routine)
            .send()
            .await
            .map_err(gax_error)
    }

    async fn patch_table(
        &self,
        project: &str,
        dataset: &str,
        name: &str,
        patch: bq::Table,
    ) -> Result<()> {
        self.bq()
            .await?
            .tables
            .patch_table()
            .set_project_id(project)
            .set_dataset_id(dataset)
            .set_table_id(name)
            .set_table(patch)
            .send()
            .await
            .map_err(gax_error)?;
        Ok(())
    }

    async fn update_view(&self, project: &str, dataset: &str, name: &str, sql: &str) -> Result<()> {
        let table = self.get_table(project, dataset, name).await?;
        let mut view = table.view.unwrap_or_default();
        view.query = sql.to_owned();
        view.use_legacy_sql.get_or_insert(false);
        self.patch_table(project, dataset, name, bq::Table::new().set_view(view))
            .await?;
        log::info!("Updated view {project}.{dataset}.{name}");
        Ok(())
    }

    async fn create_view(&self, project: &str, dataset: &str, name: &str, sql: &str) -> Result<()> {
        let table = bq::Table::new()
            .set_table_reference(
                bq::TableReference::new()
                    .set_project_id(project)
                    .set_dataset_id(dataset)
                    .set_table_id(name),
            )
            .set_view(
                bq::ViewDefinition::new()
                    .set_query(sql)
                    .set_use_legacy_sql(false),
            );
        self.bq()
            .await?
            .tables
            .insert_table()
            .set_project_id(project)
            .set_dataset_id(dataset)
            .set_table(table)
            .send()
            .await
            .map_err(gax_error)?;
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
        let mut patch = bq::Table::new().set_description(description);
        if !field_descriptions.is_empty() {
            let mut schema = table.schema.unwrap_or_default();
            for field in &mut schema.fields {
                if let Some(desc) = field_descriptions.get(&field.name) {
                    field.description = Some(desc.clone());
                }
            }
            patch = patch.set_schema(schema);
        }
        self.patch_table(project, dataset, name, patch).await?;
        log::info!("Updated description for {project}.{dataset}.{name}");
        Ok(())
    }

    /// ``routines.update`` replaces the resource: read, modify, write back.
    async fn update_routine(
        &self,
        project: &str,
        dataset: &str,
        name: &str,
        modify: impl FnOnce(&mut bq::Routine),
    ) -> Result<()> {
        let mut routine = self.get_routine(project, dataset, name).await?;
        modify(&mut routine);
        self.bq()
            .await?
            .routines
            .update_routine()
            .set_project_id(project)
            .set_dataset_id(dataset)
            .set_routine_id(name)
            .set_routine(routine)
            .send()
            .await
            .map_err(gax_error)?;
        Ok(())
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
        let mut routine = bq::Routine::new()
            .set_routine_reference(
                bq::RoutineReference::new()
                    .set_project_id(project)
                    .set_dataset_id(dataset)
                    .set_routine_id(name),
            )
            .set_definition_body(body)
            .set_language(bq::routine::Language::from(language));
        if let Some(args) = arguments.filter(|a| !a.is_empty()) {
            routine = routine.set_arguments(args.iter().map(|a| {
                bq::routine::Argument::new()
                    .set_name(&a.name)
                    .set_data_type(sql_type(&a.arg_type))
            }));
        }
        if let Some(rt) = return_type.filter(|r| !r.is_empty()) {
            routine = routine.set_return_type(sql_type(rt));
        }
        self.bq()
            .await?
            .routines
            .insert_routine()
            .set_project_id(project)
            .set_dataset_id(dataset)
            .set_routine(routine)
            .send()
            .await
            .map_err(gax_error)?;
        log::info!("Created routine {project}.{dataset}.{name}");
        Ok(())
    }

    // ------------------------------------------------------------------
    // BigQuery: jobs
    // ------------------------------------------------------------------

    fn int64_timestamps() -> bq::DataFormatOptions {
        bq::DataFormatOptions::new().set_use_int64_timestamp(true)
    }

    async fn get_job(&self, project: &str, job_id: &str, location: &str) -> Result<bq::Job> {
        let job = self
            .bq()
            .await?
            .jobs
            .get_job()
            .set_project_id(project)
            .set_job_id(job_id)
            .set_location(location)
            .send()
            .await
            .map_err(gax_error)?;
        if let Some(err) = job.status.as_ref().and_then(|s| s.error_result.as_ref()) {
            return Err(Error::Api {
                status: 400,
                message: err.message.clone(),
            });
        }
        Ok(job)
    }

    async fn query_results(
        &self,
        project: &str,
        job_id: &str,
        location: &str,
        page_token: &str,
    ) -> Result<bq::GetQueryResultsResponse> {
        self.bq()
            .await?
            .jobs
            .get_query_results()
            .set_project_id(project)
            .set_job_id(job_id)
            .set_location(location)
            .set_page_token(page_token)
            .set_timeout_ms(QUERY_TIMEOUT_MS)
            .set_format_options(Self::int64_timestamps())
            .send()
            .await
            .map_err(gax_error)
    }

    /// Run *sql*, wait for completion and return the schema and all rows.
    async fn run_sql(
        &self,
        project: &str,
        sql: &str,
        location: Option<&str>,
    ) -> Result<(Vec<BqField>, Vec<Value>)> {
        let mut request = bq::QueryRequest::new()
            .set_query(sql)
            .set_use_legacy_sql(false)
            .set_timeout_ms(QUERY_TIMEOUT_MS)
            .set_format_options(Self::int64_timestamps());
        if let Some(loc) = location {
            request = request.set_location(loc);
        }
        let first = self
            .bq()
            .await?
            .jobs
            .query()
            .set_project_id(project)
            .set_query_request(request)
            .send()
            .await
            .map_err(gax_error)?;
        let job_ref = first
            .job_reference
            .clone()
            .ok_or_else(|| anyhow!("query response without job reference"))?;
        let job_id = job_ref.job_id;
        let location = job_ref.location.unwrap_or(first.location.clone());

        let mut schema = first.schema;
        let mut rows = first.rows;
        let mut page_token = first.page_token;
        let mut complete = first.job_complete.unwrap_or(false);
        while !complete {
            let page = self.query_results(project, &job_id, &location, "").await?;
            complete = page.job_complete.unwrap_or(false);
            if complete {
                schema = page.schema;
                rows = page.rows;
                page_token = page.page_token;
            }
        }
        self.get_job(project, &job_id, &location).await?;
        while !page_token.is_empty() {
            let page = self
                .query_results(project, &job_id, &location, &page_token)
                .await?;
            rows.extend(page.rows);
            page_token = page.page_token;
        }
        Ok((
            fields_of(schema.as_ref()),
            rows.into_iter().map(Value::Object).collect(),
        ))
    }

    /// ``tabledata.list`` (not in the SDK): free, paginated row reads.
    async fn table_rows(&self, project: &str, dataset: &str, table: &str) -> Result<Vec<Value>> {
        let url = format!("{BQ_REST}/projects/{project}/datasets/{dataset}/tables/{table}/data");
        let mut rows = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut q = vec![("formatOptions.useInt64Timestamp", "true")];
            if let Some(t) = token.as_deref() {
                q.push(("pageToken", t));
            }
            let req = self
                .http
                .get(&url)
                .bearer_auth(self.access_token().await?)
                .query(&q);
            let page = send_json(req).await?;
            if let Some(r) = page.get("rows").and_then(Value::as_array) {
                rows.extend(r.iter().cloned());
            }
            match page.get("pageToken").and_then(Value::as_str) {
                Some(t) if !t.is_empty() => token = Some(t.to_owned()),
                _ => return Ok(rows),
            }
        }
    }

    /// Resumable media upload (not in the SDK) creating a load job.
    async fn upload_load_job(&self, project: &str, config: &Value, data: Vec<u8>) -> Result<Value> {
        let url = format!("{BQ_UPLOAD}/projects/{project}/jobs");
        let token = self.access_token().await?;
        let resp = self
            .http
            .request(Method::POST, &url)
            .bearer_auth(&token)
            .query(&[("uploadType", "resumable")])
            .header("X-Upload-Content-Type", "application/octet-stream")
            .json(config)
            .send()
            .await
            .context("HTTP request failed")?;
        let status = resp.status();
        if !status.is_success() {
            let bytes = resp.bytes().await.unwrap_or_default();
            return Err(http_error(status.as_u16(), &bytes));
        }
        let session = resp
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| anyhow!("resumable upload without session URI"))?
            .to_owned();
        send_json(
            self.http
                .request(Method::PUT, &session)
                .bearer_auth(&token)
                .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
                .body(data),
        )
        .await
    }

    // ------------------------------------------------------------------
    // Dataform (saved queries)
    // ------------------------------------------------------------------

    async fn list_repositories(&self, project: &str, region: &str) -> Result<Vec<df::Repository>> {
        let client = self.dataform(project).await?;
        Ok(collect_items!(
            client
                .list_repositories()
                .set_parent(format!("projects/{project}/locations/{region}"))
                .by_item()
        ))
    }

    async fn find_repository(
        &self,
        project: &str,
        region: &str,
        name: &str,
    ) -> Result<Option<df::Repository>> {
        Ok(self
            .list_repositories(project, region)
            .await?
            .into_iter()
            .find(|r| r.display_name == name))
    }

    async fn first_workspace(&self, project: &str, repo: &str) -> Result<Option<String>> {
        let mut pages = self
            .dataform(project)
            .await?
            .list_workspaces()
            .set_parent(repo)
            .by_page();
        Ok(match pages.next().await {
            Some(page) => page
                .map_err(gax_error)?
                .workspaces
                .into_iter()
                .next()
                .map(|w| w.name),
            None => None,
        })
    }

    async fn read_saved_sql(&self, project: &str, workspace: &str) -> Result<String> {
        let resp = self
            .dataform(project)
            .await?
            .read_file()
            .set_workspace(workspace)
            .set_path("content.sql")
            .send()
            .await
            .map_err(gax_error)?;
        Ok(String::from_utf8_lossy(&resp.file_contents).into_owned())
    }

    async fn write_saved_sql(&self, project: &str, workspace: &str, sql: &str) -> Result<()> {
        self.dataform(project)
            .await?
            .write_file()
            .set_workspace(workspace)
            .set_path("content.sql")
            .set_contents(sql.as_bytes().to_vec())
            .send()
            .await
            .map_err(gax_error)?;
        Ok(())
    }

    async fn list_saved_queries_inner(
        &self,
        project: &str,
        region: &str,
    ) -> Result<Vec<SavedQueryInfo>> {
        let mut saved = Vec::new();
        for repo in self.list_repositories(project, region).await? {
            if repo.display_name.is_empty() {
                continue;
            }
            let display = repo.display_name;
            let ws = match self.first_workspace(project, &repo.name).await {
                Ok(Some(ws)) => ws,
                Ok(None) => continue,
                Err(_) => {
                    log::debug!("Cannot list workspaces for repo '{display}'.");
                    continue;
                }
            };
            match self.read_saved_sql(project, &ws).await {
                Ok(sql) => saved.push(SavedQueryInfo {
                    name: display,
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
            workspace = self.first_workspace(project, &repo.name).await?;
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
        let client = self.dataform(project).await?;
        let repo = client
            .create_repository()
            .set_parent(format!("projects/{project}/locations/{region}"))
            .set_repository(df::Repository::new().set_display_name(name))
            .set_repository_id(repository_id(name))
            .send()
            .await
            .map_err(gax_error)?;
        let ws = client
            .create_workspace()
            .set_parent(&repo.name)
            .set_workspace(df::Workspace::new())
            .set_workspace_id("default")
            .send()
            .await
            .map_err(gax_error)?;
        self.write_saved_sql(project, &ws.name, sql).await?;
        log::info!("Created saved query '{name}'");
        Ok(())
    }
}

impl BqApi for GcpClient {
    fn list_views(&self, project: &str, dataset: &str) -> Result<Vec<ViewInfo>> {
        let tables = self.block_on(self.tables_of_type(project, dataset, "VIEW"))?;
        Ok(tables
            .into_iter()
            .map(|t| ViewInfo {
                name: table_id(&t),
                sql: t.view.as_ref().map(|v| v.query.clone()).unwrap_or_default(),
                modified: from_millis(t.last_modified_time as i64).unwrap_or(EPOCH),
                schema: schema_fields(t.schema.as_ref()),
                description: t.description.clone().unwrap_or_default(),
                created: from_millis(t.creation_time),
                region: Some(t.location.clone()).filter(|l| !l.is_empty()),
            })
            .collect())
    }

    fn list_routines(&self, project: &str, dataset: &str) -> Result<Vec<RoutineInfo>> {
        self.block_on(async {
            let routines = &self.bq().await?.routines;
            let listed: Vec<bq::Routine> = collect_items!(
                routines
                    .list_routines()
                    .set_project_id(project)
                    .set_dataset_id(dataset)
                    .by_item()
            );
            let ids: Vec<String> = listed
                .into_iter()
                .filter_map(|r| r.routine_reference.map(|r| r.routine_id))
                .collect();
            let routines: Vec<bq::Routine> = stream::iter(ids)
                .map(|id: String| async move { self.get_routine(project, dataset, &id).await })
                .buffered(CONCURRENCY)
                .try_collect()
                .await?;
            Ok(routines.iter().map(routine_info).collect())
        })
    }

    fn list_tables(&self, project: &str, dataset: &str) -> Result<Vec<TableInfo>> {
        let tables = self.block_on(self.tables_of_type(project, dataset, "TABLE"))?;
        Ok(tables.iter().map(table_info).collect())
    }

    fn list_external_tables(&self, project: &str, dataset: &str) -> Result<Vec<ExternalTableInfo>> {
        let tables = self.block_on(self.tables_of_type(project, dataset, "EXTERNAL"))?;
        Ok(tables
            .iter()
            .map(|t| {
                let info = table_info(t);
                let ext = t.external_data_configuration.as_ref();
                ExternalTableInfo {
                    name: info.name,
                    source_uris: ext.map(|e| e.source_uris.clone()).unwrap_or_default(),
                    schema: info.schema,
                    source_format: ext.map(|e| e.source_format.clone()).unwrap_or_default(),
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
        Ok(table_info(&table))
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
        let configs: Vec<dts::TransferConfig> = self.block_on(async {
            let client = self.transfer(project).await?;
            Ok::<_, Error>(collect_items!(
                client
                    .list_transfer_configs()
                    .set_parent(format!("projects/{project}/locations/{region}"))
                    .set_data_source_ids(["scheduled_query"])
                    .by_item()
            ))
        })?;
        Ok(configs
            .into_iter()
            .filter(|tc| tc.data_source_id == "scheduled_query")
            .map(|tc| TransferConfig {
                query: tc
                    .params
                    .as_ref()
                    .and_then(|p| p.get("query"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                update_time: tc
                    .update_time
                    .as_ref()
                    .and_then(|t| DateTime::from_timestamp(t.seconds(), t.nanos() as u32)),
                name: tc.name,
                display_name: tc.display_name,
                schedule: tc.schedule,
                data_source_id: tc.data_source_id,
            })
            .collect())
    }

    fn update_transfer_config_schedule(
        &self,
        project: &str,
        name: &str,
        schedule: &str,
    ) -> Result<()> {
        self.block_on(async {
            self.transfer(project)
                .await?
                .update_transfer_config()
                .set_transfer_config(
                    dts::TransferConfig::new()
                        .set_name(name)
                        .set_schedule(schedule),
                )
                .set_update_mask(google_cloud_wkt::FieldMask::default().set_paths(["schedule"]))
                .send()
                .await
                .map_err(gax_error)
        })?;
        Ok(())
    }

    fn start_manual_transfer_run(&self, project: &str, name: &str) -> Result<()> {
        let now = Utc::now();
        let ts = google_cloud_wkt::Timestamp::clamp(
            now.timestamp(),
            now.timestamp_subsec_nanos() as i32,
        );
        self.block_on(async {
            self.transfer(project)
                .await?
                .start_manual_transfer_runs()
                .set_parent(name)
                .set_requested_run_time(ts)
                .send()
                .await
                .map_err(gax_error)
        })?;
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
            match meta.r#type.as_str() {
                // tabledata.list does not serve views: run them instead.
                "VIEW" | "MATERIALIZED_VIEW" => {
                    let sql = format!("SELECT * FROM `{project}.{dataset}.{table}`");
                    let location = Some(meta.location.as_str()).filter(|l| !l.is_empty());
                    self.run_sql(project, &sql, location).await
                }
                _ => {
                    let rows = self.table_rows(project, dataset, table).await?;
                    Ok((fields_of(meta.schema.as_ref()), rows))
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
        let (schema, rows) = self.block_on(self.run_sql(project, sql, None))?;
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
            let job = self.upload_load_job(project, &config, data).await?;
            let reference = job.get("jobReference");
            let field = |k: &str| {
                reference
                    .and_then(|r| r.get(k))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned()
            };
            let (job_id, location) = (field("jobId"), field("location"));
            loop {
                let job = self.get_job(project, &job_id, &location).await?;
                if job.status.as_ref().is_some_and(|s| s.state == "DONE") {
                    return Ok::<_, Error>(());
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
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
                .update_routine(project, dataset, name, |r| {
                    r.definition_body = body.to_owned()
                })
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
        let result = self.block_on(self.update_routine(project, dataset, name, |r| {
            r.description = description.to_owned()
        }));
        match result {
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
        self.block_on(self.delete_table_resource(project, dataset, name))?;
        log::info!("Deleted view {project}.{dataset}.{name}");
        Ok(())
    }

    fn delete_table(&self, project: &str, dataset: &str, name: &str) -> Result<()> {
        self.block_on(self.delete_table_resource(project, dataset, name))?;
        log::info!("Deleted table {project}.{dataset}.{name}");
        Ok(())
    }

    fn delete_routine(&self, project: &str, dataset: &str, name: &str) -> Result<()> {
        self.block_on(async {
            self.bq()
                .await?
                .routines
                .delete_routine()
                .set_project_id(project)
                .set_dataset_id(dataset)
                .set_routine_id(name)
                .send()
                .await
                .map_err(gax_error)
        })?;
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
            self.dataform(project)
                .await?
                .delete_repository()
                .set_name(repo.name)
                .set_force(true)
                .send()
                .await
                .map_err(gax_error)
        })?;
        log::info!("Deleted saved query '{name}'");
        Ok(())
    }

    fn run_query(&self, project: &str, sql: &str, location: Option<&str>) -> Result<()> {
        self.block_on(self.run_sql(project, sql, location))?;
        log::info!("Executed query on project '{project}'.");
        Ok(())
    }

    fn dry_run_query(&self, project: &str, sql: &str) -> Result<()> {
        let request = bq::QueryRequest::new()
            .set_query(sql)
            .set_use_legacy_sql(false)
            .set_dry_run(true)
            .set_use_query_cache(false);
        self.block_on(async {
            self.bq()
                .await?
                .jobs
                .query()
                .set_project_id(project)
                .set_query_request(request)
                .send()
                .await
                .map_err(gax_error)
        })?;
        Ok(())
    }
}

impl GcpClient {
    async fn delete_table_resource(&self, project: &str, dataset: &str, name: &str) -> Result<()> {
        self.bq()
            .await?
            .tables
            .delete_table()
            .set_project_id(project)
            .set_dataset_id(dataset)
            .set_table_id(name)
            .send()
            .await
            .map_err(gax_error)?;
        Ok(())
    }
}

// ----------------------------------------------------------------------
// Conversions
// ----------------------------------------------------------------------

fn gax_error(e: google_cloud_gax::error::Error) -> Error {
    if e.is_authentication() {
        return credentials_error(e);
    }
    let message = e
        .status()
        .map(|s| s.message.clone())
        .filter(|m| !m.is_empty())
        .unwrap_or_else(|| e.to_string());
    let not_found = e.http_status_code() == Some(404)
        || e.status()
            .is_some_and(|s| s.code == google_cloud_gax::error::rpc::Code::NotFound);
    match e.http_status_code() {
        _ if not_found => Error::NotFound(message),
        Some(status) => Error::Api { status, message },
        None => Error::Other(anyhow!("Google Cloud request failed: {message}")),
    }
}

fn build_error(e: impl std::fmt::Display) -> Error {
    Error::Other(anyhow!("failed to create Google Cloud client: {e}"))
}

fn credentials_error(e: impl std::fmt::Display) -> Error {
    Error::Other(anyhow!(
        "no usable Google Cloud credentials ({e}); run \
         `gcloud auth application-default login` or set GOOGLE_APPLICATION_CREDENTIALS"
    ))
}

async fn send_json(req: reqwest::RequestBuilder) -> Result<Value> {
    let resp = req.send().await.context("HTTP request failed")?;
    let status = resp.status();
    let bytes = resp.bytes().await.context("failed to read response")?;
    if !status.is_success() {
        return Err(http_error(status.as_u16(), &bytes));
    }
    if bytes.is_empty() {
        return Ok(Value::Null);
    }
    Ok(serde_json::from_slice(&bytes).context("invalid JSON response")?)
}

fn http_error(status: u16, body: &[u8]) -> Error {
    let message = serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|v| {
            v.pointer("/error/message")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| String::from_utf8_lossy(body).trim().to_owned());
    if status == 404 {
        Error::NotFound(message)
    } else {
        Error::Api { status, message }
    }
}

/// Dataform repository IDs must be URL-safe (lowercase, hyphens).
fn repository_id(name: &str) -> String {
    REPO_ID_RE
        .replace_all(&name.to_lowercase(), "-")
        .into_owned()
}

fn from_millis(ms: i64) -> Option<DateTime<Utc>> {
    (ms > 0)
        .then(|| DateTime::from_timestamp_millis(ms))
        .flatten()
}

fn table_id(t: &bq::Table) -> String {
    t.table_reference
        .as_ref()
        .map(|r| r.table_id.clone())
        .unwrap_or_default()
}

fn schema_fields(schema: Option<&bq::TableSchema>) -> Vec<SchemaField> {
    schema
        .map(|s| {
            s.fields
                .iter()
                .map(|f| SchemaField {
                    name: f.name.clone(),
                    field_type: f.r#type.to_uppercase(),
                    mode: if f.mode.is_empty() {
                        "NULLABLE".to_owned()
                    } else {
                        f.mode.to_uppercase()
                    },
                    description: f.description.clone().unwrap_or_default(),
                })
                .collect()
        })
        .unwrap_or_default()
}

fn fields_of(schema: Option<&bq::TableSchema>) -> Vec<BqField> {
    fn convert(f: &bq::TableFieldSchema) -> BqField {
        BqField {
            name: f.name.clone(),
            field_type: f.r#type.to_uppercase(),
            mode: if f.mode.is_empty() {
                "NULLABLE".to_owned()
            } else {
                f.mode.to_uppercase()
            },
            fields: f.fields.iter().map(convert).collect(),
        }
    }
    schema
        .map(|s| s.fields.iter().map(convert).collect())
        .unwrap_or_default()
}

fn table_info(t: &bq::Table) -> TableInfo {
    TableInfo {
        name: table_id(t),
        schema: schema_fields(t.schema.as_ref()),
        description: t.description.clone().unwrap_or_default(),
        row_count: t.num_rows.unwrap_or(0) as i64,
        modified: from_millis(t.last_modified_time as i64).unwrap_or(EPOCH),
        partitioning: t.time_partitioning.as_ref().map(|tp| {
            tp.field
                .clone()
                .filter(|f| !f.is_empty())
                .unwrap_or_else(|| "ingestion_time".to_owned())
        }),
        clustering: t
            .clustering
            .as_ref()
            .map(|c| c.fields.clone())
            .filter(|c| !c.is_empty()),
        created: from_millis(t.creation_time),
        region: Some(t.location.clone()).filter(|l| !l.is_empty()),
        primary_keys: t
            .table_constraints
            .as_ref()
            .and_then(|c| c.primary_key.as_ref())
            .map(|pk| pk.columns.clone()),
        total_logical_bytes: t.num_bytes,
    }
}

fn enum_name(name: Option<&str>, unspecified_suffix: &str, default: &str) -> String {
    match name {
        Some(n) if !n.ends_with(unspecified_suffix) => n.to_owned(),
        _ => default.to_owned(),
    }
}

fn routine_info(r: &bq::Routine) -> RoutineInfo {
    RoutineInfo {
        name: r
            .routine_reference
            .as_ref()
            .map(|rr| rr.routine_id.clone())
            .unwrap_or_default(),
        sql: r.definition_body.clone(),
        language: enum_name(r.language.name(), "_UNSPECIFIED", "SQL"),
        modified: from_millis(r.last_modified_time).unwrap_or(EPOCH),
        description: r.description.clone(),
        created: from_millis(r.creation_time),
        arguments: r
            .arguments
            .iter()
            .map(|a| RoutineArgument {
                name: a.name.clone(),
                arg_type: a
                    .data_type
                    .as_ref()
                    .map(|dt| {
                        dt.type_kind
                            .name()
                            .unwrap_or("TYPE_KIND_UNSPECIFIED")
                            .to_owned()
                    })
                    .unwrap_or_else(|| "ANY".to_owned()),
                mode: enum_name(a.mode.name(), "_UNSPECIFIED", "IN"),
            })
            .collect(),
        return_type: r
            .return_type
            .as_ref()
            .and_then(|rt| rt.type_kind.name())
            .map(str::to_owned),
    }
}

/// Standard SQL type for a type name (unknown names → unspecified).
fn sql_type(name: &str) -> bq::StandardSqlDataType {
    use bq::standard_sql_data_type::TypeKind;
    let kind = match TypeKind::from(name) {
        TypeKind::UnknownValue(_) => TypeKind::Unspecified,
        known => known,
    };
    bq::StandardSqlDataType::new().set_type_kind(kind)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_table_metadata() {
        let t: bq::Table = serde_json::from_value(json!({
            "tableReference": {"projectId": "p", "datasetId": "d", "tableId": "events"},
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
        }))
        .unwrap();
        let info = table_info(&t);
        assert_eq!(info.name, "events");
        assert_eq!(info.row_count, 1000);
        assert_eq!(info.total_logical_bytes, Some(2048));
        assert_eq!(info.partitioning.as_deref(), Some("ingestion_time"));
        assert_eq!(info.clustering, Some(vec!["id".to_owned()]));
        assert_eq!(info.primary_keys, Some(vec!["id".to_owned()]));
        assert_eq!(info.region.as_deref(), Some("US"));
        assert_eq!(
            crate::utils::isoformat(&info.modified),
            "2025-01-01T00:00:00.123000+00:00"
        );
        assert_eq!(info.schema[0].description, "PK");
        assert_eq!(info.schema[1].mode, "NULLABLE");
        assert_eq!(info.schema[1].description, "");
    }

    #[test]
    fn converts_routine() {
        let r: bq::Routine = serde_json::from_value(json!({
            "routineReference": {"projectId": "p", "datasetId": "d", "routineId": "fn"},
            "routineType": "SCALAR_FUNCTION",
            "definitionBody": "x + 1",
            "arguments": [
                {"name": "x", "dataType": {"typeKind": "INT64"}},
                {"name": "y", "argumentKind": "ANY_TYPE", "mode": "OUT"}
            ],
            "returnType": {"typeKind": "INT64"},
            "lastModifiedTime": "0"
        }))
        .unwrap();
        let info = routine_info(&r);
        assert_eq!(info.name, "fn");
        assert_eq!(info.language, "SQL");
        assert_eq!(info.modified, EPOCH);
        assert_eq!(info.arguments[0].arg_type, "INT64");
        assert_eq!(info.arguments[0].mode, "IN");
        assert_eq!(info.arguments[1].arg_type, "ANY");
        assert_eq!(info.arguments[1].mode, "OUT");
        assert_eq!(info.return_type.as_deref(), Some("INT64"));

        let js: bq::Routine = serde_json::from_value(json!({"language": "JAVASCRIPT"})).unwrap();
        assert_eq!(routine_info(&js).language, "JAVASCRIPT");
    }

    #[test]
    fn query_schema_and_types() {
        let schema: bq::TableSchema = serde_json::from_value(json!({"fields": [
            {"name": "rec", "type": "RECORD", "mode": "REPEATED", "fields": [{"name": "a", "type": "INTEGER"}]}
        ]}))
        .unwrap();
        let fields = fields_of(Some(&schema));
        assert_eq!(fields[0].mode, "REPEATED");
        assert_eq!(fields[0].fields[0].mode, "NULLABLE");

        use bq::standard_sql_data_type::TypeKind;
        assert_eq!(sql_type("INT64").type_kind, TypeKind::Int64);
        assert_eq!(sql_type("ANY").type_kind, TypeKind::Unspecified);
        assert_eq!(repository_id("My Query_v2"), "my-query-v2");
    }

    #[test]
    fn http_errors() {
        let body = br#"{"error": {"code": 404, "message": "Not found: Table p:d.t"}}"#;
        assert!(
            matches!(http_error(404, body), Error::NotFound(m) if m == "Not found: Table p:d.t")
        );
        assert!(matches!(
            http_error(500, b"boom"),
            Error::Api { status: 500, .. }
        ));
    }

    #[test]
    fn patch_bodies_only_carry_changed_fields() {
        let patch = bq::Table::new().set_description("Doc");
        assert_eq!(
            serde_json::to_value(&patch).unwrap(),
            json!({"description": "Doc"})
        );
        let view = bq::Table::new().set_view(
            bq::ViewDefinition::new()
                .set_query("SELECT 1")
                .set_use_legacy_sql(false),
        );
        assert_eq!(
            serde_json::to_value(&view).unwrap(),
            json!({"view": {"query": "SELECT 1", "useLegacySql": false}})
        );
        let tc = dts::TransferConfig::new()
            .set_name("n")
            .set_schedule("every 2 hours");
        assert_eq!(
            serde_json::to_value(&tc).unwrap(),
            json!({"name": "n", "schedule": "every 2 hours"})
        );
    }

    #[test]
    fn sdk_clients_build_without_network() {
        // Constructing clients resolves ADC lazily; no request is sent.
        let client = GcpClient::new().unwrap();
        client.block_on(async {
            client.bq().await.unwrap();
            client.transfer("p").await.unwrap();
            client.dataform("p").await.unwrap();
        });
    }
}
