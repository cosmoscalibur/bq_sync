//! In-memory [`BqApi`] fake that records calls (tests only).

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::Path;

use crate::api::BqApi;
use crate::error::{Error, Result};
use crate::resources::{
    DataFormat, ExternalTableInfo, RoutineArgument, RoutineInfo, SavedQueryInfo,
    ScheduledQueryInfo, TableInfo, TransferConfig, ViewInfo,
};

#[derive(Default)]
pub struct FakeApi {
    pub views: RefCell<Vec<ViewInfo>>,
    pub routines: RefCell<Vec<RoutineInfo>>,
    pub tables: RefCell<Vec<TableInfo>>,
    pub externals: RefCell<Vec<ExternalTableInfo>>,
    pub scheduled: RefCell<Vec<ScheduledQueryInfo>>,
    pub saved: RefCell<Vec<SavedQueryInfo>>,
    pub transfer_configs: RefCell<Vec<TransferConfig>>,
    pub dry_run_error: RefCell<Option<String>>,
    calls: RefCell<Vec<String>>,
}

impl FakeApi {
    pub fn calls(&self) -> Vec<String> {
        self.calls.borrow().clone()
    }

    fn record(&self, call: String) {
        self.calls.borrow_mut().push(call);
    }
}

impl BqApi for FakeApi {
    fn list_views(&self, _: &str, _: &str) -> Result<Vec<ViewInfo>> {
        Ok(self.views.borrow().clone())
    }
    fn list_routines(&self, _: &str, _: &str) -> Result<Vec<RoutineInfo>> {
        Ok(self.routines.borrow().clone())
    }
    fn list_tables(&self, _: &str, _: &str) -> Result<Vec<TableInfo>> {
        Ok(self.tables.borrow().clone())
    }
    fn list_external_tables(&self, _: &str, _: &str) -> Result<Vec<ExternalTableInfo>> {
        Ok(self.externals.borrow().clone())
    }
    fn get_table_info(&self, project: &str, dataset: &str, name: &str) -> Result<TableInfo> {
        self.record(format!("get_table_info {project}.{dataset}.{name}"));
        Ok(TableInfo {
            name: name.into(),
            ..Default::default()
        })
    }
    fn list_saved_queries(&self, _: &str, _: &str) -> Result<Vec<SavedQueryInfo>> {
        Ok(self.saved.borrow().clone())
    }
    fn list_scheduled_queries(&self, _: &str, _: &str) -> Result<Vec<ScheduledQueryInfo>> {
        Ok(self.scheduled.borrow().clone())
    }
    fn list_transfer_configs(&self, _: &str, _: &str) -> Result<Vec<TransferConfig>> {
        Ok(self.transfer_configs.borrow().clone())
    }
    fn update_transfer_config_schedule(&self, _: &str, name: &str, schedule: &str) -> Result<()> {
        self.record(format!("update_schedule {name} {schedule}"));
        Ok(())
    }
    fn start_manual_transfer_run(&self, _: &str, name: &str) -> Result<()> {
        self.record(format!("start_manual_run {name}"));
        Ok(())
    }
    fn fetch_table_to_file(
        &self,
        p: &str,
        d: &str,
        t: &str,
        dest: &Path,
        fmt: DataFormat,
    ) -> Result<()> {
        self.record(format!(
            "fetch_table_to_file {p}.{d}.{t} {} {fmt:?}",
            dest.display()
        ));
        Ok(())
    }
    fn fetch_query_to_file(&self, p: &str, sql: &str, dest: &Path, fmt: DataFormat) -> Result<()> {
        self.record(format!(
            "fetch_query_to_file {p} {sql} {} {fmt:?}",
            dest.display()
        ));
        Ok(())
    }
    fn load_table_from_file(
        &self,
        p: &str,
        d: &str,
        t: &str,
        _: &Path,
        fmt: DataFormat,
    ) -> Result<()> {
        self.record(format!("load_table_from_file {p}.{d}.{t} {fmt:?}"));
        Ok(())
    }
    fn upsert_view(&self, p: &str, d: &str, n: &str, sql: &str) -> Result<()> {
        self.record(format!("upsert_view {p}.{d}.{n} {sql}"));
        Ok(())
    }
    fn upsert_table_description(
        &self,
        p: &str,
        d: &str,
        n: &str,
        desc: &str,
        fields: &HashMap<String, String>,
    ) -> Result<()> {
        self.record(format!(
            "upsert_table_description {p}.{d}.{n} {desc} fields={}",
            fields.len()
        ));
        Ok(())
    }
    fn upsert_routine(
        &self,
        p: &str,
        d: &str,
        n: &str,
        _body: &str,
        language: &str,
        arguments: Option<&[RoutineArgument]>,
        return_type: Option<&str>,
    ) -> Result<()> {
        let args = arguments.map_or("None".to_owned(), |a| {
            let items: Vec<String> = a
                .iter()
                .map(|a| format!("{}:{}", a.name, a.arg_type))
                .collect();
            format!("[{}]", items.join(","))
        });
        self.record(format!(
            "upsert_routine {p}.{d}.{n} {language} args={args} ret={return_type:?}"
        ));
        Ok(())
    }
    fn upsert_routine_description(&self, p: &str, d: &str, n: &str, desc: &str) -> Result<()> {
        self.record(format!("upsert_routine_description {p}.{d}.{n} {desc}"));
        Ok(())
    }
    fn upsert_saved_query(&self, p: &str, region: &str, name: &str, _: &str) -> Result<()> {
        self.record(format!("upsert_saved_query {p}/{region} {name}"));
        Ok(())
    }
    fn delete_view(&self, p: &str, d: &str, n: &str) -> Result<()> {
        self.record(format!("delete_view {p}.{d}.{n}"));
        Ok(())
    }
    fn delete_table(&self, p: &str, d: &str, n: &str) -> Result<()> {
        self.record(format!("delete_table {p}.{d}.{n}"));
        Ok(())
    }
    fn delete_routine(&self, p: &str, d: &str, n: &str) -> Result<()> {
        self.record(format!("delete_routine {p}.{d}.{n}"));
        Ok(())
    }
    fn delete_saved_query(&self, p: &str, region: &str, name: &str) -> Result<()> {
        self.record(format!("delete_saved_query {p}/{region} {name}"));
        Ok(())
    }
    fn run_query(&self, p: &str, _: &str, location: Option<&str>) -> Result<()> {
        self.record(format!("run_query {p} @{location:?}"));
        Ok(())
    }
    fn dry_run_query(&self, p: &str, sql: &str) -> Result<()> {
        self.record(format!("dry_run_query {p} {sql}"));
        match self.dry_run_error.borrow().clone() {
            Some(msg) => Err(Error::Api {
                status: 400,
                message: msg,
            }),
            None => Ok(()),
        }
    }
}
