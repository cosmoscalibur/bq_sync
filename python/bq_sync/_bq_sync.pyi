"""Type stubs for the ``bq_sync._bq_sync`` Rust extension module."""

from os import PathLike
from pathlib import Path
from typing import Literal, TypedDict

_PathArg = str | PathLike[str]

__version__: str

class BqSyncError(Exception): ...
class NotFoundError(BqSyncError): ...

class ProjectConfig(TypedDict):
    id: str
    default_region: str

class SyncConfig(TypedDict):
    project: ProjectConfig
    datasets: list[str]
    output_dir: str

class ViewUpdate(TypedDict):
    name: str
    sql: str

class SavedQueryUpdate(TypedDict):
    name: str
    sql: str

class RoutineUpdate(TypedDict):
    name: str
    body: str
    language: str

class ModelUpdate(TypedDict):
    name: str
    description: str
    field_descriptions: dict[str, str]

class RoutineArgument(TypedDict):
    name: str
    type: str
    mode: str

class RoutineModelUpdate(TypedDict):
    name: str
    description: str
    language: str
    arguments: list[RoutineArgument]
    return_type: str | None

class CheckResult(TypedDict):
    path: Path
    level: Literal["error", "warning", "info"]
    errors: list[str]
    warnings: list[str]
    info: list[str]
    tables_referenced: list[str]
    tables_resolved: list[str]
    tables_unresolved: list[str]
    columns_unresolved: list[str]

class CheckSummary(TypedDict):
    results: list[CheckResult]
    passed: int
    failed: int
    warned: int

def main(argv: list[str] | None = None) -> int: ...
def set_verbose(verbose: bool) -> None: ...
def humanize_bytes(n: int | None) -> str: ...
def parse_resource_path(path: str) -> tuple[str, str, str]: ...
def discover_config(start: _PathArg | None = None) -> Path: ...
def load_config(path: _PathArg) -> SyncConfig: ...
def resolve_output_dir(config_path: _PathArg) -> Path: ...
def read_view_sql(path: _PathArg) -> ViewUpdate: ...
def read_saved_query_sql(path: _PathArg) -> SavedQueryUpdate: ...
def read_routine_sql(path: _PathArg) -> RoutineUpdate: ...
def read_model_yaml(path: _PathArg) -> ModelUpdate: ...
def read_routine_model_yaml(path: _PathArg) -> RoutineModelUpdate: ...
def parse_schedule_from_file(path: _PathArg) -> tuple[str, str]: ...
def discover_sql_files(output_root: _PathArg) -> list[Path]: ...
def check_files(
    files: list[_PathArg],
    output_root: _PathArg,
    project: str,
    online: bool = False,
) -> CheckSummary: ...
def check(
    paths: list[_PathArg] | None = None,
    config: _PathArg | None = None,
    online: bool = False,
) -> CheckSummary: ...
def pull(
    config: _PathArg | None = None,
    dataset: str | None = None,
    dry_run: bool = False,
    force: bool = False,
    force_files: list[str] | None = None,
) -> Path: ...
def push(
    paths: list[str] | None = None,
    data: tuple[str, str] | None = None,
    since: float | None = None,
    dry_run: bool = False,
    yes: bool = False,
    include_models: bool = False,
    config: _PathArg | None = None,
) -> None: ...
def fetch(
    model: str,
    format: Literal["csv", "parquet"] = "csv",
    output_dir: _PathArg | None = None,
    config: _PathArg | None = None,
) -> Path: ...
def rm(
    paths: list[str],
    dry_run: bool = False,
    yes: bool = False,
    config: _PathArg | None = None,
) -> None: ...
def materialize(
    resource: str,
    target: str | None = None,
    dry_run: bool = False,
    yes: bool = False,
    config: _PathArg | None = None,
) -> Path | None: ...
def reschedule(
    display_name: str | None = None,
    schedule: str | None = None,
    from_file: bool = False,
    dry_run: bool = False,
    trigger: bool = False,
    list: bool = False,
    config: _PathArg | None = None,
) -> None: ...
def list_scheduled_queries(config: _PathArg | None = None) -> list[tuple[str, str]]: ...
