"""Google Cloud API read wrapper for BigQuery resources.

Returns ``resources.*Info`` dataclass instances.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

from google.api_core import client_options as client_options_lib
from google.cloud import bigquery
from google.cloud import bigquery_datatransfer_v1 as datatransfer
from google.cloud import dataform_v1beta1 as dataform

from bq_sync.resources import (
    ExternalTableInfo,
    RoutineInfo,
    SavedQueryInfo,
    ScheduledQueryInfo,
    TableInfo,
    ViewInfo,
)

logger = logging.getLogger(__name__)

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def list_views(project: str, dataset: str) -> list[ViewInfo]:
    """List all views in a dataset.

    Args:
        project: GCP project ID.
        dataset: BigQuery dataset ID.

    Returns:
        List of ``ViewInfo`` for each view in the dataset.
    """
    client = bigquery.Client(project=project)
    dataset_ref = f"{project}.{dataset}"
    views: list[ViewInfo] = []

    for table_item in client.list_tables(dataset_ref):
        if table_item.table_type != "VIEW":
            continue
        table = client.get_table(table_item.reference)
        schema = [
            {
                "name": f.name,
                "type": f.field_type,
                "mode": f.mode,
                "description": f.description or "",
            }
            for f in table.schema
        ]
        views.append(
            ViewInfo(
                name=table.table_id,
                sql=table.view_query or "",
                modified=table.modified or _EPOCH,
                schema=schema,
                description=table.description or "",
                created=table.created,
                region=table.location,
            )
        )
    return views


def list_routines(project: str, dataset: str) -> list[RoutineInfo]:
    """List all routines (functions/procedures) in a dataset.

    Args:
        project: GCP project ID.
        dataset: BigQuery dataset ID.

    Returns:
        List of ``RoutineInfo`` for each routine.
    """
    client = bigquery.Client(project=project)
    dataset_ref = f"{project}.{dataset}"
    routines: list[RoutineInfo] = []

    for routine_item in client.list_routines(dataset_ref):
        routine = client.get_routine(routine_item.reference)
        args: list[dict[str, str]] = []
        for arg in routine.arguments or []:
            data_type = arg.data_type
            type_str = data_type.type_kind.name if data_type else "ANY"
            args.append(
                {
                    "name": arg.name or "",
                    "type": type_str,
                    "mode": arg.mode or "IN",
                }
            )
        ret = None
        if routine.return_type:
            ret = routine.return_type.type_kind.name
        routines.append(
            RoutineInfo(
                name=routine.routine_id,
                sql=routine.body or "",
                language=routine.language or "SQL",
                modified=routine.modified or _EPOCH,
                description=routine.description or "",
                created=routine.created,
                arguments=args,
                return_type=ret,
            )
        )
    return routines


def list_tables(project: str, dataset: str) -> list[TableInfo]:
    """List all regular tables in a dataset (for model/metadata export).

    Args:
        project: GCP project ID.
        dataset: BigQuery dataset ID.

    Returns:
        List of ``TableInfo`` with schema and metadata.
    """
    client = bigquery.Client(project=project)
    dataset_ref = f"{project}.{dataset}"
    tables: list[TableInfo] = []

    for table_item in client.list_tables(dataset_ref):
        if table_item.table_type != "TABLE":
            continue
        table = client.get_table(table_item.reference)
        schema = [
            {
                "name": f.name,
                "type": f.field_type,
                "mode": f.mode,
                "description": f.description or "",
            }
            for f in table.schema
        ]
        partitioning = None
        if table.time_partitioning:
            partitioning = table.time_partitioning.field or "ingestion_time"

        clustering = list(table.clustering_fields) if table.clustering_fields else None

        pk_columns: list[str] | None = None
        constraints = getattr(table, "table_constraints", None)
        if constraints and getattr(constraints, "primary_key", None):
            pk_columns = list(constraints.primary_key.columns)

        tables.append(
            TableInfo(
                name=table.table_id,
                schema=schema,
                description=table.description or "",
                row_count=table.num_rows or 0,
                modified=table.modified or _EPOCH,
                partitioning=partitioning,
                clustering=clustering,
                created=table.created,
                region=table.location,
                primary_keys=pk_columns,
                total_logical_bytes=table.num_bytes,
            )
        )
    return tables


def list_external_tables(project: str, dataset: str) -> list[ExternalTableInfo]:
    """List all external tables in a dataset.

    Args:
        project: GCP project ID.
        dataset: BigQuery dataset ID.

    Returns:
        List of ``ExternalTableInfo`` with source URIs and schema.
    """
    client = bigquery.Client(project=project)
    dataset_ref = f"{project}.{dataset}"
    externals: list[ExternalTableInfo] = []

    for table_item in client.list_tables(dataset_ref):
        if table_item.table_type != "EXTERNAL":
            continue
        table = client.get_table(table_item.reference)
        schema = [
            {
                "name": f.name,
                "type": f.field_type,
                "mode": f.mode,
                "description": f.description or "",
            }
            for f in table.schema
        ]
        ext_config = table.external_data_configuration

        partitioning = None
        if table.time_partitioning:
            partitioning = table.time_partitioning.field or "ingestion_time"

        clustering = list(table.clustering_fields) if table.clustering_fields else None

        pk_columns: list[str] | None = None
        constraints = getattr(table, "table_constraints", None)
        if constraints and getattr(constraints, "primary_key", None):
            pk_columns = list(constraints.primary_key.columns)

        externals.append(
            ExternalTableInfo(
                name=table.table_id,
                source_uris=list(ext_config.source_uris) if ext_config else [],
                schema=schema,
                source_format=ext_config.source_format if ext_config else "",
                modified=table.modified or _EPOCH,
                description=table.description or "",
                created=table.created,
                region=table.location,
                total_logical_bytes=table.num_bytes,
                row_count=table.num_rows or 0,
                partitioning=partitioning,
                clustering=clustering,
                primary_keys=pk_columns,
            )
        )
    return externals


def list_scheduled_queries(project: str, region: str) -> list[ScheduledQueryInfo]:
    """List all scheduled queries in a project/region.

    Args:
        project: GCP project ID.
        region: GCP region (e.g. ``us-east1``).

    Returns:
        List of ``ScheduledQueryInfo`` at project level.
    """
    options = client_options_lib.ClientOptions(quota_project_id=project)
    client = datatransfer.DataTransferServiceClient(client_options=options)
    parent = f"projects/{project}/locations/{region}"
    configs: list[ScheduledQueryInfo] = []

    for config in client.list_transfer_configs(parent=parent):
        # Scheduled queries have data_source_id == "scheduled_query"
        if config.data_source_id != "scheduled_query":
            continue
        modified = config.update_time or _EPOCH
        if hasattr(modified, "timestamp"):
            modified = datetime.fromtimestamp(modified.timestamp(), tz=timezone.utc)
        configs.append(
            ScheduledQueryInfo(
                name=config.display_name,
                sql=config.params.get("query", ""),
                schedule=config.schedule or "",
                modified=modified,
            )
        )
    return configs


def list_saved_queries(project: str, region: str) -> list[SavedQueryInfo]:
    """List saved queries via Dataform API.

    .. warning::

        BigQuery Studio saved queries are backed by Dataform
        repositories.  This is the official path but relatively new;
        the API surface may change.

    Args:
        project: GCP project ID.
        region: GCP region (e.g. ``us-east1``).

    Returns:
        List of ``SavedQueryInfo``.  Returns an empty list if Dataform
        is not enabled or no repositories exist.
    """
    logger.warning(
        "Saved queries use the Dataform API which is an unstable feature. "
        "Results may be incomplete or change without notice."
    )
    try:
        options = client_options_lib.ClientOptions(quota_project_id=project)
        client = dataform.DataformClient(client_options=options)
        parent = f"projects/{project}/locations/{region}"
        saved: list[SavedQueryInfo] = []

        for repo in client.list_repositories(parent=parent):
            if not repo.display_name:
                continue
            # Each saved query repo has one workspace with content.sql.
            try:
                ws_iter = client.list_workspaces(parent=repo.name)
                ws = next(iter(ws_iter), None)
            except Exception:
                logger.debug("Cannot list workspaces for repo '%s'.", repo.display_name)
                continue
            if ws is None:
                continue
            try:
                file_resp = client.read_file(
                    request={"workspace": ws.name, "path": "content.sql"},
                )
            except Exception:
                logger.debug("Cannot read content.sql in repo '%s'.", repo.display_name)
                continue
            sql = (
                file_resp.file_contents.decode()
                if isinstance(file_resp.file_contents, bytes)
                else file_resp.file_contents
            )
            saved.append(
                SavedQueryInfo(
                    name=repo.display_name,
                    sql=sql,
                    modified=_EPOCH,
                )
            )
        return saved
    except Exception:
        logger.warning(
            "Failed to list saved queries via Dataform API. "
            "Dataform may not be enabled for project '%s'.",
            project,
            exc_info=True,
        )
        return []


def fetch_table_to_file(
    project: str,
    dataset: str,
    table: str,
    dest: Path,
    fmt: str = "csv",
) -> None:
    """Fetch all rows from a BigQuery table or view and write to a local file.

    Uses ``list_rows`` to stream data and Polars for efficient serialisation
    to CSV or Parquet.

    Args:
        project: GCP project ID.
        dataset: BigQuery dataset ID.
        table: Table or view name.
        dest: Target file path (parent directories are created automatically).
        fmt: Output format, ``"csv"`` or ``"parquet"``.

    Raises:
        ValueError: If *fmt* is not ``"csv"`` or ``"parquet"``.
    """
    import polars as pl

    if fmt not in ("csv", "parquet"):
        msg = f"Unsupported format: {fmt!r}. Expected 'csv' or 'parquet'."
        raise ValueError(msg)

    client = bigquery.Client(project=project)
    table_ref = f"{project}.{dataset}.{table}"
    rows_iter = client.list_rows(table_ref)

    columns: list[str] = [field.name for field in rows_iter.schema]
    data: dict[str, list[object]] = {col: [] for col in columns}
    for row in rows_iter:
        for col in columns:
            data[col].append(row[col])

    df = pl.DataFrame(data)

    dest.parent.mkdir(parents=True, exist_ok=True)
    if fmt == "csv":
        df.write_csv(dest)
    else:
        df.write_parquet(dest)
