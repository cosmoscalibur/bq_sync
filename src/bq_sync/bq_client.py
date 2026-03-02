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


def fetch_query_to_file(
    project: str,
    sql: str,
    dest: Path,
    fmt: str = "csv",
) -> None:
    """Execute a SQL query on BigQuery and write results to a local file.

    Useful for saved queries and views whose data must be recovered by
    running their SQL definition.

    Args:
        project: GCP project ID.
        sql: SQL query string to execute.
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
    query_job = client.query(sql)
    rows_iter = query_job.result()

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


# ---------------------------------------------------------------------------
# Write-side functions (push mode)
# ---------------------------------------------------------------------------


def update_view(project: str, dataset: str, name: str, sql: str) -> None:
    """Update the SQL definition of an existing BigQuery view.

    Args:
        project: GCP project ID.
        dataset: BigQuery dataset ID.
        name: View name.
        sql: New SQL query for the view.
    """
    client = bigquery.Client(project=project)
    table_ref = f"{project}.{dataset}.{name}"
    table = client.get_table(table_ref)
    table.view_query = sql
    client.update_table(table, ["view_query"])
    logger.info("Updated view %s.%s.%s", project, dataset, name)


def update_table_description(
    project: str,
    dataset: str,
    name: str,
    description: str,
    field_descriptions: dict[str, str] | None = None,
) -> None:
    """Update the description of a table or view and its fields.

    Only the ``description`` attribute of the table and the
    ``description`` attribute of each schema field listed in
    *field_descriptions* are modified.

    Args:
        project: GCP project ID.
        dataset: BigQuery dataset ID.
        name: Table or view name.
        description: New top-level description.
        field_descriptions: Mapping of field name to new description.
            Fields not present in this mapping are left unchanged.
    """
    client = bigquery.Client(project=project)
    table_ref = f"{project}.{dataset}.{name}"
    table = client.get_table(table_ref)

    table.description = description

    fields_map = field_descriptions or {}
    if fields_map:
        new_schema = []
        for sf in table.schema:
            if sf.name in fields_map:
                api_repr = sf.to_api_repr()
                api_repr["description"] = fields_map[sf.name]
                sf = bigquery.SchemaField.from_api_repr(api_repr)
            new_schema.append(sf)
        table.schema = new_schema

    update_fields = ["description"]
    if fields_map:
        update_fields.append("schema")
    client.update_table(table, update_fields)
    logger.info("Updated description for %s.%s.%s", project, dataset, name)


def update_saved_query(
    project: str,
    region: str,
    name: str,
    sql: str,
) -> None:
    """Update the SQL content of a saved query via Dataform API.

    Locates the repository matching *name*, finds its workspace, and
    writes the new SQL to ``content.sql``.

    Args:
        project: GCP project ID.
        region: GCP region (e.g. ``us-east1``).
        name: Saved query display name.
        sql: New SQL content.

    Raises:
        ValueError: If the saved query is not found.
    """
    options = client_options_lib.ClientOptions(quota_project_id=project)
    client = dataform.DataformClient(client_options=options)
    parent = f"projects/{project}/locations/{region}"

    target_ws = None
    for repo in client.list_repositories(parent=parent):
        if repo.display_name == name:
            ws_iter = client.list_workspaces(parent=repo.name)
            target_ws = next(iter(ws_iter), None)
            break

    if target_ws is None:
        msg = (
            f"Saved query '{name}' not found in project '{project}' region '{region}'."
        )
        raise ValueError(msg)

    client.write_file(
        request={
            "workspace": target_ws.name,
            "path": "content.sql",
            "contents": sql.encode(),
        },
    )
    logger.info("Updated saved query '%s'", name)


def load_table_from_file(
    project: str,
    dataset: str,
    table: str,
    source: Path,
    fmt: str = "csv",
) -> None:
    """Replace a BigQuery table's contents with data from a local file.

    Uses ``WRITE_TRUNCATE`` to fully replace the table data.

    Args:
        project: GCP project ID.
        dataset: BigQuery dataset ID.
        table: Target table name.
        source: Local CSV or Parquet file.
        fmt: Source format, ``"csv"`` or ``"parquet"``.

    Raises:
        ValueError: If *fmt* is not ``"csv"`` or ``"parquet"``.
        FileNotFoundError: If *source* does not exist.
    """
    if fmt not in ("csv", "parquet"):
        msg = f"Unsupported format: {fmt!r}. Expected 'csv' or 'parquet'."
        raise ValueError(msg)

    if not source.is_file():
        msg = f"Source file not found: {source}"
        raise FileNotFoundError(msg)

    client = bigquery.Client(project=project)
    table_ref = f"{project}.{dataset}.{table}"

    source_format = (
        bigquery.SourceFormat.CSV if fmt == "csv" else bigquery.SourceFormat.PARQUET
    )
    job_config = bigquery.LoadJobConfig(
        source_format=source_format,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    if fmt == "csv":
        job_config.autodetect = True

    with source.open("rb") as fh:
        load_job = client.load_table_from_file(fh, table_ref, job_config=job_config)

    load_job.result()
    logger.info("Loaded %s into %s (WRITE_TRUNCATE)", source, table_ref)


def update_routine(
    project: str,
    dataset: str,
    name: str,
    body: str,
) -> None:
    """Update the body of an existing BigQuery routine.

    Works for both SQL and JavaScript routines — the ``language``
    attribute is preserved from the existing routine definition.

    Args:
        project: GCP project ID.
        dataset: BigQuery dataset ID.
        name: Routine name.
        body: New routine body (SQL or JS code).
    """
    client = bigquery.Client(project=project)
    routine_ref = f"{project}.{dataset}.{name}"
    routine = client.get_routine(routine_ref)
    routine.body = body
    client.update_routine(routine, ["body"])
    logger.info("Updated routine %s.%s.%s", project, dataset, name)


def update_routine_description(
    project: str,
    dataset: str,
    name: str,
    description: str,
) -> None:
    """Update the description of an existing BigQuery routine.

    Args:
        project: GCP project ID.
        dataset: BigQuery dataset ID.
        name: Routine name.
        description: New routine description.
    """
    client = bigquery.Client(project=project)
    routine_ref = f"{project}.{dataset}.{name}"
    routine = client.get_routine(routine_ref)
    routine.description = description
    client.update_routine(routine, ["description"])
    logger.info("Updated routine description %s.%s.%s", project, dataset, name)


# ---------------------------------------------------------------------------
# Delete functions (rm mode)
# ---------------------------------------------------------------------------


def delete_view(project: str, dataset: str, name: str) -> None:
    """Delete a BigQuery view.

    Args:
        project: GCP project ID.
        dataset: BigQuery dataset ID.
        name: View name.
    """
    client = bigquery.Client(project=project)
    table_ref = f"{project}.{dataset}.{name}"
    client.delete_table(table_ref)
    logger.info("Deleted view %s.%s.%s", project, dataset, name)


def delete_table(project: str, dataset: str, name: str) -> None:
    """Delete a BigQuery table.

    Args:
        project: GCP project ID.
        dataset: BigQuery dataset ID.
        name: Table name.
    """
    client = bigquery.Client(project=project)
    table_ref = f"{project}.{dataset}.{name}"
    client.delete_table(table_ref)
    logger.info("Deleted table %s.%s.%s", project, dataset, name)


def delete_routine(project: str, dataset: str, name: str) -> None:
    """Delete a BigQuery routine.

    Args:
        project: GCP project ID.
        dataset: BigQuery dataset ID.
        name: Routine name.
    """
    client = bigquery.Client(project=project)
    routine_ref = f"{project}.{dataset}.{name}"
    client.delete_routine(routine_ref)
    logger.info("Deleted routine %s.%s.%s", project, dataset, name)


def delete_saved_query(project: str, region: str, name: str) -> None:
    """Delete a saved query via Dataform API.

    Locates the Dataform repository matching *name* and deletes it.

    Args:
        project: GCP project ID.
        region: GCP region (e.g. ``us-east1``).
        name: Saved query display name.

    Raises:
        ValueError: If the saved query is not found.
    """
    options = client_options_lib.ClientOptions(quota_project_id=project)
    client = dataform.DataformClient(client_options=options)
    parent = f"projects/{project}/locations/{region}"

    target_repo = None
    for repo in client.list_repositories(parent=parent):
        if repo.display_name == name:
            target_repo = repo
            break

    if target_repo is None:
        msg = (
            f"Saved query '{name}' not found in project '{project}' region '{region}'."
        )
        raise ValueError(msg)

    client.delete_repository(name=target_repo.name, force=True)
    logger.info("Deleted saved query '%s'", name)
