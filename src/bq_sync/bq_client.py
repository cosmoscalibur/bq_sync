"""Google Cloud API read wrapper for BigQuery resources.

Returns ``resources.*Info`` dataclass instances.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

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
        views.append(
            ViewInfo(
                name=table.table_id,
                sql=table.view_query or "",
                modified=table.modified or _EPOCH,
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
        routines.append(
            RoutineInfo(
                name=routine.routine_id,
                sql=routine.body or "",
                language=routine.language or "SQL",
                modified=routine.modified or _EPOCH,
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
            {"name": f.name, "type": f.field_type, "mode": f.mode} for f in table.schema
        ]
        partitioning = None
        if table.time_partitioning:
            partitioning = table.time_partitioning.field or "ingestion_time"

        clustering = list(table.clustering_fields) if table.clustering_fields else None

        tables.append(
            TableInfo(
                name=table.table_id,
                schema=schema,
                description=table.description or "",
                row_count=table.num_rows or 0,
                modified=table.modified or _EPOCH,
                partitioning=partitioning,
                clustering=clustering,
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
            {"name": f.name, "type": f.field_type, "mode": f.mode} for f in table.schema
        ]
        ext_config = table.external_data_configuration
        externals.append(
            ExternalTableInfo(
                name=table.table_id,
                source_uris=list(ext_config.source_uris) if ext_config else [],
                schema=schema,
                source_format=ext_config.source_format if ext_config else "",
                modified=table.modified or _EPOCH,
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
    client = datatransfer.DataTransferServiceClient()
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
        client = dataform.DataformClient()
        parent = f"projects/{project}/locations/{region}"
        saved: list[SavedQueryInfo] = []

        for repo in client.list_repositories(parent=parent):
            workspace_parent = f"{repo.name}/workspaces"
            for ws in client.list_workspaces(parent=workspace_parent):
                resp = client.query_directory_contents(
                    workspace=ws.name,
                )
                for entry in resp:
                    if not entry.file or not entry.file.path.endswith(".sql"):
                        continue
                    file_resp = client.read_file(
                        workspace=ws.name,
                        path=entry.file.path,
                    )
                    saved.append(
                        SavedQueryInfo(
                            name=entry.file.path.rsplit("/", 1)[-1].removesuffix(
                                ".sql"
                            ),
                            sql=file_resp.file_contents.decode()
                            if isinstance(file_resp.file_contents, bytes)
                            else file_resp.file_contents,
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
