"""Materialize orchestrator — create a permanent table from a view or external table.

Executes ``CREATE OR REPLACE TABLE <target> AS SELECT * FROM <source>``
via the BigQuery Python client (no ``bq`` CLI dependency), then pulls
the newly created table's model YAML so the local tree stays in sync.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from bq_sync import bq_client
from bq_sync.config import SyncConfig, resolve_output_dir
from bq_sync.writers import write_model_yaml

logger = logging.getLogger(__name__)

# Prefixes replaced when deriving the default materialized table name.
_RENAME_PREFIXES = ("view_", "external_")


def _default_target_name(source_name: str) -> str:
    """Derive a default target table name from the source resource name.

    Replaces a leading ``view_`` or ``external_`` prefix with
    ``materialize_`` so the output table is clearly distinguished from
    its source while preserving the rest of the name.

    Examples::

        view_auth_user_valid   -> materialize_auth_user_valid
        external_nps_contadia  -> materialize_nps_contadia
        snapshot_events        -> snapshot_events  (no known prefix, unchanged)

    Args:
        source_name: Source resource name (view or external table).

    Returns:
        The target table name to use by default.
    """
    for prefix in _RENAME_PREFIXES:
        if source_name.startswith(prefix):
            return "materialize_" + source_name[len(prefix):]
    # No recognised prefix: keep the same name (user can override with --target).
    return source_name


def _build_ddl(
    project: str,
    dataset: str,
    source_name: str,
    target_name: str,
) -> str:
    """Return the DDL for materializing *source_name* into *target_name*.

    Always uses ``CREATE OR REPLACE TABLE`` so running the command a
    second time refreshes the snapshot without manual cleanup.

    Args:
        project: GCP project ID.
        dataset: BigQuery dataset ID.
        source_name: Source view or external table name.
        target_name: Target permanent table name.

    Returns:
        A ``CREATE OR REPLACE TABLE … AS SELECT * FROM …`` DDL string.
    """
    return (
        f"CREATE OR REPLACE TABLE `{project}.{dataset}.{target_name}`\n"
        f"AS SELECT * FROM `{project}.{dataset}.{source_name}`"
    )


def _confirm(prompt: str) -> bool:
    """Prompt for ``[y/N]`` confirmation.

    Args:
        prompt: Prompt text shown before ``[y/N]``.

    Returns:
        ``True`` if the user confirms.
    """
    try:
        answer = input(f"  {prompt} [y/N] ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        print()
        return False
    return answer in ("y", "yes")


def materialize_resource(
    config: SyncConfig,
    config_path: Path,
    resource_path: str,
    *,
    dry_run: bool = False,
    yes: bool = False,
    target_name: str | None = None,
) -> None:
    """Materialize a view or external table into a permanent BQ table.

    Resolution order for *resource_path*:

    - ``<project>/<dataset>/<name>`` — dataset-level resource.

    After the DDL executes successfully, the new table's metadata is
    pulled and written to the local model YAML without a full dataset
    scan (targeted auto-pull).

    Args:
        config: Parsed sync configuration.
        config_path: Path to the ``bq_sync.toml`` that was loaded.
        resource_path: Resource path in ``project/dataset/name`` form.
        dry_run: If ``True``, print the DDL and skip execution.
        yes: If ``True``, skip interactive confirmation.
        target_name: Override the target table name.  Defaults to
            the result of ``_default_target_name(source_name)``.
    """
    output_root = resolve_output_dir(config, config_path)

    # Parse resource path.
    parts = resource_path.split("/")
    if len(parts) != 3:
        logger.error(
            "Invalid resource path '%s': expected <project>/<dataset>/<name>.",
            resource_path,
        )
        sys.exit(1)

    path_project, dataset, source_name = parts

    # Derive the target table name when not explicitly given.
    effective_target = target_name or _default_target_name(source_name)

    ddl = _build_ddl(path_project, dataset, source_name, effective_target)

    print(f"\n  Source : {path_project}.{dataset}.{source_name}")
    print(f"  Target : {path_project}.{dataset}.{effective_target}")
    print(f"\n  DDL:\n\n    {ddl.replace(chr(10), chr(10) + '    ')}\n")

    if dry_run:
        logger.info("Dry-run: no changes written.")
        return

    if not yes:
        if not _confirm("Proceed with materialization?"):
            logger.info("Materialization cancelled.")
            return

    logger.info(
        "Materializing %s.%s.%s -> %s …",
        path_project,
        dataset,
        source_name,
        effective_target,
    )
    bq_client.run_query(path_project, ddl)
    logger.info("Materialization complete.")

    # Targeted auto-pull: fetch only the new table's metadata and write its
    # model YAML so the local tree reflects the newly created table without
    # requiring a full `bq-sync pull` run.
    logger.info("Pulling model YAML for '%s' …", effective_target)
    table_info = bq_client.get_table_info(path_project, dataset, effective_target)
    model_path = output_root / dataset / "models" / f"{effective_target}.yaml"
    write_model_yaml(model_path, table_info)
    logger.info("Wrote model YAML to %s", model_path)
    logger.info(
        "Hint: review with 'git diff' and commit with 'git add -A && git commit'."
    )
