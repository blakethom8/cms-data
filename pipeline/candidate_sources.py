"""Verified CMS source loading for an isolated DuckDB candidate.

The loader accepts only immutable acquisition runs whose bytes, schema fingerprint,
encoding, and row count still match their manifests. Windows-1252 files are
transcoded through a short-lived staging file; source artifacts are never changed.
Production path selection belongs to the release control plane.
"""

from __future__ import annotations

import os
import shutil
import tempfile
from collections.abc import Iterable
from contextlib import contextmanager
from pathlib import Path

import duckdb

from .acquisition import CMS_CSV_PROFILES, inspect_cms_csv
from .manifests import ManifestStore, RunManifest, ValidationState
from .releases import ReleaseError


CMS_RAW_TABLES: dict[str, str] = {
    "cms_physician_by_provider": "raw_physician_by_provider",
    "cms_physician_by_provider_and_service": (
        "raw_physician_by_provider_and_service"
    ),
    "cms_part_d_by_provider": "raw_part_d_by_provider",
    "cms_part_d_by_provider_and_drug": "raw_part_d_by_provider_and_drug",
    "cms_dme_by_referring_provider": "raw_dme_by_referring_provider",
    "cms_qpp_experience": "raw_qpp_experience",
    "cms_pecos_public_provider_enrollment": "raw_pecos_enrollment",
    "cms_pecos_reassignment": "raw_pecos_reassignment",
    "cms_pecos_practice_location": "raw_pecos_practice_location",
    "cms_order_and_referring": "raw_order_and_referring",
    "cms_revalidation_group_reassignment": "raw_reassignment",
}


def _quoted_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _existing_column_types(
    connection: duckdb.DuckDBPyConnection,
    table: str,
) -> dict[str, str]:
    """Capture the serving-compatible raw schema before replacing a table."""
    try:
        rows = connection.execute(
            f"PRAGMA table_info({_quoted_identifier(table)})"
        ).fetchall()
    except duckdb.CatalogException:
        return {}
    return {str(row[1]).casefold(): str(row[2]) for row in rows}


def _typed_replacements(
    connection: duckdb.DuckDBPyConnection,
    incoming_table: str,
    existing_types: dict[str, str],
) -> list[str]:
    """Return strict casts that preserve an established raw-table contract.

    Acquisition intentionally reads publisher CSV values as strings so schema
    inference cannot change between releases.  The copied warehouse, however,
    already carries the types consumed by the read API.  Preserve those types
    for matching columns and leave newly published columns as VARCHAR.  CAST is
    deliberately strict: a new non-empty value that violates the established
    contract aborts the candidate instead of becoming NULL.
    """
    rows = connection.execute(
        f"PRAGMA table_info({_quoted_identifier(incoming_table)})"
    ).fetchall()
    replacements: list[str] = []
    for row in rows:
        column = str(row[1])
        target_type = existing_types.get(column.casefold())
        if target_type is None or target_type.upper().startswith("VARCHAR"):
            continue
        if not all(
            character.isalnum() or character in "_(), "
            for character in target_type
        ):
            raise ReleaseError(
                f"Existing raw schema has an unsafe type for {column}: {target_type}"
            )
        quoted = _quoted_identifier(column)
        replacements.append(
            f"CAST(NULLIF(trim(CAST({quoted} AS VARCHAR)), '') AS {target_type}) "
            f"AS {quoted}"
        )
    return replacements


def _manifest_by_run_id(data_root: Path) -> dict[str, RunManifest]:
    document = ManifestStore(data_root / "manifests.json").load()
    result: dict[str, RunManifest] = {}
    for manifest in document.manifests:
        if manifest.run_id in result:
            raise ReleaseError(f"Duplicate source manifest run ID: {manifest.run_id}")
        result[manifest.run_id] = manifest
    return result


def verified_cms_runs(
    data_root: Path,
    run_ids: Iterable[str],
) -> tuple[tuple[RunManifest, Path], ...]:
    """Return verified non-hospital CMS CSV runs in stable source-ID order."""
    requested = tuple(run_ids)
    if not requested:
        raise ReleaseError("At least one CMS source run is required")
    if len(requested) != len(set(requested)):
        raise ReleaseError("CMS source run IDs must be unique")

    available = _manifest_by_run_id(data_root)
    verified: list[tuple[RunManifest, Path]] = []
    seen_sources: set[str] = set()
    for run_id in requested:
        manifest = available.get(run_id)
        if manifest is None:
            raise ReleaseError(f"Source manifest is missing for run {run_id}")
        if manifest.source_id not in CMS_RAW_TABLES:
            raise ReleaseError(
                f"Candidate raw loader does not support source {manifest.source_id}"
            )
        if manifest.source_id in seen_sources:
            raise ReleaseError(
                f"Candidate contains more than one run for source {manifest.source_id}"
            )
        seen_sources.add(manifest.source_id)
        if manifest.validation_state != ValidationState.PASSED:
            raise ReleaseError(f"Source run {run_id} has not passed validation")
        if (
            not manifest.sha256
            or not manifest.source_encoding
            or not manifest.retrieval_timestamp
        ):
            raise ReleaseError(
                f"Source run {run_id} lacks checksum, encoding, or retrieval provenance"
            )

        artifact = data_root / "runs" / manifest.source_id / run_id / "source.csv"
        if not artifact.is_file() or artifact.is_symlink():
            raise ReleaseError(
                f"Source artifact is missing or not a regular file: {artifact}"
            )
        inspection = inspect_cms_csv(
            artifact,
            profile=CMS_CSV_PROFILES[manifest.source_id],
        )
        expected_rows = manifest.row_counts.get("source_rows")
        expected_invalid_identifiers = manifest.row_counts.get(
            "invalid_identifier_rows", 0
        )
        if (
            inspection.sha256 != manifest.sha256
            or inspection.byte_size != manifest.byte_size
            or inspection.schema_fingerprint != manifest.schema_fingerprint
            or inspection.source_encoding != manifest.source_encoding
            or inspection.row_count != expected_rows
            or inspection.invalid_identifier_rows != expected_invalid_identifiers
        ):
            raise ReleaseError(
                f"Source artifact no longer matches acquisition manifest for run {run_id}"
            )
        verified.append((manifest, artifact))

    return tuple(sorted(verified, key=lambda item: item[0].source_id))


@contextmanager
def _utf8_artifact(
    data_root: Path,
    manifest: RunManifest,
    artifact: Path,
):
    if manifest.source_encoding == "utf-8-sig":
        yield artifact
        return
    if manifest.source_encoding != "cp1252":
        raise ReleaseError(
            f"Unsupported source encoding for run {manifest.run_id}: "
            f"{manifest.source_encoding}"
        )

    staging = data_root / "staging" / "transcodes"
    staging.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="",
        prefix=f"{manifest.run_id}-",
        suffix=".csv.partial",
        dir=staging,
        delete=False,
    )
    temporary = Path(handle.name)
    try:
        with artifact.open("r", encoding="cp1252", newline="") as source, handle:
            shutil.copyfileobj(source, handle, length=1024 * 1024)
            handle.flush()
            os.fsync(handle.fileno())
        yield temporary
    finally:
        temporary.unlink(missing_ok=True)


def load_cms_raw_tables(
    connection: duckdb.DuckDBPyConnection,
    *,
    data_root: Path,
    run_ids: Iterable[str],
) -> dict[str, int]:
    """Strictly replace raw CMS tables in an already-isolated candidate database."""
    verified = verified_cms_runs(data_root, run_ids)
    counts: dict[str, int] = {}
    connection.execute("BEGIN TRANSACTION")
    try:
        for manifest, artifact in verified:
            table = CMS_RAW_TABLES[manifest.source_id]
            quoted_table = _quoted_identifier(table)
            temporary = _quoted_identifier(f"{table}__candidate_load")
            typed_temporary = _quoted_identifier(f"{table}__candidate_typed")
            existing_types = _existing_column_types(connection, table)
            connection.execute(f"DROP TABLE IF EXISTS {temporary}")
            connection.execute(f"DROP TABLE IF EXISTS {typed_temporary}")
            with _utf8_artifact(data_root, manifest, artifact) as load_artifact:
                connection.execute(
                    f"""
                    CREATE TABLE {temporary} AS
                    SELECT
                        *,
                        ?::VARCHAR AS source_run_id,
                        ?::VARCHAR AS source_release_id,
                        ?::VARCHAR AS source_data_period,
                        ?::TIMESTAMPTZ AS ingested_at
                    FROM read_csv(
                        ?,
                        header = true,
                        all_varchar = true,
                        -- Acquisition already validates every RFC-4180 row and
                        -- exact width with Python's CSV parser. CMS sometimes
                        -- introduces a quoted field only after DuckDB's dialect
                        -- sample, so fix the dialect and allow that late quote.
                        quote = chr(34),
                        escape = chr(34),
                        strict_mode = false,
                        ignore_errors = false,
                        encoding = 'utf-8'
                    )
                    """,
                    [
                        manifest.run_id,
                        manifest.release_id,
                        manifest.source_data_period,
                        manifest.retrieval_timestamp,
                        str(load_artifact),
                    ],
                )
            loaded = connection.execute(
                f"SELECT count(*) FROM {temporary}"
            ).fetchone()[0]
            expected = manifest.row_counts["source_rows"]
            if loaded != expected:
                raise ReleaseError(
                    f"Loaded {loaded} rows for {manifest.source_id}; expected {expected}"
                )
            replacements = _typed_replacements(
                connection,
                f"{table}__candidate_load",
                existing_types,
            )
            if replacements:
                connection.execute(
                    f"""
                    CREATE TABLE {typed_temporary} AS
                    SELECT * REPLACE ({', '.join(replacements)})
                    FROM {temporary}
                    """
                )
                connection.execute(f"DROP TABLE {temporary}")
                temporary = typed_temporary
            connection.execute(f"DROP TABLE IF EXISTS {quoted_table}")
            connection.execute(f"ALTER TABLE {temporary} RENAME TO {quoted_table}")
            counts[table] = loaded
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise
    return dict(sorted(counts.items()))
