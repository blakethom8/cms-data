import csv
import io
import json
import sys
from pathlib import Path

import duckdb
import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT))

from pipeline.acquisition import (
    CMS_CSV_PROFILES,
    inspect_cms_csv,
    inspect_hospital_enrollments,
)
from pipeline.data_platform import EXIT_HEALTHY, EXIT_RELEASE_FAILURE, main
from pipeline.manifests import (
    ManifestDocument,
    ManifestStore,
    PromotionState,
    RunManifest,
    ValidationState,
)
from pipeline.releases import (
    FULL_PLATFORM_SMOKE_TABLES,
    FULL_PLATFORM_WAREHOUSE_SOURCE_IDS,
    HOSPITAL_COLUMN_MAP,
    PPEF_CHANGED_TABLES,
    ReleaseError,
    WAREHOUSE_RELEASE_SCHEMA_VERSION,
    WarehouseReleaseStore,
    _rebuild_hospital_affiliations,
    _validate_ppef_relationships,
    build_full_cms_warehouse_release,
    build_ppef_warehouse_release,
    build_warehouse_release,
    compare_warehouse_release,
    promote_staging_release,
    rollback_staging_release,
    sha256_file,
)
from pipeline.source_registry import SOURCE_REGISTRY

SOURCE_RUN_ID = "20990720T010000Z-hospital"
SOURCE_RELEASE_ID = "cms_hospital_enrollments-fixture"
CODE_COMMIT = "a" * 40
PPEF_PERIOD = "2026-01-01/2026-03-31"
PPEF_PROVIDER_ENROLLMENT = "I00000000000001"
PPEF_RECEIVER_ENROLLMENT = "O00000000000002"


def _ppef_validation_connection() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(":memory:")
    connection.execute(
        "CREATE TABLE raw_pecos_enrollment (ENRLMT_ID VARCHAR)"
    )
    connection.execute(
        "CREATE TABLE raw_pecos_reassignment "
        "(REASGN_BNFT_ENRLMT_ID VARCHAR, RCV_BNFT_ENRLMT_ID VARCHAR)"
    )
    connection.execute(
        "CREATE TABLE raw_pecos_practice_location "
        "(ENRLMT_ID VARCHAR, CITY_NAME VARCHAR, STATE_CD VARCHAR, ZIP_CD VARCHAR)"
    )
    connection.execute(
        "CREATE TABLE pecos_provider_organizations "
        "(receiving_organization_name VARCHAR)"
    )
    connection.execute(
        "CREATE TABLE pecos_enrollment_practice_locations (state VARCHAR)"
    )
    connection.execute(
        "INSERT INTO raw_pecos_enrollment VALUES ('provider'), ('receiver')"
    )
    connection.execute(
        "INSERT INTO raw_pecos_reassignment VALUES ('provider', 'receiver')"
    )
    connection.execute(
        "INSERT INTO raw_pecos_practice_location "
        "VALUES ('receiver', 'Los Angeles', 'CA', '90048')"
    )
    connection.execute(
        "INSERT INTO pecos_provider_organizations VALUES ('Example Medical Group')"
    )
    connection.execute(
        "INSERT INTO pecos_enrollment_practice_locations VALUES ('CA')"
    )
    return connection


def test_validate_ppef_relationships_accepts_declared_grain_and_enrollment_keys():
    connection = _ppef_validation_connection()
    try:
        details = _validate_ppef_relationships(
            connection,
            {
                "raw_pecos_reassignment": 1,
                "raw_pecos_practice_location": 1,
                "pecos_provider_organizations": 1,
                "pecos_enrollment_practice_locations": 1,
            },
        )
    finally:
        connection.close()

    assert details["curated_named_organization_rate"] == 1.0
    assert details["curated_california_location_rows"] == 1
    assert details["orphan_receiving_enrollments"] == 0


def test_validate_ppef_relationships_rejects_orphan_enrollment_keys():
    connection = _ppef_validation_connection()
    connection.execute(
        "INSERT INTO raw_pecos_reassignment VALUES ('provider', 'missing')"
    )
    try:
        with pytest.raises(
            ReleaseError, match="orphan_receiving_enrollments=1"
        ):
            _validate_ppef_relationships(
                connection,
                {
                    "raw_pecos_reassignment": 2,
                    "raw_pecos_practice_location": 1,
                    "pecos_provider_organizations": 1,
                    "pecos_enrollment_practice_locations": 1,
                },
            )
    finally:
        connection.close()


def _hospital_csv(
    *,
    header: tuple[str, ...] | None = None,
    rows: tuple[dict[str, str], ...] | None = None,
) -> bytes:
    columns = header or tuple(source for source, _ in HOSPITAL_COLUMN_MAP)
    default = {
        "ENROLLMENT ID": "E100",
        "ENROLLMENT STATE": "CA",
        "PROVIDER TYPE CODE": "00-09",
        "PROVIDER TYPE TEXT": "PART A PROVIDER - HOSPITAL",
        "NPI": "1234567890",
        "MULTIPLE NPI FLAG": "N",
        "CCN": "050001",
        "ASSOCIATE ID": "A100",
        "ORGANIZATION NAME": "Example Hospital",
        "STATE": "CA",
        "ZIP CODE": "90001",
        "SUBGROUP - GENERAL": "Y",
    }
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, lineterminator="\n")
    writer.writerow(columns)
    for override in rows or ({},):
        values = {column: "" for column in columns}
        values.update(default)
        values.update(override)
        writer.writerow([values[column] for column in columns])
    return stream.getvalue().encode("utf-8")


def _stage_source(data_root: Path, payload: bytes | None = None) -> RunManifest:
    artifact = (
        data_root
        / "runs"
        / "cms_hospital_enrollments"
        / SOURCE_RUN_ID
        / "source.csv"
    )
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(payload or _hospital_csv())
    inspection = inspect_hospital_enrollments(artifact)
    manifest = RunManifest(
        run_id=SOURCE_RUN_ID,
        release_id=SOURCE_RELEASE_ID,
        source_id="cms_hospital_enrollments",
        publisher=SOURCE_REGISTRY["cms_hospital_enrollments"].publisher.value,
        publisher_version="cms-resource:10000000-0000-4000-8000-000000000009",
        source_data_period="2099-07-01/2099-07-31",
        publisher_release_timestamp="2099-07-14T00:00:00+00:00",
        discovery_timestamp="2099-07-20T00:00:00+00:00",
        retrieval_timestamp="2099-07-20T01:00:00+00:00",
        source_url="https://data.cms.gov/example/hospital-enrollments.csv",
        byte_size=inspection.byte_size,
        sha256=inspection.sha256,
        schema_fingerprint=inspection.schema_fingerprint,
        source_encoding=inspection.source_encoding,
        row_counts={"source_rows": inspection.row_count},
        pipeline_code_commit=CODE_COMMIT,
        validation_state=ValidationState.PASSED,
        validation_timestamp="2099-07-20T02:00:00+00:00",
    )
    ManifestStore(data_root / "manifests.json").save(
        ManifestDocument(manifests=[manifest])
    )
    return manifest


def _verified_backup(
    tmp_path: Path,
    *,
    practices: tuple[tuple[str, str, str, str], ...] | None = None,
) -> tuple[Path, Path, str]:
    backup = tmp_path / "backup" / "provider_searcher.duckdb"
    backup.parent.mkdir(parents=True)
    connection = duckdb.connect(str(backup))
    try:
        practice_rows = practices or (
            ("9999999999", "PAC100", "Example Hospital", "CA"),
        )
        connection.execute("CREATE TABLE core_providers (npi VARCHAR PRIMARY KEY)")
        connection.executemany(
            "INSERT INTO core_providers VALUES (?)",
            [(row[0],) for row in practice_rows],
        )
        connection.execute(
            """
            CREATE TABLE practice_locations (
                npi VARCHAR,
                group_pac_id VARCHAR,
                group_legal_name VARCHAR,
                group_state VARCHAR,
                state VARCHAR
            )
            """
        )
        connection.executemany(
            "INSERT INTO practice_locations VALUES (?, ?, ?, ?, ?)",
            [(npi, pac, name, state, state) for npi, pac, name, state in practice_rows],
        )
        connection.execute(
            """
            CREATE TABLE hospital_affiliations (
                npi VARCHAR NOT NULL,
                hospital_npi VARCHAR NOT NULL,
                hospital_ccn VARCHAR,
                hospital_name VARCHAR,
                hospital_city VARCHAR,
                hospital_state VARCHAR,
                hospital_zip VARCHAR,
                hospital_subgroup VARCHAR,
                affiliation_source VARCHAR NOT NULL,
                confidence_level VARCHAR,
                group_pac_id VARCHAR,
                data_year INTEGER NOT NULL,
                PRIMARY KEY (npi, hospital_npi)
            )
            """
        )
        connection.execute("CREATE TABLE baseline_marker (value VARCHAR)")
        connection.execute("INSERT INTO baseline_marker VALUES ('preserved')")
        connection.execute(
            "CREATE TABLE raw_hospital_enrollments (npi VARCHAR, organization_name VARCHAR)"
        )
        connection.execute(
            "INSERT INTO raw_hospital_enrollments VALUES ('0000000000', 'Old data')"
        )
        connection.execute("CHECKPOINT")
    finally:
        connection.close()
    digest = sha256_file(backup)
    manifest_path = backup.parent / "backup-manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "backup_path": str(backup),
                "backup_identity": {"byte_size": backup.stat().st_size},
                "sha256": digest,
                "validation": {"read_only_open": "passed"},
            }
        )
    )
    return backup, manifest_path, digest


def _build(tmp_path: Path):
    data_root = tmp_path / "data"
    _stage_source(data_root)
    backup, backup_manifest, baseline_hash = _verified_backup(tmp_path)
    result = build_warehouse_release(
        data_root=data_root,
        source_run_id=SOURCE_RUN_ID,
        backup_manifest_path=backup_manifest,
        code_commit=CODE_COMMIT,
    )
    return data_root, backup, baseline_hash, result


def _stage_ppef_sources(data_root: Path, *, period: str = PPEF_PERIOD) -> tuple[str, ...]:
    payloads = {
        "cms_pecos_reassignment": (
            "REASGN_BNFT_ENRLMT_ID,RCV_BNFT_ENRLMT_ID\n"
            f"{PPEF_PROVIDER_ENROLLMENT},{PPEF_RECEIVER_ENROLLMENT}\n"
        ).encode(),
        "cms_pecos_practice_location": (
            "ENRLMT_ID,CITY_NAME,STATE_CD,ZIP_CD\n"
            f"{PPEF_RECEIVER_ENROLLMENT},Los Angeles,CA,90048\n"
        ).encode(),
    }
    manifests: list[RunManifest] = []
    run_ids: list[str] = []
    for index, (source_id, payload) in enumerate(payloads.items(), start=1):
        run_id = f"20260401T00000{index}Z-{source_id}"
        artifact = data_root / "runs" / source_id / run_id / "source.csv"
        artifact.parent.mkdir(parents=True)
        artifact.write_bytes(payload)
        inspection = inspect_cms_csv(
            artifact,
            profile=CMS_CSV_PROFILES[source_id],
        )
        manifests.append(
            RunManifest(
                run_id=run_id,
                release_id=f"{source_id}-2026q1",
                source_id=source_id,
                publisher=SOURCE_REGISTRY[source_id].publisher.value,
                publisher_version="fixture-2026q1",
                source_data_period=period,
                publisher_release_timestamp="2026-04-01T00:00:00+00:00",
                discovery_timestamp="2026-04-01T00:01:00+00:00",
                retrieval_timestamp="2026-04-01T00:02:00+00:00",
                source_url=f"https://data.cms.gov/fixture/{source_id}.csv",
                byte_size=inspection.byte_size,
                sha256=inspection.sha256,
                schema_fingerprint=inspection.schema_fingerprint,
                source_encoding=inspection.source_encoding,
                row_counts={
                    "source_rows": inspection.row_count,
                    "invalid_identifier_rows": inspection.invalid_identifier_rows,
                },
                pipeline_code_commit=CODE_COMMIT,
                validation_state=ValidationState.PASSED,
                validation_timestamp="2026-04-01T00:03:00+00:00",
            )
        )
        run_ids.append(run_id)
    ManifestStore(data_root / "manifests.json").save(
        ManifestDocument(manifests=manifests)
    )
    return tuple(run_ids)


def _ppef_baseline(tmp_path: Path, *, period: str = PPEF_PERIOD) -> tuple[Path, Path]:
    baseline = tmp_path / "ppef-backup" / "warehouse.duckdb"
    baseline.parent.mkdir(parents=True)
    connection = duckdb.connect(str(baseline))
    try:
        connection.execute(
            """
            CREATE TABLE raw_pecos_enrollment (
                NPI VARCHAR,
                ENRLMT_ID VARCHAR,
                ORG_NAME VARCHAR,
                PROVIDER_TYPE_CD VARCHAR,
                PROVIDER_TYPE_DESC VARCHAR,
                STATE_CD VARCHAR,
                source_run_id VARCHAR,
                source_data_period VARCHAR
            )
            """
        )
        connection.executemany(
            "INSERT INTO raw_pecos_enrollment VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "1234567890",
                    PPEF_PROVIDER_ENROLLMENT,
                    None,
                    "14",
                    "Physician",
                    "CA",
                    "enrollment-run",
                    period,
                ),
                (
                    "1999999999",
                    PPEF_RECEIVER_ENROLLMENT,
                    "Example Medical Group",
                    "70",
                    "Clinic/Group Practice",
                    "CA",
                    "enrollment-run",
                    period,
                ),
            ],
        )
        connection.execute(
            """
            CREATE TABLE hospital_affiliations (
                npi VARCHAR,
                hospital_npi VARCHAR,
                hospital_name VARCHAR,
                hospital_state VARCHAR,
                affiliation_source VARCHAR,
                confidence_level VARCHAR
            )
            """
        )
        connection.execute(
            "INSERT INTO hospital_affiliations VALUES "
            "('1234567890', '1888888888', 'Example Hospital', 'CA', 'fixture', 'high')"
        )
        for table in FULL_PLATFORM_SMOKE_TABLES:
            if table in PPEF_CHANGED_TABLES or table in {
                "raw_pecos_enrollment",
                "hospital_affiliations",
            }:
                continue
            connection.execute(f'CREATE TABLE "{table}" (value INTEGER)')
            connection.execute(f'INSERT INTO "{table}" VALUES (1)')
        connection.execute("CREATE TABLE baseline_marker (value VARCHAR)")
        connection.execute("INSERT INTO baseline_marker VALUES ('preserved')")
        connection.execute("CHECKPOINT")
    finally:
        connection.close()
    digest = sha256_file(baseline)
    manifest = baseline.parent / "backup-manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "backup_path": str(baseline),
                "backup_identity": {"byte_size": baseline.stat().st_size},
                "sha256": digest,
                "validation": {"read_only_open": "passed"},
            }
        )
    )
    return baseline, manifest


def test_targeted_ppef_release_changes_only_relationship_tables(tmp_path: Path) -> None:
    data_root = tmp_path / "ppef-data"
    run_ids = _stage_ppef_sources(data_root)
    baseline, backup_manifest = _ppef_baseline(tmp_path)

    result = build_ppef_warehouse_release(
        data_root=data_root,
        source_run_ids=run_ids,
        backup_manifest_path=backup_manifest,
        code_commit=CODE_COMMIT,
        memory_limit_gb=1,
        threads=1,
    )
    comparison = compare_warehouse_release(
        data_root=data_root,
        warehouse_release_id=result.release.warehouse_release_id,
        backup_manifest_path=backup_manifest,
    )

    assert comparison["state"] == "passed"
    assert comparison["comparison_policy"] == "ppef_additive_v1"
    assert comparison["unexpected_differences"] == []
    assert set(comparison["changed_tables"]) == PPEF_CHANGED_TABLES
    assert result.release.validation_details["release_scope"] == "targeted_additive"
    assert result.release.validation_details["resource_limits"]["threads"] == 1
    assert result.release.validation_details["baseline_dependencies"][
        "cms_pecos_public_provider_enrollment"
    ]["source_data_period"] == PPEF_PERIOD
    assert result.release.table_counts == {
        "pecos_provider_organizations": 1,
        "pecos_enrollment_practice_locations": 1,
        "raw_pecos_practice_location": 1,
        "raw_pecos_reassignment": 1,
    }
    baseline_connection = duckdb.connect(str(baseline), read_only=True)
    candidate_connection = duckdb.connect(str(result.database_path), read_only=True)
    try:
        assert baseline_connection.execute(
            "SELECT count(*) FROM raw_pecos_enrollment"
        ).fetchone()[0] == 2
        with pytest.raises(duckdb.CatalogException):
            baseline_connection.execute("SELECT * FROM raw_pecos_reassignment")
        assert candidate_connection.execute(
            "SELECT receiving_organization_name FROM pecos_provider_organizations"
        ).fetchone()[0] == "Example Medical Group"
        assert candidate_connection.execute(
            "SELECT value FROM baseline_marker"
        ).fetchone()[0] == "preserved"
    finally:
        candidate_connection.close()
        baseline_connection.close()


def test_targeted_ppef_release_rejects_enrollment_period_mismatch(tmp_path: Path) -> None:
    data_root = tmp_path / "ppef-data"
    run_ids = _stage_ppef_sources(data_root)
    _, backup_manifest = _ppef_baseline(
        tmp_path, period="2025-10-01/2025-12-31"
    )

    with pytest.raises(ReleaseError, match="does not match baseline PECOS enrollment"):
        build_ppef_warehouse_release(
            data_root=data_root,
            source_run_ids=run_ids,
            backup_manifest_path=backup_manifest,
            code_commit=CODE_COMMIT,
            memory_limit_gb=1,
            threads=1,
        )


def test_build_release_copies_baseline_loads_source_and_records_provenance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    active = tmp_path / "active.duckdb"
    active.write_bytes(b"active warehouse sentinel")
    active_before = (active.read_bytes(), active.stat().st_mtime_ns)
    monkeypatch.setenv("DUCKDB_PATH", str(active))

    data_root, backup, baseline_hash, result = _build(tmp_path)

    assert result.database_path != backup
    assert sha256_file(backup) == baseline_hash
    assert (active.read_bytes(), active.stat().st_mtime_ns) == active_before
    assert not result.database_path.with_suffix(".duckdb.partial").exists()
    assert result.release.validation_state == ValidationState.PASSED
    assert result.release.promotion_state == PromotionState.NOT_PROMOTED
    assert result.release.pipeline_code_commit == CODE_COMMIT
    assert result.release.duckdb_version == duckdb.__version__
    assert result.release.table_counts["raw_hospital_enrollments"] == 1
    assert result.release.table_counts["core_providers"] == 1
    assert result.release.table_counts["hospital_affiliations"] == 1
    assert result.release.table_counts["ambiguous_hospital_name_state_keys"] == 0
    assert result.release.table_counts["database_tables"] == 5
    assert (
        result.release.validation_details["affiliation_match_policy"]
        == "normalized_name_and_state_unique_hospital_npi_v1"
    )
    assert result.release.sha256 == sha256_file(result.database_path)

    connection = duckdb.connect(str(result.database_path), read_only=True)
    try:
        row = connection.execute(
            """
            SELECT npi, organization_name, source_run_id, source_data_period
            FROM raw_hospital_enrollments
            """
        ).fetchone()
        marker = connection.execute("SELECT value FROM baseline_marker").fetchone()[0]
        affiliation = connection.execute(
            """
            SELECT npi, hospital_npi, hospital_name, affiliation_source,
                   confidence_level, data_year
            FROM hospital_affiliations
            """
        ).fetchone()
    finally:
        connection.close()
    assert row == (
        "1234567890",
        "Example Hospital",
        SOURCE_RUN_ID,
        "2099-07-01/2099-07-31",
    )
    assert marker == "preserved"
    assert affiliation == (
        "9999999999",
        "1234567890",
        "Example Hospital",
        "cms_reassignment_legal_name_state",
        "medium",
        2099,
    )

    stored = WarehouseReleaseStore(data_root / "warehouse-releases.json").load()
    assert stored.releases[0].to_dict() == result.release.to_dict()
    per_release = json.loads(result.release_manifest_path.read_text())
    assert per_release["schema_version"] == WAREHOUSE_RELEASE_SCHEMA_VERSION
    assert per_release["release"] == result.release.to_dict()


def test_affiliation_rebuild_returns_the_final_table_count(tmp_path: Path) -> None:
    _, _, _, result = _build(tmp_path)
    result.database_path.chmod(0o640)
    connection = duckdb.connect(str(result.database_path))
    try:
        counts = _rebuild_hospital_affiliations(connection, data_year=2099)
    finally:
        connection.close()
        result.database_path.chmod(0o440)

    assert counts["hospital_affiliations"] == 1


def test_schema_ddl_matches_canonical_raw_hospital_loader() -> None:
    connection = duckdb.connect(":memory:")
    try:
        connection.execute((REPOSITORY_ROOT / "schema" / "ddl.sql").read_text())
        columns = [
            row[1]
            for row in connection.execute(
                "PRAGMA table_info('raw_hospital_enrollments')"
            ).fetchall()
        ]
    finally:
        connection.close()

    assert columns == [
        *(target for _, target in HOSPITAL_COLUMN_MAP),
        "source_run_id",
        "source_release_id",
        "source_data_period",
        "ingested_at",
    ]


def test_full_cms_build_refuses_an_incomplete_source_set(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    manifest = _stage_source(data_root)

    with pytest.raises(ReleaseError, match="source set is incomplete: missing="):
        build_full_cms_warehouse_release(
            data_root=data_root,
            source_run_ids=(manifest.run_id,),
            backup_manifest_path=tmp_path / "unused-backup.json",
            code_commit=CODE_COMMIT,
        )


def test_affiliation_transform_excludes_ambiguous_names_and_labels_dba_matches(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    _stage_source(
        data_root,
        _hospital_csv(
            rows=(
                {
                    "ENROLLMENT ID": "E101",
                    "NPI": "1111111111",
                    "CCN": "050101",
                    "ORGANIZATION NAME": "Shared Health System",
                },
                {
                    "ENROLLMENT ID": "E102",
                    "NPI": "2222222222",
                    "CCN": "050102",
                    "ORGANIZATION NAME": "Shared Health System",
                },
                {
                    "ENROLLMENT ID": "E103",
                    "NPI": "3333333333",
                    "CCN": "050103",
                    "ORGANIZATION NAME": "Unique Legal Hospital",
                    "DOING BUSINESS AS NAME": "Community DBA Hospital",
                },
            )
        ),
    )
    _, backup_manifest, _ = _verified_backup(
        tmp_path,
        practices=(
            ("9000000001", "PAC101", "Shared Health System", "CA"),
            ("9000000002", "PAC102", "Community DBA Hospital", "CA"),
        ),
    )

    result = build_warehouse_release(
        data_root=data_root,
        source_run_id=SOURCE_RUN_ID,
        backup_manifest_path=backup_manifest,
        code_commit=CODE_COMMIT,
    )

    connection = duckdb.connect(str(result.database_path), read_only=True)
    try:
        rows = connection.execute(
            """
            SELECT npi, hospital_npi, affiliation_source, confidence_level
            FROM hospital_affiliations
            ORDER BY npi
            """
        ).fetchall()
    finally:
        connection.close()
    assert rows == [
        (
            "9000000002",
            "3333333333",
            "cms_reassignment_dba_name_state",
            "low",
        )
    ]
    assert result.release.table_counts["ambiguous_hospital_name_state_keys"] == 1
    assert result.release.table_counts["unambiguous_hospital_name_state_keys"] == 2


def test_warehouse_release_store_upgrades_schema_version_one(tmp_path: Path) -> None:
    data_root, _, _, result = _build(tmp_path)
    legacy_release = result.release.to_dict()
    legacy_release.pop("duckdb_version")
    legacy_release.pop("validation_details")
    store_path = data_root / "legacy-releases.json"
    store_path.write_text(
        json.dumps({"schema_version": 1, "releases": [legacy_release]})
    )

    loaded = WarehouseReleaseStore(store_path).load()

    assert loaded.schema_version == WAREHOUSE_RELEASE_SCHEMA_VERSION
    assert loaded.releases[0].duckdb_version is None
    assert loaded.releases[0].validation_details == {}


def test_release_comparison_is_read_only_and_records_evidence(tmp_path: Path) -> None:
    data_root, backup, _, result = _build(tmp_path)
    backup_manifest = backup.parent / "backup-manifest.json"
    before = {
        backup: (backup.stat().st_size, backup.stat().st_mtime_ns, sha256_file(backup)),
        result.database_path: (
            result.database_path.stat().st_size,
            result.database_path.stat().st_mtime_ns,
            sha256_file(result.database_path),
        ),
    }

    comparison = compare_warehouse_release(
        data_root=data_root,
        warehouse_release_id=result.release.warehouse_release_id,
        backup_manifest_path=backup_manifest,
    )

    assert comparison["state"] == "passed"
    assert comparison["comparison_policy"] == "hospital_affiliations_v1"
    assert comparison["unexpected_differences"] == []
    assert comparison["changed_tables"]["hospital_affiliations"] == {
        "baseline_rows": 0,
        "candidate_rows": 1,
    }
    assert comparison["representative_affiliations"][0]["npi"] == "9999999999"
    assert Path(comparison["comparison_path"]).is_file()
    for path, identity in before.items():
        assert (path.stat().st_size, path.stat().st_mtime_ns, sha256_file(path)) == identity


def test_full_platform_comparison_allows_only_source_owned_tables(
    tmp_path: Path,
) -> None:
    data_root, _, _, result = _build(tmp_path)
    result.database_path.chmod(0o640)
    connection = duckdb.connect(str(result.database_path))
    try:
        existing = {
            row[0]
            for row in connection.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main' AND table_type = 'BASE TABLE'"
            ).fetchall()
        }
        for table in FULL_PLATFORM_SMOKE_TABLES:
            if table not in existing:
                connection.execute(f'CREATE TABLE "{table}" (value INTEGER)')
                connection.execute(f'INSERT INTO "{table}" VALUES (1)')
        connection.execute("CHECKPOINT")
        smoke_counts = {
            table: connection.execute(
                f'SELECT count(*) FROM "{table}"'
            ).fetchone()[0]
            for table in FULL_PLATFORM_SMOKE_TABLES
        }
    finally:
        connection.close()
    result.database_path.chmod(0o440)

    store = WarehouseReleaseStore(data_root / "warehouse-releases.json")
    document = store.load()
    release = document.releases[0]
    release.sha256 = sha256_file(result.database_path)
    release.byte_size = result.database_path.stat().st_size
    release.validation_details["source_periods"] = {
        source_id: "2099" for source_id in FULL_PLATFORM_WAREHOUSE_SOURCE_IDS
    }
    release.validation_details["smoke_table_counts"] = smoke_counts
    store.save(document)

    comparison = compare_warehouse_release(
        data_root=data_root,
        warehouse_release_id=result.release.warehouse_release_id,
        backup_manifest_path=tmp_path / "backup" / "backup-manifest.json",
    )

    assert comparison["state"] == "passed"
    assert comparison["comparison_policy"] == "full_platform_v1"
    assert comparison["evidence_mismatches"] == []
    assert "practice_locations" in comparison["changed_tables"]
    assert "baseline_marker" not in comparison["changed_tables"]


def test_full_platform_comparison_rejects_stale_count_evidence(
    tmp_path: Path,
) -> None:
    data_root, _, _, result = _build(tmp_path)
    store = WarehouseReleaseStore(data_root / "warehouse-releases.json")
    document = store.load()
    release = document.releases[0]
    release.validation_details["source_periods"] = {
        source_id: "2099" for source_id in FULL_PLATFORM_WAREHOUSE_SOURCE_IDS
    }
    release.validation_details["smoke_table_counts"] = {
        table: 1 for table in FULL_PLATFORM_SMOKE_TABLES
    }
    store.save(document)

    with pytest.raises(ReleaseError, match="evidence_mismatches"):
        compare_warehouse_release(
            data_root=data_root,
            warehouse_release_id=result.release.warehouse_release_id,
            backup_manifest_path=tmp_path / "backup" / "backup-manifest.json",
        )


def test_build_release_fails_closed_on_publisher_header_change(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    expected = tuple(source for source, _ in HOSPITAL_COLUMN_MAP)
    changed = (expected[1], expected[0], *expected[2:])
    _stage_source(data_root, _hospital_csv(header=changed))
    _, backup_manifest, _ = _verified_backup(tmp_path)

    with pytest.raises(ReleaseError, match="column order changed"):
        build_warehouse_release(
            data_root=data_root,
            source_run_id=SOURCE_RUN_ID,
            backup_manifest_path=backup_manifest,
            code_commit=CODE_COMMIT,
        )

    release = WarehouseReleaseStore(
        data_root / "warehouse-releases.json"
    ).load().releases[0]
    assert release.validation_state == ValidationState.FAILED
    assert release.promotion_state == PromotionState.NOT_PROMOTED


def test_staging_promotion_rollback_and_repromotion_are_audited(
    tmp_path: Path,
) -> None:
    data_root, _, _, result = _build(tmp_path)

    promoted = promote_staging_release(
        data_root, result.release.warehouse_release_id
    )
    pointer = data_root / "staging" / "warehouse-current"
    assert promoted["state"] == "completed"
    assert pointer.resolve() == result.database_path.resolve()
    active_manifest = ManifestStore(data_root / "manifests.json").load().manifests[0]
    assert active_manifest.proves_active_installation
    assert (
        WarehouseReleaseStore(data_root / "warehouse-releases.json")
        .load()
        .releases[0]
        .promotion_state
        == PromotionState.ACTIVE
    )

    rolled_back = rollback_staging_release(data_root)
    assert rolled_back["state"] == "completed"
    assert not pointer.exists()
    rolled_back_manifest = ManifestStore(
        data_root / "manifests.json"
    ).load().manifests[0]
    assert rolled_back_manifest.promotion_state == PromotionState.ROLLED_BACK
    assert rolled_back_manifest.active_release_id is None

    promoted_again = promote_staging_release(
        data_root, result.release.warehouse_release_id
    )
    assert promoted_again["state"] == "completed"
    assert pointer.resolve() == result.database_path.resolve()
    journal = json.loads((data_root / "promotion-journal.json").read_text())
    assert [event["action"] for event in journal["events"]] == [
        "promote",
        "rollback",
        "promote",
    ]
    assert {event["state"] for event in journal["events"]} == {"completed"}


def test_promotion_failure_restores_pointer_and_manifest_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root, _, _, result = _build(tmp_path)
    original_save = ManifestStore.save
    calls = 0

    def fail_once(store: ManifestStore, document: ManifestDocument) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("simulated manifest write failure")
        original_save(store, document)

    monkeypatch.setattr(ManifestStore, "save", fail_once)

    with pytest.raises(ReleaseError, match="promotion rolled back"):
        promote_staging_release(data_root, result.release.warehouse_release_id)

    assert not (data_root / "staging" / "warehouse-current").exists()
    source = ManifestStore(data_root / "manifests.json").load().manifests[0]
    release = WarehouseReleaseStore(
        data_root / "warehouse-releases.json"
    ).load().releases[0]
    journal = json.loads((data_root / "promotion-journal.json").read_text())
    assert source.promotion_state == PromotionState.NOT_PROMOTED
    assert release.promotion_state == PromotionState.NOT_PROMOTED
    assert journal["events"][-1]["state"] == "rolled_back"


def test_unresolved_promotion_journal_blocks_another_transition(
    tmp_path: Path,
) -> None:
    data_root, _, _, result = _build(tmp_path)
    (data_root / "promotion-journal.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "events": [
                    {
                        "transaction_id": "interrupted",
                        "state": "pending",
                        "action": "promote",
                    }
                ],
            }
        )
    )

    with pytest.raises(ReleaseError, match="unresolved pending transaction"):
        promote_staging_release(data_root, result.release.warehouse_release_id)

    assert not (data_root / "staging" / "warehouse-current").exists()


def test_release_cli_json_and_exit_codes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    data_root = tmp_path / "data"
    _stage_source(data_root)
    _, backup_manifest, _ = _verified_backup(tmp_path)
    monkeypatch.setattr("pipeline.releases.pipeline_commit", lambda: CODE_COMMIT)

    build_code = main(
        [
            "build-release",
            "--environment",
            "staging",
            "--source-run-id",
            SOURCE_RUN_ID,
            "--backup-manifest",
            str(backup_manifest),
            "--data-root",
            str(data_root),
            "--json",
        ]
    )
    built = json.loads(capsys.readouterr().out)
    release_id = built["release"]["warehouse_release_id"]
    assert build_code == EXIT_HEALTHY

    compare_code = main(
        [
            "compare-release",
            "--environment",
            "staging",
            "--warehouse-release-id",
            release_id,
            "--backup-manifest",
            str(backup_manifest),
            "--data-root",
            str(data_root),
            "--json",
        ]
    )
    compared = json.loads(capsys.readouterr().out)
    assert compare_code == EXIT_HEALTHY
    assert compared["state"] == "passed"

    promote_code = main(
        [
            "promote",
            "--environment",
            "staging",
            "--warehouse-release-id",
            release_id,
            "--data-root",
            str(data_root),
            "--json",
        ]
    )
    promoted = json.loads(capsys.readouterr().out)
    assert promote_code == EXIT_HEALTHY
    assert promoted["state"] == "completed"

    rollback_code = main(
        [
            "rollback",
            "--environment",
            "staging",
            "--data-root",
            str(data_root),
            "--json",
        ]
    )
    rolled_back = json.loads(capsys.readouterr().out)
    assert rollback_code == EXIT_HEALTHY
    assert rolled_back["state"] == "completed"

    failure_code = main(
        [
            "promote",
            "--environment",
            "staging",
            "--warehouse-release-id",
            "missing-release",
            "--data-root",
            str(data_root),
            "--json",
        ]
    )
    failure = json.loads(capsys.readouterr().out)
    assert failure_code == EXIT_RELEASE_FAILURE
    assert "missing-release" in failure["error"]


def test_release_cli_does_not_accept_a_production_environment(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as raised:
        main(
            [
                "promote",
                "--environment",
                "production",
                "--warehouse-release-id",
                "anything",
            ]
        )

    assert raised.value.code == 2
    assert "invalid choice: 'production'" in capsys.readouterr().err
