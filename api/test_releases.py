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
    NPPES_SERVING_PRACTICE_CHANGED_TABLES,
    PPEF_CHANGED_TABLES,
    PROVIDER_PROFILE_CHANGED_TABLES,
    PROVIDER_PROFILE_CORE_CHANGED_TABLES,
    SERVING_PRACTICE_CHANGED_TABLES,
    SERVING_PRACTICE_MANAGED_DAC_CHANGED_TABLES,
    ReleaseError,
    WAREHOUSE_RELEASE_SCHEMA_VERSION,
    WarehouseRelease,
    WarehouseReleaseDocument,
    WarehouseReleaseStore,
    _rebuild_hospital_affiliations,
    _single_table_source_provenance,
    _table_logical_fingerprint,
    _validate_ppef_relationships,
    build_full_cms_warehouse_release,
    build_managed_dac_serving_practice_warehouse_release,
    build_nppes_serving_practice_warehouse_release,
    build_ppef_warehouse_release,
    build_provider_profile_core_warehouse_release,
    build_provider_profile_warehouse_release,
    build_serving_practice_warehouse_release,
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


def _serving_practice_baseline(tmp_path: Path) -> tuple[Path, Path, str]:
    data_root = tmp_path / "serving-data"
    baseline = tmp_path / "serving-baseline.duckdb"
    connection = duckdb.connect(str(baseline))
    try:
        connection.execute(
            '''
            CREATE TABLE raw_dac_national (
                "NPI" VARCHAR, "Provider First Name" VARCHAR,
                "Provider Last Name" VARCHAR, pri_spec VARCHAR,
                "Facility Name" VARCHAR, org_pac_id VARCHAR,
                num_org_mem INTEGER, adr_ln_1 VARCHAR, "ZIP Code" VARCHAR,
                "City/Town" VARCHAR, "State" VARCHAR,
                "Telephone Number" VARCHAR, source_run_id VARCHAR,
                source_data_period VARCHAR
            );
            INSERT INTO raw_dac_national VALUES
                ('1234567890', 'Jamie', 'Rivera', 'Cardiology',
                 'Cardio Group', 'PAC-1', 20, '10 MAIN ST', '90001',
                 'Los Angeles', 'CA', '111', 'dac-run', '2026-07');

            CREATE TABLE raw_physician_by_provider (
                "Rndrng_NPI" VARCHAR, "Tot_Mdcr_Pymt_Amt" DOUBLE,
                source_run_id VARCHAR, source_data_period VARCHAR
            );
            INSERT INTO raw_physician_by_provider VALUES
                ('1234567890', 125.25, 'partb-run', '2024');

            CREATE TABLE raw_part_d_by_provider (
                "PRSCRBR_NPI" VARCHAR, "Tot_Drug_Cst" DOUBLE,
                source_run_id VARCHAR, source_data_period VARCHAR
            );
            INSERT INTO raw_part_d_by_provider VALUES
                ('1234567890', 50.75, 'partd-run', '2024');

            CREATE TABLE address_geocode (addr_key VARCHAR, lat DOUBLE, lng DOUBLE);
            INSERT INTO address_geocode VALUES ('10 MAIN ST|90001', 34.1, -118.2);

            CREATE TABLE core_providers (npi VARCHAR);
            INSERT INTO core_providers VALUES ('1234567890');
            CREATE TABLE practice_locations (location_id VARCHAR);
            INSERT INTO practice_locations VALUES ('location-1');
            CREATE TABLE raw_hospital_enrollments (npi VARCHAR);
            INSERT INTO raw_hospital_enrollments VALUES ('1999999999');
            CREATE TABLE hospital_affiliations (
                npi VARCHAR, hospital_npi VARCHAR, hospital_name VARCHAR,
                hospital_state VARCHAR, affiliation_source VARCHAR,
                confidence_level VARCHAR
            );
            INSERT INTO hospital_affiliations VALUES
                ('1234567890', '1999999999', 'Example Hospital', 'CA',
                 'fixture', 'high');
            CREATE TABLE baseline_marker (value VARCHAR);
            INSERT INTO baseline_marker VALUES ('preserved');
            CHECKPOINT;
            '''
        )
    finally:
        connection.close()
    digest = sha256_file(baseline)
    backup_manifest = tmp_path / "serving-backup-manifest.json"
    backup_manifest.write_text(
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
    baseline_release_id = "warehouse-20260801T000000Z-baseline"
    WarehouseReleaseStore(data_root / "warehouse-releases.json").save(
        WarehouseReleaseDocument(
            releases=[
                WarehouseRelease(
                    warehouse_release_id=baseline_release_id,
                    created_at="2026-08-01T00:00:00Z",
                    source_run_ids=("dac-run", "partb-run", "partd-run"),
                    pipeline_code_commit=CODE_COMMIT,
                    baseline_path=str(baseline),
                    baseline_sha256=digest,
                    database_path="releases/baseline/warehouse.duckdb",
                    byte_size=baseline.stat().st_size,
                    sha256=digest,
                    validation_details={
                        "source_periods": {
                            "cms_dac_national": "2026-07",
                            "cms_physician_by_provider": "2024",
                            "cms_part_d_by_provider": "2024",
                        }
                    },
                    validation_state=ValidationState.PASSED,
                )
            ]
        )
    )
    return data_root, backup_manifest, baseline_release_id


def _nppes_serving_practice_baseline(tmp_path: Path) -> tuple[Path, Path, str]:
    data_root, backup_manifest, baseline_release_id = _serving_practice_baseline(
        tmp_path
    )
    backup_document = json.loads(backup_manifest.read_text())
    baseline = Path(backup_document["backup_path"])
    connection = duckdb.connect(str(baseline))
    try:
        connection.execute(
            '''
            ALTER TABLE raw_physician_by_provider
                ADD COLUMN "Rndrng_Prvdr_Type" VARCHAR;
            ALTER TABLE raw_physician_by_provider ADD COLUMN "Tot_Srvcs" DOUBLE;
            ALTER TABLE raw_physician_by_provider ADD COLUMN "Tot_Benes" DOUBLE;
            UPDATE raw_physician_by_provider
            SET "Rndrng_Prvdr_Type" = 'Cardiology',
                "Tot_Srvcs" = 10,
                "Tot_Benes" = 8;

            CREATE TABLE raw_nppes (
                npi VARCHAR, first_name VARCHAR, last_name VARCHAR,
                credentials VARCHAR, practice_address_1 VARCHAR,
                practice_city VARCHAR, practice_state VARCHAR,
                practice_zip VARCHAR, practice_phone VARCHAR,
                deactivation_date VARCHAR, source_run_id VARCHAR,
                source_data_period VARCHAR
            );
            INSERT INTO raw_nppes VALUES
                ('1234567890', 'Jamie', 'Rivera', 'MD', '10 MAIN ST',
                 'Los Angeles', 'CA', '90001', '111', NULL,
                 'nppes-run', '2026-08');
            CHECKPOINT;
            '''
        )
    finally:
        connection.close()

    digest = sha256_file(baseline)
    backup_document["backup_identity"]["byte_size"] = baseline.stat().st_size
    backup_document["sha256"] = digest
    backup_manifest.write_text(json.dumps(backup_document))
    store = WarehouseReleaseStore(data_root / "warehouse-releases.json")
    document = store.load()
    release = document.releases[0]
    release.source_run_ids = (*release.source_run_ids, "nppes-run")
    release.byte_size = baseline.stat().st_size
    release.sha256 = digest
    release.baseline_sha256 = digest
    release.validation_details["source_periods"].update(
        {
            "nppes_monthly_v2": "2026-08",
            "nppes_weekly_incremental_v2": "2026-08-03/2026-08-09",
        }
    )
    release.validation_details["smoke_table_counts"] = {
        "core_providers": 1,
        "hospital_affiliations": 1,
        "practice_locations": 1,
        "raw_hospital_enrollments": 1,
    }
    store.save(document)
    return data_root, backup_manifest, baseline_release_id


def _provider_profile_core_baseline(tmp_path: Path) -> tuple[Path, Path, str]:
    data_root, backup_manifest, baseline_release_id = (
        _nppes_serving_practice_baseline(tmp_path)
    )
    backup_document = json.loads(backup_manifest.read_text())
    baseline = Path(backup_document["backup_path"])
    connection = duckdb.connect(str(baseline))
    try:
        connection.execute(
            '''
            ALTER TABLE raw_nppes ADD COLUMN practice_address_2 VARCHAR;
            ALTER TABLE raw_nppes ADD COLUMN taxonomy_1 VARCHAR;
            UPDATE raw_nppes SET practice_address_2 = 'SUITE 100',
                                 taxonomy_1 = '207RC0000X';

            ALTER TABLE raw_dac_national ADD COLUMN "Cred\t\t\t\t" VARCHAR;
            ALTER TABLE raw_dac_national ADD COLUMN sec_spec_all VARCHAR;
            ALTER TABLE raw_dac_national ADD COLUMN Med_sch VARCHAR;
            ALTER TABLE raw_dac_national ADD COLUMN Grd_yr INTEGER;
            ALTER TABLE raw_dac_national ADD COLUMN "Telehlth\t\t\t\t" VARCHAR;
            ALTER TABLE raw_dac_national ADD COLUMN adr_ln_2 VARCHAR;
            UPDATE raw_dac_national
            SET "Cred\t\t\t\t" = 'MD', sec_spec_all = 'Internal Medicine',
                Med_sch = 'Example University', Grd_yr = 2005,
                "Telehlth\t\t\t\t" = 'Y', adr_ln_2 = 'SUITE 100';

            CREATE TABLE raw_reassignment (
                "Individual NPI" VARCHAR, "Group PAC ID" VARCHAR,
                "Group Legal Business Name" VARCHAR,
                "Group Reassignments and Physician Assistants" BIGINT,
                source_run_id VARCHAR, source_data_period VARCHAR
            );
            INSERT INTO raw_reassignment VALUES
                ('1234567890', 'PAC-1', 'Cardio Group Legal', 25,
                 'reassign-run', '2026-07');
            CREATE TABLE nucc_taxonomy (
                taxonomy_code VARCHAR, classification VARCHAR,
                specialization VARCHAR
            );
            INSERT INTO nucc_taxonomy VALUES
                ('207RC0000X', 'Internal Medicine', 'Cardiovascular Disease');

            ALTER TABLE raw_physician_by_provider
                ADD COLUMN Rndrng_Prvdr_Ent_Cd VARCHAR;
            ALTER TABLE raw_physician_by_provider
                ADD COLUMN Tot_Mdcr_Alowd_Amt DOUBLE;
            ALTER TABLE raw_physician_by_provider
                ADD COLUMN Drug_Mdcr_Pymt_Amt DOUBLE;
            ALTER TABLE raw_physician_by_provider ADD COLUMN Bene_Avg_Age BIGINT;
            ALTER TABLE raw_physician_by_provider
                ADD COLUMN Bene_Age_75_84_Cnt BIGINT;
            ALTER TABLE raw_physician_by_provider
                ADD COLUMN Bene_Age_GT_84_Cnt BIGINT;
            ALTER TABLE raw_physician_by_provider ADD COLUMN Bene_Feml_Cnt BIGINT;
            ALTER TABLE raw_physician_by_provider ADD COLUMN Bene_Dual_Cnt BIGINT;
            ALTER TABLE raw_physician_by_provider
                ADD COLUMN Bene_Avg_Risk_Scre DOUBLE;
            ALTER TABLE raw_physician_by_provider
                ADD COLUMN Bene_CC_PH_Hypertension_V2_Pct BIGINT;
            ALTER TABLE raw_physician_by_provider
                ADD COLUMN Bene_CC_PH_Hyperlipidemia_V2_Pct BIGINT;
            ALTER TABLE raw_physician_by_provider
                ADD COLUMN Bene_CC_PH_Diabetes_V2_Pct BIGINT;
            ALTER TABLE raw_physician_by_provider
                ADD COLUMN Bene_CC_PH_IschemicHeart_V2_Pct BIGINT;
            ALTER TABLE raw_physician_by_provider
                ADD COLUMN Bene_CC_PH_HF_NonIHD_V2_Pct BIGINT;
            ALTER TABLE raw_physician_by_provider
                ADD COLUMN Bene_CC_PH_Afib_V2_Pct BIGINT;
            ALTER TABLE raw_physician_by_provider
                ADD COLUMN Bene_CC_PH_CKD_V2_Pct BIGINT;
            ALTER TABLE raw_physician_by_provider
                ADD COLUMN Bene_CC_PH_COPD_V2_Pct BIGINT;
            ALTER TABLE raw_physician_by_provider
                ADD COLUMN Bene_CC_BH_Depress_V1_Pct BIGINT;
            UPDATE raw_physician_by_provider
            SET Rndrng_Prvdr_Ent_Cd = 'I', Tot_Mdcr_Alowd_Amt = 100,
                Drug_Mdcr_Pymt_Amt = 10, Bene_Avg_Age = 75,
                Bene_Age_75_84_Cnt = 3, Bene_Age_GT_84_Cnt = 2,
                Bene_Feml_Cnt = 5, Bene_Dual_Cnt = 2,
                Bene_Avg_Risk_Scre = 1.5,
                Bene_CC_PH_Hypertension_V2_Pct = 50,
                Bene_CC_PH_Hyperlipidemia_V2_Pct = 40,
                Bene_CC_PH_Diabetes_V2_Pct = 30,
                Bene_CC_PH_IschemicHeart_V2_Pct = 20,
                Bene_CC_PH_HF_NonIHD_V2_Pct = 10,
                Bene_CC_PH_Afib_V2_Pct = 9, Bene_CC_PH_CKD_V2_Pct = 8,
                Bene_CC_PH_COPD_V2_Pct = 7,
                Bene_CC_BH_Depress_V1_Pct = 6;

            CREATE TABLE raw_physician_by_provider_and_service (
                Rndrng_NPI VARCHAR, Rndrng_Prvdr_Type VARCHAR,
                HCPCS_Cd VARCHAR, HCPCS_Desc VARCHAR, HCPCS_Drug_Ind VARCHAR,
                Place_Of_Srvc VARCHAR, Tot_Srvcs DOUBLE, Tot_Benes BIGINT,
                Avg_Mdcr_Pymt_Amt DOUBLE, source_run_id VARCHAR,
                source_data_period VARCHAR
            );
            INSERT INTO raw_physician_by_provider_and_service VALUES
                ('1234567890', 'Cardiology', '99213', 'Office visit', 'N',
                 'O', 10, 8, 12.5, 'partb-service-run', '2024');

            ALTER TABLE raw_part_d_by_provider ADD COLUMN Tot_Clms BIGINT;
            ALTER TABLE raw_part_d_by_provider ADD COLUMN Tot_Benes BIGINT;
            ALTER TABLE raw_part_d_by_provider ADD COLUMN Brnd_Tot_Clms BIGINT;
            ALTER TABLE raw_part_d_by_provider ADD COLUMN Gnrc_Tot_Clms BIGINT;
            ALTER TABLE raw_part_d_by_provider
                ADD COLUMN Brnd_Tot_Drug_Cst DOUBLE;
            ALTER TABLE raw_part_d_by_provider
                ADD COLUMN Opioid_Prscrbr_Rate DOUBLE;
            ALTER TABLE raw_part_d_by_provider ADD COLUMN LIS_Tot_Clms BIGINT;
            ALTER TABLE raw_part_d_by_provider ADD COLUMN Bene_Avg_Age DOUBLE;
            ALTER TABLE raw_part_d_by_provider
                ADD COLUMN Bene_Avg_Risk_Scre DOUBLE;
            UPDATE raw_part_d_by_provider
            SET Tot_Clms = 10, Tot_Benes = 5, Brnd_Tot_Clms = 2,
                Gnrc_Tot_Clms = 8, Brnd_Tot_Drug_Cst = 20,
                Opioid_Prscrbr_Rate = 1, LIS_Tot_Clms = 3,
                Bene_Avg_Age = 74, Bene_Avg_Risk_Scre = 1.4;

            CREATE TABLE raw_part_d_by_provider_and_drug (
                Prscrbr_NPI VARCHAR, Brnd_Name VARCHAR, Gnrc_Name VARCHAR,
                Tot_Clms BIGINT, Tot_Benes BIGINT, Tot_Drug_Cst DOUBLE,
                Tot_Day_Suply BIGINT, source_run_id VARCHAR,
                source_data_period VARCHAR
            );
            INSERT INTO raw_part_d_by_provider_and_drug VALUES
                ('1234567890', 'Example Brand', 'Example Generic', 10, 5,
                 50.75, 300, 'partd-drug-run', '2024');
            CHECKPOINT;
            '''
        )
    finally:
        connection.close()

    digest = sha256_file(baseline)
    backup_document["backup_identity"]["byte_size"] = baseline.stat().st_size
    backup_document["sha256"] = digest
    backup_manifest.write_text(json.dumps(backup_document))
    store = WarehouseReleaseStore(data_root / "warehouse-releases.json")
    document = store.load()
    release = document.releases[0]
    release.source_run_ids = (
        *release.source_run_ids,
        "reassign-run",
        "partb-service-run",
        "partd-drug-run",
    )
    release.byte_size = baseline.stat().st_size
    release.sha256 = digest
    release.baseline_sha256 = digest
    release.validation_details["source_periods"].update(
        {
            "cms_revalidation_group_reassignment": "2026-07",
            "cms_physician_by_provider_and_service": "2024",
            "cms_part_d_by_provider_and_drug": "2024",
        }
    )
    store.save(document)
    return data_root, backup_manifest, baseline_release_id


def _stage_managed_dac(data_root: Path) -> RunManifest:
    source_id = "cms_dac_national"
    run_id = "20260814T030000Z-dac"
    columns = CMS_CSV_PROFILES[source_id].required_columns
    values = {
        "NPI": "1234567890",
        "Ind_PAC_ID": "PAC-I-NEW",
        "Ind_enrl_ID": "ENROLL-I-NEW",
        "Provider Last Name": "Rivera",
        "Provider First Name": "Jamie",
        "pri_spec": "Neurology",
        "Facility Name": "Neuro Group",
        "org_pac_id": "PAC-NEW",
        "num_org_mem": "35",
        "adr_ln_1": "10 MAIN ST",
        "City/Town": "Los Angeles",
        "State": "CA",
        "ZIP Code": "90001",
        "Telephone Number": "222",
        "adrs_id": "ADDR-NEW",
    }
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, lineterminator="\n")
    writer.writerow(columns)
    writer.writerow([values.get(column, "") for column in columns])
    artifact = data_root / "runs" / source_id / run_id / "source.csv"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(stream.getvalue().encode())
    inspection = inspect_cms_csv(artifact, profile=CMS_CSV_PROFILES[source_id])
    manifest = RunManifest(
        run_id=run_id,
        release_id="cms_dac_national-2026-08-13-fixture",
        source_id=source_id,
        publisher=SOURCE_REGISTRY[source_id].publisher.value,
        publisher_version=(
            "cms-provider-data:mj5m-pzi6:2026-08-13:fixture-resource"
        ),
        source_data_period="2026-07-31",
        publisher_release_timestamp="2026-08-13T00:00:00+00:00",
        discovery_timestamp="2026-08-14T02:00:00+00:00",
        retrieval_timestamp="2026-08-14T03:00:00+00:00",
        source_url="https://data.cms.gov/provider-data/resources/fixture/source.csv",
        byte_size=inspection.byte_size,
        sha256=inspection.sha256,
        schema_fingerprint=inspection.schema_fingerprint,
        source_encoding=inspection.source_encoding,
        row_counts={"source_rows": inspection.row_count},
        pipeline_code_commit=CODE_COMMIT,
        validation_state=ValidationState.PASSED,
        validation_timestamp="2026-08-14T03:10:00+00:00",
    )
    store = ManifestStore(data_root / "manifests.json")
    document = store.load()
    document.manifests.append(manifest)
    store.save(document)
    return manifest


def _stage_reassignment(data_root: Path) -> RunManifest:
    source_id = "cms_revalidation_group_reassignment"
    run_id = "reassign-run"
    columns = CMS_CSV_PROFILES[source_id].required_columns
    values = {
        "Group PAC ID": "PAC-1",
        "Group Enrollment ID": "GROUP-1",
        "Group Legal Business Name": "Cardio Group Legal",
        "Group State Code": "CA",
        "Group Reassignments and Physician Assistants": "25",
        "Individual NPI": "1234567890",
        "Individual State Code": "CA",
    }
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, lineterminator="\n")
    writer.writerow(columns)
    writer.writerow([values[column] for column in columns])
    artifact = data_root / "runs" / source_id / run_id / "source.csv"
    artifact.parent.mkdir(parents=True, exist_ok=True)
    artifact.write_bytes(stream.getvalue().encode())
    inspection = inspect_cms_csv(artifact, profile=CMS_CSV_PROFILES[source_id])
    manifest = RunManifest(
        run_id=run_id,
        release_id="cms_revalidation_group_reassignment-2026-07-fixture",
        source_id=source_id,
        publisher=SOURCE_REGISTRY[source_id].publisher.value,
        publisher_version="cms-resource:reassignment-fixture",
        source_data_period="2026-07",
        publisher_release_timestamp="2026-07-20T00:00:00+00:00",
        discovery_timestamp="2026-07-21T22:00:00+00:00",
        retrieval_timestamp="2026-07-21T22:08:59+00:00",
        source_url="https://data.cms.gov/example/source.csv",
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
        validation_timestamp="2026-07-21T22:10:00+00:00",
    )
    store = ManifestStore(data_root / "manifests.json")
    document = store.load()
    document.manifests.append(manifest)
    store.save(document)
    return manifest


def _stage_claim_detail_source(data_root: Path, source_id: str) -> RunManifest:
    run_ids = {
        "cms_physician_by_provider_and_service": "partb-service-run",
        "cms_part_d_by_provider_and_drug": "partd-drug-run",
    }
    run_id = run_ids[source_id]
    columns = CMS_CSV_PROFILES[source_id].required_columns
    values = {column: "1" for column in columns}
    values[CMS_CSV_PROFILES[source_id].identifier_column] = "1234567890"
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, lineterminator="\n")
    writer.writerow(columns)
    writer.writerow([values[column] for column in columns])
    artifact = data_root / "runs" / source_id / run_id / "source.csv"
    artifact.parent.mkdir(parents=True, exist_ok=True)
    artifact.write_bytes(stream.getvalue().encode())
    inspection = inspect_cms_csv(artifact, profile=CMS_CSV_PROFILES[source_id])
    manifest = RunManifest(
        run_id=run_id,
        release_id=f"{source_id}-2024-fixture",
        source_id=source_id,
        publisher=SOURCE_REGISTRY[source_id].publisher.value,
        publisher_version=f"cms-resource:{source_id}-fixture",
        source_data_period="2024",
        publisher_release_timestamp="2026-05-21T00:00:00+00:00",
        discovery_timestamp="2026-07-21T22:00:00+00:00",
        retrieval_timestamp="2026-07-21T22:10:00+00:00",
        source_url="https://data.cms.gov/example/source.csv",
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
        validation_timestamp="2026-07-21T22:20:00+00:00",
    )
    store = ManifestStore(data_root / "manifests.json")
    document = store.load()
    document.manifests.append(manifest)
    store.save(document)
    return manifest


def test_targeted_serving_practice_release_inherits_baseline_provenance(
    tmp_path: Path,
) -> None:
    data_root, backup_manifest, baseline_release_id = _serving_practice_baseline(
        tmp_path
    )

    result = build_serving_practice_warehouse_release(
        data_root=data_root,
        baseline_warehouse_release_id=baseline_release_id,
        backup_manifest_path=backup_manifest,
        data_year=2026,
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
    assert comparison["comparison_policy"] == "serving_practice_additive_v1"
    assert comparison["unexpected_differences"] == []
    assert set(comparison["changed_tables"]) == SERVING_PRACTICE_CHANGED_TABLES
    assert result.release.source_run_ids == ("dac-run", "partb-run", "partd-run")
    assert result.release.table_counts == {"serving_practice_provider_sites": 1}
    details = result.release.validation_details
    assert details["baseline_warehouse_release_id"] == baseline_release_id
    assert details["mart_contract_validation"]["passed"] is True
    candidate = duckdb.connect(str(result.database_path), read_only=True)
    try:
        assert candidate.execute(
            "SELECT value FROM baseline_marker"
        ).fetchone()[0] == "preserved"
        assert candidate.execute(
            "SELECT specialties FROM serving_practice_provider_sites"
        ).fetchone()[0] == ["Cardiology"]
    finally:
        candidate.close()


def test_managed_dac_serving_release_replaces_only_dac_and_builds_mart(
    tmp_path: Path,
) -> None:
    data_root, backup_manifest, baseline_release_id = _serving_practice_baseline(
        tmp_path
    )
    store = WarehouseReleaseStore(data_root / "warehouse-releases.json")
    document = store.load()
    document.releases[0].source_run_ids = ("nppes-run",)
    document.releases[0].validation_details["source_periods"] = {
        "nppes_monthly_v2": "2026-08"
    }
    baseline_smoke_counts = {
        "core_providers": 7,
        "nppes_radar_events": 3,
        "nppes_radar_provider_state": 8,
        "nppes_radar_releases": 2,
        "raw_nppes": 9,
    }
    document.releases[0].validation_details[
        "smoke_table_counts"
    ] = baseline_smoke_counts
    store.save(document)
    dac_manifest = _stage_managed_dac(data_root)

    result = build_managed_dac_serving_practice_warehouse_release(
        data_root=data_root,
        baseline_warehouse_release_id=baseline_release_id,
        dac_source_run_id=dac_manifest.run_id,
        backup_manifest_path=backup_manifest,
        data_year=2026,
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
    assert comparison["comparison_policy"] == "serving_practice_managed_dac_v1"
    assert comparison["unexpected_differences"] == []
    assert set(comparison["changed_tables"]) == (
        SERVING_PRACTICE_MANAGED_DAC_CHANGED_TABLES
    )
    assert result.release.source_run_ids == (
        dac_manifest.run_id,
        "nppes-run",
        "partb-run",
        "partd-run",
    )
    assert result.release.table_counts == {
        "raw_dac_national": 1,
        "serving_practice_provider_sites": 1,
    }
    details = result.release.validation_details
    assert details["source_periods"]["cms_dac_national"] == "2026-07-31"
    assert details["source_periods"]["cms_physician_by_provider"] == "2024"
    assert details["source_periods"]["cms_part_d_by_provider"] == "2024"
    assert details["smoke_table_counts"] == baseline_smoke_counts
    candidate = duckdb.connect(str(result.database_path), read_only=True)
    try:
        raw = candidate.execute(
            '''
            SELECT pri_spec, num_org_mem, source_run_id, source_release_id,
                   source_data_period
            FROM raw_dac_national
            '''
        ).fetchone()
        mart = candidate.execute(
            "SELECT specialties, practice_name, dac_source_run_ids "
            "FROM serving_practice_provider_sites"
        ).fetchone()
        marker = candidate.execute("SELECT value FROM baseline_marker").fetchone()[0]
    finally:
        candidate.close()
    assert raw == (
        "Neurology",
        35,
        dac_manifest.run_id,
        dac_manifest.release_id,
        dac_manifest.source_data_period,
    )
    assert mart == (["Neurology"], "Neuro Group", [dac_manifest.run_id])
    assert marker == "preserved"


def test_nppes_serving_release_adds_exact_two_table_scope(tmp_path: Path) -> None:
    data_root, backup_manifest, baseline_release_id = (
        _nppes_serving_practice_baseline(tmp_path)
    )

    result = build_nppes_serving_practice_warehouse_release(
        data_root=data_root,
        baseline_warehouse_release_id=baseline_release_id,
        backup_manifest_path=backup_manifest,
        data_year=2026,
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
    assert comparison["comparison_policy"] == (
        "serving_practice_nppes_additive_v1"
    )
    assert comparison["unexpected_differences"] == []
    assert set(comparison["changed_tables"]) == (
        NPPES_SERVING_PRACTICE_CHANGED_TABLES
    )
    assert result.release.table_counts == {
        "serving_practice_nppes_org_memberships": 1,
        "serving_practice_nppes_provider_sites": 1,
    }
    details = result.release.validation_details
    assert details["baseline_warehouse_release_id"] == baseline_release_id
    assert details["mart_contract_validation"]["passed"] is True
    assert details["smoke_table_counts"] == {
        "core_providers": 1,
        "hospital_affiliations": 1,
        "practice_locations": 1,
        "raw_hospital_enrollments": 1,
    }
    candidate = duckdb.connect(str(result.database_path), read_only=True)
    try:
        provider = candidate.execute(
            "SELECT npi, specialties, partb_payments, partd_drug_cost "
            "FROM serving_practice_nppes_provider_sites"
        ).fetchone()
        membership = candidate.execute(
            "SELECT npi, org_pac_id, primary_address_match "
            "FROM serving_practice_nppes_org_memberships"
        ).fetchone()
        marker = candidate.execute("SELECT value FROM baseline_marker").fetchone()[0]
    finally:
        candidate.close()
    assert provider == ("1234567890", ["Cardiology"], 125.25, 50.75)
    assert membership == ("1234567890", "PAC-1", True)
    assert marker == "preserved"

    store = WarehouseReleaseStore(data_root / "warehouse-releases.json")
    document = store.load()
    candidate_release = next(
        release
        for release in document.releases
        if release.warehouse_release_id == result.release.warehouse_release_id
    )
    candidate_release.validation_details["changed_tables"].append("baseline_marker")
    store.save(document)
    with pytest.raises(ReleaseError, match="invalid changed-table allowlist"):
        compare_warehouse_release(
            data_root=data_root,
            warehouse_release_id=result.release.warehouse_release_id,
            backup_manifest_path=backup_manifest,
        )


def test_provider_profile_core_release_adds_exact_three_table_scope(
    tmp_path: Path,
) -> None:
    data_root, backup_manifest, baseline_release_id = (
        _provider_profile_core_baseline(tmp_path)
    )

    result = build_provider_profile_core_warehouse_release(
        data_root=data_root,
        baseline_warehouse_release_id=baseline_release_id,
        backup_manifest_path=backup_manifest,
        data_year=2026,
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
    assert comparison["comparison_policy"] == (
        "serving_provider_profile_core_additive_v1"
    )
    assert comparison["unexpected_differences"] == []
    assert set(comparison["changed_tables"]) == PROVIDER_PROFILE_CORE_CHANGED_TABLES
    assert result.release.table_counts == {
        "serving_provider_profile_groups": 1,
        "serving_provider_profile_headers": 1,
        "serving_provider_profile_locations": 1,
    }
    details = result.release.validation_details
    assert details["baseline_warehouse_release_id"] == baseline_release_id
    assert details["mart_contract_validation"]["passed"] is True
    candidate = duckdb.connect(str(result.database_path), read_only=True)
    try:
        header = candidate.execute(
            "SELECT npi, specialty, nppes_source_run_ids, dac_source_run_ids "
            "FROM serving_provider_profile_headers"
        ).fetchone()
        location = candidate.execute(
            "SELECT npi, addr_key, sources "
            "FROM serving_provider_profile_locations"
        ).fetchone()
        group = candidate.execute(
            "SELECT npi, group_id, sources "
            "FROM serving_provider_profile_groups"
        ).fetchone()
        marker = candidate.execute("SELECT value FROM baseline_marker").fetchone()[0]
    finally:
        candidate.close()
    assert header == (
        "1234567890",
        "Cardiology",
        ["nppes-run"],
        ["dac-run"],
    )
    assert location == ("1234567890", "10 MAIN ST|90001", "dac + nppes")
    assert group == ("1234567890", "PAC-1", "dac + reassignment")
    assert marker == "preserved"


def test_complete_provider_profile_release_adds_exact_six_table_scope(
    tmp_path: Path,
) -> None:
    data_root, backup_manifest, baseline_release_id = (
        _provider_profile_core_baseline(tmp_path)
    )
    result = build_provider_profile_warehouse_release(
        data_root=data_root,
        baseline_warehouse_release_id=baseline_release_id,
        backup_manifest_path=backup_manifest,
        data_year=2026,
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
    assert comparison["comparison_policy"] == (
        "serving_provider_profile_complete_additive_v1"
    )
    assert comparison["unexpected_differences"] == []
    assert set(comparison["changed_tables"]) == PROVIDER_PROFILE_CHANGED_TABLES
    assert set(result.release.table_counts) == PROVIDER_PROFILE_CHANGED_TABLES
    assert result.release.table_counts[
        "serving_provider_profile_claims_summary"
    ] == 1
    assert result.release.table_counts["serving_provider_profile_top_services"] == 1
    assert result.release.table_counts["serving_provider_profile_top_drugs"] == 1
    assert result.release.validation_details["mart_contract_validation"][
        "passed"
    ] is True

    store = WarehouseReleaseStore(data_root / "warehouse-releases.json")
    document = store.load()
    candidate_release = next(
        release
        for release in document.releases
        if release.warehouse_release_id == result.release.warehouse_release_id
    )
    candidate_release.validation_details["changed_tables"].append("baseline_marker")
    store.save(document)
    with pytest.raises(ReleaseError, match="invalid changed-table allowlist"):
        compare_warehouse_release(
            data_root=data_root,
            warehouse_release_id=result.release.warehouse_release_id,
            backup_manifest_path=backup_manifest,
        )


def test_complete_provider_profile_release_reconciles_claim_detail_runs(
    tmp_path: Path,
) -> None:
    data_root, backup_manifest, baseline_release_id = (
        _provider_profile_core_baseline(tmp_path)
    )
    service_manifest = _stage_claim_detail_source(
        data_root, "cms_physician_by_provider_and_service"
    )
    drug_manifest = _stage_claim_detail_source(
        data_root, "cms_part_d_by_provider_and_drug"
    )
    store = WarehouseReleaseStore(data_root / "warehouse-releases.json")
    document = store.load()
    baseline_release = document.releases[0]
    for manifest in (service_manifest, drug_manifest):
        del baseline_release.validation_details["source_periods"][manifest.source_id]
    baseline_release.source_run_ids = tuple(
        run_id
        for run_id in baseline_release.source_run_ids
        if run_id not in {service_manifest.run_id, drug_manifest.run_id}
    )
    store.save(document)

    result = build_provider_profile_warehouse_release(
        data_root=data_root,
        baseline_warehouse_release_id=baseline_release_id,
        backup_manifest_path=backup_manifest,
        data_year=2026,
        claims_service_run_id=service_manifest.run_id,
        claims_drug_run_id=drug_manifest.run_id,
        code_commit=CODE_COMMIT,
        memory_limit_gb=1,
        threads=1,
    )

    details = result.release.validation_details
    assert details["source_periods"][service_manifest.source_id] == "2024"
    assert details["source_periods"][drug_manifest.source_id] == "2024"
    assert service_manifest.run_id in result.release.source_run_ids
    assert drug_manifest.run_id in result.release.source_run_ids
    reconciled = {
        item["source_id"]: item for item in details["reconciled_source_runs"]
    }
    assert reconciled[service_manifest.source_id] == {
        "source_id": service_manifest.source_id,
        "run_id": service_manifest.run_id,
        "source_data_period": "2024",
        "raw_table": "raw_physician_by_provider_and_service",
        "raw_row_count": 1,
        "artifact_sha256": service_manifest.sha256,
    }
    assert reconciled[drug_manifest.source_id] == {
        "source_id": drug_manifest.source_id,
        "run_id": drug_manifest.run_id,
        "source_data_period": "2024",
        "raw_table": "raw_part_d_by_provider_and_drug",
        "raw_row_count": 1,
        "artifact_sha256": drug_manifest.sha256,
    }


def test_complete_provider_profile_release_rejects_wrong_claim_detail_source(
    tmp_path: Path,
) -> None:
    data_root, backup_manifest, baseline_release_id = (
        _provider_profile_core_baseline(tmp_path)
    )
    drug_manifest = _stage_claim_detail_source(
        data_root, "cms_part_d_by_provider_and_drug"
    )
    store = WarehouseReleaseStore(data_root / "warehouse-releases.json")
    document = store.load()
    del document.releases[0].validation_details["source_periods"][
        "cms_physician_by_provider_and_service"
    ]
    store.save(document)

    with pytest.raises(
        ReleaseError,
        match="claims service run has the wrong source",
    ):
        build_provider_profile_warehouse_release(
            data_root=data_root,
            baseline_warehouse_release_id=baseline_release_id,
            backup_manifest_path=backup_manifest,
            data_year=2026,
            claims_service_run_id=drug_manifest.run_id,
            code_commit=CODE_COMMIT,
            memory_limit_gb=1,
            threads=1,
        )


def test_provider_profile_core_release_requires_every_declared_source_period(
    tmp_path: Path,
) -> None:
    data_root, backup_manifest, baseline_release_id = (
        _provider_profile_core_baseline(tmp_path)
    )
    store = WarehouseReleaseStore(data_root / "warehouse-releases.json")
    document = store.load()
    del document.releases[0].validation_details["source_periods"][
        "cms_revalidation_group_reassignment"
    ]
    store.save(document)

    with pytest.raises(
        ReleaseError,
        match=(
            "lacks required source periods: "
            "cms_revalidation_group_reassignment"
        ),
    ):
        build_provider_profile_core_warehouse_release(
            data_root=data_root,
            baseline_warehouse_release_id=baseline_release_id,
            backup_manifest_path=backup_manifest,
            data_year=2026,
            code_commit=CODE_COMMIT,
            memory_limit_gb=1,
            threads=1,
        )


def test_provider_profile_core_release_reconciles_verified_reassignment_run(
    tmp_path: Path,
) -> None:
    data_root, backup_manifest, baseline_release_id = (
        _provider_profile_core_baseline(tmp_path)
    )
    manifest = _stage_reassignment(data_root)
    store = WarehouseReleaseStore(data_root / "warehouse-releases.json")
    document = store.load()
    del document.releases[0].validation_details["source_periods"][manifest.source_id]
    store.save(document)

    result = build_provider_profile_core_warehouse_release(
        data_root=data_root,
        baseline_warehouse_release_id=baseline_release_id,
        backup_manifest_path=backup_manifest,
        data_year=2026,
        reassignment_run_id=manifest.run_id,
        code_commit=CODE_COMMIT,
        memory_limit_gb=1,
        threads=1,
    )

    assert manifest.run_id in result.release.source_run_ids
    details = result.release.validation_details
    assert details["source_periods"][manifest.source_id] == "2026-07"
    assert details["reconciled_source_runs"] == [
        {
            "source_id": manifest.source_id,
            "run_id": manifest.run_id,
            "source_data_period": "2026-07",
            "raw_table": "raw_reassignment",
            "raw_row_count": 1,
            "artifact_sha256": manifest.sha256,
        }
    ]


def test_provider_profile_core_release_rejects_raw_reassignment_mismatch(
    tmp_path: Path,
) -> None:
    data_root, backup_manifest, baseline_release_id = (
        _provider_profile_core_baseline(tmp_path)
    )
    manifest = _stage_reassignment(data_root)
    backup_document = json.loads(backup_manifest.read_text())
    baseline = Path(backup_document["backup_path"])
    connection = duckdb.connect(str(baseline))
    try:
        connection.execute(
            "UPDATE raw_reassignment SET source_run_id = 'different-run'"
        )
        connection.execute("CHECKPOINT")
    finally:
        connection.close()
    digest = sha256_file(baseline)
    backup_document["backup_identity"]["byte_size"] = baseline.stat().st_size
    backup_document["sha256"] = digest
    backup_manifest.write_text(json.dumps(backup_document))
    store = WarehouseReleaseStore(data_root / "warehouse-releases.json")
    document = store.load()
    release = document.releases[0]
    release.sha256 = digest
    release.baseline_sha256 = digest
    release.byte_size = baseline.stat().st_size
    del release.validation_details["source_periods"][manifest.source_id]
    store.save(document)

    with pytest.raises(
        ReleaseError,
        match="raw_reassignment provenance does not match",
    ):
        build_provider_profile_core_warehouse_release(
            data_root=data_root,
            baseline_warehouse_release_id=baseline_release_id,
            backup_manifest_path=backup_manifest,
            data_year=2026,
            reassignment_run_id=manifest.run_id,
            code_commit=CODE_COMMIT,
            memory_limit_gb=1,
            threads=1,
        )


def test_provider_profile_core_release_cli_builds_staging_candidate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    data_root, backup_manifest, baseline_release_id = (
        _provider_profile_core_baseline(tmp_path)
    )
    monkeypatch.setattr("pipeline.releases.pipeline_commit", lambda: None)

    exit_code = main(
        [
            "build-provider-profile-core-release",
            "--environment",
            "staging",
            "--baseline-warehouse-release-id",
            baseline_release_id,
            "--backup-manifest",
            str(backup_manifest),
            "--data-root",
            str(data_root),
            "--data-year",
            "2026",
            "--code-commit",
            CODE_COMMIT,
            "--memory-limit-gb",
            "1",
            "--threads",
            "1",
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == EXIT_HEALTHY
    assert payload["release"]["validation_details"]["comparison_policy"] == (
        "serving_provider_profile_core_additive_v1"
    )


def test_nppes_serving_release_requires_every_declared_source_period(
    tmp_path: Path,
) -> None:
    data_root, backup_manifest, baseline_release_id = (
        _nppes_serving_practice_baseline(tmp_path)
    )
    store = WarehouseReleaseStore(data_root / "warehouse-releases.json")
    document = store.load()
    del document.releases[0].validation_details["source_periods"][
        "nppes_weekly_incremental_v2"
    ]
    store.save(document)

    with pytest.raises(
        ReleaseError,
        match="lacks required source periods: nppes_weekly_incremental_v2",
    ):
        build_nppes_serving_practice_warehouse_release(
            data_root=data_root,
            baseline_warehouse_release_id=baseline_release_id,
            backup_manifest_path=backup_manifest,
            data_year=2026,
            code_commit=CODE_COMMIT,
            memory_limit_gb=1,
            threads=1,
        )


def test_nppes_serving_release_cli_builds_staging_candidate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    data_root, backup_manifest, baseline_release_id = (
        _nppes_serving_practice_baseline(tmp_path)
    )
    monkeypatch.setattr("pipeline.releases.pipeline_commit", lambda: None)

    exit_code = main(
        [
            "build-nppes-serving-practice-release",
            "--environment",
            "staging",
            "--baseline-warehouse-release-id",
            baseline_release_id,
            "--backup-manifest",
            str(backup_manifest),
            "--data-root",
            str(data_root),
            "--data-year",
            "2026",
            "--code-commit",
            CODE_COMMIT,
            "--memory-limit-gb",
            "1",
            "--threads",
            "1",
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == EXIT_HEALTHY
    assert payload["release"]["validation_details"]["comparison_policy"] == (
        "serving_practice_nppes_additive_v1"
    )


def test_nppes_serving_release_rejects_non_full_explicit_commit(
    tmp_path: Path,
) -> None:
    data_root, backup_manifest, baseline_release_id = (
        _nppes_serving_practice_baseline(tmp_path)
    )

    with pytest.raises(ReleaseError, match="full 40-character pipeline Git commit"):
        build_nppes_serving_practice_warehouse_release(
            data_root=data_root,
            baseline_warehouse_release_id=baseline_release_id,
            backup_manifest_path=backup_manifest,
            data_year=2026,
            code_commit="not-a-full-commit",
            memory_limit_gb=1,
            threads=1,
        )


def test_managed_dac_serving_dependency_provenance_fails_closed() -> None:
    connection = duckdb.connect(":memory:")
    try:
        connection.execute(
            "CREATE TABLE raw_part_d_by_provider (source_data_period VARCHAR)"
        )
        connection.execute("INSERT INTO raw_part_d_by_provider VALUES ('2024')")
        with pytest.raises(ReleaseError, match="lacks managed source provenance"):
            _single_table_source_provenance(
                connection, "raw_part_d_by_provider"
            )
    finally:
        connection.close()


def test_logical_fingerprint_detects_equal_count_content_drift() -> None:
    left = duckdb.connect(":memory:")
    right = duckdb.connect(":memory:")
    try:
        left.execute("CREATE TABLE evidence (npi VARCHAR, value INTEGER)")
        right.execute("CREATE TABLE evidence (npi VARCHAR, value INTEGER)")
        left.execute("INSERT INTO evidence VALUES ('1234567890', 1)")
        right.execute("INSERT INTO evidence VALUES ('1234567890', 2)")

        left_fingerprint = _table_logical_fingerprint(left, "evidence")
        right_fingerprint = _table_logical_fingerprint(right, "evidence")
    finally:
        right.close()
        left.close()

    assert left_fingerprint["row_count"] == right_fingerprint["row_count"] == 1
    assert left_fingerprint != right_fingerprint


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
    assert result.release.table_counts["provider_hospital_evidence"] == 1
    assert result.release.table_counts["ambiguous_hospital_name_state_keys"] == 0
    assert result.release.table_counts["database_tables"] == 8
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
