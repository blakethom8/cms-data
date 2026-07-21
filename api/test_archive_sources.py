import csv
import io
import json
import sys
import zipfile
from pathlib import Path

import duckdb
import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT))

from pipeline.aact_releases import prepare_aact_release
from pipeline.archive_acquisition import ARCHIVE_PROFILES, inspect_archive
from pipeline.archive_sources import (
    load_nppes_sources,
    load_open_payments_sources,
    verified_archive_runs,
)
from pipeline.manifests import (
    ManifestDocument,
    ManifestStore,
    RunManifest,
    ValidationState,
)
from pipeline.releases import ReleaseError
from pipeline.source_registry import SOURCE_REGISTRY


CODE_COMMIT = "a" * 40


def _zip(path: Path, members: dict[str, bytes]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, payload in members.items():
            archive.writestr(name, payload)


def _stage_archive(
    data_root: Path,
    source_id: str,
    run_id: str,
    period: str,
    members: dict[str, bytes],
) -> RunManifest:
    artifact = data_root / "runs" / source_id / run_id / "source.zip"
    _zip(artifact, members)
    inspection = inspect_archive(artifact, ARCHIVE_PROFILES[source_id])
    manifest = RunManifest(
        run_id=run_id,
        release_id=f"{source_id}-release",
        source_id=source_id,
        publisher=SOURCE_REGISTRY[source_id].publisher.value,
        publisher_version=f"{source_id}-version",
        source_data_period=period,
        discovery_timestamp="2026-07-21T00:00:00+00:00",
        retrieval_timestamp="2026-07-21T00:01:00+00:00",
        source_url="https://example.test/source.zip",
        byte_size=inspection.byte_size,
        sha256=inspection.sha256,
        schema_fingerprint=inspection.schema_fingerprint,
        source_encoding="binary:zip",
        row_counts={
            "archive_members": inspection.member_count,
            "uncompressed_bytes": inspection.uncompressed_bytes,
        },
        pipeline_code_commit=CODE_COMMIT,
        validation_state=ValidationState.PASSED,
        validation_timestamp="2026-07-21T00:02:00+00:00",
    )
    store = ManifestStore(data_root / "manifests.json")
    document = store.load()
    document.manifests.append(manifest)
    store.save(document)
    return manifest


def _nppes_csv(rows: list[dict[str, str]]) -> bytes:
    columns = [
        "NPI",
        "Entity Type Code",
        "Provider First Name",
        "Provider Last Name (Legal Name)",
        "Provider Middle Name",
        "Provider Name Prefix Text",
        "Provider Name Suffix Text",
        "Provider Credential Text",
        "Provider Gender Code",
        "Provider Enumeration Date",
        "Last Update Date",
        "NPI Deactivation Date",
        "NPI Reactivation Date",
        "Is Sole Proprietor",
        "Provider First Line Business Practice Location Address",
        "Provider Second Line Business Practice Location Address",
        "Provider Business Practice Location Address City Name",
        "Provider Business Practice Location Address State Name",
        "Provider Business Practice Location Address Postal Code",
        "Provider Business Practice Location Address Country Code (If outside U.S.)",
        "Provider Business Practice Location Address Telephone Number",
    ]
    for position in range(1, 16):
        columns.extend(
            [
                f"Healthcare Provider Taxonomy Code_{position}",
                f"Healthcare Provider Primary Taxonomy Switch_{position}",
            ]
        )
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=columns, lineterminator="\n")
    writer.writeheader()
    for row in rows:
        value = {column: "" for column in columns}
        value.update(
            {
                "Entity Type Code": "1",
                "Provider Credential Text": "MD",
                "Provider Gender Code": "F",
                "Provider Enumeration Date": "07/01/2026",
                "Last Update Date": "07/13/2026",
                "Provider First Line Business Practice Location Address": "1 Main St",
                "Provider Business Practice Location Address City Name": "Los Angeles",
                "Provider Business Practice Location Address State Name": "CA",
                "Provider Business Practice Location Address Postal Code": "90001-1234",
                "Healthcare Provider Taxonomy Code_1": "207Q00000X",
                "Healthcare Provider Primary Taxonomy Switch_1": "Y",
            }
        )
        value.update(row)
        writer.writerow(value)
    return stream.getvalue().encode()


def test_nppes_monthly_baseline_and_weekly_overlay_are_synchronized(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    monthly = _stage_archive(
        data_root,
        "nppes_monthly_v2",
        "monthly-run",
        "2026-07-13",
        {
            "npidata_pfile_20050523-20260712.csv": _nppes_csv(
                [
                    {
                        "NPI": "1111111111",
                        "Provider First Name": "Ada",
                        "Provider Last Name (Legal Name)": "Lovelace",
                    }
                ]
            )
        },
    )
    weekly = _stage_archive(
        data_root,
        "nppes_weekly_incremental_v2",
        "weekly-run",
        "2026-07-14/2026-07-20",
        {
            "npidata_pfile_20260714-20260720.csv": _nppes_csv(
                [
                    {
                        "NPI": "1111111111",
                        "Provider First Name": "Ada",
                        "Provider Last Name (Legal Name)": "Lovelace",
                        "Provider Business Practice Location Address Postal Code": "90002",
                        "Last Update Date": "07/16/2026",
                    },
                    {
                        "NPI": "2222222222",
                        "Provider First Name": "Grace",
                        "Provider Last Name (Legal Name)": "Hopper",
                        "Provider Enumeration Date": "07/15/2026",
                        "Last Update Date": "07/15/2026",
                    },
                ]
            )
        },
    )
    connection = duckdb.connect(":memory:")
    counts, details = load_nppes_sources(
        connection,
        data_root=data_root,
        monthly_run_id=monthly.run_id,
        weekly_run_id=weekly.run_id,
    )

    assert counts["raw_nppes"] == 2
    assert counts["nppes_radar_provider_state"] == 2
    assert counts["nppes_radar_events"] == 2
    assert counts["nppes_radar_releases"] == 2
    assert connection.execute(
        "SELECT practice_zip FROM raw_nppes WHERE npi = '1111111111'"
    ).fetchone() == ("90002",)
    assert connection.execute(
        "SELECT count(*) FROM core_providers WHERE medicare_participating = 'N'"
    ).fetchone() == (2,)
    assert details["monthly"]["is_baseline"] is True
    assert details["weekly"]["event_row_count"] == 2
    assert not list((data_root / "staging" / "extracts").glob("*.partial"))


def _payments_csv(columns: list[str], row: list[str]) -> bytes:
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, lineterminator="\n")
    writer.writerow(columns)
    writer.writerow(row)
    return stream.getvalue().encode()


def _stage_open_payments(data_root: Path) -> tuple[str, str, str]:
    general_columns = [
        "Covered_Recipient_NPI",
        "Covered_Recipient_Profile_ID",
        "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name",
        "Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name",
        "Total_Amount_of_Payment_USDollars",
        "Nature_of_Payment_or_Transfer_of_Value",
        "Program_Year",
        "Record_ID",
    ]
    research_columns = [
        "Covered_Recipient_NPI",
        "Total_Amount_of_Payment_USDollars",
        "Program_Year",
        "Record_ID",
        "Name_of_Study",
        "Principal_Investigator_1_NPI",
    ]
    ownership_columns = [
        "Physician_NPI",
        "Total_Amount_Invested_USDollars",
        "Value_of_Interest",
        "Program_Year",
        "Record_ID",
    ]
    general = _stage_archive(
        data_root,
        "open_payments_general",
        "general-run",
        "2025-01-01/2025-12-31",
        {
            "OP_DTL_GNRL_PGYR2025.csv": _payments_csv(
                general_columns,
                [
                    "1111111111",
                    "99",
                    "Acme Medical",
                    "Acme Medical",
                    "12500.25",
                    "Consulting Fee",
                    "2025",
                    "G1",
                ],
            )
        },
    )
    research = _stage_archive(
        data_root,
        "open_payments_research",
        "research-run",
        "2025-01-01/2025-12-31",
        {
            "OP_DTL_RSRCH_PGYR2025.csv": _payments_csv(
                research_columns,
                ["1111111111", "500.00", "2025", "R1", "Study", "1111111111"],
            )
        },
    )
    ownership = _stage_archive(
        data_root,
        "open_payments_ownership",
        "ownership-run",
        "2025-01-01/2025-12-31",
        {
            "OP_DTL_OWNRSHP_PGYR2025.csv": _payments_csv(
                ownership_columns,
                ["1111111111", "1000", "2000", "2025", "O1"],
            )
        },
    )
    return general.run_id, research.run_id, ownership.run_id


def test_open_payments_preserves_publisher_shape_and_builds_aggregates(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    runs = _stage_open_payments(data_root)
    connection = duckdb.connect(":memory:")
    connection.execute((REPOSITORY_ROOT / "schema" / "ddl.sql").read_text())
    connection.execute(
        """
        INSERT INTO core_providers (
            npi, last_org_name, first_name, entity_type_code,
            medicare_participating, data_year
        ) VALUES ('1111111111', 'Lovelace', 'Ada', 'I', 'Y', 2025)
        """
    )

    counts, details = load_open_payments_sources(
        connection, data_root=data_root, run_ids=runs
    )

    assert counts == {
        "industry_relationships": 1,
        "kol_summary": 1,
        "raw_open_payments_general": 1,
        "raw_open_payments_ownership": 1,
        "raw_open_payments_research": 1,
    }
    assert details == {"program_year": 2025, "general_rows_without_core_provider": 0}
    assert connection.execute(
        """
        SELECT Total_Amount_of_Payment_USDollars, Program_Year, source_run_id
        FROM raw_open_payments_general
        """
    ).fetchone() == (pytest.approx(12500.25), 2025, "general-run")
    assert connection.execute(
        "SELECT total_amount_received, top_paying_company_flag FROM industry_relationships"
    ).fetchone() == (pytest.approx(12500.25), True)


def test_open_payments_schema_change_rolls_back_all_three_tables(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    runs = list(_stage_open_payments(data_root))
    research_artifact = data_root / "runs" / "open_payments_research" / runs[1] / "source.zip"
    _zip(
        research_artifact,
        {"OP_DTL_RSRCH_PGYR2025.csv": b"Program_Year,Record_ID\n2025,R1\n"},
    )
    # Re-sign the deliberately changed fixture so the loader reaches its schema gate.
    inspection = inspect_archive(
        research_artifact, ARCHIVE_PROFILES["open_payments_research"]
    )
    store = ManifestStore(data_root / "manifests.json")
    document = store.load()
    manifest = next(row for row in document.manifests if row.run_id == runs[1])
    manifest.byte_size = inspection.byte_size
    manifest.sha256 = inspection.sha256
    manifest.schema_fingerprint = inspection.schema_fingerprint
    manifest.row_counts = {
        "archive_members": inspection.member_count,
        "uncompressed_bytes": inspection.uncompressed_bytes,
    }
    store.save(document)

    connection = duckdb.connect(":memory:")
    connection.execute((REPOSITORY_ROOT / "schema" / "ddl.sql").read_text())
    connection.execute("CREATE TABLE raw_open_payments_general (sentinel VARCHAR)")
    connection.execute("INSERT INTO raw_open_payments_general VALUES ('preserved')")

    with pytest.raises(ReleaseError, match="missing required columns"):
        load_open_payments_sources(connection, data_root=data_root, run_ids=tuple(runs))

    assert connection.execute(
        "SELECT sentinel FROM raw_open_payments_general"
    ).fetchone() == ("preserved",)


def test_verified_archive_rejects_bytes_changed_after_acquisition(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    manifest = _stage_archive(
        data_root,
        "nppes_weekly_incremental_v2",
        "weekly-run",
        "2026-07-14/2026-07-20",
        {"npidata_pfile_20260714-20260720.csv": b"NPI\n1111111111\n"},
    )
    artifact = data_root / "runs" / manifest.source_id / manifest.run_id / "source.zip"
    artifact.write_bytes(artifact.read_bytes() + b"changed")

    with pytest.raises(ReleaseError, match="no longer matches"):
        verified_archive_runs(
            data_root,
            (manifest.run_id,),
            allowed_sources=frozenset({manifest.source_id}),
        )


def test_prepare_aact_release_seals_dump_and_records_hashes(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    manifest = _stage_archive(
        data_root,
        "aact_clinical_trials_snapshot",
        "aact-run",
        "2026-07-21",
        {
            "postgres.dmp": b"PGDMPfixture",
            "data_dictionary.csv": b"table,column\nstudies,nct_id\n",
        },
    )

    result = prepare_aact_release(
        data_root=data_root,
        source_run_id=manifest.run_id,
        output_root=tmp_path / "artifacts",
    )

    assert result.release.validation_state == "passed"
    assert result.release.dump_byte_size == len(b"PGDMPfixture")
    assert result.release_directory.is_dir()
    assert not result.release_directory.stat().st_mode & 0o222
    assert not (result.release_directory / "postgres.dmp").stat().st_mode & 0o222
    payload = json.loads(result.release_manifest_path.read_text())
    assert payload["schema_version"] == 1
    assert payload["release"]["source_run_id"] == "aact-run"
