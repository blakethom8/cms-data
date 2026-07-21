import sys
from pathlib import Path

import duckdb
import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT))

from pipeline.acquisition import CMS_CSV_PROFILES, inspect_cms_csv
from pipeline import candidate_sources
from pipeline.candidate_sources import load_cms_raw_tables, verified_cms_runs
from pipeline.manifests import (
    ManifestStore,
    RunManifest,
    ValidationState,
)
from pipeline.releases import ReleaseError
from pipeline.source_registry import SOURCE_REGISTRY


def _stage(
    data_root: Path,
    *,
    source_id: str = "cms_part_d_by_provider",
    run_id: str = "20990720T010000Z-partd",
    payload: bytes | None = None,
) -> RunManifest:
    content = payload or (
        b"Prscrbr_NPI,Tot_Clms,Tot_Drug_Cst,Opioid_Prscrbr_Rate\n"
        b"1234567890,10,25.50,2.5\n"
    )
    artifact = data_root / "runs" / source_id / run_id / "source.csv"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(content)
    inspection = inspect_cms_csv(artifact, profile=CMS_CSV_PROFILES[source_id])
    manifest = RunManifest(
        run_id=run_id,
        release_id=f"{source_id}-fixture",
        source_id=source_id,
        publisher=SOURCE_REGISTRY[source_id].publisher.value,
        publisher_version="cms-resource:10000000-0000-4000-8000-000000000001",
        source_data_period="2097-01-01/2097-12-31",
        publisher_release_timestamp="2099-05-21T00:00:00+00:00",
        discovery_timestamp="2099-07-20T00:00:00+00:00",
        retrieval_timestamp="2099-07-20T01:00:00+00:00",
        source_url="https://data.cms.gov/example/source.csv",
        byte_size=inspection.byte_size,
        sha256=inspection.sha256,
        schema_fingerprint=inspection.schema_fingerprint,
        source_encoding=inspection.source_encoding,
        row_counts={"source_rows": inspection.row_count},
        pipeline_code_commit="a" * 40,
        validation_state=ValidationState.PASSED,
        validation_timestamp="2099-07-20T02:00:00+00:00",
    )
    path = data_root / "manifests.json"
    document = ManifestStore(path).load()
    document.manifests.append(manifest)
    ManifestStore(path).save(document)
    return manifest


def test_load_cms_raw_table_is_strict_and_records_run_provenance(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    manifest = _stage(data_root)
    connection = duckdb.connect(":memory:")
    try:
        connection.execute("create table raw_part_d_by_provider(old varchar)")
        counts = load_cms_raw_tables(
            connection,
            data_root=data_root,
            run_ids=[manifest.run_id],
        )
        row = connection.execute(
            """
            select Prscrbr_NPI, Tot_Clms, source_run_id, source_data_period
            from raw_part_d_by_provider
            """
        ).fetchone()
        types = dict(
            connection.execute(
                "select column_name, data_type from information_schema.columns "
                "where table_name = 'raw_part_d_by_provider'"
            ).fetchall()
        )
    finally:
        connection.close()

    assert counts == {"raw_part_d_by_provider": 1}
    assert row == (
        "1234567890",
        "10",
        manifest.run_id,
        "2097-01-01/2097-12-31",
    )
    assert types["Tot_Clms"] == "VARCHAR"


def test_verification_rejects_artifact_changed_after_acquisition(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    manifest = _stage(data_root)
    artifact = data_root / "runs" / manifest.source_id / manifest.run_id / "source.csv"
    artifact.write_bytes(artifact.read_bytes().replace(b"25.50", b"99.99"))

    with pytest.raises(ReleaseError, match="no longer matches acquisition manifest"):
        verified_cms_runs(data_root, [manifest.run_id])


def test_verification_rejects_two_runs_for_one_source(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    first = _stage(data_root)
    second = _stage(data_root, run_id="20990720T020000Z-partd")

    with pytest.raises(ReleaseError, match="more than one run for source"):
        verified_cms_runs(data_root, [first.run_id, second.run_id])


def test_failed_multi_table_load_rolls_back_every_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = tmp_path / "data"
    first = _stage(data_root)
    second = _stage(
        data_root,
        source_id="cms_order_and_referring",
        run_id="20990720T030000Z-order",
        payload=(
            b"NPI,LAST_NAME,FIRST_NAME,PARTB,DME,HHA,PMD,HOSPICE\n"
            b"1234567890,Last,First,Y,Y,Y,Y,Y\n"
        ),
    )
    real_verify = candidate_sources.verified_cms_runs

    def verify_then_corrupt(root: Path, run_ids: list[str]):
        verified = real_verify(root, run_ids)
        second_artifact = root / "runs" / second.source_id / second.run_id / "source.csv"
        second_artifact.write_bytes(
            b"NPI,LAST_NAME,FIRST_NAME,PARTB,DME,HHA,PMD,HOSPICE\n"
            b"1234567890,Last,First,Y,Y,Y,Y,\xff\n"
        )
        return verified

    monkeypatch.setattr(candidate_sources, "verified_cms_runs", verify_then_corrupt)
    connection = duckdb.connect(":memory:")
    try:
        connection.execute("create table raw_part_d_by_provider(marker varchar)")
        connection.execute("insert into raw_part_d_by_provider values ('preserved')")
        with pytest.raises(duckdb.InvalidInputException):
            load_cms_raw_tables(
                connection,
                data_root=data_root,
                run_ids=[first.run_id, second.run_id],
            )
        rows = connection.execute("select * from raw_part_d_by_provider").fetchall()
    finally:
        connection.close()

    assert rows == [("preserved",)]
