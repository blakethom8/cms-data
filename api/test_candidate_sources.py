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
        b"Prscrbr_NPI,Tot_Clms,Tot_Drug_Cst,Brnd_Tot_Clms,Gnrc_Tot_Clms,"
        b"Opioid_Prscrbr_Rate\n"
        b"1234567890,10,25.50,4,6,2.5\n"
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


def test_load_cms_raw_table_preserves_established_numeric_types(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    manifest = _stage(data_root)
    connection = duckdb.connect(":memory:")
    try:
        connection.execute(
            """
            create table raw_part_d_by_provider(
                Prscrbr_NPI BIGINT,
                Tot_Clms BIGINT,
                Tot_Drug_Cst DOUBLE,
                Brnd_Tot_Clms BIGINT,
                Gnrc_Tot_Clms BIGINT,
                Opioid_Prscrbr_Rate DOUBLE
            )
            """
        )
        load_cms_raw_tables(
            connection,
            data_root=data_root,
            run_ids=[manifest.run_id],
        )
        row = connection.execute(
            "select Prscrbr_NPI, Tot_Clms, Tot_Drug_Cst from raw_part_d_by_provider"
        ).fetchone()
        types = dict(
            connection.execute(
                "select column_name, data_type from information_schema.columns "
                "where table_name = 'raw_part_d_by_provider'"
            ).fetchall()
        )
    finally:
        connection.close()

    assert row == (1234567890, 10, 25.5)
    assert types["Prscrbr_NPI"] == "BIGINT"
    assert types["Tot_Clms"] == "BIGINT"
    assert types["Tot_Drug_Cst"] == "DOUBLE"


def test_load_cms_raw_table_rejects_numeric_contract_change(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    manifest = _stage(
        data_root,
        payload=(
            b"Prscrbr_NPI,Tot_Clms,Tot_Drug_Cst,Brnd_Tot_Clms,Gnrc_Tot_Clms,"
            b"Opioid_Prscrbr_Rate\n"
            b"1234567890,not-a-number,25.50,4,6,2.5\n"
        ),
    )
    connection = duckdb.connect(":memory:")
    try:
        connection.execute(
            "create table raw_part_d_by_provider(Prscrbr_NPI BIGINT, Tot_Clms BIGINT)"
        )
        with pytest.raises(duckdb.ConversionException):
            load_cms_raw_tables(
                connection,
                data_root=data_root,
                run_ids=[manifest.run_id],
            )
    finally:
        connection.close()


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


def test_cp1252_source_is_transcoded_for_strict_candidate_load(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    manifest = _stage(
        data_root,
        source_id="cms_pecos_public_provider_enrollment",
        run_id="20990720T040000Z-pecos",
        payload=(
            b"NPI,MULTIPLE_NPI_FLAG,PECOS_ASCT_CNTL_ID,ENRLMT_ID,"
            b"PROVIDER_TYPE_CD,PROVIDER_TYPE_DESC,STATE_CD,FIRST_NAME,"
            b"MDL_NAME,LAST_NAME,ORG_NAME\n"
            b"1234567890,N,A1,E1,P1,Physician,CA,Jos\xe9,,Example,\n"
        ),
    )
    assert manifest.source_encoding == "cp1252"
    connection = duckdb.connect(":memory:")
    try:
        counts = load_cms_raw_tables(
            connection,
            data_root=data_root,
            run_ids=[manifest.run_id],
        )
        name = connection.execute(
            "select FIRST_NAME from raw_pecos_enrollment"
        ).fetchone()[0]
    finally:
        connection.close()

    assert counts == {"raw_pecos_enrollment": 1}
    assert name == "José"
    assert list((data_root / "staging" / "transcodes").glob("*.partial")) == []


def test_ppef_relationship_subfiles_load_at_raw_grain_and_join_to_enrollment(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    enrollment = _stage(
        data_root,
        source_id="cms_pecos_public_provider_enrollment",
        run_id="20990720T041000Z-pecos-enrollment",
        payload=(
            b"NPI,MULTIPLE_NPI_FLAG,PECOS_ASCT_CNTL_ID,ENRLMT_ID,"
            b"PROVIDER_TYPE_CD,PROVIDER_TYPE_DESC,STATE_CD,FIRST_NAME,"
            b"MDL_NAME,LAST_NAME,ORG_NAME\n"
            b"1234567890,N,PAC-I,I20031103000001,14-00,Physician,CA,Jane,,Doe,\n"
            b"1098765432,N,PAC-O,O20031216000213,12-00,Group Practice,CA,,,,Example Group\n"
        ),
    )
    reassignment = _stage(
        data_root,
        source_id="cms_pecos_reassignment",
        run_id="20990720T042000Z-pecos-reassignment",
        payload=(
            b"REASGN_BNFT_ENRLMT_ID,RCV_BNFT_ENRLMT_ID\n"
            b"I20031103000001,O20031216000213\n"
        ),
    )
    practice_location = _stage(
        data_root,
        source_id="cms_pecos_practice_location",
        run_id="20990720T043000Z-pecos-location",
        payload=(
            b"ENRLMT_ID,CITY_NAME,STATE_CD,ZIP_CD\n"
            b"O20031216000213,LOS ANGELES,CA,090048001\n"
        ),
    )
    connection = duckdb.connect(":memory:")
    try:
        counts = load_cms_raw_tables(
            connection,
            data_root=data_root,
            run_ids=[
                enrollment.run_id,
                reassignment.run_id,
                practice_location.run_id,
            ],
        )
        row = connection.execute(
            """
            SELECT individual.NPI, receiving.ORG_NAME, location.CITY_NAME,
                   location.STATE_CD, location.ZIP_CD,
                   relationship.source_data_period
            FROM raw_pecos_reassignment relationship
            JOIN raw_pecos_enrollment individual
              ON individual.ENRLMT_ID = relationship.REASGN_BNFT_ENRLMT_ID
            JOIN raw_pecos_enrollment receiving
              ON receiving.ENRLMT_ID = relationship.RCV_BNFT_ENRLMT_ID
            JOIN raw_pecos_practice_location location
              ON location.ENRLMT_ID = relationship.RCV_BNFT_ENRLMT_ID
            """
        ).fetchone()
    finally:
        connection.close()

    assert counts == {
        "raw_pecos_enrollment": 2,
        "raw_pecos_practice_location": 1,
        "raw_pecos_reassignment": 1,
    }
    assert row == (
        "1234567890",
        "Example Group",
        "LOS ANGELES",
        "CA",
        "090048001",
        "2097-01-01/2097-12-31",
    )


def test_cms_loader_handles_quoted_field_after_dialect_sample(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    header = b"NPI,LAST_NAME,FIRST_NAME,PARTB,DME,HHA,PMD,HOSPICE\n"
    ordinary = b"1234567890,Last,First,Y,Y,Y,Y,Y\n" * 3000
    late_quoted = b'2222222222,"Last, Jr.",Second,Y,Y,Y,Y,Y\n'
    manifest = _stage(
        data_root,
        source_id="cms_order_and_referring",
        run_id="20990720T050000Z-order-quotes",
        payload=header + ordinary + late_quoted,
    )
    connection = duckdb.connect(":memory:")
    try:
        counts = load_cms_raw_tables(
            connection,
            data_root=data_root,
            run_ids=[manifest.run_id],
        )
        last_name = connection.execute(
            "SELECT LAST_NAME FROM raw_order_and_referring WHERE NPI = '2222222222'"
        ).fetchone()[0]
    finally:
        connection.close()

    assert counts == {"raw_order_and_referring": 3001}
    assert last_name == "Last, Jr."


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
