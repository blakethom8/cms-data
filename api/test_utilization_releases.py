import hashlib
import json
import sys
from pathlib import Path

import duckdb
import pytest


REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT))

from pipeline import utilization_releases


COMMIT = "a" * 40
SOURCE_RELEASE_ID = "warehouse-20260814T183948Z-e5ff46dce9"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()


def _source(tmp_path: Path) -> tuple[Path, Path]:
    database = tmp_path / "source.duckdb"
    connection = duckdb.connect(str(database))
    connection.execute(
        """
        CREATE TABLE serving_practice_nppes_provider_sites (
            npi VARCHAR, first_name VARCHAR, last_name VARCHAR, credentials VARCHAR,
            specialties VARCHAR[], address VARCHAR, city VARCHAR, state VARCHAR,
            zip5 VARCHAR, latitude DOUBLE, longitude DOUBLE, partb_services DOUBLE,
            partb_payments DOUBLE, partd_drug_cost DOUBLE, data_year INTEGER
        );
        INSERT INTO serving_practice_nppes_provider_sites VALUES
            ('1000000001', 'Ada', 'One', 'MD', ['Cardiology'], '1 Main St',
             'Columbus', 'OH', '43215', 39.96, -82.99, 100, 2000, 5000, 2024);

        CREATE TABLE utilization_metrics (
            npi VARCHAR, metric_year INTEGER, rx_total_claims INTEGER
        );
        INSERT INTO utilization_metrics VALUES
            ('1000000001', 2024, 40), ('2000000002', 2024, 99);

        CREATE TABLE provider_service_detail (
            npi VARCHAR, hcpcs_code VARCHAR, hcpcs_description VARCHAR,
            hcpcs_drug_ind VARCHAR, place_of_service VARCHAR,
            tot_beneficiaries INTEGER, tot_services DECIMAL(15,2),
            tot_bene_day_srvcs DECIMAL(15,2), avg_submitted_chrg DECIMAL(15,2),
            avg_medicare_allowed DECIMAL(15,2), avg_medicare_payment DECIMAL(15,2),
            avg_medicare_standardized DECIMAL(15,2), data_year INTEGER
        );
        INSERT INTO provider_service_detail VALUES
            ('1000000001', '99213', 'Office visit', 'N', 'O', 8, 10, 9, 100, 80, 60, 55, 2024),
            ('2000000002', '99213', 'Office visit', 'N', 'O', 5, 7, 6, 100, 80, 60, 55, 2024);

        CREATE TABLE raw_part_d_by_provider_and_drug (
            Prscrbr_NPI BIGINT, Brnd_Name VARCHAR, Gnrc_Name VARCHAR,
            Tot_Clms BIGINT, Tot_30day_Fills DOUBLE, Tot_Day_Suply BIGINT,
            Tot_Drug_Cst DOUBLE, Tot_Benes BIGINT, GE65_Tot_Clms BIGINT,
            GE65_Tot_Drug_Cst DOUBLE, GE65_Tot_Benes BIGINT,
            source_run_id VARCHAR, source_data_period VARCHAR
        );
        INSERT INTO raw_part_d_by_provider_and_drug VALUES
            (1000000001, 'Brand A', 'Generic A', 10, 11, 300, 100.25, 7, 8, 80.25, 6,
             'run-drug', '2024-01-01/2024-12-31'),
            (1000000001, 'Brand A', 'Generic A', 5, 5, 150, 50.50, 4, 3, 30.50, 2,
             'run-drug', '2024-01-01/2024-12-31'),
            (2000000002, 'Excluded', 'Excluded', 99, 99, 99, 999, 9, 9, 99, 9,
             'run-drug', '2024-01-01/2024-12-31');
        """
    )
    connection.close()
    manifest = tmp_path / "source-release.json"
    manifest.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "release": {
                    "warehouse_release_id": SOURCE_RELEASE_ID,
                    "validation_state": "passed",
                    "byte_size": database.stat().st_size,
                    "sha256": _sha256(database),
                    "pipeline_code_commit": "b" * 40,
                },
            }
        )
    )
    return database, manifest


def test_builds_self_contained_validated_utilization_release(tmp_path: Path) -> None:
    source, manifest = _source(tmp_path)
    data_root = tmp_path / "utilization-data"
    spill_root = tmp_path / "spill"

    result = utilization_releases.build_release(
        data_root=data_root,
        source_warehouse=source,
        source_release_manifest=manifest,
        spill_root=spill_root,
        memory_limit_gb=1,
        threads=1,
        pipeline_code_commit=COMMIT,
    )

    assert result["state"] == "passed"
    assert result["table_counts"] == {
        "serving_practice_nppes_provider_sites": 1,
        "utilization_metrics": 1,
        "provider_service_detail": 1,
        "provider_drug_detail": 1,
        "utilization_procedure_dictionary": 1,
        "utilization_drug_dictionary": 1,
    }
    database = Path(result["database_path"])
    assert database.stat().st_mode & 0o222 == 0
    assert not Path(str(database) + ".partial").exists()
    assert not any(spill_root.iterdir())

    connection = duckdb.connect(str(database), read_only=True)
    try:
        drug = connection.execute(
            "SELECT npi, brand_name, generic_name, tot_claims, tot_drug_cost "
            "FROM provider_drug_detail"
        ).fetchone()
        assert drug == ("1000000001", "Brand A", "Generic A", 15, pytest.approx(150.75))
        assert connection.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_name LIKE '%build_stage%'"
        ).fetchone()[0] == 0
    finally:
        connection.close()

    verified = utilization_releases.verify_release(
        data_root, result["utilization_release_id"]
    )
    assert verified["sha256"] == result["sha256"]
    comparison = json.loads(Path(result["comparison"]).read_text())
    assert comparison["comparison_policy"] == "independent_utilization_v1"
    assert comparison["failed_requirements"] == []


def test_rejects_source_warehouse_whose_hash_does_not_match_manifest(
    tmp_path: Path,
) -> None:
    source, manifest = _source(tmp_path)
    value = json.loads(manifest.read_text())
    value["release"]["sha256"] = "0" * 64
    manifest.write_text(json.dumps(value))

    with pytest.raises(
        utilization_releases.UtilizationReleaseError,
        match="SHA-256",
    ):
        utilization_releases.build_release(
            data_root=tmp_path / "utilization-data",
            source_warehouse=source,
            source_release_manifest=manifest,
            spill_root=tmp_path / "spill",
            pipeline_code_commit=COMMIT,
        )


def test_augments_sealed_release_without_rebuilding_base_facts(tmp_path: Path) -> None:
    source, manifest = _source(tmp_path)
    data_root = tmp_path / "utilization-data"
    baseline = utilization_releases.build_release(
        data_root=data_root,
        source_warehouse=source,
        source_release_manifest=manifest,
        spill_root=tmp_path / "spill",
        memory_limit_gb=1,
        threads=1,
        pipeline_code_commit=COMMIT,
    )
    taxonomy = tmp_path / "taxonomy"
    taxonomy.mkdir()
    files = {
        "procedures": taxonomy / "procedure_taxonomy.csv",
        "classes": taxonomy / "drug_classes.csv",
        "members": taxonomy / "drug_class_members.csv",
    }
    files["procedures"].write_text(
        "hcpcs_code,rbcs_id,category_id,category_name,subcategory_id,subcategory_name,"
        "family_id,family_name,major_indicator,hcpcs_add_date,hcpcs_end_date,"
        "rbcs_release_year\n"
        "99213,EM001N,E,Evaluation and Management,EM,Office visits,EM-001,"
        "Office visits,N,01/01/2000,12/31/9999,2025\n"
    )
    files["classes"].write_text(
        "source,class_type,class_id,class_name,parent_class_id,parent_class_name,level\n"
        "ATC,ATC,A01,Stomatological preparations,,,1\n"
    )
    files["members"].write_text(
        "source,class_type,class_id,generic_name,rxcui,concept_name,concept_tty,"
        "match_score,match_method,source_version\n"
        "ATC,ATC,A01,Generic A,123,Generic A,IN,100,exact_normalized,v1\n"
    )
    reference = {
        "schema_version": 1,
        "reference": {
            "taxonomy_reference_id": "taxonomy-20260818T000000Z-aaaaaaaaaa",
            "source_utilization_release_id": baseline["utilization_release_id"],
            "source_utilization_sha256": baseline["sha256"],
            "rbcs": {"release_year": "2025"},
            "rxclass": {"versions": {"ATC": "v1", "FDASPL": "v1"}},
            "files": {
                name: {
                    "path": path.name,
                    "byte_size": path.stat().st_size,
                    "sha256": _sha256(path),
                }
                for name, path in files.items()
            },
        },
    }
    taxonomy_manifest = taxonomy / "manifest.json"
    taxonomy_manifest.write_text(json.dumps(reference))

    augmented = utilization_releases.augment_release(
        data_root=data_root,
        source_utilization=Path(baseline["database_path"]),
        source_release_manifest=Path(baseline["release_manifest"]),
        taxonomy_manifest=taxonomy_manifest,
        pipeline_code_commit="c" * 40,
    )

    assert augmented["state"] == "passed"
    for table, count in baseline["table_counts"].items():
        assert augmented["table_counts"][table] == count
    assert augmented["table_counts"]["utilization_procedure_taxonomy"] == 1
    assert augmented["table_counts"]["utilization_drug_classes"] == 1
    assert augmented["table_counts"]["utilization_drug_class_members"] == 1
    verified = utilization_releases.verify_release(
        data_root, augmented["utilization_release_id"]
    )
    assert verified["table_counts"] == augmented["table_counts"]
    connection = duckdb.connect(augmented["database_path"], read_only=True)
    try:
        assert connection.execute(
            "SELECT family_name FROM utilization_procedure_taxonomy"
        ).fetchone()[0] == "Office visits"
    finally:
        connection.close()
