"""Tests for immutable RBCS and RxClass reference acquisition."""

import hashlib
import json
import sys
from pathlib import Path

import duckdb


REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT))

from pipeline import utilization_taxonomy


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _source(tmp_path: Path) -> tuple[Path, Path]:
    database = tmp_path / "utilization.duckdb"
    connection = duckdb.connect(str(database))
    connection.execute(
        """
        CREATE TABLE utilization_drug_dictionary (
          brand_name VARCHAR, generic_name VARCHAR, physician_count INTEGER,
          total_claims BIGINT, total_drug_cost DOUBLE, data_year INTEGER
        );
        INSERT INTO utilization_drug_dictionary VALUES
          ('Lipitor', 'Atorvastatin Calcium', 10, 100, 500, 2024),
          ('Caduet', 'Amlodipine/Atorvastatin', 5, 40, 300, 2024);
        """
    )
    connection.close()
    manifest = tmp_path / "release.json"
    manifest.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "release": {
                    "utilization_release_id": "utilization-20260818T000000Z-aaaaaaaaaa",
                    "validation_state": "passed",
                    "byte_size": database.stat().st_size,
                    "sha256": _sha256(database),
                },
            }
        )
    )
    return database, manifest


def _rbcs_download(_url: str, path: Path, **_kwargs) -> None:
    path.write_text(
        "HCPCS_Cd,RBCS_Id,RBCS_Cat,RBCS_Cat_Desc,RBCS_Cat_Subcat,"
        "RBCS_Subcat_Desc,RBCS_FamNumb,RBCS_Family_Desc,RBCS_Major_Ind,"
        "HCPCS_Cd_Add_Dt,HCPCS_Cd_End_Dt,RBCS_Latest_Assignment,"
        "First_RBCS_Release_Year,RBCS_Analysis_Start_Dt,RBCS_Analysis_End_Dt,"
        "Alt_Assignment_Method,RBCS_Id_Ever_Reassigned\n"
        "27447,MP010N,M,Procedures,MP,Musculoskeletal,010,Arthroplasty-Knee,N,"
        "01/01/1989,12/31/9999,1,2025,01/01/2014,12/31/9999,0,0\n"
        "27446,MP010N,M,Procedures,MP,Musculoskeletal,010,Arthroplasty-Knee,N,"
        "01/01/1989,12/31/9999,0,2020,01/01/2014,12/31/2024,0,0\n"
    )


def _lookup(_cache: Path, _base: str, generic: str, source: str) -> dict:
    if source == "ATC":
        values = [
            {
                "minConcept": {"rxcui": "83367", "name": "atorvastatin", "tty": "IN"},
                "rxclassMinConceptItem": {
                    "classId": "C10AA",
                    "className": "HMG CoA reductase inhibitors",
                    "classType": "ATC1-4",
                },
                "relaSource": "ATC",
            },
            {
                "minConcept": {
                    "rxcui": "404773",
                    "name": "amlodipine / atorvastatin",
                    "tty": "MIN",
                },
                "rxclassMinConceptItem": {
                    "classId": "C10BX",
                    "className": "Lipid agents in combination",
                    "classType": "ATC1-4",
                },
                "relaSource": "ATC",
            },
        ]
    else:
        values = [
            {
                "minConcept": {"rxcui": "83367", "name": "atorvastatin", "tty": "IN"},
                "rxclassMinConceptItem": {
                    "classId": "N1",
                    "className": "HMG-CoA Reductase Inhibitor",
                    "classType": "EPC",
                },
                "relaSource": "FDASPL",
            },
            {
                "minConcept": {"rxcui": "83367", "name": "atorvastatin", "tty": "IN"},
                "rxclassMinConceptItem": {
                    "classId": "N2",
                    "className": "Reductase inhibition",
                    "classType": "MOA",
                },
                "relaSource": "FDASPL",
            },
        ]
    return {"rxclassDrugInfoList": {"rxclassDrugInfo": values}}


def test_match_score_rejects_single_ingredient_to_combination() -> None:
    assert utilization_taxonomy.match_score("Atorvastatin Calcium", "atorvastatin") == (
        95,
        "salt_normalized",
    )
    assert (
        utilization_taxonomy.match_score(
            "Atorvastatin Calcium", "amlodipine / atorvastatin"
        )
        is None
    )


def test_acquire_seals_current_rbcs_and_conservative_drug_mappings(
    tmp_path: Path, monkeypatch
) -> None:
    database, source_manifest = _source(tmp_path)
    monkeypatch.setattr(utilization_taxonomy, "_download", _rbcs_download)
    monkeypatch.setattr(utilization_taxonomy, "_cached_drug_lookup", _lookup)
    monkeypatch.setattr(
        utilization_taxonomy,
        "_atc_catalog",
        lambda _base: [
            {
                "source": "ATC",
                "class_type": "ATC",
                "class_id": "C10AA",
                "class_name": "HMG CoA reductase inhibitors",
                "parent_class_id": "",
                "parent_class_name": "",
                "level": 1,
            },
            {
                "source": "ATC",
                "class_type": "ATC",
                "class_id": "C10BX",
                "class_name": "Lipid agents in combination",
                "parent_class_id": "",
                "parent_class_name": "",
                "level": 1,
            },
        ],
    )
    monkeypatch.setattr(utilization_taxonomy, "_rxclass_version", lambda *_args: "v1")

    result = utilization_taxonomy.acquire_reference(
        output_root=tmp_path / "references",
        utilization_database=database,
        utilization_release_manifest=source_manifest,
        workers=2,
    )

    assert result["state"] == "passed"
    assert result["procedure_count"] == 1
    assert result["mapped_generic_count"] == 2
    release_dir = Path(result["reference_dir"])
    members = list(csv_dicts(release_dir / "drug_class_members.csv"))
    atorvastatin = [row for row in members if row["generic_name"] == "Atorvastatin Calcium"]
    assert {(row["source"], row["class_id"]) for row in atorvastatin} == {
        ("ATC", "C10AA"),
        ("FDASPL", "N1"),
    }
    assert all(path.stat().st_mode & 0o222 == 0 for path in release_dir.iterdir())
    manifest = json.loads((release_dir / "manifest.json").read_text())
    assert manifest["reference"]["rxclass"]["query_count"] == 4


def csv_dicts(path: Path):
    import csv

    with path.open(newline="") as handle:
        yield from csv.DictReader(handle)
