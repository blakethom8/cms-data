import sys
from pathlib import Path

import duckdb
import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT))

from pipeline.lineage import TRANSFORMS, table_kind
from pipeline.mart_contracts import (
    MART_CONTRACTS,
    MART_CONTRACT_BY_TABLE,
    MartContractError,
    MartSpec,
    inspect_mart_contracts,
    validate_mart_contracts,
)
from pipeline.source_registry import SOURCE_REGISTRY


EXPECTED_MARTS = {
    "core_providers",
    "practice_locations",
    "serving_practice_provider_sites",
    "serving_practice_nppes_provider_sites",
    "serving_practice_nppes_org_memberships",
    "utilization_metrics",
    "industry_relationships",
    "hospital_affiliations",
    "provider_service_detail",
    "provider_drug_detail",
    "provider_quality_scores",
    "order_referring_eligibility",
    "kol_summary",
    "nppes_radar_provider_state",
    "nppes_radar_events",
}


def _spec(**overrides) -> MartSpec:
    values = {
        "table": "example_mart",
        "transform_ids": ("build_example",),
        "grain": "one row per NPI",
        "key_columns": ("npi",),
        "upstream_tables": ("core_providers",),
        "source_ids": ("cms_physician_by_provider",),
        "required_columns": ("npi", "data_year"),
        "source_period_policy": "release manifest",
        "provenance_scope": "release_manifest",
    }
    values.update(overrides)
    return MartSpec(**values)


def test_registered_marts_have_complete_source_and_lineage_contracts() -> None:
    assert len(MART_CONTRACTS) == 15
    assert set(MART_CONTRACT_BY_TABLE) == EXPECTED_MARTS

    transforms = {transform.transform_id: transform for transform in TRANSFORMS}
    for spec in MART_CONTRACTS:
        assert set(spec.key_columns).issubset(spec.required_columns)
        assert spec.source_period_policy
        assert spec.provenance_scope in {
            "release_manifest",
            "row_and_release_manifest",
        }
        assert set(spec.source_ids).issubset(SOURCE_REGISTRY)
        for transform_id in spec.transform_ids:
            assert transform_id in transforms
            assert spec.table in transforms[transform_id].outputs

    assert table_kind("kol_summary") == "summary"
    assert table_kind("nppes_radar_events") == "summary"
    assert table_kind("serving_practice_provider_sites") == "serving"
    assert table_kind("serving_practice_nppes_provider_sites") == "serving"
    assert table_kind("serving_practice_nppes_org_memberships") == "serving"
    assert MART_CONTRACT_BY_TABLE[
        "serving_practice_provider_sites"
    ].authorized_routes == ("/practices/search",)
    assert table_kind("core_providers") == "mart"


def test_schema_inspection_does_not_claim_missing_marts_are_ready() -> None:
    connection = duckdb.connect(":memory:")
    try:
        connection.execute("CREATE TABLE core_providers (npi VARCHAR)")
        report = inspect_mart_contracts(connection)
    finally:
        connection.close()

    assert report["registered_count"] == 15
    assert report["available_count"] == 1
    assert report["schema_valid_count"] == 0
    assert report["serving_authorized_count"] == 0
    assert report["passed"] is False
    core = next(mart for mart in report["marts"] if mart["table"] == "core_providers")
    assert core["available"] is True
    assert core["schema_valid"] is False
    assert core["missing_required_columns"] == [
        "data_year",
        "entity_type_code",
        "last_org_name",
    ]


def test_row_validation_records_key_null_npi_orphan_and_provenance_checks() -> None:
    connection = duckdb.connect(":memory:")
    try:
        connection.execute("CREATE TABLE core_providers (npi VARCHAR PRIMARY KEY)")
        connection.execute("INSERT INTO core_providers VALUES ('1234567890')")
        connection.execute("CREATE TABLE example_mart (npi VARCHAR, data_year INTEGER)")
        connection.execute("INSERT INTO example_mart VALUES ('1234567890', 2024)")

        report = validate_mart_contracts(
            connection,
            contracts=(_spec(),),
            source_periods={"cms_physician_by_provider": "2024"},
        )
    finally:
        connection.close()

    assert report["passed"] is True
    assert report["data_valid_count"] == 1
    mart = report["marts"][0]
    assert mart["row_count"] == 1
    assert mart["duplicate_key_groups"] == 0
    assert mart["required_null_rows"] == 0
    assert mart["invalid_npis"] == 0
    assert mart["orphan_npis"] == 0
    assert mart["missing_source_periods"] == []


def test_row_validation_fails_closed_with_actionable_issues() -> None:
    connection = duckdb.connect(":memory:")
    try:
        connection.execute("CREATE TABLE core_providers (npi VARCHAR PRIMARY KEY)")
        connection.execute("CREATE TABLE example_mart (npi VARCHAR, data_year INTEGER)")
        connection.execute(
            "INSERT INTO example_mart VALUES "
            "('bad', 2024), ('bad', 2024), ('1234567890', NULL)"
        )

        with pytest.raises(MartContractError) as caught:
            validate_mart_contracts(
                connection,
                contracts=(_spec(),),
                source_periods={},
            )
    finally:
        connection.close()

    message = str(caught.value)
    assert "duplicate_key_groups:1" in message
    assert "required_null_rows:1" in message
    assert "invalid_npis:2" in message
    assert "orphan_npis:3" in message
    assert "source_periods_missing:cms_physician_by_provider" in message


def test_declared_row_predicates_are_counted_and_fail_closed() -> None:
    connection = duckdb.connect(":memory:")
    try:
        connection.execute("CREATE TABLE example_mart (npi VARCHAR, data_year INTEGER)")
        connection.execute("INSERT INTO example_mart VALUES ('1234567890', 1999)")
        report = validate_mart_contracts(
            connection,
            contracts=(
                _spec(
                    npi_parent_table=None,
                    row_validations=(("invalid_year", "data_year < 2000"),),
                ),
            ),
            source_periods={"cms_physician_by_provider": "2024"},
            raise_on_error=False,
        )
    finally:
        connection.close()

    mart = report["marts"][0]
    assert report["passed"] is False
    assert mart["row_validation_failures"] == {"invalid_year": 1}
    assert mart["issues"] == ["invalid_year:1"]
