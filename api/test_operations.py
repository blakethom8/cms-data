import sys
from pathlib import Path

import duckdb
from fastapi import FastAPI
from fastapi.testclient import TestClient

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT))

from operations import get_operations_router
from pipeline.manifests import (
    ManifestDocument,
    ManifestStore,
    PromotionState,
    RunManifest,
    ValidationState,
)
from pipeline.source_registry import SOURCE_REGISTRY


def _manifest(
    *,
    run_id: str,
    release_id: str,
    validation: ValidationState,
    promotion: PromotionState,
    timestamp: str,
) -> RunManifest:
    active = release_id if promotion == PromotionState.ACTIVE else None
    return RunManifest(
        run_id=run_id,
        release_id=release_id,
        source_id="nppes_monthly_v2",
        publisher="CMS NPPES",
        publisher_version=f"version-{run_id}",
        source_data_period="2026-07",
        discovery_timestamp=timestamp,
        retrieval_timestamp=timestamp,
        schema_fingerprint="sha256:" + "a" * 64,
        row_counts={"source_rows": 2},
        validation_state=validation,
        validation_timestamp=timestamp,
        promotion_state=promotion,
        promotion_timestamp=timestamp if promotion == PromotionState.ACTIVE else None,
        active_release_id=active,
        error_summary="fixture failure" if validation == ValidationState.FAILED else None,
    )


def _client(tmp_path: Path) -> TestClient:
    connection = duckdb.connect(":memory:")
    connection.execute("CREATE TABLE raw_nppes (npi VARCHAR)")
    connection.execute("INSERT INTO raw_nppes VALUES ('1'), ('2')")
    connection.execute("CREATE TABLE core_providers (npi VARCHAR)")
    connection.execute("INSERT INTO core_providers VALUES ('1')")

    manifest_path = tmp_path / "manifests.json"
    ManifestStore(manifest_path).save(
        ManifestDocument(
            manifests=[
                _manifest(
                    run_id="older",
                    release_id="release-older",
                    validation=ValidationState.FAILED,
                    promotion=PromotionState.NOT_PROMOTED,
                    timestamp="2026-07-01T00:00:00+00:00",
                ),
                _manifest(
                    run_id="active",
                    release_id="release-active",
                    validation=ValidationState.PASSED,
                    promotion=PromotionState.ACTIVE,
                    timestamp="2026-07-02T00:00:00+00:00",
                ),
            ]
        )
    )

    app = FastAPI()
    app.include_router(get_operations_router(lambda: connection, manifest_path))
    return TestClient(app)


def test_overview_reports_warehouse_evidence_without_enabling_writes(tmp_path: Path) -> None:
    payload = _client(tmp_path).get("/operations/overview").json()

    assert payload["warehouse"]["table_count"] == 2
    assert payload["warehouse"]["raw_table_count"] == 1
    assert payload["warehouse"]["data_mart_count"] == 1
    assert payload["warehouse"]["estimated_rows"] == 3
    assert payload["contracts"]["registered_sources"] == len(SOURCE_REGISTRY)
    assert payload["contracts"]["sources_with_active_evidence"] == 1
    assert payload["contracts"]["failed_run_count"] == 1
    assert payload["control_plane"]["manual_refresh_enabled"] is False
    assert payload["control_plane"]["mode"] == "observation_only"


def test_sources_join_registry_contracts_to_latest_manifest_evidence(tmp_path: Path) -> None:
    payload = _client(tmp_path).get("/operations/sources").json()
    nppes = next(
        source for source in payload["sources"] if source["source_id"] == "nppes_monthly_v2"
    )

    assert nppes["cadence"] == "monthly_full"
    assert "core_providers" in nppes["downstream_tables"]
    assert nppes["evidence_status"] == "validated_active"
    assert nppes["latest_manifest"]["run_id"] == "active"


def test_runs_are_newest_first_and_respect_limit(tmp_path: Path) -> None:
    payload = _client(tmp_path).get("/operations/runs?limit=1").json()

    assert [run["run_id"] for run in payload["runs"]] == ["active"]


def test_missing_manifest_is_reported_as_missing_evidence(tmp_path: Path) -> None:
    connection = duckdb.connect(":memory:")
    app = FastAPI()
    app.include_router(
        get_operations_router(lambda: connection, tmp_path / "missing-manifests.json")
    )

    payload = TestClient(app).get("/operations/sources").json()

    assert payload["evidence_error"] is None
    assert all(source["evidence_status"] == "missing" for source in payload["sources"])
