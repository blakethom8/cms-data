import json
import sys
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT))

from release_info import (
    REPRESENTATION_VERSION,
    get_release_router,
    load_release_metadata,
    make_release_resolver,
)
from pipeline.manifests import (
    ManifestDocument,
    ManifestStore,
    PromotionState,
    RunManifest,
    ValidationState,
)

DEPLOYMENT_ID = "deployment-20260721T202014Z-28465a2bbf"
WAREHOUSE_RELEASE_ID = "warehouse-20260721T180000Z-abc123"


def _active_manifest(source_id: str, period: str) -> RunManifest:
    timestamp = "2026-07-21T18:00:00+00:00"
    return RunManifest(
        run_id=f"run-{source_id}",
        release_id=f"release-{source_id}",
        source_id=source_id,
        publisher="CMS",
        publisher_version=f"version-{source_id}",
        source_data_period=period,
        discovery_timestamp=timestamp,
        retrieval_timestamp=timestamp,
        schema_fingerprint="sha256:" + "a" * 64,
        row_counts={"source_rows": 2},
        validation_state=ValidationState.PASSED,
        validation_timestamp=timestamp,
        promotion_state=PromotionState.ACTIVE,
        promotion_timestamp=timestamp,
        active_release_id=f"release-{source_id}",
    )


def _production_root(
    tmp_path: Path,
    *,
    with_ledger: bool = True,
    with_evidence: bool = True,
) -> Path:
    root = tmp_path / "production"
    bundle = root / "releases" / DEPLOYMENT_ID
    bundle.mkdir(parents=True)

    warehouse_file = tmp_path / "artifacts" / "warehouse.duckdb"
    warehouse_file.parent.mkdir(parents=True)
    warehouse_file.write_bytes(b"not opened by the resolver")
    (bundle / "warehouse").symlink_to(warehouse_file)
    (root / "release-current").symlink_to(bundle)

    if with_ledger:
        (root / "deployments.json").write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "selected_deployment_id": DEPLOYMENT_ID,
                    "deployments": [
                        {
                            "deployment_id": DEPLOYMENT_ID,
                            "state": "verified",
                            "selected_at": "2026-07-21T20:31:07+00:00",
                            "verified_at": "2026-07-21T20:35:11+00:00",
                            "warehouse_release_id": WAREHOUSE_RELEASE_ID,
                            "warehouse_sha256": "f" * 64,
                            "warehouse_pipeline_commit": "b" * 40,
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )

    if with_evidence:
        ManifestStore(
            root / "evidence" / DEPLOYMENT_ID / "source-manifests.json"
        ).save(
            ManifestDocument(
                manifests=[
                    _active_manifest("nppes_monthly_v2", "2026-06"),
                    _active_manifest("open_payments_general", "PY2024"),
                ]
            )
        )

    return root


def _duckdb_path(root: Path) -> str:
    return str(root / "release-current" / "warehouse")


def _client(duckdb_path: str) -> TestClient:
    app = FastAPI()
    app.include_router(get_release_router(make_release_resolver(duckdb_path)))
    return TestClient(app)


def test_release_reports_bundle_identity_with_ledger_and_vintages(tmp_path: Path) -> None:
    root = _production_root(tmp_path)
    response = _client(_duckdb_path(root)).get("/release")

    assert response.status_code == 200
    payload = response.json()
    assert payload["release_id"] == DEPLOYMENT_ID
    assert payload["promoted_at"] == "2026-07-21T20:31:07+00:00"
    assert payload["verified_at"] == "2026-07-21T20:35:11+00:00"
    assert payload["representation_version"] == REPRESENTATION_VERSION
    assert payload["source_vintages"] == {
        "nppes_monthly_v2": "2026-06",
        "open_payments_general": "PY2024",
    }
    assert payload["build"]["checksum"] == "f" * 64
    assert payload["build"]["pipeline_ref"] == "b" * 40
    assert payload["build"]["warehouse_release_id"] == WAREHOUSE_RELEASE_ID
    assert payload["compatibility"] == "current"


def test_release_survives_missing_ledger_and_evidence(tmp_path: Path) -> None:
    root = _production_root(tmp_path, with_ledger=False, with_evidence=False)
    response = _client(_duckdb_path(root)).get("/release")

    assert response.status_code == 200
    payload = response.json()
    assert payload["release_id"] == DEPLOYMENT_ID
    assert payload["promoted_at"] is None
    assert payload["source_vintages"] == {}
    assert payload["build"]["checksum"] is None


def test_release_is_503_without_a_production_bundle(tmp_path: Path) -> None:
    database = tmp_path / "provider_searcher.duckdb"
    database.write_bytes(b"plain development warehouse")

    response = _client(str(database)).get("/release")

    assert response.status_code == 503
    assert "unavailable" in response.json()["detail"]


def test_release_is_503_for_a_missing_database_path(tmp_path: Path) -> None:
    response = _client(str(tmp_path / "absent" / "warehouse")).get("/release")

    assert response.status_code == 503


def test_override_file_wins_and_malformed_override_is_unavailable(
    tmp_path: Path, monkeypatch
) -> None:
    override = tmp_path / "release-metadata.json"
    override.write_text(
        json.dumps(
            {
                "release_id": "deployment-20260801T000000Z-1234567890",
                "promoted_at": "2026-08-01T00:10:00+00:00",
                "source_vintages": {"nppes_monthly_v2": "2026-07"},
                "build": {"checksum": "e" * 64, "pipeline_ref": "c" * 40},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("CMS_RELEASE_METADATA_PATH", str(override))

    metadata = load_release_metadata(str(tmp_path / "ignored.duckdb"))
    assert metadata is not None
    assert metadata.release_id == "deployment-20260801T000000Z-1234567890"
    assert metadata.source_vintages == {"nppes_monthly_v2": "2026-07"}
    assert metadata.checksum == "e" * 64
    assert metadata.compatibility == "current"

    override.write_text("{not json", encoding="utf-8")
    assert load_release_metadata(str(tmp_path / "ignored.duckdb")) is None

    override.write_text(json.dumps({"promoted_at": "no release id"}), encoding="utf-8")
    assert load_release_metadata(str(tmp_path / "ignored.duckdb")) is None


def test_resolver_caches_success_for_process_lifetime_but_retries_failure(
    tmp_path: Path,
) -> None:
    root = tmp_path / "production"
    bundle = root / "releases" / DEPLOYMENT_ID
    resolver = make_release_resolver(str(root / "release-current" / "warehouse"))

    # No bundle yet: failure is reported and not cached.
    assert resolver() is None

    bundle.mkdir(parents=True)
    warehouse_file = tmp_path / "warehouse.duckdb"
    warehouse_file.write_bytes(b"")
    (bundle / "warehouse").symlink_to(warehouse_file)
    (root / "release-current").symlink_to(bundle)

    first = resolver()
    assert first is not None
    assert first.release_id == DEPLOYMENT_ID

    # A repointed bundle does not change the cached identity mid-process;
    # cutover restarts the service to pick up a new release.
    (root / "release-current").unlink()
    assert resolver() is first
