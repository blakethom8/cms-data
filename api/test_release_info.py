import json
import sys
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT))

from fastapi import Depends, HTTPException
from starlette.requests import Request

from release_info import (
    CACHE_CONTROL,
    REPRESENTATION_VERSION,
    ReleaseCacheMiddleware,
    ReleaseMetadata,
    get_release_router,
    load_release_metadata,
    make_release_resolver,
    release_etag,
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


def test_resolver_refreshes_verification_for_the_same_cached_bundle(
    tmp_path: Path,
) -> None:
    root = _production_root(tmp_path)
    ledger_path = root / "deployments.json"
    ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
    ledger["deployments"][0]["verified_at"] = None
    ledger_path.write_text(json.dumps(ledger), encoding="utf-8")
    resolver = make_release_resolver(_duckdb_path(root))

    selected = resolver()
    assert selected is not None
    assert selected.release_id == DEPLOYMENT_ID
    assert selected.promoted_at == "2026-07-21T20:31:07+00:00"
    assert selected.verified_at is None

    ledger["deployments"][0]["verified_at"] = "2026-07-21T20:35:11+00:00"
    ledger_path.write_text(json.dumps(ledger), encoding="utf-8")

    verified = resolver()
    assert verified is not None
    assert verified.release_id == DEPLOYMENT_ID
    assert verified.verified_at == "2026-07-21T20:35:11+00:00"

    # Once complete, the metadata is stable even if the control-plane file
    # becomes temporarily unreadable later in the process lifetime.
    ledger_path.unlink()
    assert resolver() is verified


def test_resolver_does_not_refresh_from_a_repointed_bundle(tmp_path: Path) -> None:
    root = _production_root(tmp_path)
    ledger_path = root / "deployments.json"
    ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
    ledger["deployments"][0]["verified_at"] = None
    ledger_path.write_text(json.dumps(ledger), encoding="utf-8")
    resolver = make_release_resolver(_duckdb_path(root))

    selected = resolver()
    assert selected is not None
    assert selected.verified_at is None

    other_id = "deployment-20260721T212014Z-18465a2bbf"
    other_bundle = root / "releases" / other_id
    other_bundle.mkdir()
    (other_bundle / "warehouse").symlink_to(tmp_path / "artifacts" / "warehouse.duckdb")
    (root / "release-current").unlink()
    (root / "release-current").symlink_to(other_bundle)
    ledger["deployments"].append(
        {
            "deployment_id": other_id,
            "state": "verified",
            "selected_at": "2026-07-21T21:20:07+00:00",
            "verified_at": "2026-07-21T21:25:11+00:00",
        }
    )
    ledger_path.write_text(json.dumps(ledger), encoding="utf-8")

    unchanged = resolver()
    assert unchanged is selected
    assert unchanged.release_id == DEPLOYMENT_ID
    assert unchanged.verified_at is None


def test_release_endpoint_exposes_completed_verification_without_a_stale_304(
    tmp_path: Path,
) -> None:
    root = _production_root(tmp_path)
    ledger_path = root / "deployments.json"
    ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
    ledger["deployments"][0]["verified_at"] = None
    ledger_path.write_text(json.dumps(ledger), encoding="utf-8")
    resolver = make_release_resolver(_duckdb_path(root))
    app = FastAPI()
    app.include_router(get_release_router(resolver))
    app.add_middleware(
        ReleaseCacheMiddleware,
        resolve_metadata=resolver,
        is_authorized=lambda request: True,
    )
    client = TestClient(app)

    selected = client.get("/release")
    assert selected.status_code == 200
    assert selected.json()["verified_at"] is None
    assert selected.headers["Cache-Control"] == "no-store"
    assert "ETag" not in selected.headers

    ledger["deployments"][0]["verified_at"] = "2026-07-21T20:35:11+00:00"
    ledger_path.write_text(json.dumps(ledger), encoding="utf-8")

    verified = client.get(
        "/release",
        headers={"If-None-Match": f'"{DEPLOYMENT_ID}:{REPRESENTATION_VERSION}"'},
    )
    assert verified.status_code == 200
    assert verified.json()["verified_at"] == "2026-07-21T20:35:11+00:00"
    assert verified.headers["Cache-Control"] == "no-store"
    assert "ETag" not in verified.headers


# --- Cache validators (ETag / If-None-Match) ---

RELEASE = ReleaseMetadata(release_id=DEPLOYMENT_ID)
EXPECTED_ETAG = f'"{DEPLOYMENT_ID}:{REPRESENTATION_VERSION}"'


def _cached_app(
    metadata: ReleaseMetadata | None,
    *,
    require_key: str | None = None,
) -> tuple[TestClient, dict[str, int]]:
    """App with one counted data route, mirroring main.py's auth wiring."""

    queries = {"count": 0}
    app = FastAPI()

    async def check_key(request: Request) -> None:
        if require_key and request.headers.get("X-API-Key") != require_key:
            raise HTTPException(status_code=401, detail="Invalid or missing API key")

    @app.get("/practices", dependencies=[Depends(check_key)])
    async def practices():
        queries["count"] += 1
        return {"contract_version": 2, "practices": []}

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    app.add_middleware(
        ReleaseCacheMiddleware,
        resolve_metadata=lambda: metadata,
        is_authorized=lambda request: not require_key
        or request.headers.get("X-API-Key") == require_key,
    )
    return TestClient(app), queries


def test_data_responses_carry_release_etag_and_cache_control() -> None:
    client, _ = _cached_app(RELEASE)
    response = client.get("/practices")

    assert response.status_code == 200
    assert response.headers["ETag"] == EXPECTED_ETAG
    assert response.headers["Cache-Control"] == CACHE_CONTROL
    assert response.json()["contract_version"] == 2


def test_matching_if_none_match_returns_304_without_querying(tmp_path: Path) -> None:
    client, queries = _cached_app(RELEASE)
    first = client.get("/practices")
    assert queries["count"] == 1

    revalidation = client.get(
        "/practices", headers={"If-None-Match": first.headers["ETag"]}
    )

    assert revalidation.status_code == 304
    assert revalidation.content == b""
    assert revalidation.headers["ETag"] == EXPECTED_ETAG
    assert revalidation.headers["Cache-Control"] == CACHE_CONTROL
    assert queries["count"] == 1  # the route (and DuckDB) never ran


def test_weak_and_listed_if_none_match_forms_match() -> None:
    client, queries = _cached_app(RELEASE)
    header = f'"stale:1", W/{EXPECTED_ETAG}'

    assert client.get("/practices", headers={"If-None-Match": header}).status_code == 304
    assert client.get("/practices", headers={"If-None-Match": "*"}).status_code == 304
    assert queries["count"] == 0


def test_stale_validator_is_answered_fresh() -> None:
    client, queries = _cached_app(RELEASE)
    response = client.get(
        "/practices", headers={"If-None-Match": '"deployment-old:1"'}
    )

    assert response.status_code == 200
    assert response.headers["ETag"] == EXPECTED_ETAG
    assert queries["count"] == 1


def test_etag_is_stable_within_a_release_and_changes_across_releases() -> None:
    other = ReleaseMetadata(release_id="deployment-20260801T000000Z-abcdef0123")

    assert release_etag(RELEASE) == EXPECTED_ETAG
    assert release_etag(ReleaseMetadata(release_id=DEPLOYMENT_ID)) == EXPECTED_ETAG
    assert release_etag(other) != EXPECTED_ETAG

    client, queries = _cached_app(other)
    response = client.get("/practices", headers={"If-None-Match": EXPECTED_ETAG})
    assert response.status_code == 200
    assert response.headers["ETag"] == release_etag(other)
    assert queries["count"] == 1


def test_without_release_metadata_responses_are_unchanged() -> None:
    client, queries = _cached_app(None)
    response = client.get("/practices", headers={"If-None-Match": EXPECTED_ETAG})

    assert response.status_code == 200
    assert "ETag" not in response.headers
    assert "Cache-Control" not in response.headers
    assert queries["count"] == 1


def test_unauthorized_conditional_requests_get_401_not_304() -> None:
    client, queries = _cached_app(RELEASE, require_key="secret")

    denied = client.get("/practices", headers={"If-None-Match": EXPECTED_ETAG})
    assert denied.status_code == 401
    assert "ETag" not in denied.headers

    allowed = client.get(
        "/practices",
        headers={"If-None-Match": EXPECTED_ETAG, "X-API-Key": "secret"},
    )
    assert allowed.status_code == 304
    assert queries["count"] == 0


def test_health_and_non_get_requests_are_exempt() -> None:
    client, _ = _cached_app(RELEASE)

    health = client.get("/health", headers={"If-None-Match": EXPECTED_ETAG})
    assert health.status_code == 200
    assert "ETag" not in health.headers

    post = client.post("/practices", headers={"If-None-Match": EXPECTED_ETAG})
    assert post.status_code == 405
    assert "ETag" not in post.headers
