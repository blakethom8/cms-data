import json
import shutil
import sys
import urllib.error
from pathlib import Path

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT))

from pipeline.data_platform import (
    EXIT_DISCOVERY_FAILURE,
    EXIT_HEALTHY,
    EXIT_STALE_OR_UNKNOWN,
    FreshnessStatus,
    evaluate_source,
    main,
)
from pipeline.discovery import (
    DiscoveryError,
    DiscoveryResult,
    DiscoveryState,
    ReleaseMetadata,
    discover_all,
    parse_aact_downloads,
    parse_cms_catalog,
    parse_nppes_index,
    parse_open_payments_index,
    parse_ppef_resources,
)
from pipeline.manifests import (
    ManifestDocument,
    ManifestStore,
    PromotionState,
    RunManifest,
    ValidationState,
)
from pipeline.source_registry import (
    SOURCE_REGISTRY,
    DiscoveryMechanism,
    sources_for,
)

FIXTURES = REPOSITORY_ROOT / "pipeline" / "fixtures" / "publisher_metadata"


def _active_manifest(source_id: str, publisher_version: str) -> RunManifest:
    release_id = f"release-{source_id}"
    return RunManifest(
        run_id=f"run-{source_id}",
        release_id=release_id,
        source_id=source_id,
        publisher=SOURCE_REGISTRY[source_id].publisher.value,
        publisher_version=publisher_version,
        source_data_period="2098-01-01/2098-12-31",
        publisher_release_timestamp="2099-06-30T00:00:00+00:00",
        discovery_timestamp="2099-07-20T00:00:00+00:00",
        retrieval_timestamp="2099-07-20T01:00:00+00:00",
        source_url="https://example.invalid/source.zip",
        byte_size=123,
        sha256="a" * 64,
        schema_fingerprint="sha256:" + "b" * 64,
        row_counts={"example": 10},
        pipeline_code_commit="0123456789abcdef",
        validation_state=ValidationState.PASSED,
        validation_timestamp="2099-07-20T02:00:00+00:00",
        promotion_state=PromotionState.ACTIVE,
        promotion_timestamp="2099-07-20T03:00:00+00:00",
        active_release_id=release_id,
    )


def _available(source_id: str, publisher_version: str = "publisher-v2") -> DiscoveryResult:
    return DiscoveryResult(
        source_id=source_id,
        state=DiscoveryState.AVAILABLE,
        discovered_at="2099-07-20T00:00:00+00:00",
        release=ReleaseMetadata(
            source_id=source_id,
            publisher_version=publisher_version,
            source_data_period="2098-01-01/2098-12-31",
            publisher_release_timestamp="2099-06-30T00:00:00+00:00",
            source_url="https://example.invalid/source.zip",
        ),
    )


def _manifest_for_discoveries(discoveries: dict[str, DiscoveryResult]) -> ManifestDocument:
    return ManifestDocument(
        manifests=[
            _active_manifest(source_id, result.release.publisher_version)
            for source_id, result in discoveries.items()
            if result.release is not None
        ]
    )


def test_registry_covers_all_required_source_families() -> None:
    assert len(SOURCE_REGISTRY) == 18
    assert {spec.discovery for spec in SOURCE_REGISTRY.values()} == set(DiscoveryMechanism)
    assert all(spec.downstream_tables for spec in SOURCE_REGISTRY.values())
    assert all(spec.source_period_semantics for spec in SOURCE_REGISTRY.values())
    assert all(spec.licensing_notes for spec in SOURCE_REGISTRY.values())


def test_cms_catalog_parsing_uses_stable_dataset_and_resource_ids() -> None:
    results = parse_cms_catalog(
        (FIXTURES / "cms-data.json").read_bytes(),
        sources_for(DiscoveryMechanism.CMS_DATA_JSON),
    )

    physician = results["cms_physician_by_provider"]
    assert physician.state == DiscoveryState.AVAILABLE
    assert physician.release.publisher_version == (
        "cms-resource:10000000-0000-4000-8000-000000000001"
    )
    assert physician.release.source_data_period == "2097-01-01/2097-12-31"


def test_cms_missing_distribution_field_is_a_discovery_error() -> None:
    payload = json.loads((FIXTURES / "cms-data.json").read_text())
    del payload["dataset"][0]["distribution"][0]["resourcesAPI"]

    result = parse_cms_catalog(
        json.dumps(payload).encode(),
        (SOURCE_REGISTRY["cms_physician_by_provider"],),
    )["cms_physician_by_provider"]

    assert result.state == DiscoveryState.ERROR
    assert "resourcesAPI" in result.error_summary


def test_ppef_resource_discovery_selects_relational_subfiles() -> None:
    results = parse_ppef_resources(
        (FIXTURES / "ppef-resources.json").read_bytes(),
        sources_for(DiscoveryMechanism.CMS_DATASET_RESOURCES),
    )

    reassignment = results["cms_pecos_reassignment"].release
    location = results["cms_pecos_practice_location"].release
    assert reassignment.publisher_version == (
        "cms-file:10000000-0000-4000-8000-000000000011"
    )
    assert reassignment.source_data_period == "2099-01-01/2099-03-31"
    assert reassignment.byte_size == 127281801
    assert location.publisher_version.endswith("000000000012")
    assert location.source_url.endswith(
        "PPEF_Practice_Location_Extract_2099.04.01.csv"
    )


def test_ppef_resource_discovery_rejects_missing_expected_subfile() -> None:
    payload = json.loads((FIXTURES / "ppef-resources.json").read_text())
    payload["data"] = [payload["data"][0]]

    result = parse_ppef_resources(
        json.dumps(payload).encode(),
        (SOURCE_REGISTRY["cms_pecos_practice_location"],),
    )["cms_pecos_practice_location"]

    assert result.state == DiscoveryState.ERROR
    assert "found 0" in result.error_summary


def test_nppes_v2_monthly_and_latest_weekly_discovery() -> None:
    results = parse_nppes_index(
        (FIXTURES / "nppes.html").read_bytes(),
        sources_for(DiscoveryMechanism.NPPES_DOWNLOAD_INDEX),
    )

    monthly = results["nppes_monthly_v2"].release
    weekly = results["nppes_weekly_incremental_v2"].release
    assert monthly.publisher_version == "NPPES_Data_Dissemination_July_2067_V2"
    assert monthly.publisher_release_timestamp == "2067-07-13T00:00:00+00:00"
    assert monthly.byte_size == round(1092.10 * 1024**2)
    assert weekly.publisher_version.endswith("071367_071967_Weekly_V2")
    assert weekly.source_data_period == "2067-07-13/2067-07-19"
    assert weekly.publisher_release_timestamp is None


def test_nppes_markup_change_is_not_reported_current() -> None:
    results = parse_nppes_index(
        b"<html><body>publisher changed this page</body></html>",
        sources_for(DiscoveryMechanism.NPPES_DOWNLOAD_INDEX),
    )

    assert {result.state for result in results.values()} == {DiscoveryState.ERROR}
    assert all(result.release is None for result in results.values())


def test_open_payments_release_discovery_covers_all_three_categories() -> None:
    results = parse_open_payments_index(
        [(FIXTURES / "open-payments.html").read_bytes()],
        sources_for(DiscoveryMechanism.OPEN_PAYMENTS_DOWNLOAD_INDEX),
    )

    assert set(results) == {
        "open_payments_general",
        "open_payments_research",
        "open_payments_ownership",
    }
    assert results["open_payments_general"].release.publisher_version.endswith(":general")
    assert results["open_payments_research"].release.publisher_version.endswith(":research")
    assert results["open_payments_ownership"].release.publisher_version.endswith(":ownership")
    assert results["open_payments_general"].release.source_data_period == (
        "2098-01-01/2098-12-31"
    )


def test_open_payments_missing_download_metadata_is_an_error() -> None:
    with pytest.raises(DiscoveryError, match="no parseable program-year ZIP"):
        parse_open_payments_index(
            [b"<html><body>No current datasets</body></html>"],
            sources_for(DiscoveryMechanism.OPEN_PAYMENTS_DOWNLOAD_INDEX),
        )


def test_aact_postgresql_snapshot_discovery() -> None:
    result = parse_aact_downloads(
        (FIXTURES / "aact.html").read_bytes(),
        SOURCE_REGISTRY["aact_clinical_trials_snapshot"],
    )

    assert result.state == DiscoveryState.AVAILABLE
    assert result.release.publisher_version == "20990720_clinical_trials_ctgov.zip"
    assert result.release.source_data_period == "2099-07-20"
    assert result.release.byte_size == round(2.33 * 1024**3)


def test_aact_markup_change_is_a_discovery_error() -> None:
    with pytest.raises(DiscoveryError, match="PostgreSQL snapshot card"):
        parse_aact_downloads(
            b"<html><body>No cards</body></html>",
            SOURCE_REGISTRY["aact_clinical_trials_snapshot"],
        )


def test_network_failure_marks_every_source_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_fetch(url: str, timeout: float = 30.0) -> bytes:
        raise urllib.error.URLError("publisher offline")

    monkeypatch.setattr("pipeline.discovery.fetch_metadata", fail_fetch)

    results = discover_all()

    assert set(results) == set(SOURCE_REGISTRY)
    assert {result.state for result in results.values()} == {DiscoveryState.UNAVAILABLE}
    assert all("publisher offline" in result.error_summary for result in results.values())


def test_manifest_round_trip_and_schema_versioning(tmp_path: Path) -> None:
    manifest = _active_manifest("open_payments_general", "publisher-v1")
    store = ManifestStore(tmp_path / "manifests.json")
    store.save(ManifestDocument(manifests=[manifest]))

    loaded = store.load()
    assert loaded.to_dict() == ManifestDocument(manifests=[manifest]).to_dict()
    assert loaded.manifests[0].proves_active_installation

    payload = loaded.to_dict()
    payload["schema_version"] = 2
    with pytest.raises(ValueError, match="Unsupported manifest schema_version"):
        ManifestDocument.from_dict(payload)


def test_status_decisions_cover_current_stale_unknown_and_unavailable() -> None:
    source_id = "open_payments_general"
    spec = SOURCE_REGISTRY[source_id]
    discovery = _available(source_id)

    current = evaluate_source(
        spec,
        discovery,
        ManifestDocument(manifests=[_active_manifest(source_id, "publisher-v2")]),
    )
    stale = evaluate_source(
        spec,
        discovery,
        ManifestDocument(manifests=[_active_manifest(source_id, "publisher-v1")]),
    )
    unknown = evaluate_source(spec, discovery, ManifestDocument())
    unavailable = evaluate_source(
        spec,
        DiscoveryResult(
            source_id,
            DiscoveryState.UNAVAILABLE,
            "2099-07-20T00:00:00+00:00",
            error_summary="publisher timeout",
        ),
        ManifestDocument(),
    )

    assert current.freshness_status == FreshnessStatus.CURRENT
    assert stale.freshness_status == FreshnessStatus.STALE
    assert unknown.freshness_status == FreshnessStatus.UNKNOWN
    assert unknown.installed_version is None
    assert unavailable.freshness_status == FreshnessStatus.UNAVAILABLE

    unavailable_with_manifest = evaluate_source(
        spec,
        DiscoveryResult(
            source_id,
            DiscoveryState.UNAVAILABLE,
            "2099-07-20T00:00:00+00:00",
            error_summary="publisher timeout",
        ),
        ManifestDocument(manifests=[_active_manifest(source_id, "publisher-v1")]),
    )
    assert unavailable_with_manifest.installed_version == "publisher-v1"
    assert unavailable_with_manifest.ingestion_timestamp == "2099-07-20T01:00:00+00:00"


def test_cli_json_output_and_healthy_stale_unknown_exit_codes(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    manifest_path = tmp_path / "manifests.json"

    unknown_code = main(
        ["status", "--offline", "--json", "--manifest", str(manifest_path)]
    )
    unknown_payload = json.loads(capsys.readouterr().out)
    assert unknown_code == EXIT_STALE_OR_UNKNOWN
    assert unknown_payload["summary"]["unknown"] == len(SOURCE_REGISTRY)

    discoveries = discover_all(fixture_dir=FIXTURES)
    healthy_document = _manifest_for_discoveries(discoveries)
    ManifestStore(manifest_path).save(healthy_document)
    healthy_code = main(
        ["status", "--offline", "--json", "--manifest", str(manifest_path)]
    )
    healthy_payload = json.loads(capsys.readouterr().out)
    assert healthy_code == EXIT_HEALTHY
    assert healthy_payload["summary"]["current"] == len(SOURCE_REGISTRY)

    healthy_document.manifests[0].publisher_version = "older-publisher-version"
    ManifestStore(manifest_path).save(healthy_document)
    stale_code = main(
        ["status", "--offline", "--json", "--manifest", str(manifest_path)]
    )
    stale_payload = json.loads(capsys.readouterr().out)
    assert stale_code == EXIT_STALE_OR_UNKNOWN
    assert stale_payload["summary"]["stale"] == 1


def test_cli_discovery_failure_exit_code(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    fixture_dir = tmp_path / "publisher_metadata"
    shutil.copytree(FIXTURES, fixture_dir)
    (fixture_dir / "aact.html").write_text("<html>changed</html>")

    code = main(["status", "--fixtures", str(fixture_dir), "--json"])
    payload = json.loads(capsys.readouterr().out)

    assert code == EXIT_DISCOVERY_FAILURE
    assert payload["summary"]["unavailable"] == 1
    aact = next(
        source
        for source in payload["sources"]
        if source["source_id"] == "aact_clinical_trials_snapshot"
    )
    assert aact["discovery_state"] == "discovery_error"


def test_status_does_not_mutate_duckdb_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database = tmp_path / "active.duckdb"
    database.write_bytes(b"immutable warehouse sentinel")
    before = (database.read_bytes(), database.stat().st_mtime_ns)
    monkeypatch.setenv("DUCKDB_PATH", str(database))

    code = main(
        [
            "status",
            "--offline",
            "--json",
            "--manifest",
            str(tmp_path / "missing-manifests.json"),
        ]
    )
    capsys.readouterr()

    assert code == EXIT_STALE_OR_UNKNOWN
    assert (database.read_bytes(), database.stat().st_mtime_ns) == before
