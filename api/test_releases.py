import csv
import io
import json
import sys
from pathlib import Path

import duckdb
import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT))

from pipeline.acquisition import inspect_hospital_enrollments
from pipeline.data_platform import EXIT_HEALTHY, EXIT_RELEASE_FAILURE, main
from pipeline.manifests import (
    ManifestDocument,
    ManifestStore,
    PromotionState,
    RunManifest,
    ValidationState,
)
from pipeline.releases import (
    HOSPITAL_COLUMN_MAP,
    ReleaseError,
    WarehouseReleaseStore,
    build_warehouse_release,
    promote_staging_release,
    rollback_staging_release,
    sha256_file,
)
from pipeline.source_registry import SOURCE_REGISTRY

SOURCE_RUN_ID = "20990720T010000Z-hospital"
SOURCE_RELEASE_ID = "cms_hospital_enrollments-fixture"
CODE_COMMIT = "a" * 40


def _hospital_csv(*, header: tuple[str, ...] | None = None) -> bytes:
    columns = header or tuple(source for source, _ in HOSPITAL_COLUMN_MAP)
    values = {column: "" for column in columns}
    values.update(
        {
            "ENROLLMENT ID": "E100",
            "ENROLLMENT STATE": "CA",
            "PROVIDER TYPE CODE": "00-09",
            "PROVIDER TYPE TEXT": "PART A PROVIDER - HOSPITAL",
            "NPI": "1234567890",
            "MULTIPLE NPI FLAG": "N",
            "CCN": "050001",
            "ASSOCIATE ID": "A100",
            "ORGANIZATION NAME": "Example Hospital",
            "STATE": "CA",
            "ZIP CODE": "90001",
            "SUBGROUP - GENERAL": "Y",
        }
    )
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, lineterminator="\n")
    writer.writerow(columns)
    writer.writerow([values[column] for column in columns])
    return stream.getvalue().encode("utf-8")


def _stage_source(data_root: Path, payload: bytes | None = None) -> RunManifest:
    artifact = (
        data_root
        / "runs"
        / "cms_hospital_enrollments"
        / SOURCE_RUN_ID
        / "source.csv"
    )
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(payload or _hospital_csv())
    inspection = inspect_hospital_enrollments(artifact)
    manifest = RunManifest(
        run_id=SOURCE_RUN_ID,
        release_id=SOURCE_RELEASE_ID,
        source_id="cms_hospital_enrollments",
        publisher=SOURCE_REGISTRY["cms_hospital_enrollments"].publisher.value,
        publisher_version="cms-resource:10000000-0000-4000-8000-000000000009",
        source_data_period="2099-07-01/2099-07-31",
        publisher_release_timestamp="2099-07-14T00:00:00+00:00",
        discovery_timestamp="2099-07-20T00:00:00+00:00",
        retrieval_timestamp="2099-07-20T01:00:00+00:00",
        source_url="https://data.cms.gov/example/hospital-enrollments.csv",
        byte_size=inspection.byte_size,
        sha256=inspection.sha256,
        schema_fingerprint=inspection.schema_fingerprint,
        source_encoding=inspection.source_encoding,
        row_counts={"source_rows": inspection.row_count},
        pipeline_code_commit=CODE_COMMIT,
        validation_state=ValidationState.PASSED,
        validation_timestamp="2099-07-20T02:00:00+00:00",
    )
    ManifestStore(data_root / "manifests.json").save(
        ManifestDocument(manifests=[manifest])
    )
    return manifest


def _verified_backup(tmp_path: Path) -> tuple[Path, Path, str]:
    backup = tmp_path / "backup" / "provider_searcher.duckdb"
    backup.parent.mkdir(parents=True)
    connection = duckdb.connect(str(backup))
    try:
        connection.execute("CREATE TABLE core_providers (npi VARCHAR PRIMARY KEY)")
        connection.execute("INSERT INTO core_providers VALUES ('9999999999')")
        connection.execute("CREATE TABLE baseline_marker (value VARCHAR)")
        connection.execute("INSERT INTO baseline_marker VALUES ('preserved')")
        connection.execute(
            "CREATE TABLE raw_hospital_enrollments (npi VARCHAR, organization_name VARCHAR)"
        )
        connection.execute(
            "INSERT INTO raw_hospital_enrollments VALUES ('0000000000', 'Old data')"
        )
        connection.execute("CHECKPOINT")
    finally:
        connection.close()
    digest = sha256_file(backup)
    manifest_path = backup.parent / "backup-manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "backup_path": str(backup),
                "backup_identity": {"byte_size": backup.stat().st_size},
                "sha256": digest,
                "validation": {"read_only_open": "passed"},
            }
        )
    )
    return backup, manifest_path, digest


def _build(tmp_path: Path):
    data_root = tmp_path / "data"
    _stage_source(data_root)
    backup, backup_manifest, baseline_hash = _verified_backup(tmp_path)
    result = build_warehouse_release(
        data_root=data_root,
        source_run_id=SOURCE_RUN_ID,
        backup_manifest_path=backup_manifest,
        code_commit=CODE_COMMIT,
    )
    return data_root, backup, baseline_hash, result


def test_build_release_copies_baseline_loads_source_and_records_provenance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    active = tmp_path / "active.duckdb"
    active.write_bytes(b"active warehouse sentinel")
    active_before = (active.read_bytes(), active.stat().st_mtime_ns)
    monkeypatch.setenv("DUCKDB_PATH", str(active))

    data_root, backup, baseline_hash, result = _build(tmp_path)

    assert result.database_path != backup
    assert sha256_file(backup) == baseline_hash
    assert (active.read_bytes(), active.stat().st_mtime_ns) == active_before
    assert not result.database_path.with_suffix(".duckdb.partial").exists()
    assert result.release.validation_state == ValidationState.PASSED
    assert result.release.promotion_state == PromotionState.NOT_PROMOTED
    assert result.release.pipeline_code_commit == CODE_COMMIT
    assert result.release.table_counts["raw_hospital_enrollments"] == 1
    assert result.release.table_counts["core_providers"] == 1
    assert result.release.sha256 == sha256_file(result.database_path)

    connection = duckdb.connect(str(result.database_path), read_only=True)
    try:
        row = connection.execute(
            """
            SELECT npi, organization_name, source_run_id, source_data_period
            FROM raw_hospital_enrollments
            """
        ).fetchone()
        marker = connection.execute("SELECT value FROM baseline_marker").fetchone()[0]
    finally:
        connection.close()
    assert row == (
        "1234567890",
        "Example Hospital",
        SOURCE_RUN_ID,
        "2099-07-01/2099-07-31",
    )
    assert marker == "preserved"

    stored = WarehouseReleaseStore(data_root / "warehouse-releases.json").load()
    assert stored.releases[0].to_dict() == result.release.to_dict()
    per_release = json.loads(result.release_manifest_path.read_text())
    assert per_release["release"] == result.release.to_dict()


def test_schema_ddl_matches_canonical_raw_hospital_loader() -> None:
    connection = duckdb.connect(":memory:")
    try:
        connection.execute((REPOSITORY_ROOT / "schema" / "ddl.sql").read_text())
        columns = [
            row[1]
            for row in connection.execute(
                "PRAGMA table_info('raw_hospital_enrollments')"
            ).fetchall()
        ]
    finally:
        connection.close()

    assert columns == [
        *(target for _, target in HOSPITAL_COLUMN_MAP),
        "source_run_id",
        "source_release_id",
        "source_data_period",
        "ingested_at",
    ]


def test_build_release_fails_closed_on_publisher_header_change(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    expected = tuple(source for source, _ in HOSPITAL_COLUMN_MAP)
    changed = (expected[1], expected[0], *expected[2:])
    _stage_source(data_root, _hospital_csv(header=changed))
    _, backup_manifest, _ = _verified_backup(tmp_path)

    with pytest.raises(ReleaseError, match="column order changed"):
        build_warehouse_release(
            data_root=data_root,
            source_run_id=SOURCE_RUN_ID,
            backup_manifest_path=backup_manifest,
            code_commit=CODE_COMMIT,
        )

    release = WarehouseReleaseStore(
        data_root / "warehouse-releases.json"
    ).load().releases[0]
    assert release.validation_state == ValidationState.FAILED
    assert release.promotion_state == PromotionState.NOT_PROMOTED


def test_staging_promotion_rollback_and_repromotion_are_audited(
    tmp_path: Path,
) -> None:
    data_root, _, _, result = _build(tmp_path)

    promoted = promote_staging_release(
        data_root, result.release.warehouse_release_id
    )
    pointer = data_root / "staging" / "warehouse-current"
    assert promoted["state"] == "completed"
    assert pointer.resolve() == result.database_path.resolve()
    active_manifest = ManifestStore(data_root / "manifests.json").load().manifests[0]
    assert active_manifest.proves_active_installation
    assert (
        WarehouseReleaseStore(data_root / "warehouse-releases.json")
        .load()
        .releases[0]
        .promotion_state
        == PromotionState.ACTIVE
    )

    rolled_back = rollback_staging_release(data_root)
    assert rolled_back["state"] == "completed"
    assert not pointer.exists()
    rolled_back_manifest = ManifestStore(
        data_root / "manifests.json"
    ).load().manifests[0]
    assert rolled_back_manifest.promotion_state == PromotionState.ROLLED_BACK
    assert rolled_back_manifest.active_release_id is None

    promoted_again = promote_staging_release(
        data_root, result.release.warehouse_release_id
    )
    assert promoted_again["state"] == "completed"
    assert pointer.resolve() == result.database_path.resolve()
    journal = json.loads((data_root / "promotion-journal.json").read_text())
    assert [event["action"] for event in journal["events"]] == [
        "promote",
        "rollback",
        "promote",
    ]
    assert {event["state"] for event in journal["events"]} == {"completed"}


def test_promotion_failure_restores_pointer_and_manifest_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root, _, _, result = _build(tmp_path)
    original_save = ManifestStore.save
    calls = 0

    def fail_once(store: ManifestStore, document: ManifestDocument) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("simulated manifest write failure")
        original_save(store, document)

    monkeypatch.setattr(ManifestStore, "save", fail_once)

    with pytest.raises(ReleaseError, match="promotion rolled back"):
        promote_staging_release(data_root, result.release.warehouse_release_id)

    assert not (data_root / "staging" / "warehouse-current").exists()
    source = ManifestStore(data_root / "manifests.json").load().manifests[0]
    release = WarehouseReleaseStore(
        data_root / "warehouse-releases.json"
    ).load().releases[0]
    journal = json.loads((data_root / "promotion-journal.json").read_text())
    assert source.promotion_state == PromotionState.NOT_PROMOTED
    assert release.promotion_state == PromotionState.NOT_PROMOTED
    assert journal["events"][-1]["state"] == "rolled_back"


def test_unresolved_promotion_journal_blocks_another_transition(
    tmp_path: Path,
) -> None:
    data_root, _, _, result = _build(tmp_path)
    (data_root / "promotion-journal.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "events": [
                    {
                        "transaction_id": "interrupted",
                        "state": "pending",
                        "action": "promote",
                    }
                ],
            }
        )
    )

    with pytest.raises(ReleaseError, match="unresolved pending transaction"):
        promote_staging_release(data_root, result.release.warehouse_release_id)

    assert not (data_root / "staging" / "warehouse-current").exists()


def test_release_cli_json_and_exit_codes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    data_root = tmp_path / "data"
    _stage_source(data_root)
    _, backup_manifest, _ = _verified_backup(tmp_path)
    monkeypatch.setattr("pipeline.releases.pipeline_commit", lambda: CODE_COMMIT)

    build_code = main(
        [
            "build-release",
            "--environment",
            "staging",
            "--source-run-id",
            SOURCE_RUN_ID,
            "--backup-manifest",
            str(backup_manifest),
            "--data-root",
            str(data_root),
            "--json",
        ]
    )
    built = json.loads(capsys.readouterr().out)
    release_id = built["release"]["warehouse_release_id"]
    assert build_code == EXIT_HEALTHY

    promote_code = main(
        [
            "promote",
            "--environment",
            "staging",
            "--warehouse-release-id",
            release_id,
            "--data-root",
            str(data_root),
            "--json",
        ]
    )
    promoted = json.loads(capsys.readouterr().out)
    assert promote_code == EXIT_HEALTHY
    assert promoted["state"] == "completed"

    rollback_code = main(
        [
            "rollback",
            "--environment",
            "staging",
            "--data-root",
            str(data_root),
            "--json",
        ]
    )
    rolled_back = json.loads(capsys.readouterr().out)
    assert rollback_code == EXIT_HEALTHY
    assert rolled_back["state"] == "completed"

    failure_code = main(
        [
            "promote",
            "--environment",
            "staging",
            "--warehouse-release-id",
            "missing-release",
            "--data-root",
            str(data_root),
            "--json",
        ]
    )
    failure = json.loads(capsys.readouterr().out)
    assert failure_code == EXIT_RELEASE_FAILURE
    assert "missing-release" in failure["error"]


def test_release_cli_does_not_accept_a_production_environment(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as raised:
        main(
            [
                "promote",
                "--environment",
                "production",
                "--warehouse-release-id",
                "anything",
            ]
        )

    assert raised.value.code == 2
    assert "invalid choice: 'production'" in capsys.readouterr().err
