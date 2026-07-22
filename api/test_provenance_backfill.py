import hashlib
import json
import sys
from dataclasses import replace
from pathlib import Path

import duckdb
import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT))

from pipeline.manifests import ManifestDocument, ManifestStore, PromotionState
from pipeline.provenance_backfill import (
    AssessmentState,
    BackfillError,
    BackfillSpec,
    DatabaseKind,
    SourceEvidence,
    build_backfill,
    main,
)
from pipeline.source_registry import SOURCE_REGISTRY


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _warehouse(tmp_path: Path) -> Path:
    path = tmp_path / "warehouse.duckdb"
    connection = duckdb.connect(str(path))
    connection.execute("create table raw_open_payments_general(npi varchar, amount double)")
    connection.execute("insert into raw_open_payments_general values ('1234567890', 12.5)")
    connection.close()
    return path


def _complete_sources(proven: SourceEvidence) -> tuple[SourceEvidence, ...]:
    rows = [proven]
    for source_id in SOURCE_REGISTRY:
        if source_id == proven.source_id:
            continue
        rows.append(
            SourceEvidence(
                source_id=source_id,
                assessment=AssessmentState.NOT_INSTALLED,
                reason="Audit found no compatible installed release.",
            )
        )
    return tuple(rows)


def _spec(warehouse: Path, source: SourceEvidence) -> BackfillSpec:
    return BackfillSpec(
        target_warehouse_sha256=_sha256(warehouse),
        promotion_timestamp="2099-07-21T00:00:00+00:00",
        sources=_complete_sources(source),
    )


def _proven_source(artifact: Path) -> SourceEvidence:
    return SourceEvidence(
        source_id="open_payments_general",
        assessment=AssessmentState.PROVEN,
        reason="Retained download log, artifact, and warehouse table agree.",
        publisher_version="PGYR2098_P07012099_06012099:general",
        source_data_period="2098-01-01/2098-12-31",
        publisher_release_timestamp="2099-07-01T00:00:00+00:00",
        discovery_timestamp="2099-07-20T00:00:00+00:00",
        retrieval_timestamp="2099-07-20T01:00:00+00:00",
        source_url="https://download.cms.gov/openpayments/test/open-payments.csv",
        artifact_path=str(artifact),
        byte_size=artifact.stat().st_size,
        sha256=_sha256(artifact),
        table_counts={"raw_open_payments_general": 1},
    )


def test_backfill_proves_exact_artifact_and_read_only_warehouse(tmp_path: Path) -> None:
    warehouse = _warehouse(tmp_path)
    before = _sha256(warehouse)
    artifact = tmp_path / "source.csv"
    artifact.write_text("npi,amount\n1234567890,12.5\n")

    result = build_backfill(
        _spec(warehouse, _proven_source(artifact)), warehouse_path=warehouse
    )

    assert _sha256(warehouse) == before
    assert result.to_dict()["summary"] == {
        "proven": 1,
        "unresolved": 0,
        "not_installed": len(SOURCE_REGISTRY) - 1,
    }
    manifest, reason = result.manifest.proven_active("open_payments_general")
    assert reason is None
    assert manifest.publisher_version.endswith(":general")
    assert manifest.row_counts == {"raw_open_payments_general": 1}
    assert manifest.schema_fingerprint.startswith("sha256:")
    assert manifest.promotion_state == PromotionState.ACTIVE
    assert "Retrospective provenance backfill" in manifest.operator_summary


@pytest.mark.parametrize("failure", ["hash", "rows"])
def test_backfill_rejects_artifact_or_row_mismatch(
    tmp_path: Path, failure: str
) -> None:
    warehouse = _warehouse(tmp_path)
    artifact = tmp_path / "source.csv"
    artifact.write_text("npi,amount\n1234567890,12.5\n")
    source = _proven_source(artifact)
    if failure == "hash":
        source = replace(source, sha256="a" * 64)
    else:
        source = replace(
            source, table_counts={"raw_open_payments_general": 2}
        )

    with pytest.raises(BackfillError, match="SHA-256|row count"):
        build_backfill(_spec(warehouse, source), warehouse_path=warehouse)


def test_backfill_requires_every_registry_source_to_be_assessed(tmp_path: Path) -> None:
    warehouse = _warehouse(tmp_path)
    artifact = tmp_path / "source.csv"
    artifact.write_text("npi,amount\n1234567890,12.5\n")
    incomplete = BackfillSpec(
        target_warehouse_sha256=_sha256(warehouse),
        promotion_timestamp="2099-07-21T00:00:00+00:00",
        sources=(_proven_source(artifact),),
    )

    with pytest.raises(BackfillError, match="does not assess every registry source"):
        build_backfill(incomplete, warehouse_path=warehouse)


def test_aact_requires_read_only_database_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    warehouse = _warehouse(tmp_path)
    artifact = tmp_path / "aact.zip"
    artifact.write_bytes(b"small fixture")
    source = SourceEvidence(
        source_id="aact_clinical_trials_snapshot",
        assessment=AssessmentState.PROVEN,
        reason="Snapshot checksum, marker, restore, and live table counts agree.",
        publisher_version="20990720_clinical_trials_ctgov.zip",
        source_data_period="2099-07-20",
        publisher_release_timestamp="2099-07-20T00:00:00+00:00",
        discovery_timestamp="2099-07-20T00:01:00+00:00",
        retrieval_timestamp="2099-07-20T00:02:00+00:00",
        source_url="https://aact.ctti-clinicaltrials.org/static/test/aact.zip",
        artifact_path=str(artifact),
        byte_size=artifact.stat().st_size,
        sha256=_sha256(artifact),
        database_kind=DatabaseKind.AACT_POSTGRES,
        table_counts={"ctgov.studies": 5},
    )
    seen = {}

    def fake_postgres(url: str, evidence: SourceEvidence):
        seen["url"] = url
        return {"ctgov.studies": 5}, {"ctgov.studies": [("nct_id", "text")]}

    monkeypatch.setattr("pipeline.provenance_backfill._postgres_evidence", fake_postgres)
    result = build_backfill(
        _spec(warehouse, source),
        warehouse_path=warehouse,
        aact_database_url="postgresql://read-only.example.invalid/aact",
    )

    assert seen["url"].startswith("postgresql://")
    assert result.manifest.proven_active(source.source_id)[0] is not None


def test_cli_failure_does_not_replace_existing_outputs_or_warehouse(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    warehouse = _warehouse(tmp_path)
    warehouse_sha = _sha256(warehouse)
    artifact = tmp_path / "source.csv"
    artifact.write_text("npi,amount\n1234567890,12.5\n")
    source = _proven_source(artifact)
    payload = {
        "schema_version": 1,
        "target_warehouse_sha256": warehouse_sha,
        "promotion_timestamp": "2099-07-21T00:00:00+00:00",
        "sources": [
            {
                "source_id": row.source_id,
                "assessment": row.assessment.value,
                "reason": row.reason,
                "publisher_version": row.publisher_version,
                "source_data_period": row.source_data_period,
                "publisher_release_timestamp": row.publisher_release_timestamp,
                "discovery_timestamp": row.discovery_timestamp,
                "retrieval_timestamp": row.retrieval_timestamp,
                "source_url": row.source_url,
                "artifact_path": row.artifact_path,
                "byte_size": row.byte_size,
                "sha256": ("b" * 64 if row.source_id == source.source_id else row.sha256),
                "database_kind": row.database_kind.value,
                "table_counts": row.table_counts,
            }
            for row in _complete_sources(source)
        ],
    }
    evidence = tmp_path / "evidence.json"
    evidence.write_text(json.dumps(payload))
    manifest_output = tmp_path / "candidate-manifest.json"
    audit_output = tmp_path / "audit.json"
    manifest_output.write_text("preserve manifest")
    audit_output.write_text("preserve audit")

    exit_code = main(
        [
            "--evidence",
            str(evidence),
            "--warehouse",
            str(warehouse),
            "--manifest-output",
            str(manifest_output),
            "--audit-output",
            str(audit_output),
            "--json",
        ]
    )

    response = json.loads(capsys.readouterr().out)
    assert exit_code == 2
    assert "SHA-256" in response["error"]
    assert manifest_output.read_text() == "preserve manifest"
    assert audit_output.read_text() == "preserve audit"
    assert _sha256(warehouse) == warehouse_sha


def test_cli_writes_candidate_outputs_only_after_validation(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    warehouse = _warehouse(tmp_path)
    artifact = tmp_path / "source.csv"
    artifact.write_text("npi,amount\n1234567890,12.5\n")
    source = _proven_source(artifact)
    payload = {
        "schema_version": 1,
        "target_warehouse_sha256": _sha256(warehouse),
        "promotion_timestamp": "2099-07-21T00:00:00+00:00",
        "sources": [
            {
                "source_id": row.source_id,
                "assessment": row.assessment.value,
                "reason": row.reason,
                "publisher_version": row.publisher_version,
                "source_data_period": row.source_data_period,
                "publisher_release_timestamp": row.publisher_release_timestamp,
                "discovery_timestamp": row.discovery_timestamp,
                "retrieval_timestamp": row.retrieval_timestamp,
                "source_url": row.source_url,
                "artifact_path": row.artifact_path,
                "byte_size": row.byte_size,
                "sha256": row.sha256,
                "database_kind": row.database_kind.value,
                "table_counts": row.table_counts,
            }
            for row in _complete_sources(source)
        ],
    }
    evidence = tmp_path / "evidence.json"
    evidence.write_text(json.dumps(payload))
    manifest_output = tmp_path / "candidate-manifest.json"
    audit_output = tmp_path / "audit.json"

    exit_code = main(
        [
            "--evidence",
            str(evidence),
            "--warehouse",
            str(warehouse),
            "--manifest-output",
            str(manifest_output),
            "--audit-output",
            str(audit_output),
        ]
    )

    assert exit_code == 0
    assert capsys.readouterr().out == ""
    assert ManifestStore(manifest_output).load().proven_active(source.source_id)[0]
    assert json.loads(audit_output.read_text())["summary"]["proven"] == 1
