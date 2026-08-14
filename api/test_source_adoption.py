import csv
import io
import json
import sys
from dataclasses import replace
from pathlib import Path

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT))

from pipeline.acquisition import CMS_CSV_PROFILES, inspect_cms_csv
from pipeline.data_platform import EXIT_HEALTHY, main
from pipeline.manifests import (
    ManifestDocument,
    ManifestStore,
    PromotionState,
    RunManifest,
    ValidationState,
)
from pipeline.source_adoption import (
    SourceAdoptionError,
    adopt_validated_source_run,
)
from pipeline.source_registry import SOURCE_REGISTRY

SOURCE_ID = "cms_revalidation_group_reassignment"
RUN_ID = "20260721T220859Z-0353abdb"


def _source_payload() -> bytes:
    values = {
        "Group PAC ID": "PAC-1",
        "Group Enrollment ID": "GROUP-1",
        "Group Legal Business Name": "Cardio Group",
        "Group State Code": "CA",
        "Group Reassignments and Physician Assistants": "25",
        "Individual NPI": "1234567890",
        "Individual State Code": "CA",
    }
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, lineterminator="\n")
    columns = CMS_CSV_PROFILES[SOURCE_ID].required_columns
    writer.writerow(columns)
    writer.writerow([values[column] for column in columns])
    return stream.getvalue().encode()


def _retained_source(tmp_path: Path) -> tuple[Path, Path, Path, RunManifest]:
    retained = tmp_path / "retained"
    retained.mkdir()
    artifact = retained / "source.csv"
    artifact.write_bytes(_source_payload())
    inspection = inspect_cms_csv(artifact, profile=CMS_CSV_PROFILES[SOURCE_ID])
    manifest = RunManifest(
        run_id=RUN_ID,
        release_id="cms_revalidation_group_reassignment-2026-07-fixture",
        source_id=SOURCE_ID,
        publisher=SOURCE_REGISTRY[SOURCE_ID].publisher.value,
        publisher_version="cms-resource:fixture",
        source_data_period="2026-07-01/2026-07-31",
        publisher_release_timestamp="2026-07-20T00:00:00+00:00",
        discovery_timestamp="2026-07-21T22:00:00+00:00",
        retrieval_timestamp="2026-07-21T22:08:59+00:00",
        source_url="https://data.cms.gov/example/source.csv",
        byte_size=inspection.byte_size,
        sha256=inspection.sha256,
        schema_fingerprint=inspection.schema_fingerprint,
        source_encoding=inspection.source_encoding,
        row_counts={
            "source_rows": inspection.row_count,
            "invalid_identifier_rows": inspection.invalid_identifier_rows,
        },
        pipeline_code_commit="a" * 40,
        validation_state=ValidationState.PASSED,
        validation_timestamp="2026-07-21T22:10:00+00:00",
    )
    source_manifest = retained / "manifest.json"
    ManifestStore(source_manifest).save(ManifestDocument(manifests=[manifest]))
    production = replace(
        manifest,
        promotion_state=PromotionState.ACTIVE,
        promotion_timestamp="2026-08-14T20:13:11+00:00",
        active_release_id=manifest.release_id,
        operator_summary="Installed by verified production deployment",
    )
    production_evidence = retained / "source-manifests.json"
    ManifestStore(production_evidence).save(
        ManifestDocument(manifests=[production])
    )
    return source_manifest, artifact, production_evidence, manifest


def test_adopt_validated_source_run_is_verified_read_only_and_idempotent(
    tmp_path: Path,
) -> None:
    source_manifest, source_artifact, production_evidence, manifest = (
        _retained_source(tmp_path)
    )
    data_root = tmp_path / "managed"

    first = adopt_validated_source_run(
        data_root=data_root,
        source_manifest_path=source_manifest,
        source_artifact_path=source_artifact,
        production_evidence_path=production_evidence,
        expected_source_id=SOURCE_ID,
        expected_run_id=RUN_ID,
    )
    second = adopt_validated_source_run(
        data_root=data_root,
        source_manifest_path=source_manifest,
        source_artifact_path=source_artifact,
        production_evidence_path=production_evidence,
        expected_source_id=SOURCE_ID,
        expected_run_id=RUN_ID,
    )

    assert first.adopted is True
    assert second.adopted is False
    assert first.artifact_path.read_bytes() == source_artifact.read_bytes()
    assert first.artifact_path.stat().st_mode & 0o222 == 0
    assert not first.artifact_path.with_name("source.csv.partial").exists()
    assert ManifestStore(first.run_manifest_path).load().manifests[0].to_dict() == (
        manifest.to_dict()
    )
    managed = ManifestStore(first.manifest_store_path).load().manifests
    assert [row.run_id for row in managed] == [RUN_ID]


def test_adopt_validated_source_run_rejects_changed_source_bytes(
    tmp_path: Path,
) -> None:
    source_manifest, source_artifact, production_evidence, _ = _retained_source(
        tmp_path
    )
    source_artifact.write_bytes(source_artifact.read_bytes() + b"changed")

    with pytest.raises(
        SourceAdoptionError,
        match="does not match its acquisition manifest",
    ):
        adopt_validated_source_run(
            data_root=tmp_path / "managed",
            source_manifest_path=source_manifest,
            source_artifact_path=source_artifact,
            production_evidence_path=production_evidence,
            expected_source_id=SOURCE_ID,
            expected_run_id=RUN_ID,
        )


def test_adopt_validated_source_run_rejects_mismatched_production_evidence(
    tmp_path: Path,
) -> None:
    source_manifest, source_artifact, production_evidence, manifest = (
        _retained_source(tmp_path)
    )
    production = replace(
        manifest,
        source_data_period="2026-06-01/2026-06-30",
        promotion_state=PromotionState.ACTIVE,
        promotion_timestamp="2026-08-14T20:13:11+00:00",
        active_release_id=manifest.release_id,
    )
    ManifestStore(production_evidence).save(
        ManifestDocument(manifests=[production])
    )

    with pytest.raises(SourceAdoptionError, match="does not match"):
        adopt_validated_source_run(
            data_root=tmp_path / "managed",
            source_manifest_path=source_manifest,
            source_artifact_path=source_artifact,
            production_evidence_path=production_evidence,
            expected_source_id=SOURCE_ID,
            expected_run_id=RUN_ID,
        )


def test_adopt_validated_source_run_cli_is_staging_only_and_emits_evidence(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source_manifest, source_artifact, production_evidence, _ = _retained_source(
        tmp_path
    )

    exit_code = main(
        [
            "adopt-validated-source-run",
            "--environment",
            "staging",
            "--data-root",
            str(tmp_path / "managed"),
            "--source-manifest",
            str(source_manifest),
            "--source-artifact",
            str(source_artifact),
            "--production-evidence",
            str(production_evidence),
            "--expected-source-id",
            SOURCE_ID,
            "--expected-run-id",
            RUN_ID,
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == EXIT_HEALTHY
    assert payload["adopted"] is True
    assert payload["source_id"] == SOURCE_ID
    assert payload["run_id"] == RUN_ID
