import hashlib
import json
import shutil
import sys
import zipfile
from pathlib import Path

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT))

from pipeline.acquisition import AcquisitionError
from pipeline import archive_acquisition, data_platform
from pipeline.archive_acquisition import ARCHIVE_PROFILES, inspect_archive
from pipeline.data_platform import EXIT_HEALTHY, main
from pipeline.discovery import discover_all
from pipeline.manifests import ManifestStore

FIXTURES = REPOSITORY_ROOT / "pipeline" / "fixtures" / "publisher_metadata"


def _zip(path: Path, members: dict[str, bytes]) -> None:
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, payload in members.items():
            archive.writestr(name, payload)


def test_nppes_archive_requires_v2_provider_csv_and_records_shape(tmp_path: Path) -> None:
    path = tmp_path / "nppes.zip"
    _zip(
        path,
        {
            "npidata_pfile_20260713-20260719.csv": b"NPI,Entity Type Code\n1234567890,1\n",
            "npidata_pfile_20260713-20260719_fileheader.csv": b"name\nNPI\n",
        },
    )

    result = inspect_archive(path, ARCHIVE_PROFILES["nppes_weekly_incremental_v2"])

    assert result.member_count == 2
    assert result.uncompressed_bytes > 0
    assert result.schema_fingerprint.startswith("sha256:")


def test_aact_archive_requires_dump_and_dictionary(tmp_path: Path) -> None:
    path = tmp_path / "aact.zip"
    _zip(path, {"postgres.dmp": b"dump"})

    with pytest.raises(AcquisitionError, match="missing required member pattern"):
        inspect_archive(path, ARCHIVE_PROFILES["aact_clinical_trials_snapshot"])


@pytest.mark.parametrize(
    ("source_id", "member"),
    [
        ("open_payments_general", "OP_DTL_GNRL_PGYR2025.csv"),
        ("open_payments_research", "OP_DTL_RSRCH_PGYR2025.csv"),
        ("open_payments_ownership", "OP_DTL_OWNRSHP_PGYR2025.csv"),
    ],
)
def test_open_payments_category_member_contract(
    source_id: str, member: str, tmp_path: Path
) -> None:
    path = tmp_path / f"{source_id}.zip"
    _zip(path, {member: b"header\nvalue\n"})

    assert inspect_archive(path, ARCHIVE_PROFILES[source_id]).member_count == 1


def test_archive_rejects_path_traversal_member(tmp_path: Path) -> None:
    path = tmp_path / "unsafe.zip"
    _zip(
        path,
        {
            "../npidata_pfile_20260713-20260719.csv": b"unsafe",
        },
    )

    with pytest.raises(AcquisitionError, match="unsafe member path"):
        inspect_archive(path, ARCHIVE_PROFILES["nppes_weekly_incremental_v2"])


@pytest.mark.parametrize("source_id", sorted(ARCHIVE_PROFILES))
def test_archive_acquisition_dry_run_uses_discovery_fixtures_without_writes(
    source_id: str,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root = tmp_path / "data"

    code = main(
        [
            "acquire",
            source_id,
            "--fixtures",
            str(FIXTURES),
            "--dry-run",
            "--json",
            "--data-root",
            str(root),
        ]
    )

    assert code == EXIT_HEALTHY
    assert not root.exists()
    assert f'"source_id": "{source_id}"' in capsys.readouterr().out


@pytest.mark.parametrize(
    ("source_id", "member"),
    [
        ("nppes_monthly_v2", "npidata_pfile_20050523-20670712.csv"),
        (
            "nppes_weekly_incremental_v2",
            "npidata_pfile_20670713-20670719.csv",
        ),
    ],
)
def test_nppes_discovery_acquires_extracts_and_records_same_version_noop(
    source_id: str,
    member: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source_archive = tmp_path / "publisher.zip"
    provider_csv = b"NPI,Entity Type Code\n1234567890,1\n"
    _zip(source_archive, {member: provider_csv})
    discovery = discover_all(fixture_dir=FIXTURES)[source_id]
    assert discovery.release is not None

    monkeypatch.setattr(
        data_platform,
        "_discover_for_acquisition",
        lambda *_args, **_kwargs: discovery,
    )

    download_calls = 0

    def fake_download(_release, destination, **_kwargs):
        nonlocal download_calls
        download_calls += 1
        shutil.copyfile(source_archive, destination)
        payload = destination.read_bytes()
        return len(payload), hashlib.sha256(payload).hexdigest()

    monkeypatch.setattr(archive_acquisition, "download_artifact", fake_download)
    data_root = tmp_path / "data"
    command = ["acquire", source_id, "--data-root", str(data_root), "--json"]

    assert main(command) == EXIT_HEALTHY
    first_payload = json.loads(capsys.readouterr().out)
    manifest = first_payload["manifest"]
    run_directory = Path(first_payload["run_directory"])
    assert (run_directory / "npidata_pfile.csv").read_bytes() == provider_csv
    assert manifest["source_id"] == source_id
    assert manifest["publisher_version"] == discovery.release.publisher_version
    assert manifest["run_id"] == run_directory.name
    assert manifest["artifact_checksums"] == {
        "npidata_pfile.csv": hashlib.sha256(provider_csv).hexdigest(),
        "source.zip": manifest["sha256"],
    }

    assert main(command) == EXIT_HEALTHY
    second_payload = json.loads(capsys.readouterr().out)
    assert second_payload["status"] == "no_op"
    assert second_payload["reason"] == "publisher_version_already_acquired"
    assert second_payload["manifest"]["run_id"] == manifest["run_id"]
    assert download_calls == 1
    assert len(ManifestStore(data_root / "manifests.json").load().manifests) == 1
