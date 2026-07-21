import sys
import zipfile
from pathlib import Path

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT))

from pipeline.acquisition import AcquisitionError
from pipeline.archive_acquisition import ARCHIVE_PROFILES, inspect_archive
from pipeline.data_platform import EXIT_HEALTHY, main

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
