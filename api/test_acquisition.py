import hashlib
import json
import sys
from pathlib import Path

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT))

from pipeline.acquisition import (
    AcquisitionError,
    CMS_CSV_PROFILES,
    SUPPORTED_ACQUISITION_SOURCES,
    acquire_release,
    inspect_cms_csv,
    inspect_hospital_enrollments,
)
from pipeline.data_platform import EXIT_HEALTHY, main
from pipeline.discovery import ReleaseMetadata
from pipeline.manifests import ManifestStore, PromotionState, ValidationState

FIXTURES = REPOSITORY_ROOT / "pipeline" / "fixtures" / "publisher_metadata"
VALID_CSV = (
    b"ENROLLMENT ID,NPI,CCN,ORGANIZATION NAME,STATE\n"
    b"E1,1234567890,123456,Example Hospital,CA\n"
    b"E2,0987654321,654321,Second Hospital,NY\n"
)


class FakeResponse:
    def __init__(self, payload: bytes, *, url: str) -> None:
        self.payload = payload
        self.url = url
        self.offset = 0
        self.headers = {"Content-Length": str(len(payload))}

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def geturl(self) -> str:
        return self.url

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            size = len(self.payload) - self.offset
        chunk = self.payload[self.offset : self.offset + size]
        self.offset += len(chunk)
        return chunk


def _release() -> ReleaseMetadata:
    return ReleaseMetadata(
        source_id="cms_hospital_enrollments",
        publisher_version="cms-resource:10000000-0000-4000-8000-000000000009",
        source_data_period="2099-07-01/2099-07-31",
        publisher_release_timestamp="2099-07-14T00:00:00+00:00",
        source_url="https://data.cms.gov/example/hospital-enrollments.csv",
    )


def _cms_release(source_id: str) -> ReleaseMetadata:
    return ReleaseMetadata(
        source_id=source_id,
        publisher_version="cms-resource:10000000-0000-4000-8000-000000000001",
        source_data_period="2097-01-01/2097-12-31",
        publisher_release_timestamp="2099-05-21T00:00:00+00:00",
        source_url="https://data.cms.gov/example/source.csv",
    )


def _install_response(monkeypatch: pytest.MonkeyPatch, payload: bytes) -> None:
    monkeypatch.setattr(
        "pipeline.acquisition.urllib.request.urlopen",
        lambda request, timeout: FakeResponse(payload, url=request.full_url),
    )


def test_hospital_acquisition_is_atomic_hashed_validated_and_manifested(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install_response(monkeypatch, VALID_CSV)
    data_root = tmp_path / "data"
    manifest_path = data_root / "manifests.json"

    result = acquire_release(
        _release(),
        discovery_timestamp="2099-07-20T00:00:00+00:00",
        data_root=data_root,
        manifest_path=manifest_path,
        code_commit="a" * 40,
    )

    assert result.artifact_path.read_bytes() == VALID_CSV
    assert not result.artifact_path.with_name("source.csv.partial").exists()
    assert result.manifest.byte_size == len(VALID_CSV)
    assert result.manifest.sha256 == hashlib.sha256(VALID_CSV).hexdigest()
    assert result.manifest.schema_fingerprint.startswith("sha256:")
    assert result.manifest.source_encoding == "utf-8-sig"
    assert result.manifest.row_counts == {"source_rows": 2}
    assert result.manifest.validation_state == ValidationState.PASSED
    assert result.manifest.promotion_state == PromotionState.NOT_PROMOTED
    assert result.manifest.active_release_id is None

    stored = ManifestStore(manifest_path).load()
    run_document = ManifestStore(result.run_manifest_path).load()
    assert stored.manifests[0].to_dict() == result.manifest.to_dict()
    assert run_document.manifests[0].to_dict() == result.manifest.to_dict()


def test_hospital_schema_change_records_failed_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    changed_csv = b"ENROLLMENT ID,CCN,ORGANIZATION NAME,STATE\nE1,1,Example,CA\n"
    _install_response(monkeypatch, changed_csv)
    manifest_path = tmp_path / "manifests.json"

    with pytest.raises(AcquisitionError, match="missing required columns: NPI"):
        acquire_release(
            _release(),
            discovery_timestamp="2099-07-20T00:00:00+00:00",
            data_root=tmp_path,
            manifest_path=manifest_path,
            code_commit="b" * 40,
        )

    failed = ManifestStore(manifest_path).load().manifests[0]
    assert failed.validation_state == ValidationState.FAILED
    assert failed.failure_timestamp is not None
    assert failed.byte_size == len(changed_csv)
    assert failed.sha256 == hashlib.sha256(changed_csv).hexdigest()
    assert failed.error_summary == "Hospital Enrollments CSV is missing required columns: NPI"
    assert failed.promotion_state == PromotionState.NOT_PROMOTED


def test_hospital_acquisition_enforces_hard_byte_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install_response(monkeypatch, VALID_CSV)

    with pytest.raises(AcquisitionError, match="exceeds the 20 byte"):
        acquire_release(
            _release(),
            discovery_timestamp="2099-07-20T00:00:00+00:00",
            data_root=tmp_path,
            manifest_path=tmp_path / "manifests.json",
            max_bytes=20,
            code_commit="c" * 40,
        )

    failed = ManifestStore(tmp_path / "manifests.json").load().manifests[0]
    assert failed.validation_state == ValidationState.FAILED
    assert failed.byte_size is None


def test_hospital_validator_rejects_bad_npi(tmp_path: Path) -> None:
    artifact = tmp_path / "hospital.csv"
    artifact.write_bytes(
        b"ENROLLMENT ID,NPI,CCN,ORGANIZATION NAME,STATE\n"
        b"E1,not-an-npi,123456,Example Hospital,CA\n"
    )

    with pytest.raises(AcquisitionError, match="row 2 has an invalid NPI"):
        inspect_hospital_enrollments(artifact)


def test_hospital_validator_accepts_and_records_windows_1252(tmp_path: Path) -> None:
    artifact = tmp_path / "hospital.csv"
    artifact.write_bytes(
        b"ENROLLMENT ID,NPI,CCN,ORGANIZATION NAME,STATE\n"
        b"E1,1234567890,123456,MERCY HEALTH \xbf DILLER,CA\n"
    )

    inspection = inspect_hospital_enrollments(artifact)

    assert inspection.row_count == 1
    assert inspection.source_encoding == "cp1252"


@pytest.mark.parametrize("source_id", sorted(SUPPORTED_ACQUISITION_SOURCES))
def test_every_cms_source_has_a_bounded_acquisition_profile(source_id: str) -> None:
    profile = CMS_CSV_PROFILES[source_id]

    assert profile.required_columns
    assert profile.identifier_column in profile.required_columns
    assert profile.max_download_bytes > 0


def test_generic_cms_acquisition_accepts_case_stable_schema_and_records_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = (
        b"Prscrbr_NPI,Tot_Clms,Tot_Drug_Cst,Brnd_Tot_Clms,Gnrc_Tot_Clms,"
        b"Opioid_Prscrbr_Rate\n"
        b"1234567890,12,34.50,4,8,1.25\n"
    )
    _install_response(monkeypatch, payload)

    result = acquire_release(
        _cms_release("cms_part_d_by_provider"),
        discovery_timestamp="2099-07-20T00:00:00+00:00",
        data_root=tmp_path,
        manifest_path=tmp_path / "manifests.json",
        code_commit="d" * 40,
    )

    assert result.manifest.row_counts == {"source_rows": 1}
    assert result.manifest.validation_state == ValidationState.PASSED
    assert result.manifest.sha256 == hashlib.sha256(payload).hexdigest()


def test_generic_cms_validator_rejects_missing_transform_contract_column(
    tmp_path: Path,
) -> None:
    artifact = tmp_path / "part-d.csv"
    artifact.write_bytes(
        b"Prscrbr_NPI,Tot_Clms,Tot_Drug_Cst,Brnd_Tot_Clms,Gnrc_Tot_Clms\n"
        b"1234567890,12,34.50,4,8\n"
    )

    with pytest.raises(AcquisitionError, match="missing required columns: Opioid_Prscrbr_Rate"):
        inspect_cms_csv(
            artifact,
            profile=CMS_CSV_PROFILES["cms_part_d_by_provider"],
        )


def test_generic_cms_validator_rejects_row_width_change(tmp_path: Path) -> None:
    artifact = tmp_path / "order.csv"
    artifact.write_bytes(
        b"NPI,LAST_NAME,FIRST_NAME,PARTB,DME,HHA,PMD,HOSPICE\n"
        b"1234567890,Last,First,Y,Y,Y,Y\n"
    )

    with pytest.raises(AcquisitionError, match="row 2 has 7 fields; expected 8"):
        inspect_cms_csv(
            artifact,
            profile=CMS_CSV_PROFILES["cms_order_and_referring"],
        )


def test_reassignment_records_publisher_row_without_individual_npi(tmp_path: Path) -> None:
    artifact = tmp_path / "reassignment.csv"
    artifact.write_bytes(
        b"Group PAC ID,Group Enrollment ID,Group Legal Business Name,Group State Code,"
        b"Group Reassignments and Physician Assistants,Individual NPI,Individual State Code\n"
        b"100,P100,Example Group,CA,4,,CA\n"
    )

    inspection = inspect_cms_csv(
        artifact,
        profile=CMS_CSV_PROFILES["cms_revalidation_group_reassignment"],
    )

    assert inspection.row_count == 1
    assert inspection.invalid_identifier_rows == 1


@pytest.mark.parametrize(
    ("source_id", "payload"),
    [
        (
            "cms_pecos_reassignment",
            b"REASGN_BNFT_ENRLMT_ID,RCV_BNFT_ENRLMT_ID\n"
            b"I20031103000001,O20031216000213\n",
        ),
        (
            "cms_pecos_practice_location",
            b"ENRLMT_ID,CITY_NAME,STATE_CD,ZIP_CD\n"
            b"O20031216000213,LOS ANGELES,CA,90048\n",
        ),
    ],
)
def test_ppef_relational_subfiles_accept_pecos_enrollment_ids(
    tmp_path: Path,
    source_id: str,
    payload: bytes,
) -> None:
    artifact = tmp_path / f"{source_id}.csv"
    artifact.write_bytes(payload)

    inspection = inspect_cms_csv(
        artifact,
        profile=CMS_CSV_PROFILES[source_id],
    )

    assert inspection.row_count == 1


def test_ppef_relational_subfile_rejects_invalid_enrollment_id(tmp_path: Path) -> None:
    artifact = tmp_path / "ppef-reassignment.csv"
    artifact.write_bytes(
        b"REASGN_BNFT_ENRLMT_ID,RCV_BNFT_ENRLMT_ID\n"
        b"not-an-enrollment,O20031216000213\n"
    )

    with pytest.raises(AcquisitionError, match="invalid PECOS enrollment ID"):
        inspect_cms_csv(
            artifact,
            profile=CMS_CSV_PROFILES["cms_pecos_reassignment"],
        )


def test_acquire_dry_run_uses_fixtures_and_writes_nothing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    data_root = tmp_path / "staging"
    database = tmp_path / "active.duckdb"
    database.write_bytes(b"active warehouse sentinel")
    before = (database.read_bytes(), database.stat().st_mtime_ns)
    monkeypatch.setenv("DUCKDB_PATH", str(database))

    code = main(
        [
            "acquire",
            "cms_hospital_enrollments",
            "--fixtures",
            str(FIXTURES),
            "--dry-run",
            "--json",
            "--data-root",
            str(data_root),
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == EXIT_HEALTHY
    assert payload["dry_run"] is True
    assert payload["wrote_files"] is False
    assert payload["source_id"] == "cms_hospital_enrollments"
    assert not data_root.exists()
    assert (database.read_bytes(), database.stat().st_mtime_ns) == before


@pytest.mark.parametrize("source_id", sorted(SUPPORTED_ACQUISITION_SOURCES))
def test_acquire_dry_run_discovers_every_supported_cms_source_without_writes(
    source_id: str,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    data_root = tmp_path / source_id

    code = main(
        [
            "acquire",
            source_id,
            "--fixtures",
            str(FIXTURES),
            "--dry-run",
            "--json",
            "--data-root",
            str(data_root),
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == EXIT_HEALTHY
    assert payload["source_id"] == source_id
    assert payload["dry_run"] is True
    assert payload["wrote_files"] is False
    assert not data_root.exists()
