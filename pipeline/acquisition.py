"""Immutable source acquisition for staged data-platform runs.

CMS CSV downloads are bounded, streamed to a ``.partial`` file, and atomically
renamed only after the response is complete. Each source is then checked against a
small explicit schema contract while every CSV row is parsed. This module never
opens DuckDB and never promotes a release.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import subprocess
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from .discovery import ReleaseMetadata, safe_error, utc_now
from .manifests import (
    ManifestDocument,
    ManifestStore,
    RunManifest,
    ValidationState,
)
from .source_registry import SOURCE_REGISTRY

DEFAULT_MAX_DOWNLOAD_BYTES = 100 * 1024 * 1024
DOWNLOAD_CHUNK_BYTES = 1024 * 1024
USER_AGENT = "cms-data-platform-acquisition/1.0 (+staged immutable download)"


@dataclass(frozen=True, slots=True)
class CsvAcquisitionProfile:
    label: str
    required_columns: tuple[str, ...]
    identifier_column: str
    max_download_bytes: int
    allow_invalid_identifiers: bool = False


GIB = 1024 * 1024 * 1024
CMS_CSV_PROFILES: dict[str, CsvAcquisitionProfile] = {
    "cms_physician_by_provider": CsvAcquisitionProfile(
        "Physician by Provider CSV",
        (
            "Rndrng_NPI",
            "Rndrng_Prvdr_Last_Org_Name",
            "Rndrng_Prvdr_First_Name",
            "Rndrng_Prvdr_MI",
            "Rndrng_Prvdr_Crdntls",
            "Rndrng_Prvdr_Ent_Cd",
            "Rndrng_Prvdr_St1",
            "Rndrng_Prvdr_St2",
            "Rndrng_Prvdr_City",
            "Rndrng_Prvdr_State_Abrvtn",
            "Rndrng_Prvdr_Zip5",
            "Rndrng_Prvdr_RUCA",
            "Rndrng_Prvdr_Cntry",
            "Rndrng_Prvdr_Type",
            "Rndrng_Prvdr_Mdcr_Prtcptg_Ind",
            "Tot_HCPCS_Cds",
            "Tot_Srvcs",
            "Tot_Benes",
            "Tot_Sbmtd_Chrg",
            "Tot_Mdcr_Alowd_Amt",
            "Tot_Mdcr_Pymt_Amt",
            "Tot_Mdcr_Stdzd_Amt",
            "Drug_Tot_Srvcs",
            "Med_Tot_Srvcs",
            "Bene_Avg_Age",
            "Bene_Avg_Risk_Scre",
            "Bene_Dual_Cnt",
            "Bene_CC_PH_Diabetes_V2_Pct",
            "Bene_CC_PH_Hypertension_V2_Pct",
            "Bene_CC_PH_HF_NonIHD_V2_Pct",
            "Bene_CC_PH_CKD_V2_Pct",
            "Bene_CC_PH_COPD_V2_Pct",
            "Bene_CC_PH_Cancer6_V2_Pct",
            "Bene_CC_BH_Depress_V1_Pct",
            "Bene_CC_BH_Alz_NonAlzdem_V2_Pct",
            "Bene_CC_PH_Afib_V2_Pct",
            "Bene_CC_PH_Hyperlipidemia_V2_Pct",
            "Bene_CC_PH_IschemicHeart_V2_Pct",
            "Bene_CC_PH_Osteoporosis_V2_Pct",
            "Bene_CC_PH_Arthritis_V2_Pct",
            "Bene_CC_PH_Stroke_TIA_V2_Pct",
        ),
        "Rndrng_NPI",
        2 * GIB,
    ),
    "cms_physician_by_provider_and_service": CsvAcquisitionProfile(
        "Physician by Provider and Service CSV",
        (
            "Rndrng_NPI",
            "Rndrng_Prvdr_Ent_Cd",
            "HCPCS_Cd",
            "HCPCS_Desc",
            "HCPCS_Drug_Ind",
            "Place_Of_Srvc",
            "Tot_Benes",
            "Tot_Srvcs",
            "Tot_Bene_Day_Srvcs",
            "Avg_Sbmtd_Chrg",
            "Avg_Mdcr_Alowd_Amt",
            "Avg_Mdcr_Pymt_Amt",
            "Avg_Mdcr_Stdzd_Amt",
        ),
        "Rndrng_NPI",
        12 * GIB,
    ),
    "cms_part_d_by_provider": CsvAcquisitionProfile(
        "Part D by Provider CSV",
        (
            "PRSCRBR_NPI",
            "Tot_Clms",
            "Tot_Drug_Cst",
            "Brnd_Tot_Clms",
            "Gnrc_Tot_Clms",
            "Opioid_Prscrbr_Rate",
        ),
        "PRSCRBR_NPI",
        2 * GIB,
    ),
    "cms_part_d_by_provider_and_drug": CsvAcquisitionProfile(
        "Part D by Provider and Drug CSV",
        (
            "Prscrbr_NPI",
            "Brnd_Name",
            "Gnrc_Name",
            "Tot_Clms",
            "Tot_30day_Fills",
            "Tot_Day_Suply",
            "Tot_Drug_Cst",
            "Tot_Benes",
            "GE65_Tot_Clms",
            "GE65_Tot_Drug_Cst",
            "GE65_Tot_Benes",
        ),
        "Prscrbr_NPI",
        12 * GIB,
    ),
    "cms_dme_by_referring_provider": CsvAcquisitionProfile(
        "DME by Referring Provider CSV",
        ("Rfrg_NPI", "Tot_Suplr_Clms", "Suplr_Mdcr_Pymt_Amt"),
        "Rfrg_NPI",
        2 * GIB,
    ),
    "cms_qpp_experience": CsvAcquisitionProfile(
        "QPP Experience CSV",
        (
            "provider key",
            "npi",
            "practice state or us territory",
            "practice size",
            "clinician type",
            "clinician specialty",
            "years in medicare",
            "participation option",
            "small practice status",
            "rural status",
            "health professional shortage area status",
            "hospital-based status",
            "facility-based status",
            "dual eligibility ratio",
            "final score",
            "payment adjustment percentage",
            "complex patient bonus",
            "quality category score",
            "quality category weight",
            "promoting interoperability (pi) category score",
            "promoting interoperability (pi) category weight",
            "improvement activities (ia) category score",
            "improvement activities (ia) category weight",
            "cost category score",
            "cost category weight",
        ),
        "npi",
        2 * GIB,
    ),
    "cms_pecos_public_provider_enrollment": CsvAcquisitionProfile(
        "PECOS Public Provider Enrollment CSV",
        ("NPI", "MULTIPLE_NPI_FLAG", "ENRLMT_ID", "PROVIDER_TYPE_CD"),
        "NPI",
        2 * GIB,
    ),
    "cms_order_and_referring": CsvAcquisitionProfile(
        "Order and Referring CSV",
        (
            "NPI",
            "LAST_NAME",
            "FIRST_NAME",
            "PARTB",
            "DME",
            "HHA",
            "PMD",
            "HOSPICE",
        ),
        "NPI",
        1 * GIB,
    ),
    "cms_hospital_enrollments": CsvAcquisitionProfile(
        "Hospital Enrollments CSV",
        ("ENROLLMENT ID", "NPI", "CCN", "ORGANIZATION NAME", "STATE"),
        "NPI",
        256 * 1024 * 1024,
    ),
    "cms_revalidation_group_reassignment": CsvAcquisitionProfile(
        "Revalidation Group Reassignment CSV",
        (
            "Group PAC ID",
            "Group Enrollment ID",
            "Group Legal Business Name",
            "Group State Code",
            "Group Reassignments and Physician Assistants",
            "Individual NPI",
            "Individual State Code",
        ),
        "Individual NPI",
        2 * GIB,
        allow_invalid_identifiers=True,
    ),
}
SUPPORTED_ACQUISITION_SOURCES = frozenset(CMS_CSV_PROFILES)


class AcquisitionError(RuntimeError):
    """The publisher artifact could not be safely retrieved or validated."""


@dataclass(frozen=True, slots=True)
class AcquisitionResult:
    manifest: RunManifest
    run_directory: Path
    artifact_path: Path
    run_manifest_path: Path
    manifest_store_path: Path

    def to_dict(self) -> dict:
        return {
            "manifest": self.manifest.to_dict(),
            "run_directory": str(self.run_directory),
            "artifact_path": str(self.artifact_path),
            "run_manifest_path": str(self.run_manifest_path),
            "manifest_store_path": str(self.manifest_store_path),
        }


@dataclass(frozen=True, slots=True)
class ArtifactInspection:
    byte_size: int
    sha256: str
    schema_fingerprint: str
    source_encoding: str
    row_count: int
    invalid_identifier_rows: int = 0


def make_run_id(now: datetime | None = None) -> str:
    timestamp = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    return f"{timestamp:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"


def release_id(source_id: str, publisher_version: str) -> str:
    digest = hashlib.sha256(
        f"{source_id}\0{publisher_version}".encode("utf-8")
    ).hexdigest()[:16]
    return f"{source_id}-{digest}"


def pipeline_commit(repository_root: Path | None = None) -> str | None:
    """Return the exact checked-out commit without requiring a writable repository."""
    root = repository_root or Path(__file__).resolve().parent.parent
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    commit = result.stdout.strip().lower()
    if len(commit) == 40 and all(character in "0123456789abcdef" for character in commit):
        return commit
    return None


def _allowed_source_hosts(source_id: str) -> frozenset[str]:
    if source_id.startswith("cms_"):
        return frozenset({"data.cms.gov"})
    if source_id.startswith("nppes_") or source_id.startswith("open_payments_"):
        return frozenset({"download.cms.gov"})
    if source_id == "aact_clinical_trials_snapshot":
        return frozenset(
            {
                "aact.ctti-clinicaltrials.org",
                "ctti-aact.nyc3.digitaloceanspaces.com",
            }
        )
    raise AcquisitionError(f"No publisher host policy exists for {source_id}")


def _validate_source_url(source_id: str, url: str) -> frozenset[str]:
    parsed = urlparse(url)
    allowed = _allowed_source_hosts(source_id)
    if parsed.scheme != "https" or parsed.hostname not in allowed:
        raise AcquisitionError(
            f"{source_id} artifact must use HTTPS on an approved publisher host"
        )
    return allowed


def _response_length(headers: object) -> int | None:
    value = headers.get("Content-Length") if hasattr(headers, "get") else None
    if value is None:
        return None
    try:
        length = int(value)
    except (TypeError, ValueError) as error:
        raise AcquisitionError("Publisher response has an invalid Content-Length") from error
    if length < 0:
        raise AcquisitionError("Publisher response has a negative Content-Length")
    return length


def download_artifact(
    release: ReleaseMetadata,
    destination: Path,
    *,
    max_bytes: int = DEFAULT_MAX_DOWNLOAD_BYTES,
    timeout: float = 60.0,
) -> tuple[int, str]:
    """Stream one publisher artifact to an atomic immutable destination."""
    if max_bytes <= 0:
        raise ValueError("max_bytes must be positive")
    allowed_hosts = _validate_source_url(release.source_id, release.source_url)
    if destination.exists():
        raise AcquisitionError(f"Refusing to overwrite existing artifact: {destination}")
    partial = destination.with_name(destination.name + ".partial")
    if partial.exists():
        raise AcquisitionError(f"Refusing to overwrite partial artifact: {partial}")

    request = urllib.request.Request(
        release.source_url,
        headers={"User-Agent": USER_AGENT, "Accept": "text/csv,*/*;q=0.1"},
    )
    digest = hashlib.sha256()
    byte_size = 0
    announced_size: int | None = None
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            final_url = response.geturl()
            final_host = urlparse(final_url).hostname
            if urlparse(final_url).scheme != "https" or final_host not in allowed_hosts:
                raise AcquisitionError("Publisher redirected the artifact outside approved hosts")
            announced_size = _response_length(response.headers)
            if announced_size is not None and announced_size > max_bytes:
                raise AcquisitionError(
                    f"Publisher artifact exceeds the {max_bytes} byte acquisition limit"
                )
            with partial.open("xb") as handle:
                while True:
                    chunk = response.read(DOWNLOAD_CHUNK_BYTES)
                    if not chunk:
                        break
                    byte_size += len(chunk)
                    if byte_size > max_bytes:
                        raise AcquisitionError(
                            f"Publisher artifact exceeds the {max_bytes} byte acquisition limit"
                        )
                    digest.update(chunk)
                    handle.write(chunk)
                handle.flush()
                os.fsync(handle.fileno())
    except urllib.error.URLError as error:
        raise AcquisitionError(f"Publisher artifact unavailable: {safe_error(error)}") from error

    if byte_size == 0:
        raise AcquisitionError("Publisher returned an empty artifact")
    if announced_size is not None and byte_size != announced_size:
        raise AcquisitionError(
            f"Publisher response ended at {byte_size} bytes; expected {announced_size}"
        )
    os.replace(partial, destination)
    return byte_size, digest.hexdigest()


def _normalized_column(value: str) -> str:
    return " ".join(value.strip().lower().split())


def inspect_cms_csv(
    path: Path,
    *,
    profile: CsvAcquisitionProfile,
) -> ArtifactInspection:
    """Validate a CMS CSV and derive durable artifact metrics without loading it."""
    digest = hashlib.sha256()
    byte_size = 0
    with path.open("rb") as raw:
        while chunk := raw.read(DOWNLOAD_CHUNK_BYTES):
            byte_size += len(chunk)
            digest.update(chunk)

    source_encoding: str | None = None
    for candidate in ("utf-8-sig", "cp1252"):
        try:
            with path.open("r", encoding=candidate, newline="") as handle:
                while handle.read(DOWNLOAD_CHUNK_BYTES):
                    pass
        except UnicodeDecodeError:
            continue
        source_encoding = candidate
        break
    if source_encoding is None:
        raise AcquisitionError(
            f"{profile.label} is neither valid UTF-8 nor Windows-1252"
        )

    try:
        with path.open("r", encoding=source_encoding, newline="") as handle:
            reader = csv.reader(handle)
            header = next(reader, None)
            if not header:
                raise AcquisitionError(f"{profile.label} has no header")
            if any(not column.strip() for column in header):
                raise AcquisitionError(f"{profile.label} has a blank column name")
            normalized_header = [_normalized_column(column) for column in header]
            if len(normalized_header) != len(set(normalized_header)):
                raise AcquisitionError(f"{profile.label} has duplicate column names")
            normalized_required = {
                _normalized_column(column): column for column in profile.required_columns
            }
            missing = [
                original
                for normalized, original in normalized_required.items()
                if normalized not in normalized_header
            ]
            if missing:
                raise AcquisitionError(
                    f"{profile.label} is missing required columns: "
                    + ", ".join(missing)
                )
            identifier_index = normalized_header.index(
                _normalized_column(profile.identifier_column)
            )
            row_count = 0
            invalid_identifier_rows = 0
            for line_number, row in enumerate(reader, start=2):
                if len(row) != len(header):
                    raise AcquisitionError(
                        f"{profile.label} row {line_number} has {len(row)} fields; "
                        f"expected {len(header)}"
                    )
                npi = row[identifier_index].strip()
                if not (len(npi) == 10 and npi.isdigit()):
                    if profile.allow_invalid_identifiers:
                        invalid_identifier_rows += 1
                        row_count += 1
                        continue
                    raise AcquisitionError(
                        f"{profile.label} row {line_number} has an invalid NPI"
                    )
                row_count += 1
    except csv.Error as error:
        raise AcquisitionError(f"{profile.label} is malformed: {safe_error(error)}") from error

    if row_count == 0:
        raise AcquisitionError(f"{profile.label} contains no data rows")
    schema_payload = json.dumps(header, ensure_ascii=False, separators=(",", ":"))
    schema_fingerprint = "sha256:" + hashlib.sha256(
        schema_payload.encode("utf-8")
    ).hexdigest()
    return ArtifactInspection(
        byte_size=byte_size,
        sha256=digest.hexdigest(),
        schema_fingerprint=schema_fingerprint,
        source_encoding=source_encoding,
        row_count=row_count,
        invalid_identifier_rows=invalid_identifier_rows,
    )


def inspect_hospital_enrollments(path: Path) -> ArtifactInspection:
    """Validate Hospital Enrollments using its source-specific CMS CSV profile."""
    return inspect_cms_csv(
        path,
        profile=CMS_CSV_PROFILES["cms_hospital_enrollments"],
    )


def _save_manifest(manifest: RunManifest, run_path: Path, store_path: Path) -> None:
    run_document = ManifestDocument(manifests=[manifest])
    ManifestStore(run_path).save(run_document)
    store = ManifestStore(store_path)
    document = store.load()
    if any(existing.run_id == manifest.run_id for existing in document.manifests):
        raise AcquisitionError(f"Manifest store already contains run {manifest.run_id}")
    document.manifests.append(manifest)
    store.save(document)


def acquire_release(
    release: ReleaseMetadata,
    *,
    discovery_timestamp: str,
    data_root: Path,
    manifest_path: Path,
    max_bytes: int | None = None,
    timeout: float = 60.0,
    code_commit: str | None = None,
) -> AcquisitionResult:
    """Acquire and validate one supported release into a new immutable run directory."""
    if release.source_id not in SUPPORTED_ACQUISITION_SOURCES:
        raise AcquisitionError(
            f"Immutable acquisition is not implemented for {release.source_id}"
        )
    spec = SOURCE_REGISTRY[release.source_id]
    profile = CMS_CSV_PROFILES[release.source_id]
    run_id = make_run_id()
    run_directory = data_root / "runs" / release.source_id / run_id
    artifact_path = run_directory / "source.csv"
    run_manifest_path = run_directory / "manifest.json"
    run_directory.mkdir(parents=True, exist_ok=False)

    manifest = RunManifest(
        run_id=run_id,
        release_id=release_id(release.source_id, release.publisher_version),
        source_id=release.source_id,
        publisher=spec.publisher.value,
        publisher_version=release.publisher_version,
        source_data_period=release.source_data_period,
        publisher_release_timestamp=release.publisher_release_timestamp,
        discovery_timestamp=discovery_timestamp,
        source_url=release.source_url,
        pipeline_code_commit=code_commit or pipeline_commit(),
    )
    try:
        downloaded_size, downloaded_sha = download_artifact(
            release,
            artifact_path,
            max_bytes=(
                profile.max_download_bytes if max_bytes is None else max_bytes
            ),
            timeout=timeout,
        )
        manifest.retrieval_timestamp = utc_now()
        manifest.byte_size = downloaded_size
        manifest.sha256 = downloaded_sha
        inspection = inspect_cms_csv(artifact_path, profile=profile)
        if inspection.byte_size != downloaded_size or inspection.sha256 != downloaded_sha:
            raise AcquisitionError("Artifact changed between retrieval and validation")
        manifest.schema_fingerprint = inspection.schema_fingerprint
        manifest.source_encoding = inspection.source_encoding
        manifest.row_counts = {"source_rows": inspection.row_count}
        if inspection.invalid_identifier_rows:
            manifest.row_counts["invalid_identifier_rows"] = (
                inspection.invalid_identifier_rows
            )
        manifest.validation_state = ValidationState.PASSED
        manifest.validation_timestamp = utc_now()
    except (AcquisitionError, OSError, ValueError) as error:
        manifest.validation_state = ValidationState.FAILED
        manifest.failure_timestamp = utc_now()
        manifest.error_summary = safe_error(error)
        _save_manifest(manifest, run_manifest_path, manifest_path)
        raise AcquisitionError(manifest.error_summary) from error

    _save_manifest(manifest, run_manifest_path, manifest_path)
    return AcquisitionResult(
        manifest=manifest,
        run_directory=run_directory,
        artifact_path=artifact_path,
        run_manifest_path=run_manifest_path,
        manifest_store_path=manifest_path,
    )
