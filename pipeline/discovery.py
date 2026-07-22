"""Read-only publisher release discovery.

Only small metadata/index documents are fetched. This module never opens DuckDB and
never downloads a publisher data archive.
"""

from __future__ import annotations

import html
import json
import re
import urllib.error
import urllib.request
import uuid
from calendar import monthrange
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlparse

from .source_registry import (
    SOURCE_REGISTRY,
    DiscoveryMechanism,
    SourceSpec,
    sources_for,
)

CMS_CATALOG_URL = "https://data.cms.gov/data.json"
PPEF_DATASET_UUID = "2457ea29-fc82-48b0-86ec-3b0755de7515"
PPEF_RESOURCES_URL = (
    f"https://data.cms.gov/data-api/v1/dataset/{PPEF_DATASET_UUID}/resources"
)
NPPES_INDEX_URL = "https://download.cms.gov/nppes/NPI_Files.html"
# The download-route data is embedded in the official Dataset Explorer's compiled
# asset. The client-side /datasets/download route currently returns 404 to direct
# HTTP clients, while /datasets exposes the same official datasetDownloads config.
OPEN_PAYMENTS_INDEX_URL = "https://openpaymentsdata.cms.gov/datasets"
AACT_DOWNLOADS_URL = "https://aact.ctti-clinicaltrials.org/downloads"

MAX_METADATA_BYTES = 64 * 1024 * 1024
USER_AGENT = "cms-data-platform-discovery/1.0 (+read-only metadata check)"


class DiscoveryState(str, Enum):
    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"
    ERROR = "discovery_error"


class DiscoveryError(ValueError):
    """Publisher metadata was reachable but could not be interpreted safely."""


@dataclass(frozen=True, slots=True)
class ReleaseMetadata:
    source_id: str
    publisher_version: str
    source_data_period: str
    publisher_release_timestamp: str | None
    source_url: str
    byte_size: int | None = None

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class DiscoveryResult:
    source_id: str
    state: DiscoveryState
    discovered_at: str
    release: ReleaseMetadata | None = None
    error_summary: str | None = None

    def to_dict(self) -> dict:
        result = asdict(self)
        result["state"] = self.state.value
        return result


@dataclass(frozen=True, slots=True)
class OpenPaymentsArchive:
    program_year: int
    publisher_version: str
    publisher_release_timestamp: str
    source_url: str
    byte_size: int | None


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def safe_error(error: object) -> str:
    """Return a short, single-line diagnostic suitable for a manifest or status API."""
    message = " ".join(str(error).split()) or error.__class__.__name__
    return message[:240]


def fetch_metadata(url: str, timeout: float = 30.0) -> bytes:
    """Fetch a bounded metadata document without following it into bulk data."""
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        content_length = response.headers.get("Content-Length")
        if content_length:
            try:
                announced_size = int(content_length)
            except ValueError as error:
                raise DiscoveryError("Metadata response has an invalid Content-Length") from error
            if announced_size > MAX_METADATA_BYTES:
                raise DiscoveryError(
                    f"Metadata response exceeds {MAX_METADATA_BYTES} byte safety limit"
                )
        payload = response.read(MAX_METADATA_BYTES + 1)
    if len(payload) > MAX_METADATA_BYTES:
        raise DiscoveryError(
            f"Metadata response exceeds {MAX_METADATA_BYTES} byte safety limit"
        )
    return payload


def _iso_date_timestamp(value: str) -> str:
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError as error:
        raise DiscoveryError(f"Invalid publisher date: {value!r}") from error
    return parsed.isoformat()


def _resource_id(url: str) -> str | None:
    match = re.search(r"/dataset-resources/([0-9a-f-]{36})(?:$|[/?])", url, re.I)
    return match.group(1).lower() if match else None


def parse_cms_catalog(payload: bytes, specs: tuple[SourceSpec, ...]) -> dict[str, DiscoveryResult]:
    """Parse the Project Open Data catalog using stable CMS dataset identifiers."""
    discovered_at = utc_now()
    try:
        catalog = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise DiscoveryError("CMS data.json is not valid JSON") from error
    datasets = catalog.get("dataset") if isinstance(catalog, dict) else None
    if not isinstance(datasets, list):
        raise DiscoveryError("CMS data.json is missing the dataset array")

    results: dict[str, DiscoveryResult] = {}
    for spec in specs:
        try:
            marker = f"/dataset/{spec.discovery_key}/data-viewer"
            matches = [
                item
                for item in datasets
                if isinstance(item, dict) and marker in str(item.get("identifier", ""))
            ]
            if len(matches) != 1:
                raise DiscoveryError(
                    f"Expected one CMS catalog dataset for {spec.discovery_key}; "
                    f"found {len(matches)}"
                )
            distributions = matches[0].get("distribution")
            if not isinstance(distributions, list):
                raise DiscoveryError("CMS dataset is missing its distribution array")
            csv_distributions = [
                item
                for item in distributions
                if isinstance(item, dict)
                and str(item.get("format", "")).upper() == "CSV"
                and item.get("downloadURL")
            ]
            if not csv_distributions:
                raise DiscoveryError("CMS dataset has no downloadable CSV distribution")
            latest = max(
                csv_distributions,
                key=lambda item: (
                    str(item.get("modified", "")),
                    str(item.get("temporal", "")),
                    str(item.get("title", "")),
                ),
            )
            required = ("modified", "temporal", "downloadURL", "resourcesAPI")
            missing = [field for field in required if not latest.get(field)]
            if missing:
                raise DiscoveryError(
                    "CMS distribution is missing required fields: " + ", ".join(missing)
                )
            resource_id = _resource_id(str(latest["resourcesAPI"]))
            if not resource_id:
                raise DiscoveryError("CMS distribution resourcesAPI lacks a resource identifier")
            release = ReleaseMetadata(
                source_id=spec.source_id,
                publisher_version=f"cms-resource:{resource_id}",
                source_data_period=str(latest["temporal"]),
                publisher_release_timestamp=_iso_date_timestamp(str(latest["modified"])),
                source_url=str(latest["downloadURL"]),
            )
            results[spec.source_id] = DiscoveryResult(
                spec.source_id, DiscoveryState.AVAILABLE, discovered_at, release=release
            )
        except DiscoveryError as error:
            results[spec.source_id] = DiscoveryResult(
                spec.source_id,
                DiscoveryState.ERROR,
                discovered_at,
                error_summary=safe_error(error),
            )
    return results


def _ppef_quarter_period(title: str) -> str:
    match = re.search(r"\bQ([1-4])\s+(\d{4})\b", title, re.I)
    if not match:
        raise DiscoveryError("PPEF resource title is missing its quarter and year")
    quarter = int(match.group(1))
    year = int(match.group(2))
    first_month = (quarter - 1) * 3 + 1
    last_month = first_month + 2
    last_day = monthrange(year, last_month)[1]
    return (
        f"{year:04d}-{first_month:02d}-01/"
        f"{year:04d}-{last_month:02d}-{last_day:02d}"
    )


def parse_ppef_resources(
    payload: bytes,
    specs: tuple[SourceSpec, ...],
) -> dict[str, DiscoveryResult]:
    """Parse PPEF ancillary CSVs from CMS's stable dataset-resources endpoint."""
    discovered_at = utc_now()
    try:
        document = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise DiscoveryError("CMS PPEF resources response is not valid JSON") from error
    resources = document.get("data") if isinstance(document, dict) else None
    if not isinstance(resources, list):
        raise DiscoveryError("CMS PPEF resources response is missing the data array")

    results: dict[str, DiscoveryResult] = {}
    for spec in specs:
        try:
            matches = [
                resource
                for resource in resources
                if isinstance(resource, dict)
                and str(resource.get("file_name", "")).startswith(spec.discovery_key)
                and str(resource.get("file_name", "")).lower().endswith(".csv")
            ]
            if len(matches) != 1:
                raise DiscoveryError(
                    f"Expected one current PPEF resource beginning {spec.discovery_key!r}; "
                    f"found {len(matches)}"
                )
            resource = matches[0]
            required = (
                "title",
                "file_uuid",
                "file_name",
                "file_mime",
                "file_size",
                "file_url",
            )
            missing = [
                field for field in required if resource.get(field) in (None, "")
            ]
            if missing:
                raise DiscoveryError(
                    "CMS PPEF resource is missing required fields: "
                    + ", ".join(missing)
                )
            if resource["file_mime"] != "text/csv":
                raise DiscoveryError("CMS PPEF resource is not published as text/csv")
            file_uuid = str(resource["file_uuid"]).lower()
            try:
                uuid.UUID(file_uuid)
            except ValueError as error:
                raise DiscoveryError("CMS PPEF resource has an invalid file UUID") from error
            source_url = str(resource["file_url"])
            parsed_url = urlparse(source_url)
            if parsed_url.scheme != "https" or parsed_url.hostname != "data.cms.gov":
                raise DiscoveryError(
                    "CMS PPEF resource URL is outside the official CMS host"
                )
            try:
                byte_size = int(resource["file_size"])
            except (TypeError, ValueError) as error:
                raise DiscoveryError("CMS PPEF resource has an invalid file size") from error
            if byte_size <= 0:
                raise DiscoveryError("CMS PPEF resource file size must be positive")
            release = ReleaseMetadata(
                source_id=spec.source_id,
                publisher_version=f"cms-file:{file_uuid}",
                source_data_period=_ppef_quarter_period(str(resource["title"])),
                publisher_release_timestamp=None,
                source_url=source_url,
                byte_size=byte_size,
            )
            results[spec.source_id] = DiscoveryResult(
                spec.source_id,
                DiscoveryState.AVAILABLE,
                discovered_at,
                release=release,
            )
        except DiscoveryError as error:
            results[spec.source_id] = DiscoveryResult(
                spec.source_id,
                DiscoveryState.ERROR,
                discovered_at,
                error_summary=safe_error(error),
            )
    return results


class _LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[dict[str, str]] = []
        self._current: dict[str, str] | None = None
        self._text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        values = {key.lower(): value or "" for key, value in attrs}
        self._current = values
        self._text = []

    def handle_data(self, data: str) -> None:
        if self._current is not None:
            self._text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "a" and self._current is not None:
            self._current["text"] = " ".join("".join(self._text).split())
            self.links.append(self._current)
            self._current = None
            self._text = []


def _size_to_bytes(value: str) -> int | None:
    match = re.search(r"([0-9][0-9,]*(?:\.[0-9]+)?)\s*(KB|MB|GB)\b", value, re.I)
    if not match:
        return None
    number = float(match.group(1).replace(",", ""))
    multiplier = {"KB": 1024, "MB": 1024**2, "GB": 1024**3}[match.group(2).upper()]
    return round(number * multiplier)


def _nearby_size(document: str, href: str) -> int | None:
    position = document.find(href)
    return _size_to_bytes(document[position : position + 500]) if position >= 0 else None


def parse_nppes_index(payload: bytes, specs: tuple[SourceSpec, ...]) -> dict[str, DiscoveryResult]:
    """Discover the current monthly full and newest weekly incremental V2 files."""
    discovered_at = utc_now()
    try:
        document = payload.decode("utf-8")
    except UnicodeDecodeError:
        document = payload.decode("latin-1")
    parser = _LinkParser()
    parser.feed(document)

    monthly: list[tuple[datetime, ReleaseMetadata]] = []
    weekly: list[tuple[datetime, ReleaseMetadata]] = []
    monthly_pattern = re.compile(
        r"^NPPES_Data_Dissemination_([A-Za-z]+)_(\d{4})_V2\.zip$", re.I
    )
    weekly_pattern = re.compile(
        r"^NPPES_Data_Dissemination_(\d{6})_(\d{6})_Weekly_V2\.zip$", re.I
    )
    for link in parser.links:
        href = html.unescape(link.get("href", ""))
        filename = Path(urlparse(href).path).name
        label = link.get("aria-label") or link.get("text", "")
        monthly_match = monthly_pattern.fullmatch(filename)
        if monthly_match:
            date_match = re.search(
                r"\(([A-Za-z]+\s+\d{1,2},\s+\d{4})\)", label
            )
            if not date_match:
                continue
            try:
                release_date = datetime.strptime(date_match.group(1), "%B %d, %Y").replace(
                    tzinfo=timezone.utc
                )
            except ValueError:
                continue
            monthly.append(
                (
                    release_date,
                    ReleaseMetadata(
                        source_id="nppes_monthly_v2",
                        publisher_version=filename.removesuffix(".zip"),
                        source_data_period=release_date.date().isoformat(),
                        publisher_release_timestamp=release_date.isoformat(),
                        source_url=urljoin(NPPES_INDEX_URL, href),
                        byte_size=_nearby_size(document, href),
                    ),
                )
            )
            continue
        weekly_match = weekly_pattern.fullmatch(filename)
        if weekly_match:
            try:
                start = datetime.strptime(weekly_match.group(1), "%m%d%y").replace(
                    tzinfo=timezone.utc
                )
                end = datetime.strptime(weekly_match.group(2), "%m%d%y").replace(
                    tzinfo=timezone.utc
                )
            except ValueError:
                continue
            weekly.append(
                (
                    end,
                    ReleaseMetadata(
                        source_id="nppes_weekly_incremental_v2",
                        publisher_version=filename.removesuffix(".zip"),
                        source_data_period=f"{start.date().isoformat()}/{end.date().isoformat()}",
                        publisher_release_timestamp=None,
                        source_url=urljoin(NPPES_INDEX_URL, href),
                        byte_size=_nearby_size(document, href),
                    ),
                )
            )

    found = {
        "monthly_v2": max(monthly, key=lambda item: item[0])[1] if monthly else None,
        "weekly_v2": max(weekly, key=lambda item: item[0])[1] if weekly else None,
    }
    results: dict[str, DiscoveryResult] = {}
    for spec in specs:
        release = found.get(spec.discovery_key)
        if release is None:
            results[spec.source_id] = DiscoveryResult(
                spec.source_id,
                DiscoveryState.ERROR,
                discovered_at,
                error_summary=(
                    f"Official NPPES index has no parseable {spec.discovery_key} release"
                ),
            )
        else:
            results[spec.source_id] = DiscoveryResult(
                spec.source_id, DiscoveryState.AVAILABLE, discovered_at, release=release
            )
    return results


_OPEN_PAYMENTS_PATTERN = re.compile(
    r"https://download\.cms\.gov/openpayments/(?:staging/)?"
    r"PGYR(?P<year>\d{4})_P(?P<published>\d{8})"
    r"(?:_(?P<generated>\d{8}))?\.zip",
    re.I,
)


def parse_open_payments_archives(documents: list[bytes]) -> dict[int, OpenPaymentsArchive]:
    """Extract current program-year ZIP metadata from the official download page assets."""
    archives: dict[int, OpenPaymentsArchive] = {}
    for payload in documents:
        document = html.unescape(payload.decode("utf-8", errors="replace"))
        for match in _OPEN_PAYMENTS_PATTERN.finditer(document):
            year = int(match.group("year"))
            try:
                published = datetime.strptime(match.group("published"), "%m%d%Y").replace(
                    tzinfo=timezone.utc
                )
            except ValueError:
                continue
            url = match.group(0)
            version = Path(urlparse(url).path).stem
            candidate = OpenPaymentsArchive(
                program_year=year,
                publisher_version=version,
                publisher_release_timestamp=published.isoformat(),
                source_url=url,
                byte_size=_size_to_bytes(document[match.end() : match.end() + 250]),
            )
            existing = archives.get(year)
            if (
                existing is None
                or candidate.publisher_release_timestamp
                > existing.publisher_release_timestamp
            ):
                archives[year] = candidate
    if not archives:
        raise DiscoveryError(
            "Official Open Payments download metadata has no parseable program-year ZIP"
        )
    return archives


def parse_open_payments_index(
    documents: list[bytes], specs: tuple[SourceSpec, ...]
) -> dict[str, DiscoveryResult]:
    discovered_at = utc_now()
    archive = max(
        parse_open_payments_archives(documents).values(),
        key=lambda item: item.program_year,
    )
    results: dict[str, DiscoveryResult] = {}
    for spec in specs:
        release = ReleaseMetadata(
            source_id=spec.source_id,
            publisher_version=f"{archive.publisher_version}:{spec.discovery_key}",
            source_data_period=f"{archive.program_year}-01-01/{archive.program_year}-12-31",
            publisher_release_timestamp=archive.publisher_release_timestamp,
            source_url=archive.source_url,
            byte_size=archive.byte_size,
        )
        results[spec.source_id] = DiscoveryResult(
            spec.source_id, DiscoveryState.AVAILABLE, discovered_at, release=release
        )
    return results


def parse_aact_downloads(payload: bytes, spec: SourceSpec) -> DiscoveryResult:
    """Parse the latest PostgreSQL snapshot card from AACT's downloads page."""
    discovered_at = utc_now()
    document = html.unescape(payload.decode("utf-8", errors="replace"))
    marker = re.search(r"snapshot-card\s+pgdump", document, re.I)
    if not marker:
        raise DiscoveryError("AACT downloads page is missing the PostgreSQL snapshot card")
    next_card = re.search(r"snapshot-card\s+", document[marker.end() :], re.I)
    end = marker.end() + next_card.start() if next_card else len(document)
    card = document[marker.start() : end]
    date_match = re.search(r"Last Exported:\s*(\d{2}-\d{2}-\d{4})", card, re.I)
    filename_match = re.search(r"(\d{8}_clinical_trials_ctgov\.zip)", card, re.I)
    href_match = re.search(
        r"href=[\"']([^\"']*/static/static_db_copies/daily/\d{4}-\d{2}-\d{2}[^\"']*)[\"']",
        card,
        re.I,
    )
    missing = [
        name
        for name, match in (
            ("Last Exported", date_match),
            ("snapshot filename", filename_match),
            ("snapshot download link", href_match),
        )
        if match is None
    ]
    if missing:
        raise DiscoveryError("AACT snapshot card is missing: " + ", ".join(missing))
    release_date = datetime.strptime(date_match.group(1), "%m-%d-%Y").replace(
        tzinfo=timezone.utc
    )
    release = ReleaseMetadata(
        source_id=spec.source_id,
        publisher_version=filename_match.group(1),
        source_data_period=release_date.date().isoformat(),
        publisher_release_timestamp=release_date.isoformat(),
        source_url=urljoin(AACT_DOWNLOADS_URL, href_match.group(1)),
        byte_size=_size_to_bytes(card),
    )
    return DiscoveryResult(
        spec.source_id, DiscoveryState.AVAILABLE, discovered_at, release=release
    )


FIXTURE_FILES = {
    DiscoveryMechanism.CMS_DATA_JSON: "cms-data.json",
    DiscoveryMechanism.CMS_DATASET_RESOURCES: "ppef-resources.json",
    DiscoveryMechanism.NPPES_DOWNLOAD_INDEX: "nppes.html",
    DiscoveryMechanism.OPEN_PAYMENTS_DOWNLOAD_INDEX: "open-payments.html",
    DiscoveryMechanism.AACT_DOWNLOADS_PAGE: "aact.html",
}


def _publisher_unavailable(
    specs: tuple[SourceSpec, ...], error: object, discovered_at: str
) -> dict[str, DiscoveryResult]:
    summary = safe_error(error)
    return {
        spec.source_id: DiscoveryResult(
            spec.source_id,
            DiscoveryState.UNAVAILABLE,
            discovered_at,
            error_summary=summary,
        )
        for spec in specs
    }


def _publisher_error(
    specs: tuple[SourceSpec, ...], error: object, discovered_at: str
) -> dict[str, DiscoveryResult]:
    summary = safe_error(error)
    return {
        spec.source_id: DiscoveryResult(
            spec.source_id,
            DiscoveryState.ERROR,
            discovered_at,
            error_summary=summary,
        )
        for spec in specs
    }


def _open_payments_documents(page: bytes, timeout: float) -> list[bytes]:
    documents = [page]
    if _OPEN_PAYMENTS_PATTERN.search(page.decode("utf-8", errors="replace")):
        return documents
    script_sources = re.findall(
        r"<script[^>]+src=[\"']([^\"']+)[\"']", page.decode("utf-8", errors="replace"), re.I
    )
    candidates = [
        urljoin(OPEN_PAYMENTS_INDEX_URL, source)
        for source in script_sources
        if "/frontend/build/static/js/index.js" in source
    ]
    if not candidates:
        raise DiscoveryError("Open Payments page does not expose its official index asset")
    for url in candidates:
        parsed = urlparse(url)
        if parsed.scheme != "https" or parsed.hostname != "openpaymentsdata.cms.gov":
            raise DiscoveryError("Open Payments index asset is outside the official CMS host")
        documents.append(fetch_metadata(url, timeout=timeout))
    return documents


def discover_all(
    *, fixture_dir: Path | None = None, timeout: float = 30.0
) -> dict[str, DiscoveryResult]:
    """Discover every registered source from live publishers or checked-in fixtures."""
    results: dict[str, DiscoveryResult] = {}
    for mechanism in DiscoveryMechanism:
        specs = sources_for(mechanism)
        discovered_at = utc_now()
        try:
            if fixture_dir is not None:
                payload = (fixture_dir / FIXTURE_FILES[mechanism]).read_bytes()
            else:
                url = {
                    DiscoveryMechanism.CMS_DATA_JSON: CMS_CATALOG_URL,
                    DiscoveryMechanism.CMS_DATASET_RESOURCES: PPEF_RESOURCES_URL,
                    DiscoveryMechanism.NPPES_DOWNLOAD_INDEX: NPPES_INDEX_URL,
                    DiscoveryMechanism.OPEN_PAYMENTS_DOWNLOAD_INDEX: OPEN_PAYMENTS_INDEX_URL,
                    DiscoveryMechanism.AACT_DOWNLOADS_PAGE: AACT_DOWNLOADS_URL,
                }[mechanism]
                payload = fetch_metadata(url, timeout=timeout)
        except (OSError, urllib.error.URLError, TimeoutError) as error:
            results.update(_publisher_unavailable(specs, error, discovered_at))
            continue
        except DiscoveryError as error:
            results.update(_publisher_error(specs, error, discovered_at))
            continue

        try:
            if mechanism == DiscoveryMechanism.CMS_DATA_JSON:
                results.update(parse_cms_catalog(payload, specs))
            elif mechanism == DiscoveryMechanism.CMS_DATASET_RESOURCES:
                results.update(parse_ppef_resources(payload, specs))
            elif mechanism == DiscoveryMechanism.NPPES_DOWNLOAD_INDEX:
                results.update(parse_nppes_index(payload, specs))
            elif mechanism == DiscoveryMechanism.OPEN_PAYMENTS_DOWNLOAD_INDEX:
                documents = [payload] if fixture_dir else _open_payments_documents(payload, timeout)
                results.update(parse_open_payments_index(documents, specs))
            else:
                results[specs[0].source_id] = parse_aact_downloads(payload, specs[0])
        except DiscoveryError as error:
            results.update(_publisher_error(specs, error, discovered_at))
    return {source_id: results[source_id] for source_id in sorted(SOURCE_REGISTRY)}


def discover_source(source_id: str, *, timeout: float = 30.0) -> DiscoveryResult:
    """Discover one source without contacting unrelated publishers."""
    spec = SOURCE_REGISTRY[source_id]
    if spec.discovery == DiscoveryMechanism.NPPES_DOWNLOAD_INDEX:
        payload = fetch_metadata(NPPES_INDEX_URL, timeout=timeout)
        return parse_nppes_index(payload, sources_for(spec.discovery))[source_id]
    if spec.discovery == DiscoveryMechanism.OPEN_PAYMENTS_DOWNLOAD_INDEX:
        page = fetch_metadata(OPEN_PAYMENTS_INDEX_URL, timeout=timeout)
        return parse_open_payments_index(
            _open_payments_documents(page, timeout), sources_for(spec.discovery)
        )[source_id]
    if spec.discovery == DiscoveryMechanism.CMS_DATA_JSON:
        payload = fetch_metadata(CMS_CATALOG_URL, timeout=timeout)
        return parse_cms_catalog(payload, (spec,))[source_id]
    if spec.discovery == DiscoveryMechanism.CMS_DATASET_RESOURCES:
        payload = fetch_metadata(PPEF_RESOURCES_URL, timeout=timeout)
        return parse_ppef_resources(payload, (spec,))[source_id]
    payload = fetch_metadata(AACT_DOWNLOADS_URL, timeout=timeout)
    return parse_aact_downloads(payload, spec)


def discover_open_payments_archive(year: int, *, timeout: float = 30.0) -> OpenPaymentsArchive:
    """Resolve a requested program-year ZIP from the live official download index."""
    page = fetch_metadata(OPEN_PAYMENTS_INDEX_URL, timeout=timeout)
    archives = parse_open_payments_archives(_open_payments_documents(page, timeout))
    if year not in archives:
        raise DiscoveryError(f"Open Payments index does not list program year {year}")
    return archives[year]
