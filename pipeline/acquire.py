"""Data acquisition: download bulk CSVs and fetch small datasets via CMS API."""

import json
import logging
import time
import urllib.request
import urllib.error
from pathlib import Path

from .config import (
    CMS_API_PAGE_SIZE,
    DATASETS,
    DatasetConfig,
    RAW_DIR,
)

logger = logging.getLogger(__name__)


def ensure_dirs():
    """Create data directories if they don't exist."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)


def download_csv(dataset: DatasetConfig, overwrite: bool = False) -> Path:
    """Download a bulk CSV from the CMS data API.

    Uses the /data endpoint with size=-1 to request all rows as CSV.
    Falls back to paginated JSON->CSV if the server doesn't support bulk CSV.
    """
    ensure_dirs()
    dest = dataset.csv_path

    if dest.exists() and not overwrite:
        logger.info("CSV already exists: %s (skipping)", dest)
        return dest

    # CMS data API supports CSV download via Accept header
    url = f"{dataset.api_url}?size=-1"
    logger.info("Downloading %s from %s", dataset.name, url)

    req = urllib.request.Request(url, headers={"Accept": "text/csv"})
    try:
        with urllib.request.urlopen(req, timeout=600) as resp:
            content_type = resp.headers.get("Content-Type", "")
            data = resp.read()

            if "text/csv" in content_type or data[:3] != b'[{"':
                dest.write_bytes(data)
                logger.info("Saved CSV: %s (%d bytes)", dest, len(data))
                return dest

        # If API returned JSON instead, fall back to paginated fetch
        logger.info("API returned JSON; falling back to paginated download")
        return _download_paginated_as_csv(dataset)

    except urllib.error.HTTPError as e:
        if e.code == 404:
            logger.warning("Dataset %s returned 404; trying paginated download", dataset.name)
            return _download_paginated_as_csv(dataset)
        raise


def _download_paginated_as_csv(dataset: DatasetConfig) -> Path:
    """Fetch all pages from the CMS JSON API and write as CSV."""
    import csv
    import io

    dest = dataset.csv_path
    offset = 0
    all_rows: list[dict] = []
    headers_written = False

    logger.info("Paginated download: %s", dataset.name)

    while True:
        url = f"{dataset.api_url}?offset={offset}&size={CMS_API_PAGE_SIZE}"
        req = urllib.request.Request(url)

        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                page = json.loads(resp.read())
        except urllib.error.HTTPError as e:
            logger.error("HTTP %d fetching %s at offset %d", e.code, dataset.name, offset)
            raise

        if not page:
            break

        all_rows.extend(page)
        offset += CMS_API_PAGE_SIZE
        logger.info("  fetched %d rows (total: %d)", len(page), len(all_rows))

        if len(page) < CMS_API_PAGE_SIZE:
            break

        time.sleep(0.5)  # rate-limit courtesy

    if not all_rows:
        logger.warning("No data returned for %s", dataset.name)
        return dest

    # Write as CSV
    fieldnames = list(all_rows[0].keys())
    with open(dest, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    logger.info("Saved paginated CSV: %s (%d rows)", dest, len(all_rows))
    return dest


def fetch_api_dataset(dataset: DatasetConfig, overwrite: bool = False) -> Path:
    """Fetch a small dataset via paginated CMS API calls, save as CSV."""
    return _download_paginated_as_csv(dataset) if overwrite or not dataset.csv_path.exists() else dataset.csv_path


def acquire_all(overwrite: bool = False) -> dict[str, Path]:
    """Download all configured datasets. Returns {name: csv_path}."""
    ensure_dirs()
    results = {}

    for name, ds in DATASETS.items():
        try:
            if ds.acquisition == "csv":
                results[name] = download_csv(ds, overwrite=overwrite)
            elif ds.acquisition == "api":
                results[name] = fetch_api_dataset(ds, overwrite=overwrite)
            else:
                logger.warning("Unknown acquisition method for %s: %s", name, ds.acquisition)
        except Exception:
            logger.exception("Failed to acquire %s", name)

    return results


def acquire_one(name: str, overwrite: bool = False) -> Path:
    """Download a single dataset by name."""
    ds = DATASETS[name]
    if ds.acquisition == "api":
        return fetch_api_dataset(ds, overwrite=overwrite)
    return download_csv(ds, overwrite=overwrite)
