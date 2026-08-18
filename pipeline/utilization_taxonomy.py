"""Acquire immutable RBCS and RxClass references for utilization discovery."""

from __future__ import annotations

import argparse
import csv
import difflib
import hashlib
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import duckdb


SCHEMA_VERSION = 1
RBCS_URL = (
    "https://data.cms.gov/sites/default/files/2026-05/"
    "37fde1d7-b350-4f22-87f0-07ca84402e44/RBCS%20Taxonomy_RY2025.csv"
)
RXCLASS_BASE_URL = "https://rxnav.nlm.nih.gov/REST/rxclass"
RXCLASS_SOURCES = ("ATC", "FDASPL")
MIN_ATC_CLAIM_COVERAGE_PCT = 90.0
SALT_WORDS = frozenset(
    {
        "acetate",
        "besylate",
        "bitartrate",
        "calcium",
        "chloride",
        "citrate",
        "fumarate",
        "hydrobromide",
        "hydrochloride",
        "hcl",
        "lactate",
        "maleate",
        "mesylate",
        "phosphate",
        "potassium",
        "sodium",
        "succinate",
        "sulfate",
        "tartrate",
        "bisulfate",
        "hyclate",
        "oxalate",
        "propanediol",
        "propionate",
        "hum",
        "rec",
        "anlog",
    }
)


class TaxonomyError(RuntimeError):
    """A taxonomy acquisition or validation invariant failed."""


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def safe_error(error: BaseException) -> str:
    text = " ".join(str(error).split())
    return (text or error.__class__.__name__)[:500]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_file(path: Path, label: str) -> Path:
    if not path.is_absolute():
        raise TaxonomyError(f"{label} must be an absolute path")
    try:
        resolved = path.resolve(strict=True)
    except (FileNotFoundError, OSError) as error:
        raise TaxonomyError(f"{label} does not resolve: {path}") from error
    if resolved.is_symlink() or not resolved.is_file():
        raise TaxonomyError(f"{label} must resolve to a regular file")
    return resolved


def _canonical_directory(path: Path, label: str, *, create: bool = False) -> Path:
    if not path.is_absolute() or path == Path("/"):
        raise TaxonomyError(f"{label} must be a specific absolute path")
    if create:
        path.mkdir(parents=True, exist_ok=True)
    resolved = path.resolve(strict=True)
    if path.is_symlink() or resolved != path or not path.is_dir():
        raise TaxonomyError(f"{label} must be a canonical non-symlink directory")
    return path


def _load_json(path: Path, label: str) -> dict:
    path = _canonical_file(path, label)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise TaxonomyError(f"Could not read {label}: {safe_error(error)}") from error
    if not isinstance(value, dict):
        raise TaxonomyError(f"{label} must contain a JSON object")
    return value


def _write_json(path: Path, value: dict) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    with temporary.open("x", encoding="utf-8") as handle:
        json.dump(value, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    with temporary.open("x", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _source_utilization(database: Path, manifest: Path) -> tuple[Path, dict]:
    database = _canonical_file(database, "source utilization database")
    document = _load_json(manifest, "source utilization release manifest")
    release = document.get("release")
    if document.get("schema_version") != 1 or not isinstance(release, dict):
        raise TaxonomyError("Source utilization release manifest is unsupported")
    if release.get("validation_state") != "passed":
        raise TaxonomyError("Source utilization release validation has not passed")
    if database.stat().st_size != int(release.get("byte_size", -1)):
        raise TaxonomyError("Source utilization byte size does not match release evidence")
    if sha256_file(database) != release.get("sha256"):
        raise TaxonomyError("Source utilization SHA-256 does not match release evidence")
    return database, release


def _download(url: str, path: Path, *, timeout: int = 90) -> None:
    request = urllib.request.Request(url, headers={"User-Agent": "cms-data-taxonomy/1"})
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response, temporary.open(
            "xb"
        ) as out:
            while chunk := response.read(1024 * 1024):
                out.write(chunk)
            out.flush()
            os.fsync(out.fileno())
        os.replace(temporary, path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _fetch_json(url: str, *, attempts: int = 4, timeout: int = 45) -> dict:
    last_error: BaseException | None = None
    for attempt in range(attempts):
        try:
            request = urllib.request.Request(url, headers={"User-Agent": "cms-data-taxonomy/1"})
            with urllib.request.urlopen(request, timeout=timeout) as response:
                value = json.load(response)
            if not isinstance(value, dict):
                raise TaxonomyError(f"Upstream JSON at {url} was not an object")
            return value
        except (OSError, urllib.error.URLError, json.JSONDecodeError, TaxonomyError) as error:
            last_error = error
            if attempt + 1 < attempts:
                time.sleep(0.5 * (2**attempt))
    assert last_error is not None
    raise TaxonomyError(f"Could not fetch {url}: {safe_error(last_error)}") from last_error


def _normalized_tokens(value: str, *, strip_salts: bool = False) -> tuple[str, ...]:
    normalized = re.sub(r"[^a-z0-9]+", " ", value.casefold())
    tokens = tuple(token for token in normalized.split() if token)
    if strip_salts:
        tokens = tuple(token for token in tokens if token not in SALT_WORDS)
    return tokens


def _ingredient_set(value: str, *, strip_salts: bool = False) -> tuple[tuple[str, ...], ...]:
    parts = re.split(r"\s*(?:/|\+|\band\b)\s*", value.casefold())
    ingredients = [_normalized_tokens(part, strip_salts=strip_salts) for part in parts]
    return tuple(sorted(item for item in ingredients if item))


def match_score(generic_name: str, concept_name: str) -> tuple[int, str] | None:
    """Return a conservative warehouse-generic to RxNorm ingredient match."""
    if _ingredient_set(generic_name) == _ingredient_set(concept_name):
        return 100, "exact_normalized"
    if _ingredient_set(generic_name, strip_salts=True) == _ingredient_set(
        concept_name, strip_salts=True
    ):
        return 95, "salt_normalized"
    return None


def _parse_rbcs(path: Path) -> tuple[list[dict], str]:
    rows_by_code: dict[str, dict] = {}
    release_year = ""
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        expected = {
            "HCPCS_Cd",
            "RBCS_Id",
            "RBCS_Cat",
            "RBCS_Cat_Desc",
            "RBCS_Cat_Subcat",
            "RBCS_Subcat_Desc",
            "RBCS_FamNumb",
            "RBCS_Family_Desc",
            "RBCS_Latest_Assignment",
            "First_RBCS_Release_Year",
        }
        if not reader.fieldnames or not expected.issubset(reader.fieldnames):
            raise TaxonomyError("RBCS CSV is missing required columns")
        for raw in reader:
            if raw["RBCS_Latest_Assignment"].strip() != "1":
                continue
            code = raw["HCPCS_Cd"].strip().upper()
            if not re.fullmatch(r"[A-Z0-9]{5}", code):
                raise TaxonomyError(f"RBCS contains an invalid HCPCS code: {code!r}")
            category_id = raw["RBCS_Cat"].strip()
            subcategory_id = raw["RBCS_Cat_Subcat"].strip()
            family_number = raw["RBCS_FamNumb"].strip().zfill(3)
            row = {
                "hcpcs_code": code,
                "rbcs_id": raw["RBCS_Id"].strip(),
                "category_id": category_id,
                "category_name": raw["RBCS_Cat_Desc"].strip(),
                "subcategory_id": subcategory_id,
                "subcategory_name": raw["RBCS_Subcat_Desc"].strip(),
                "family_id": f"{subcategory_id}-{family_number}",
                "family_name": raw["RBCS_Family_Desc"].strip(),
                "major_indicator": raw.get("RBCS_Major_Ind", "").strip(),
                "hcpcs_add_date": raw.get("HCPCS_Cd_Add_Dt", "").strip(),
                "hcpcs_end_date": raw.get("HCPCS_Cd_End_Dt", "").strip(),
                "rbcs_release_year": raw["First_RBCS_Release_Year"].strip(),
            }
            prior = rows_by_code.get(code)
            if prior is not None and prior != row:
                raise TaxonomyError(f"RBCS has conflicting current assignments for {code}")
            rows_by_code[code] = row
            release_year = max(release_year, row["rbcs_release_year"])
    rows = sorted(rows_by_code.values(), key=lambda item: item["hcpcs_code"])
    if not rows:
        raise TaxonomyError("RBCS current assignment set is empty")
    return rows, release_year


def _rxclass_version(base_url: str, source: str) -> str:
    payload = _fetch_json(f"{base_url}/version/{source}.json")
    version = payload.get("relaSourceVersion")
    if not isinstance(version, str) or not version.strip():
        raise TaxonomyError(f"RxClass did not report a {source} version")
    return version.strip()


def _class_rows(payload: dict) -> list[dict]:
    values = payload.get("rxclassDrugInfoList", {}).get("rxclassDrugInfo", [])
    return values if isinstance(values, list) else []


def _cached_drug_lookup(
    cache_root: Path, base_url: str, generic_name: str, source: str
) -> dict:
    key = hashlib.sha256(f"{source}\0{generic_name}".encode()).hexdigest()
    path = cache_root / source.casefold() / f"{key}.json"
    if path.is_file():
        return _load_json(path, f"cached {source} lookup")
    path.parent.mkdir(parents=True, exist_ok=True)
    query = urllib.parse.urlencode({"drugName": generic_name, "relaSource": source})
    payload = _fetch_json(f"{base_url}/class/byDrugName.json?{query}")
    _write_json(path, payload)
    return payload


def _cached_url(cache_root: Path, namespace: str, key: str, url: str) -> dict:
    identity = hashlib.sha256(key.encode()).hexdigest()
    path = cache_root / namespace / f"{identity}.json"
    if path.is_file():
        return _load_json(path, f"cached {namespace} lookup")
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = _fetch_json(url)
    _write_json(path, payload)
    return payload


def _related_concepts(payload: dict) -> list[dict]:
    groups = payload.get("relatedGroup", {}).get("conceptGroup", [])
    values: list[dict] = []
    for group in groups if isinstance(groups, list) else []:
        if not isinstance(group, dict):
            continue
        properties = group.get("conceptProperties", []) or []
        if isinstance(properties, list):
            values.extend(item for item in properties if isinstance(item, dict))
    return values


def _select_related_target(generic_name: str, concepts: list[dict]) -> dict | None:
    desired_parts = len(_ingredient_set(generic_name))
    desired_tty = "MIN" if desired_parts > 1 else "IN"
    desired_text = " ".join(_normalized_tokens(generic_name, strip_salts=True))
    ranked: list[tuple[float, dict]] = []
    for concept in concepts:
        name = str(concept.get("name", ""))
        if concept.get("tty") != desired_tty or len(_ingredient_set(name)) != desired_parts:
            continue
        candidate_text = " ".join(_normalized_tokens(name, strip_salts=True))
        similarity = difflib.SequenceMatcher(None, desired_text, candidate_text).ratio()
        if similarity >= 0.55:
            ranked.append((similarity, concept))
    if not ranked:
        return None
    return max(ranked, key=lambda item: item[0])[1]


def _rxnorm_fallback_lookup(
    cache_root: Path,
    rxclass_base_url: str,
    generic_name: str,
    source: str,
) -> tuple[dict, list[str]] | None:
    rxnorm_base = rxclass_base_url.removesuffix("/rxclass")
    approximate_query = urllib.parse.urlencode(
        {"term": generic_name, "maxEntries": 3, "option": 1}
    )
    approximate = _cached_url(
        cache_root,
        "rxnorm-approximate",
        generic_name,
        f"{rxnorm_base}/approximateTerm.json?{approximate_query}",
    )
    candidates = approximate.get("approximateGroup", {}).get("candidate", [])
    if not isinstance(candidates, list):
        return None
    top_rxcui = next(
        (
            str(item.get("rxcui"))
            for item in candidates
            if isinstance(item, dict) and str(item.get("rank")) == "1" and item.get("rxcui")
        ),
        None,
    )
    if top_rxcui is None:
        return None
    related = _cached_url(
        cache_root,
        "rxnorm-related",
        top_rxcui,
        f"{rxnorm_base}/rxcui/{urllib.parse.quote(top_rxcui)}/related.json?tty=IN+MIN",
    )
    related_concepts = _related_concepts(related)
    target = _select_related_target(generic_name, related_concepts)
    if target is None or not target.get("rxcui") or not target.get("name"):
        return None
    ingredients = [
        concept
        for concept in related_concepts
        if concept.get("tty") == "IN" and concept.get("rxcui") and concept.get("name")
    ]
    if not ingredients:
        return None
    combined: list[dict] = []
    for ingredient in ingredients:
        ingredient_rxcui = str(ingredient["rxcui"])
        query = urllib.parse.urlencode({"rxcui": ingredient_rxcui, "relaSource": source})
        payload = _cached_url(
            cache_root,
            f"rxclass-by-rxcui-{source.casefold()}",
            f"{source}\0{ingredient_rxcui}",
            f"{rxclass_base_url}/class/byRxcui.json?{query}",
        )
        combined.extend(_class_rows(payload))
    return (
        {"rxclassDrugInfoList": {"rxclassDrugInfo": combined}},
        [str(ingredient["name"]) for ingredient in ingredients],
    )


def _resolve_drug_lookup(
    cache_root: Path,
    base_url: str,
    generic_name: str,
    source: str,
) -> tuple[dict, list[str], str, int]:
    payload = _cached_drug_lookup(cache_root, base_url, generic_name, source)
    if any(
        match_score(generic_name, str(item.get("minConcept", {}).get("name", "")))
        for item in _class_rows(payload)
        if isinstance(item, dict)
    ):
        return payload, [generic_name], "publisher_name", 100
    fallback = _rxnorm_fallback_lookup(cache_root, base_url, generic_name, source)
    if fallback is None:
        return payload, [generic_name], "publisher_name", 100
    fallback_payload, target_names = fallback
    return fallback_payload, target_names, "rxnorm_approximate", 90


def _atc_catalog(base_url: str) -> list[dict]:
    payload = _fetch_json(f"{base_url}/allClasses.json?classTypes=ATC1-4")
    concepts = payload.get("rxclassMinConceptList", {}).get("rxclassMinConcept", [])
    if not isinstance(concepts, list) or not concepts:
        raise TaxonomyError("RxClass ATC catalog is empty")
    names = {
        str(item["classId"]): str(item["className"])
        for item in concepts
        if isinstance(item, dict) and item.get("classId") and item.get("className")
    }
    rows: list[dict] = []
    for class_id, class_name in sorted(names.items()):
        parent_candidates = [candidate for candidate in names if class_id.startswith(candidate)]
        parent_candidates = [candidate for candidate in parent_candidates if candidate != class_id]
        parent_id = max(parent_candidates, key=len) if parent_candidates else ""
        rows.append(
            {
                "source": "ATC",
                "class_type": "ATC",
                "class_id": class_id,
                "class_name": class_name,
                "parent_class_id": parent_id,
                "parent_class_name": names.get(parent_id, ""),
                "level": len(parent_candidates) + 1,
            }
        )
    return rows


def acquire_reference(
    *,
    output_root: Path,
    utilization_database: Path,
    utilization_release_manifest: Path,
    rbcs_url: str = RBCS_URL,
    rxclass_base_url: str = RXCLASS_BASE_URL,
    workers: int = 8,
) -> dict:
    """Acquire and seal one reference set for a specific utilization release."""
    if workers < 1 or workers > 32:
        raise TaxonomyError("Workers must be between 1 and 32")
    output_root = _canonical_directory(output_root, "taxonomy output root", create=True)
    database, source_release = _source_utilization(
        utilization_database, utilization_release_manifest
    )
    source_id = str(source_release.get("utilization_release_id", ""))
    if not source_id.startswith("utilization-"):
        raise TaxonomyError("Source utilization release ID is invalid")

    connection = duckdb.connect(str(database), read_only=True)
    try:
        generic_claims = {
            row[0]: int(row[1])
            for row in connection.execute(
                "SELECT trim(generic_name), sum(total_claims)::BIGINT "
                "FROM utilization_drug_dictionary "
                "WHERE nullif(trim(generic_name), '') IS NOT NULL GROUP BY 1 ORDER BY 1"
            ).fetchall()
        }
    finally:
        connection.close()
    generics = list(generic_claims)
    if not generics:
        raise TaxonomyError("Source utilization drug dictionary has no generic names")

    work_root = output_root / "acquisition-work" / source_id
    work_root.mkdir(parents=True, exist_ok=True)
    rbcs_path = work_root / "rbcs.csv"
    if not rbcs_path.is_file():
        _download(rbcs_url, rbcs_path)
    procedure_rows, rbcs_release_year = _parse_rbcs(rbcs_path)
    atc_rows = _atc_catalog(rxclass_base_url)
    versions = {
        source: _rxclass_version(rxclass_base_url, source) for source in RXCLASS_SOURCES
    }

    cache_root = work_root / "rxclass-cache"
    lookups: dict[tuple[str, str], tuple[dict, list[str], str, int]] = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                _resolve_drug_lookup, cache_root, rxclass_base_url, generic_name, source
            ): (generic_name, source)
            for generic_name in generics
            for source in RXCLASS_SOURCES
        }
        for future in as_completed(futures):
            key = futures[future]
            lookups[key] = future.result()

    class_index = {(row["source"], row["class_id"]): row for row in atc_rows}
    member_index: dict[tuple[str, str, str], dict] = {}
    mapped_generics: set[str] = set()
    mapped_by_source: dict[str, set[str]] = {source: set() for source in RXCLASS_SOURCES}
    for generic_name in generics:
        for source in RXCLASS_SOURCES:
            candidates: list[tuple[int, str, dict]] = []
            payload, target_names, resolution_method, resolution_score = lookups[
                (generic_name, source)
            ]
            for item in _class_rows(payload):
                if not isinstance(item, dict):
                    continue
                concept = item.get("minConcept", {})
                class_item = item.get("rxclassMinConceptItem", {})
                if not isinstance(concept, dict) or not isinstance(class_item, dict):
                    continue
                if source == "FDASPL" and class_item.get("classType") != "EPC":
                    continue
                matched = next(
                    (
                        score
                        for target_name in target_names
                        if (
                            score := match_score(
                                target_name, str(concept.get("name", ""))
                            )
                        )
                        is not None
                    ),
                    None,
                )
                if matched is None:
                    continue
                score, method = matched
                candidates.append((score, method, item))
            if not candidates:
                continue
            best_score = max(candidate[0] for candidate in candidates)
            for score, method, item in candidates:
                if score != best_score:
                    continue
                concept = item["minConcept"]
                class_item = item["rxclassMinConceptItem"]
                class_id = str(class_item["classId"])
                class_name = str(class_item["className"])
                class_type = "EPC" if source == "FDASPL" else "ATC"
                class_index.setdefault(
                    (source, class_id),
                    {
                        "source": source,
                        "class_type": class_type,
                        "class_id": class_id,
                        "class_name": class_name,
                        "parent_class_id": "",
                        "parent_class_name": "",
                        "level": 1,
                    },
                )
                key = (source, class_id, generic_name.casefold())
                member_index[key] = {
                    "source": source,
                    "class_type": class_type,
                    "class_id": class_id,
                    "generic_name": generic_name,
                    "rxcui": str(concept.get("rxcui", "")),
                    "concept_name": str(concept.get("name", "")),
                    "concept_tty": str(concept.get("tty", "")),
                    "match_score": min(score, resolution_score),
                    "match_method": (
                        resolution_method if resolution_method != "publisher_name" else method
                    ),
                    "source_version": versions[source],
                }
                mapped_generics.add(generic_name)
                mapped_by_source[source].add(generic_name)

    class_rows = sorted(class_index.values(), key=lambda row: (row["source"], row["class_id"]))
    member_rows = sorted(
        member_index.values(),
        key=lambda row: (row["source"], row["class_id"], row["generic_name"].casefold()),
    )
    if not member_rows:
        raise TaxonomyError("RxClass mapping produced no accepted drug class members")
    total_claims = sum(generic_claims.values())
    claim_coverage = {
        source: round(
            100.0
            * sum(generic_claims[name] for name in mapped_by_source[source])
            / total_claims,
            2,
        )
        for source in RXCLASS_SOURCES
    }
    if claim_coverage["ATC"] < MIN_ATC_CLAIM_COVERAGE_PCT:
        raise TaxonomyError(
            "ATC mapping covers only "
            f"{claim_coverage['ATC']}% of Part D claims; "
            f"minimum is {MIN_ATC_CLAIM_COVERAGE_PCT}%"
        )

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    identity = hashlib.sha256(
        f"{timestamp}\0{source_id}\0{sha256_file(rbcs_path)}\0{versions}".encode()
    ).hexdigest()[:10]
    reference_id = f"taxonomy-{timestamp}-{identity}"
    release_dir = output_root / "taxonomy-references" / reference_id
    release_dir.mkdir(parents=True, exist_ok=False)
    files = {
        "procedures": release_dir / "procedure_taxonomy.csv",
        "classes": release_dir / "drug_classes.csv",
        "members": release_dir / "drug_class_members.csv",
    }
    _write_csv(files["procedures"], list(procedure_rows[0]), procedure_rows)
    _write_csv(files["classes"], list(class_rows[0]), class_rows)
    _write_csv(files["members"], list(member_rows[0]), member_rows)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "reference": {
            "taxonomy_reference_id": reference_id,
            "created_at": utc_now(),
            "source_utilization_release_id": source_id,
            "source_utilization_sha256": source_release["sha256"],
            "rbcs": {
                "url": rbcs_url,
                "download_sha256": sha256_file(rbcs_path),
                "release_year": rbcs_release_year,
                "current_assignment_count": len(procedure_rows),
            },
            "rxclass": {
                "base_url": rxclass_base_url,
                "versions": versions,
                "generic_count": len(generics),
                "query_count": len(generics) * len(RXCLASS_SOURCES),
                "mapped_generic_count": len(mapped_generics),
                "unmapped_generic_count": len(generics) - len(mapped_generics),
                "mapped_generic_count_by_source": {
                    source: len(values) for source, values in mapped_by_source.items()
                },
                "claim_coverage_pct_by_source": claim_coverage,
                "class_count": len(class_rows),
                "member_count": len(member_rows),
            },
            "files": {
                name: {
                    "path": path.name,
                    "byte_size": path.stat().st_size,
                    "sha256": sha256_file(path),
                }
                for name, path in files.items()
            },
        },
    }
    manifest_path = release_dir / "manifest.json"
    _write_json(manifest_path, manifest)
    for path in [*files.values(), manifest_path]:
        path.chmod(0o440)
    return {
        "state": "passed",
        "taxonomy_reference_id": reference_id,
        "reference_dir": str(release_dir),
        "manifest": str(manifest_path),
        "procedure_count": len(procedure_rows),
        "class_count": len(class_rows),
        "member_count": len(member_rows),
        "mapped_generic_count": len(mapped_generics),
        "unmapped_generic_count": len(generics) - len(mapped_generics),
        "claim_coverage_pct_by_source": claim_coverage,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Acquire utilization taxonomy references")
    parser.add_argument("command", choices=("acquire",))
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--utilization-database", type=Path, required=True)
    parser.add_argument("--utilization-release-manifest", type=Path, required=True)
    parser.add_argument("--rbcs-url", default=RBCS_URL)
    parser.add_argument("--rxclass-base-url", default=RXCLASS_BASE_URL)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = acquire_reference(
            output_root=args.output_root,
            utilization_database=args.utilization_database,
            utilization_release_manifest=args.utilization_release_manifest,
            rbcs_url=args.rbcs_url,
            rxclass_base_url=args.rxclass_base_url,
            workers=args.workers,
        )
    except Exception as error:
        payload = {"state": "error", "error_summary": safe_error(error)}
        if args.json:
            print(json.dumps(payload, sort_keys=True))
        else:
            print(f"Taxonomy acquisition error: {payload['error_summary']}", file=sys.stderr)
        return 4
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(f"Taxonomy reference: {result['taxonomy_reference_id']}")
        print(f"State: {result['state']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
