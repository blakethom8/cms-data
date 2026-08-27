"""Code-first Medicare procedure and Part D utilization discovery."""

from __future__ import annotations

import math
import os
import re
import base64
import hashlib
import hmac
import json
import secrets
from typing import Literal

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from practices import (
    MAX_RADIUS_MILES,
    METRIC_SCOPE,
    parse_zip_codes,
    validate_proximity,
)

CONTRACT_VERSION = 1
CATALOG_CONTRACT_VERSION = 2
MAX_BASKET_SIZE = 50
NPI_RE = re.compile(r"^\d{10}$")
CATALOG_CURSOR_VERSION = 1
_CATALOG_CURSOR_SECRET = os.getenv("CMS_CATALOG_CURSOR_SECRET", "").encode() or secrets.token_bytes(32)


class ProcedureOption(BaseModel):
    value: str
    description: str | None = None
    is_drug_code: bool = False
    physician_count: int
    total_services: float
    total_payments: float


class DrugOption(BaseModel):
    brand: str
    generic: str
    physician_count: int
    claims: int
    drug_cost: float


class ProcedureOptionsResponse(BaseModel):
    contract_version: Literal[1] = CONTRACT_VERSION
    query: str
    data_year: int
    descriptions_enabled: bool
    results: list[ProcedureOption]


class DrugOptionsResponse(BaseModel):
    contract_version: Literal[1] = CONTRACT_VERSION
    query: str
    data_year: int
    results: list[DrugOption]


class ProcedureCatalogResponse(BaseModel):
    contract_version: Literal[1] = CONTRACT_VERSION
    query: str | None = None
    prefix: str | None = None
    data_year: int
    ordering: Literal["hcpcs_code"] = "hcpcs_code"
    descriptions_enabled: bool
    total: int
    offset: int
    limit: int
    returned_count: int
    has_more: bool
    results: list[ProcedureOption]


class DrugCatalogResponse(BaseModel):
    contract_version: Literal[1] = CONTRACT_VERSION
    query: str | None = None
    prefix: str | None = None
    data_year: int
    ordering: Literal["brand_generic"] = "brand_generic"
    total: int
    offset: int
    limit: int
    returned_count: int
    has_more: bool
    results: list[DrugOption]


class CatalogSnapshot(BaseModel):
    id: str
    data_year: int
    ordering: Literal[
        "hcpcs_code_v1",
        "brand_generic_v1",
        "procedure_family_v1",
        "drug_class_v1",
        "drug_class_member_v1",
        "request_order_v1",
    ]


class CatalogScope(BaseModel):
    query: str | None = None
    prefix: str | None = None
    code_from: str | None = None
    code_to: str | None = None
    family_id: str | None = None
    category_id: str | None = None
    subcategory_id: str | None = None
    class_source: Literal["ATC", "FDASPL"] | None = None
    class_id: str | None = None
    parent_class_id: str | None = None


class CatalogCount(BaseModel):
    value: int | None = None
    relation: Literal["exact", "unknown"] = "unknown"


class CatalogWindow(BaseModel):
    start_index: int
    previous_cursor: str | None = None
    next_cursor: str | None = None
    anchor_key: str | None = None
    anchor_resolution: Literal["exact", "nearest_before", "nearest_after", "start"]


class ProcedureCatalogRow(ProcedureOption):
    row_key: str


class DrugCatalogRow(DrugOption):
    row_key: str


class ProcedureCatalogV2Response(BaseModel):
    contract_version: Literal[2] = CATALOG_CONTRACT_VERSION
    snapshot: CatalogSnapshot
    scope: CatalogScope
    count: CatalogCount
    window: CatalogWindow
    descriptions_enabled: bool
    returned_count: int
    results: list[ProcedureCatalogRow]


class DrugCatalogV2Response(BaseModel):
    contract_version: Literal[2] = CATALOG_CONTRACT_VERSION
    snapshot: CatalogSnapshot
    scope: CatalogScope
    count: CatalogCount
    window: CatalogWindow
    returned_count: int
    results: list[DrugCatalogRow]


class CatalogResolveRequest(BaseModel):
    keys: list[str]


class CatalogResolvedValue(BaseModel):
    key: str
    selection_key: str
    kind: Literal["procedure", "brand", "generic"]
    available: Literal[True] = True
    row_key: str | None = None
    value: str | None = None
    brand: str | None = None
    generic: str | None = None
    description: str | None = None


class CatalogUnavailableValue(BaseModel):
    key: str
    available: Literal[False] = False


class CatalogResolveResponse(BaseModel):
    contract_version: Literal[2] = CATALOG_CONTRACT_VERSION
    snapshot: CatalogSnapshot
    descriptions_enabled: bool
    results: list[CatalogResolvedValue | CatalogUnavailableValue]


class ProcedureFamilySummary(BaseModel):
    family_id: str
    family_name: str
    category_id: str
    category_name: str
    subcategory_id: str
    subcategory_name: str
    available_code_count: int
    total_services: float
    total_payments: float


class ProcedureTaxonomyResponse(BaseModel):
    contract_version: Literal[1] = CONTRACT_VERSION
    query: str | None = None
    data_year: int
    total: int
    returned_count: int
    results: list[ProcedureFamilySummary]


class ProcedureFamilyResponse(BaseModel):
    contract_version: Literal[1] = CONTRACT_VERSION
    data_year: int
    family: ProcedureFamilySummary
    members: list[ProcedureOption]


class DrugClassSummary(BaseModel):
    source: Literal["ATC", "FDASPL"]
    class_type: Literal["ATC", "EPC"]
    class_id: str
    class_name: str
    parent_class_id: str | None = None
    parent_class_name: str | None = None
    hierarchy_level: int
    available_generic_count: int
    descendant_class_count: int
    total_claims: int
    total_drug_cost: float


class DrugClassesResponse(BaseModel):
    contract_version: Literal[1] = CONTRACT_VERSION
    query: str | None = None
    source: Literal["ATC", "FDASPL"]
    data_year: int
    attribution: str
    total: int
    returned_count: int
    results: list[DrugClassSummary]


class DrugClassMember(BaseModel):
    generic: str
    brands: list[str]
    physician_count: int
    claims: int
    drug_cost: float


class DrugClassResponse(BaseModel):
    contract_version: Literal[1] = CONTRACT_VERSION
    data_year: int
    attribution: str
    drug_class: DrugClassSummary
    members: list[DrugClassMember]


class ProcedureFamilyCatalogRow(ProcedureFamilySummary):
    row_key: str


class DrugClassCatalogRow(DrugClassSummary):
    row_key: str


class DrugClassMemberCatalogRow(DrugClassMember):
    row_key: str
    selection_key: str


class DrugClassMembersV2Response(BaseModel):
    contract_version: Literal[2] = CATALOG_CONTRACT_VERSION
    snapshot: CatalogSnapshot
    scope: CatalogScope
    count: CatalogCount
    window: CatalogWindow
    attribution: str
    returned_count: int
    results: list[DrugClassMemberCatalogRow]


class ProcedureTaxonomyV2Response(BaseModel):
    contract_version: Literal[2] = CATALOG_CONTRACT_VERSION
    snapshot: CatalogSnapshot
    scope: CatalogScope
    count: CatalogCount
    window: CatalogWindow
    returned_count: int
    results: list[ProcedureFamilyCatalogRow]


class DrugClassesV2Response(BaseModel):
    contract_version: Literal[2] = CATALOG_CONTRACT_VERSION
    snapshot: CatalogSnapshot
    scope: CatalogScope
    count: CatalogCount
    window: CatalogWindow
    attribution: str
    returned_count: int
    results: list[DrugClassCatalogRow]


class ProcedureBreakdown(BaseModel):
    value: str
    description: str | None = None
    is_drug_code: bool = False
    services: float
    payments: float
    beneficiaries: int | None = None


class DrugBreakdown(BaseModel):
    brand: str
    generic: str
    claims: int
    drug_cost: float


class ProviderIdentity(BaseModel):
    npi: str = Field(pattern=r"^\d{10}$")
    name: str
    first_name: str | None = None
    last_name: str | None = None
    credentials: str | None = None
    specialty: str | None = None
    address: str
    city: str
    state: str
    zip5: str
    lat: float | None = None
    lng: float | None = None
    distance_miles: float | None = None
    metric_scope: Literal["national_npi_totals"] = METRIC_SCOPE


class ProcedureSearchResult(ProviderIdentity):
    selected_services: float
    selected_payments: float
    selected_beneficiaries: int | None = None
    partb_services: float | None = None
    partb_payments: float | None = None
    selected_service_share: float | None = None
    selected_payment_share: float | None = None
    matched_codes: list[ProcedureBreakdown] = Field(default_factory=list)


class DrugSearchResult(ProviderIdentity):
    selected_claims: int
    selected_drug_cost: float
    partd_claims: int | None = None
    partd_drug_cost: float | None = None
    selected_claim_share: float | None = None
    selected_cost_share: float | None = None
    matched_drugs: list[DrugBreakdown] = Field(default_factory=list)


class ProcedureSearchResponse(BaseModel):
    contract_version: Literal[1] = CONTRACT_VERSION
    mode: Literal["procedures"] = "procedures"
    metric_scope: Literal["national_npi_totals"] = METRIC_SCOPE
    data_year: int
    selected_hcpcs: list[str]
    total: int
    returned_count: int
    truncated: bool
    results: list[ProcedureSearchResult]


class DrugSearchResponse(BaseModel):
    contract_version: Literal[1] = CONTRACT_VERSION
    mode: Literal["drugs"] = "drugs"
    metric_scope: Literal["national_npi_totals"] = METRIC_SCOPE
    data_year: int
    selected_brands: list[str]
    selected_generics: list[str]
    total: int
    returned_count: int
    truncated: bool
    results: list[DrugSearchResult]


def _descriptions_enabled() -> bool:
    """Return whether the explicit AMA/CPT description release gate is open."""
    return os.getenv("HCPCS_DESCRIPTIONS_ENABLED", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }


def _normalize_values(values: list[str] | None, label: str) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in values or []:
        for item in raw.split(","):
            value = " ".join(item.split())
            if not value:
                continue
            if len(value) > 255:
                raise ValueError(f"{label} values must be at most 255 characters")
            key = value.casefold()
            if key not in seen:
                seen.add(key)
                normalized.append(value)
    if len(normalized) > MAX_BASKET_SIZE:
        raise ValueError(f"At most {MAX_BASKET_SIZE} {label} values may be selected")
    return normalized


def _location_sql(
    *,
    city: str | None,
    state: str | None,
    zip_code: str | None,
    zips: str | None,
    lat: float | None,
    lng: float | None,
    radius_miles: float,
    specialties: list[str] | None,
) -> tuple[str, list, str, list, str, list]:
    """Build bounded NPPES-primary geography and optional specialty SQL."""
    try:
        selected_zips = parse_zip_codes(zips, zip_code)
        proximity = validate_proximity(lat, lng, radius_miles)
        selected_specialties = _normalize_values(specialties, "specialty")
        if city and not state:
            raise ValueError("City searches require a two-letter state")
        if state and not re.fullmatch(r"[A-Za-z]{2}", state.strip()):
            raise ValueError("State must be a two-letter abbreviation")
        if not any((city, state, selected_zips, proximity)):
            raise ValueError("Choose a city and state, ZIP boundary, or radius origin")
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    clauses: list[str] = []
    params: list = []
    if state:
        clauses.append("upper(p.state) = ?")
        params.append(state.strip().upper())
    if city:
        clauses.append("upper(p.city) = ?")
        params.append(city.strip().upper())
    if selected_zips:
        clauses.append("p.zip5 in (" + ",".join(["?"] * len(selected_zips)) + ")")
        params.extend(selected_zips)

    distance_select = "cast(null as double) as distance_miles"
    distance_params: list = []
    distance_filter = ""
    distance_filter_params: list = []
    if proximity:
        assert lat is not None and lng is not None
        lat_delta = radius_miles / 69.0
        lng_delta = radius_miles / (69.0 * max(0.1, abs(math.cos(math.radians(lat)))))
        distance_select = """
            3959.0 * 2.0 * asin(sqrt(least(1.0, greatest(0.0,
                pow(sin(radians(p.latitude - ?) / 2.0), 2)
                + cos(radians(?)) * cos(radians(p.latitude))
                * pow(sin(radians(p.longitude - ?) / 2.0), 2)
            )))) as distance_miles
        """
        distance_params = [lat, lat, lng]
        clauses.extend(
            [
                "p.latitude between ? and ?",
                "p.longitude between ? and ?",
            ]
        )
        params.extend(
            [lat - lat_delta, lat + lat_delta, lng - lng_delta, lng + lng_delta]
        )
        distance_filter = "and distance_miles <= ?"
        distance_filter_params = [radius_miles]

    if selected_specialties:
        clauses.append(
            "exists (select 1 from unnest(p.specialties) t(specialty) where "
            "upper(specialty) in (" + ",".join(["?"] * len(selected_specialties)) + "))"
        )
        params.extend(item.upper() for item in selected_specialties)
    return (
        " and ".join(clauses),
        params,
        distance_select,
        distance_params,
        distance_filter,
        distance_filter_params,
    )


def _rows(cursor) -> list[dict]:
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _latest_year(conn, table: str) -> int:
    row = conn.execute(f"SELECT max(data_year) FROM {table}").fetchone()
    if row is None or row[0] is None:
        raise HTTPException(status_code=503, detail="Utilization data is unavailable")
    return int(row[0])


def _catalog_error(status_code: int, reason: str) -> HTTPException:
    """Return a public V2 error without database or cursor implementation detail."""
    return HTTPException(status_code=status_code, detail={"reason": reason})


def _compact(value: str | None) -> str | None:
    return " ".join(value.split()) if value else None


def _procedure_row_key(code: str) -> str:
    return f"hcpcs:{code.strip().upper()}"


def _drug_row_key(brand: str, generic: str) -> str:
    payload = json.dumps([" ".join(brand.split()), " ".join(generic.split())], separators=(",", ":"))
    token = base64.urlsafe_b64encode(payload.encode()).decode().rstrip("=")
    return f"drug:{token}"


def _decode_drug_row_key(value: str) -> tuple[str, str] | None:
    if not value.startswith("drug:"):
        return None
    token = value.removeprefix("drug:")
    try:
        decoded = base64.urlsafe_b64decode(token + "=" * (-len(token) % 4)).decode()
        pair = json.loads(decoded)
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError):
        return None
    if (
        not isinstance(pair, list)
        or len(pair) != 2
        or not all(isinstance(item, str) and item for item in pair)
    ):
        return None
    return pair[0], pair[1]


def _cursor_encode(payload: dict) -> str:
    """Encode a signed, opaque V2 cursor. Never expose decoded values to callers."""
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    signature = hmac.new(_CATALOG_CURSOR_SECRET, raw, hashlib.sha256).digest()
    return base64.urlsafe_b64encode(raw + signature).decode().rstrip("=")


def _cursor_decode(value: str, *, kind: str, snapshot_id: str, fingerprint: str) -> dict:
    try:
        raw = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
        body, signature = raw[:-32], raw[-32:]
        if not hmac.compare_digest(
            signature, hmac.new(_CATALOG_CURSOR_SECRET, body, hashlib.sha256).digest()
        ):
            raise ValueError
        payload = json.loads(body)
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError):
        raise _catalog_error(422, "catalog.invalid_cursor") from None
    if (
        not isinstance(payload, dict)
        or payload.get("v") != CATALOG_CURSOR_VERSION
        or payload.get("kind") != kind
        or payload.get("snapshot") != snapshot_id
        or payload.get("fingerprint") != fingerprint
        or payload.get("direction") not in {"after", "before"}
        or not isinstance(payload.get("ordinal"), int)
        or not isinstance(payload.get("boundary"), list)
    ):
        if isinstance(payload, dict) and payload.get("snapshot") not in {None, snapshot_id}:
            raise _catalog_error(409, "catalog.snapshot_changed")
        raise _catalog_error(422, "catalog.invalid_cursor")
    return payload


def _scope_fingerprint(kind: str, scope: BaseModel) -> str:
    document = {"kind": kind, **scope.model_dump()}
    return hashlib.sha256(
        json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _snapshot_id(get_snapshot_id, data_year: int) -> str:
    value = get_snapshot_id() if get_snapshot_id is not None else None
    if isinstance(value, str) and value.strip():
        return value.strip()
    # Tests and non-bundle developer processes have no production release ledger. The
    # data-year fallback is still stable for the immutable dictionary being served.
    return f"utilization-data-year-{data_year}"


def get_utilization_router(get_conn, get_snapshot_id=None):
    router = APIRouter(prefix="/utilization", tags=["Medicare Utilization"])

    nlm_attribution = (
        "This product uses publicly available data from the U.S. National Library of Medicine "
        "(NLM), National Institutes of Health, Department of Health and Human Services; NLM is "
        "not responsible for the product and does not endorse or recommend this or any other "
        "product."
    )

    def procedure_scope(
        q: str | None,
        prefix: str | None,
        code_from: str | None,
        code_to: str | None,
        family_id: str | None,
    ) -> tuple[CatalogScope, list[str], list]:
        query = _compact(q)
        normalized_prefix = "".join((prefix or "").split()).upper() or None
        lower = "".join((code_from or "").split()).upper() or None
        upper = "".join((code_to or "").split()).upper() or None
        family = family_id.strip().upper() if family_id else None
        if (q is not None and not query) or (prefix is not None and not normalized_prefix):
            raise _catalog_error(422, "catalog.invalid_request")
        if any(value and not re.fullmatch(r"[A-Z0-9]{1,10}", value) for value in (normalized_prefix, lower, upper)):
            raise _catalog_error(422, "catalog.invalid_request")
        if lower and upper and lower > upper:
            raise _catalog_error(422, "catalog.invalid_request")
        if family and not re.fullmatch(r"[A-Z0-9]{1,8}-[A-Z0-9]{1,8}", family):
            raise _catalog_error(422, "catalog.invalid_request")
        scope = CatalogScope(query=query, prefix=normalized_prefix, code_from=lower, code_to=upper, family_id=family)
        clauses = ["d.data_year=latest.data_year"]
        params: list = []
        if normalized_prefix:
            clauses.append("upper(d.hcpcs_code) like ?")
            params.append(normalized_prefix + "%")
        if lower:
            clauses.append("upper(d.hcpcs_code) >= ?")
            params.append(lower)
        if upper:
            clauses.append("upper(d.hcpcs_code) <= ?")
            params.append(upper)
        if query:
            predicates = ["lower(d.hcpcs_code) like ?"]
            params.append("%" + query.lower() + "%")
            if _descriptions_enabled():
                predicates.append("lower(d.hcpcs_description) like ?")
                params.append("%" + query.lower() + "%")
            clauses.append("(" + " or ".join(predicates) + ")")
        if family:
            clauses.append("exists (select 1 from utilization_procedure_taxonomy t where t.hcpcs_code=d.hcpcs_code and t.family_id=?)")
            params.append(family)
        return scope, clauses, params

    def drug_scope(
        q: str | None,
        prefix: str | None,
        class_source: Literal["ATC", "FDASPL"] | None,
        class_id: str | None,
    ) -> tuple[CatalogScope, list[str], list]:
        query = _compact(q)
        normalized_prefix = _compact(prefix)
        normalized_class = class_id.strip() if class_id else None
        if (q is not None and not query) or (prefix is not None and not normalized_prefix):
            raise _catalog_error(422, "catalog.invalid_request")
        if (class_source is None) != (normalized_class is None) or (normalized_class and len(normalized_class) > 32):
            raise _catalog_error(422, "catalog.invalid_request")
        scope = CatalogScope(query=query, prefix=normalized_prefix, class_source=class_source, class_id=normalized_class)
        clauses = ["d.data_year=latest.data_year"]
        params: list = []
        if normalized_prefix:
            clauses.append("(lower(d.brand_name) like ? or lower(d.generic_name) like ?)")
            params.extend([normalized_prefix.lower() + "%"] * 2)
        if query:
            clauses.append("(lower(d.brand_name) like ? or lower(d.generic_name) like ?)")
            params.extend(["%" + query.lower() + "%"] * 2)
        if normalized_class and class_source:
            clauses.append(
                "exists (select 1 from utilization_drug_class_members m where m.source=? "
                "and lower(m.generic_name)=lower(d.generic_name) and "
                "(m.class_id=? or (?='ATC' and m.class_id like ? || '%')) )"
            )
            params.extend([class_source, normalized_class, class_source, normalized_class])
        return scope, clauses, params

    def catalog_cursor(kind: str, snapshot_id: str, fingerprint: str, direction: str, boundary: list, ordinal: int) -> str:
        return _cursor_encode({"v": CATALOG_CURSOR_VERSION, "kind": kind, "snapshot": snapshot_id, "fingerprint": fingerprint, "direction": direction, "boundary": boundary, "ordinal": ordinal})

    @router.get("/v2/procedures/taxonomy", response_model=ProcedureTaxonomyV2Response)
    async def procedure_taxonomy_v2(
        q: str | None = Query(None, max_length=100),
        category_id: str | None = Query(None, max_length=8),
        subcategory_id: str | None = Query(None, max_length=8),
        after: str | None = Query(None, max_length=4096),
        before: str | None = Query(None, max_length=4096),
        anchor: str | None = Query(None, max_length=512),
        limit: int = Query(100, ge=1, le=200),
    ):
        if sum(value is not None for value in (after, before, anchor)) > 1:
            raise _catalog_error(422, "catalog.invalid_request")
        query = _compact(q)
        normalized_category = category_id.strip().upper() if category_id else None
        normalized_subcategory = subcategory_id.strip().upper() if subcategory_id else None
        if (q is not None and not query) or any(
            value and not re.fullmatch(r"[A-Z0-9]{1,8}", value)
            for value in (normalized_category, normalized_subcategory)
        ):
            raise _catalog_error(422, "catalog.invalid_request")
        scope = CatalogScope(
            query=query,
            category_id=normalized_category,
            subcategory_id=normalized_subcategory,
        )
        clauses = ["lower(t.family_name) != 'no rbcs family'"]
        params: list = []
        if query:
            clauses.append("(lower(t.family_name) like ? or lower(t.subcategory_name) like ? or lower(t.category_name) like ? or lower(t.hcpcs_code) like ?)")
            params.extend([f"%{query.lower()}%"] * 4)
        if normalized_category:
            clauses.append("upper(t.category_id)=?")
            params.append(normalized_category)
        if normalized_subcategory:
            clauses.append("upper(t.subcategory_id)=?")
            params.append(normalized_subcategory)
        conn = get_conn()
        data_year = _latest_year(conn, "utilization_procedure_dictionary")
        snapshot_id = _snapshot_id(get_snapshot_id, data_year)
        snapshot = CatalogSnapshot(id=snapshot_id, data_year=data_year, ordering="procedure_family_v1")
        fingerprint = _scope_fingerprint("procedure_taxonomy", scope)
        base = f"""with latest as (select max(data_year) data_year from utilization_procedure_dictionary), families as (
            select t.family_id, any_value(t.family_name) family_name, any_value(t.category_id) category_id,
                   any_value(t.category_name) category_name, any_value(t.subcategory_id) subcategory_id,
                   any_value(t.subcategory_name) subcategory_name, count(distinct d.hcpcs_code)::INTEGER available_code_count,
                   sum(d.total_services)::DOUBLE total_services, sum(d.total_payments)::DOUBLE total_payments
            from utilization_procedure_taxonomy t join utilization_procedure_dictionary d on d.hcpcs_code=t.hcpcs_code
            join latest l on d.data_year=l.data_year where {' and '.join(clauses)}
            group by t.family_id
        ) """
        total = int(conn.execute(base + "select count(*) from families", params).fetchone()[0])
        page_params = list(params)
        predicate = ""
        order = "lower(family_name), family_id"
        start_index = 0
        cursor_payload: dict | None = None
        resolution: Literal["exact", "nearest_before", "nearest_after", "start"] = "start"
        if after or before:
            direction = "after" if after else "before"
            cursor_payload = _cursor_decode(after or before or "", kind="procedure_taxonomy", snapshot_id=snapshot_id, fingerprint=fingerprint)
            if cursor_payload["direction"] != direction or len(cursor_payload["boundary"]) != 2:
                raise _catalog_error(422, "catalog.invalid_cursor")
            name, family = cursor_payload["boundary"]
            comparator = ">" if direction == "after" else "<"
            predicate = f" where (lower(family_name), family_id) {comparator} (?, ?)"
            page_params.extend([name, family])
            start_index = cursor_payload["ordinal"] + 1 if direction == "after" else max(0, cursor_payload["ordinal"] - limit)
            if direction == "before":
                order = "lower(family_name) desc, family_id desc"
        elif anchor:
            if not anchor.startswith("family:"):
                raise _catalog_error(422, "catalog.invalid_request")
            family = anchor.removeprefix("family:").strip().upper()
            if not re.fullmatch(r"[A-Z0-9]{1,8}-[A-Z0-9]{1,8}", family):
                raise _catalog_error(422, "catalog.invalid_request")
            target = conn.execute(base + "select family_id, family_name from families where family_id=?", [*params, family]).fetchone()
            if target:
                preceding = int(conn.execute(base + "select count(*) from families where (lower(family_name), family_id) < (?, ?)", [*params, target[1].lower(), target[0]]).fetchone()[0])
                start_index, resolution = max(0, preceding - limit // 2), "exact"
            else:
                # Family identifiers have no meaningful lexical insertion order for an absent record.
                start_index, resolution = 0, "start"
            page_params = [*params, limit, start_index]
        if anchor is None:
            page_params.append(limit)
        rows = _rows(conn.execute(base + f"select * from families{predicate} order by {order} limit ?" + (" offset ?" if anchor is not None else ""), page_params))
        if after or before:
            if before:
                rows.reverse()
                start_index = max(0, cursor_payload["ordinal"] - len(rows)) if cursor_payload else 0
        first, last = (rows[0], rows[-1]) if rows else (None, None)
        anchor_key = f"family:{family}" if anchor and resolution == "exact" else (f"family:{first['family_id']}" if first else None)
        previous_cursor = catalog_cursor("procedure_taxonomy", snapshot_id, fingerprint, "before", [first["family_name"].lower(), first["family_id"]], start_index) if first and start_index > 0 else None
        next_cursor = catalog_cursor("procedure_taxonomy", snapshot_id, fingerprint, "after", [last["family_name"].lower(), last["family_id"]], start_index + len(rows) - 1) if last and start_index + len(rows) < total else None
        for row in rows:
            row["row_key"] = f"family:{row['family_id']}"
        return ProcedureTaxonomyV2Response(snapshot=snapshot, scope=scope, count=CatalogCount(value=total, relation="exact"), window=CatalogWindow(start_index=start_index, previous_cursor=previous_cursor, next_cursor=next_cursor, anchor_key=anchor_key, anchor_resolution=resolution), returned_count=len(rows), results=[ProcedureFamilyCatalogRow(**row) for row in rows])

    @router.get("/v2/drugs/classes", response_model=DrugClassesV2Response)
    async def drug_classes_v2(
        q: str | None = Query(None, max_length=100),
        source: Literal["ATC", "FDASPL"] = "ATC",
        parent_class_id: str | None = Query(None, max_length=32),
        after: str | None = Query(None, max_length=4096),
        before: str | None = Query(None, max_length=4096),
        anchor: str | None = Query(None, max_length=512),
        limit: int = Query(100, ge=1, le=200),
    ):
        if sum(value is not None for value in (after, before, anchor)) > 1:
            raise _catalog_error(422, "catalog.invalid_request")
        query, normalized_parent = _compact(q), _compact(parent_class_id)
        if (q is not None and not query) or (parent_class_id is not None and not normalized_parent):
            raise _catalog_error(422, "catalog.invalid_request")
        scope = CatalogScope(
            query=query,
            class_source=source,
            parent_class_id=normalized_parent,
        )
        clauses, params = ["c.source=?"], [source]
        if normalized_parent:
            clauses.append("c.parent_class_id=?")
            params.append(normalized_parent)
        if query:
            clauses.append("(lower(c.class_name) like ? or lower(c.class_id) like ? or exists (select 1 from utilization_drug_class_members qm where qm.source=c.source and (qm.class_id=c.class_id or (c.source='ATC' and qm.class_id like c.class_id || '%')) and (lower(qm.generic_name) like ? or lower(qm.concept_name) like ?)))")
            params.extend([f"%{query.lower()}%"] * 4)
        conn = get_conn()
        data_year = _latest_year(conn, "utilization_drug_dictionary")
        snapshot_id = _snapshot_id(get_snapshot_id, data_year)
        snapshot = CatalogSnapshot(id=snapshot_id, data_year=data_year, ordering="drug_class_v1")
        fingerprint = _scope_fingerprint("drug_classes", scope)
        base = f"""with latest as (select max(data_year) data_year from utilization_drug_dictionary), classes as (
            select c.source, c.class_type, c.class_id, c.class_name, c.parent_class_id, c.parent_class_name, c.hierarchy_level,
                   count(distinct m.generic_name)::INTEGER available_generic_count, count(distinct m.class_id)::INTEGER descendant_class_count,
                   sum(d.total_claims)::BIGINT total_claims, sum(d.total_drug_cost)::DOUBLE total_drug_cost
            from utilization_drug_classes c join utilization_drug_class_members m on m.source=c.source and (m.class_id=c.class_id or (c.source='ATC' and m.class_id like c.class_id || '%'))
            join utilization_drug_dictionary d on lower(d.generic_name)=lower(m.generic_name) join latest l on d.data_year=l.data_year
            where {' and '.join(clauses)} group by c.source, c.class_type, c.class_id, c.class_name, c.parent_class_id, c.parent_class_name, c.hierarchy_level
        ) """
        total = int(conn.execute(base + "select count(*) from classes", params).fetchone()[0])
        page_params, predicate, order, start_index = list(params), "", "hierarchy_level, lower(class_name), class_id", 0
        cursor_payload: dict | None = None
        resolution: Literal["exact", "nearest_before", "nearest_after", "start"] = "start"
        if after or before:
            direction = "after" if after else "before"
            cursor_payload = _cursor_decode(after or before or "", kind="drug_classes", snapshot_id=snapshot_id, fingerprint=fingerprint)
            if cursor_payload["direction"] != direction or len(cursor_payload["boundary"]) != 3:
                raise _catalog_error(422, "catalog.invalid_cursor")
            level, name, class_id = cursor_payload["boundary"]
            comparator = ">" if direction == "after" else "<"
            predicate = f" where (hierarchy_level, lower(class_name), class_id) {comparator} (?, ?, ?)"
            page_params.extend([level, name, class_id])
            start_index = cursor_payload["ordinal"] + 1 if direction == "after" else max(0, cursor_payload["ordinal"] - limit)
            if direction == "before":
                order = "hierarchy_level desc, lower(class_name) desc, class_id desc"
        elif anchor:
            match = re.fullmatch(r"class:(ATC|FDASPL):(.{1,32})", anchor)
            if not match or match.group(1) != source:
                raise _catalog_error(422, "catalog.invalid_request")
            target = conn.execute(base + "select class_id, class_name, hierarchy_level from classes where class_id=?", [*params, match.group(2)]).fetchone()
            if target:
                preceding = int(conn.execute(base + "select count(*) from classes where (hierarchy_level, lower(class_name), class_id) < (?, ?, ?)", [*params, target[2], target[1].lower(), target[0]]).fetchone()[0])
                start_index, resolution = max(0, preceding - limit // 2), "exact"
            page_params = [*params, limit, start_index]
        if anchor is None:
            page_params.append(limit)
        rows = _rows(conn.execute(base + f"select * from classes{predicate} order by {order} limit ?" + (" offset ?" if anchor is not None else ""), page_params))
        if before:
            rows.reverse()
            start_index = max(0, cursor_payload["ordinal"] - len(rows)) if cursor_payload else 0
        first, last = (rows[0], rows[-1]) if rows else (None, None)
        anchor_key = f"class:{source}:{anchor.split(':', 2)[2]}" if anchor and resolution == "exact" else (f"class:{source}:{first['class_id']}" if first else None)
        previous_cursor = catalog_cursor("drug_classes", snapshot_id, fingerprint, "before", [first["hierarchy_level"], first["class_name"].lower(), first["class_id"]], start_index) if first and start_index > 0 else None
        next_cursor = catalog_cursor("drug_classes", snapshot_id, fingerprint, "after", [last["hierarchy_level"], last["class_name"].lower(), last["class_id"]], start_index + len(rows) - 1) if last and start_index + len(rows) < total else None
        for row in rows:
            row["row_key"] = f"class:{row['source']}:{row['class_id']}"
        return DrugClassesV2Response(snapshot=snapshot, scope=scope, count=CatalogCount(value=total, relation="exact"), window=CatalogWindow(start_index=start_index, previous_cursor=previous_cursor, next_cursor=next_cursor, anchor_key=anchor_key, anchor_resolution=resolution), attribution=nlm_attribution, returned_count=len(rows), results=[DrugClassCatalogRow(**row) for row in rows])

    @router.get("/v2/procedures/catalog", response_model=ProcedureCatalogV2Response)
    async def procedure_catalog_v2(
        q: str | None = Query(None, max_length=100),
        prefix: str | None = Query(None, max_length=10),
        code_from: str | None = Query(None, max_length=10),
        code_to: str | None = Query(None, max_length=10),
        family_id: str | None = Query(None, max_length=20),
        after: str | None = Query(None, max_length=4096),
        before: str | None = Query(None, max_length=4096),
        anchor: str | None = Query(None, max_length=512),
        limit: int = Query(100, ge=1, le=200),
    ):
        if sum(value is not None for value in (after, before, anchor)) > 1:
            raise _catalog_error(422, "catalog.invalid_request")
        scope, clauses, params = procedure_scope(q, prefix, code_from, code_to, family_id)
        conn = get_conn()
        data_year = _latest_year(conn, "utilization_procedure_dictionary")
        snapshot_id = _snapshot_id(get_snapshot_id, data_year)
        snapshot = CatalogSnapshot(id=snapshot_id, data_year=data_year, ordering="hcpcs_code_v1")
        fingerprint = _scope_fingerprint("procedures", scope)
        where = " and ".join(clauses)
        total = int(conn.execute(f"with latest as (select max(data_year) data_year from utilization_procedure_dictionary) select count(*) from utilization_procedure_dictionary d, latest where {where}", params).fetchone()[0])
        direction = "root"
        cursor_payload: dict | None = None
        anchor_resolution: Literal["exact", "nearest_before", "nearest_after", "start"] = "start"
        start_index = 0
        order = "upper(d.hcpcs_code), d.hcpcs_code"
        page_params = list(params)
        key_clause = ""
        if after or before:
            direction = "after" if after else "before"
            cursor_payload = _cursor_decode(after or before or "", kind="procedures", snapshot_id=snapshot_id, fingerprint=fingerprint)
            if cursor_payload["direction"] != direction or len(cursor_payload["boundary"]) != 2:
                raise _catalog_error(422, "catalog.invalid_cursor")
            first, second = cursor_payload["boundary"]
            comparator = ">" if direction == "after" else "<"
            key_clause = f" and (upper(d.hcpcs_code) {comparator} ? or (upper(d.hcpcs_code)=? and d.hcpcs_code {comparator} ?))"
            page_params.extend([first, first, second])
            start_index = cursor_payload["ordinal"] + 1 if direction == "after" else max(0, cursor_payload["ordinal"] - limit)
            if direction == "before":
                order = "upper(d.hcpcs_code) desc, d.hcpcs_code desc"
        elif anchor:
            if not anchor.startswith("hcpcs:"):
                raise _catalog_error(422, "catalog.invalid_request")
            requested = anchor.removeprefix("hcpcs:").strip().upper()
            if not re.fullmatch(r"[A-Z0-9]{1,10}", requested):
                raise _catalog_error(422, "catalog.invalid_request")
            preceding = int(conn.execute(f"with latest as (select max(data_year) data_year from utilization_procedure_dictionary) select count(*) from utilization_procedure_dictionary d, latest where {where} and (upper(d.hcpcs_code) < ? or (upper(d.hcpcs_code)=? and d.hcpcs_code < ?))", [*params, requested, requested, requested]).fetchone()[0])
            exact = conn.execute(f"with latest as (select max(data_year) data_year from utilization_procedure_dictionary) select d.hcpcs_code from utilization_procedure_dictionary d, latest where {where} and upper(d.hcpcs_code)=? and d.hcpcs_code=?", [*params, requested, requested]).fetchone()
            if exact:
                start_index, anchor_resolution = max(0, preceding - limit // 2), "exact"
            else:
                following = conn.execute(f"with latest as (select max(data_year) data_year from utilization_procedure_dictionary) select d.hcpcs_code from utilization_procedure_dictionary d, latest where {where} and upper(d.hcpcs_code) >= ? order by {order} limit 1", [*params, requested]).fetchone()
                if following:
                    start_index, anchor_resolution = max(0, preceding - limit // 2), "nearest_after"
                elif total:
                    start_index, anchor_resolution = max(0, total - limit), "nearest_before"
                else:
                    start_index = 0
            page_params = [*params, limit, start_index]
            key_clause = ""
            order = "upper(d.hcpcs_code), d.hcpcs_code"
        if anchor is None:
            page_params.extend([limit])
        enabled = _descriptions_enabled()
        rows = _rows(conn.execute(f"with latest as (select max(data_year) data_year from utilization_procedure_dictionary) select d.hcpcs_code as \"value\", {"d.hcpcs_description" if enabled else "cast(null as varchar)"} as description, coalesce(d.hcpcs_drug_ind='Y', false) as is_drug_code, d.physician_count, d.total_services, d.total_payments from utilization_procedure_dictionary d, latest where {where}{key_clause} order by {order} limit ?" + (" offset ?" if anchor is not None else ""), page_params))
        if direction == "before":
            rows.reverse()
            start_index = max(0, cursor_payload["ordinal"] - len(rows)) if cursor_payload else 0
        for row in rows:
            row["row_key"] = _procedure_row_key(row["value"])
        anchor_key = rows[0]["row_key"] if rows else None
        if anchor and rows:
            requested_code = anchor.removeprefix("hcpcs:").strip().upper()
            if anchor_resolution == "exact":
                anchor_key = _procedure_row_key(requested_code)
            elif anchor_resolution == "nearest_after":
                anchor_key = next(
                    (row["row_key"] for row in rows if row["value"].upper() >= requested_code),
                    rows[-1]["row_key"],
                )
            elif anchor_resolution == "nearest_before":
                anchor_key = rows[-1]["row_key"]
        previous_cursor = catalog_cursor("procedures", snapshot_id, fingerprint, "before", [rows[0]["value"].upper(), rows[0]["value"]], start_index) if rows and start_index > 0 else None
        next_cursor = catalog_cursor("procedures", snapshot_id, fingerprint, "after", [rows[-1]["value"].upper(), rows[-1]["value"]], start_index + len(rows) - 1) if rows and start_index + len(rows) < total else None
        return ProcedureCatalogV2Response(snapshot=snapshot, scope=scope, count=CatalogCount(value=total, relation="exact"), window=CatalogWindow(start_index=start_index, previous_cursor=previous_cursor, next_cursor=next_cursor, anchor_key=anchor_key, anchor_resolution=anchor_resolution), descriptions_enabled=enabled, returned_count=len(rows), results=[ProcedureCatalogRow(**row) for row in rows])

    @router.get("/v2/drugs/catalog", response_model=DrugCatalogV2Response)
    async def drug_catalog_v2(
        q: str | None = Query(None, max_length=100),
        prefix: str | None = Query(None, max_length=100),
        class_source: Literal["ATC", "FDASPL"] | None = None,
        class_id: str | None = Query(None, max_length=32),
        after: str | None = Query(None, max_length=4096),
        before: str | None = Query(None, max_length=4096),
        anchor: str | None = Query(None, max_length=512),
        limit: int = Query(100, ge=1, le=200),
    ):
        if sum(value is not None for value in (after, before, anchor)) > 1:
            raise _catalog_error(422, "catalog.invalid_request")
        scope, clauses, params = drug_scope(q, prefix, class_source, class_id)
        conn = get_conn()
        data_year = _latest_year(conn, "utilization_drug_dictionary")
        snapshot_id = _snapshot_id(get_snapshot_id, data_year)
        snapshot = CatalogSnapshot(id=snapshot_id, data_year=data_year, ordering="brand_generic_v1")
        fingerprint = _scope_fingerprint("drugs", scope)
        where = " and ".join(clauses)
        total = int(conn.execute(f"with latest as (select max(data_year) data_year from utilization_drug_dictionary) select count(*) from utilization_drug_dictionary d, latest where {where}", params).fetchone()[0])
        order = "lower(d.brand_name), lower(d.generic_name), d.brand_name, d.generic_name"
        page_params = list(params)
        key_clause = ""
        direction = "root"
        cursor_payload: dict | None = None
        anchor_resolution: Literal["exact", "nearest_before", "nearest_after", "start"] = "start"
        start_index = 0
        if after or before:
            direction = "after" if after else "before"
            cursor_payload = _cursor_decode(after or before or "", kind="drugs", snapshot_id=snapshot_id, fingerprint=fingerprint)
            if cursor_payload["direction"] != direction or len(cursor_payload["boundary"]) != 4:
                raise _catalog_error(422, "catalog.invalid_cursor")
            lb, lg, brand, generic = cursor_payload["boundary"]
            comparator = ">" if direction == "after" else "<"
            key_clause = f" and (lower(d.brand_name), lower(d.generic_name), d.brand_name, d.generic_name) {comparator} (?, ?, ?, ?)"
            page_params.extend([lb, lg, brand, generic])
            start_index = cursor_payload["ordinal"] + 1 if direction == "after" else max(0, cursor_payload["ordinal"] - limit)
            if direction == "before":
                order = "lower(d.brand_name) desc, lower(d.generic_name) desc, d.brand_name desc, d.generic_name desc"
        elif anchor:
            pair = _decode_drug_row_key(anchor)
            if pair is None:
                raise _catalog_error(422, "catalog.invalid_request")
            brand, generic = pair
            boundary = [brand.lower(), generic.lower(), brand, generic]
            preceding = int(conn.execute(f"with latest as (select max(data_year) data_year from utilization_drug_dictionary) select count(*) from utilization_drug_dictionary d, latest where {where} and (lower(d.brand_name), lower(d.generic_name), d.brand_name, d.generic_name) < (?, ?, ?, ?)", [*params, *boundary]).fetchone()[0])
            exact = conn.execute(f"with latest as (select max(data_year) data_year from utilization_drug_dictionary) select 1 from utilization_drug_dictionary d, latest where {where} and (d.brand_name, d.generic_name)=(?, ?)", [*params, brand, generic]).fetchone()
            if exact:
                start_index, anchor_resolution = max(0, preceding - limit // 2), "exact"
            else:
                following = conn.execute(f"with latest as (select max(data_year) data_year from utilization_drug_dictionary) select 1 from utilization_drug_dictionary d, latest where {where} and (lower(d.brand_name), lower(d.generic_name), d.brand_name, d.generic_name) >= (?, ?, ?, ?) limit 1", [*params, *boundary]).fetchone()
                if following:
                    start_index, anchor_resolution = max(0, preceding - limit // 2), "nearest_after"
                elif total:
                    start_index, anchor_resolution = max(0, total - limit), "nearest_before"
            page_params = [*params, limit, start_index]
        if anchor is None:
            page_params.append(limit)
        rows = _rows(conn.execute(f"with latest as (select max(data_year) data_year from utilization_drug_dictionary) select d.brand_name brand, d.generic_name generic, d.physician_count, d.total_claims claims, d.total_drug_cost drug_cost from utilization_drug_dictionary d, latest where {where}{key_clause} order by {order} limit ?" + (" offset ?" if anchor is not None else ""), page_params))
        if direction == "before":
            rows.reverse()
            start_index = max(0, cursor_payload["ordinal"] - len(rows)) if cursor_payload else 0
        for row in rows:
            row["row_key"] = _drug_row_key(row["brand"], row["generic"])
        anchor_key = rows[0]["row_key"] if rows else None
        if anchor and rows:
            requested_pair = _decode_drug_row_key(anchor)
            if anchor_resolution == "exact" and requested_pair:
                anchor_key = _drug_row_key(*requested_pair)
            elif anchor_resolution == "nearest_after" and requested_pair:
                requested_tuple = (
                    requested_pair[0].lower(), requested_pair[1].lower(), *requested_pair
                )
                anchor_key = next(
                    (
                        row["row_key"]
                        for row in rows
                        if (row["brand"].lower(), row["generic"].lower(), row["brand"], row["generic"])
                        >= requested_tuple
                    ),
                    rows[-1]["row_key"],
                )
            elif anchor_resolution == "nearest_before":
                anchor_key = rows[-1]["row_key"]
        first = rows[0] if rows else None
        last = rows[-1] if rows else None
        previous_cursor = catalog_cursor("drugs", snapshot_id, fingerprint, "before", [first["brand"].lower(), first["generic"].lower(), first["brand"], first["generic"]], start_index) if first and start_index > 0 else None
        next_cursor = catalog_cursor("drugs", snapshot_id, fingerprint, "after", [last["brand"].lower(), last["generic"].lower(), last["brand"], last["generic"]], start_index + len(rows) - 1) if last and start_index + len(rows) < total else None
        return DrugCatalogV2Response(snapshot=snapshot, scope=scope, count=CatalogCount(value=total, relation="exact"), window=CatalogWindow(start_index=start_index, previous_cursor=previous_cursor, next_cursor=next_cursor, anchor_key=anchor_key, anchor_resolution=anchor_resolution), returned_count=len(rows), results=[DrugCatalogRow(**row) for row in rows])

    @router.post("/v2/catalog/resolve", response_model=CatalogResolveResponse)
    async def catalog_resolve(request: CatalogResolveRequest):
        """Resolve a bounded basket without relying on pages currently in memory."""
        if not 1 <= len(request.keys) <= MAX_BASKET_SIZE:
            raise _catalog_error(422, "catalog.invalid_request")
        conn = get_conn()
        procedure_year = _latest_year(conn, "utilization_procedure_dictionary")
        drug_year = _latest_year(conn, "utilization_drug_dictionary")
        data_year = max(procedure_year, drug_year)
        snapshot = CatalogSnapshot(id=_snapshot_id(get_snapshot_id, data_year), data_year=data_year, ordering="request_order_v1")
        enabled = _descriptions_enabled()
        results: list[CatalogResolvedValue | CatalogUnavailableValue] = []
        for key in request.keys:
            if not isinstance(key, str) or len(key) > 500:
                results.append(CatalogUnavailableValue(key=str(key)))
                continue
            if key.startswith("hcpcs:"):
                code = key.removeprefix("hcpcs:").strip().upper()
                row = conn.execute(f"select hcpcs_code as \"value\", {"hcpcs_description" if enabled else "cast(null as varchar)"} as description from utilization_procedure_dictionary where data_year=? and upper(hcpcs_code)=? order by hcpcs_code limit 1", [procedure_year, code]).fetchone()
                if row:
                    results.append(CatalogResolvedValue(key=key, selection_key=f"hcpcs:{row[0]}", kind="procedure", row_key=_procedure_row_key(row[0]), value=row[0], description=row[1]))
                else:
                    results.append(CatalogUnavailableValue(key=key))
            elif key.startswith("brand:") or key.startswith("generic:"):
                kind, value = key.split(":", 1)
                row = conn.execute("select brand_name, generic_name from utilization_drug_dictionary where data_year=? and lower(" + ("brand_name" if kind == "brand" else "generic_name") + ")=? order by lower(brand_name), lower(generic_name), brand_name, generic_name limit 1", [drug_year, value.strip().lower()]).fetchone()
                if row:
                    results.append(CatalogResolvedValue(key=key, selection_key=f"{kind}:{row[0] if kind == 'brand' else row[1]}", kind=kind, row_key=_drug_row_key(row[0], row[1]), brand=row[0], generic=row[1]))
                else:
                    results.append(CatalogUnavailableValue(key=key))
            else:
                results.append(CatalogUnavailableValue(key=key))
        return CatalogResolveResponse(snapshot=snapshot, descriptions_enabled=enabled, results=results)

    @router.get(
        "/v2/procedures/families/{family_id}/members",
        response_model=ProcedureCatalogV2Response,
    )
    async def procedure_family_members_v2(
        family_id: str,
        q: str | None = Query(None, max_length=100),
        prefix: str | None = Query(None, max_length=10),
        code_from: str | None = Query(None, max_length=10),
        code_to: str | None = Query(None, max_length=10),
        after: str | None = Query(None, max_length=4096),
        before: str | None = Query(None, max_length=4096),
        anchor: str | None = Query(None, max_length=512),
        limit: int = Query(100, ge=1, le=200),
    ):
        """Page an RBCS family through the same bounded procedure catalog contract."""
        normalized = family_id.strip().upper()
        if not re.fullmatch(r"[A-Z0-9]{1,8}-[A-Z0-9]{1,8}", normalized):
            raise _catalog_error(422, "catalog.invalid_request")
        exists = get_conn().execute(
            "select 1 from utilization_procedure_taxonomy where family_id=? limit 1",
            [normalized],
        ).fetchone()
        if exists is None:
            raise _catalog_error(404, "catalog.reference_unavailable")
        return await procedure_catalog_v2(
            q=q,
            prefix=prefix,
            code_from=code_from,
            code_to=code_to,
            family_id=normalized,
            after=after,
            before=before,
            anchor=anchor,
            limit=limit,
        )

    @router.get(
        "/v2/drugs/classes/{source}/{class_id}/members",
        response_model=DrugClassMembersV2Response,
    )
    async def drug_class_members_v2(
        source: Literal["ATC", "FDASPL"],
        class_id: str,
        q: str | None = Query(None, max_length=100),
        prefix: str | None = Query(None, max_length=100),
        after: str | None = Query(None, max_length=4096),
        before: str | None = Query(None, max_length=4096),
        anchor: str | None = Query(None, max_length=512),
        limit: int = Query(100, ge=1, le=200),
    ):
        """Page unique generic members; brands are presentation metadata, never duplicate rows."""
        if sum(value is not None for value in (after, before, anchor)) > 1:
            raise _catalog_error(422, "catalog.invalid_request")
        normalized_class, query, normalized_prefix = class_id.strip(), _compact(q), _compact(prefix)
        if not normalized_class or len(normalized_class) > 32 or (q is not None and not query) or (prefix is not None and not normalized_prefix):
            raise _catalog_error(422, "catalog.invalid_request")
        exists = get_conn().execute(
            "select 1 from utilization_drug_classes where source=? and class_id=? limit 1",
            [source, normalized_class],
        ).fetchone()
        if exists is None:
            raise _catalog_error(404, "catalog.reference_unavailable")
        scope = CatalogScope(query=query, prefix=normalized_prefix, class_source=source, class_id=normalized_class)
        conn = get_conn()
        data_year = _latest_year(conn, "utilization_drug_dictionary")
        snapshot_id = _snapshot_id(get_snapshot_id, data_year)
        snapshot = CatalogSnapshot(id=snapshot_id, data_year=data_year, ordering="drug_class_member_v1")
        fingerprint = _scope_fingerprint("drug_class_members", scope)
        clauses, params = ["m.source=?", "(m.class_id=? or (?='ATC' and m.class_id like ? || '%'))"], [source, normalized_class, source, normalized_class]
        if query:
            clauses.append("(lower(m.generic_name) like ? or lower(d.brand_name) like ?)")
            params.extend([f"%{query.lower()}%"] * 2)
        if normalized_prefix:
            clauses.append("lower(m.generic_name) like ?")
            params.append(normalized_prefix.lower() + "%")
        base = f"""with latest as (select max(data_year) data_year from utilization_drug_dictionary), members as (
            select m.generic_name generic, list(distinct d.brand_name order by d.brand_name) brands,
                   sum(d.physician_count)::INTEGER physician_count, sum(d.total_claims)::BIGINT claims,
                   sum(d.total_drug_cost)::DOUBLE drug_cost
            from utilization_drug_class_members m join utilization_drug_dictionary d on lower(d.generic_name)=lower(m.generic_name)
            join latest l on d.data_year=l.data_year where {' and '.join(clauses)} group by m.generic_name
        ) """
        total = int(conn.execute(base + "select count(*) from members", params).fetchone()[0])
        page_params, predicate, order, start = list(params), "", "claims desc, lower(generic), generic", 0
        cursor_payload = None
        anchor_resolution: Literal["exact", "nearest_before", "nearest_after", "start"] = "start"
        if after or before:
            direction = "after" if after else "before"
            cursor_payload = _cursor_decode(after or before or "", kind="drug_class_members", snapshot_id=snapshot_id, fingerprint=fingerprint)
            if cursor_payload["direction"] != direction or len(cursor_payload["boundary"]) != 3:
                raise _catalog_error(422, "catalog.invalid_cursor")
            claims, generic_lower, generic = cursor_payload["boundary"]
            if direction == "after":
                predicate, order = " where (claims < ? or (claims=? and (lower(generic) > ? or (lower(generic)=? and generic > ?))))", order
            else:
                predicate, order = " where (claims > ? or (claims=? and (lower(generic) < ? or (lower(generic)=? and generic < ?))))", "claims asc, lower(generic) desc, generic desc"
            page_params.extend([claims, claims, generic_lower, generic_lower, generic])
            start = cursor_payload["ordinal"] + 1 if direction == "after" else max(0, cursor_payload["ordinal"] - limit)
        if anchor:
            if not anchor.startswith("generic:"):
                raise _catalog_error(422, "catalog.invalid_request")
            generic = anchor.removeprefix("generic:").strip()
            target = conn.execute(base + "select claims, generic from members where lower(generic)=?", [*params, generic.lower()]).fetchone()
            if target:
                start = max(0, int(conn.execute(base + "select count(*) from members where claims > ? or (claims=? and lower(generic) < ?)", [*params, target[0], target[0], target[1].lower()]).fetchone()[0]) - limit // 2)
                anchor_resolution = "exact"
            page_params = [*params, limit, start]
        if not anchor:
            page_params.append(limit)
        rows = _rows(conn.execute(base + f"select * from members{predicate} order by {order} limit ?" + (" offset ?" if anchor else ""), page_params))
        if before:
            rows.reverse(); start = max(0, cursor_payload["ordinal"] - len(rows))
        for row in rows:
            row["row_key"] = row["selection_key"] = f"generic:{row['generic']}"
        first, last = (rows[0], rows[-1]) if rows else (None, None)
        previous = catalog_cursor("drug_class_members", snapshot_id, fingerprint, "before", [first["claims"], first["generic"].lower(), first["generic"]], start) if first and start > 0 else None
        next_value = catalog_cursor("drug_class_members", snapshot_id, fingerprint, "after", [last["claims"], last["generic"].lower(), last["generic"]], start + len(rows) - 1) if last and start + len(rows) < total else None
        return DrugClassMembersV2Response(snapshot=snapshot, scope=scope, count=CatalogCount(value=total, relation="exact"), window=CatalogWindow(start_index=start, previous_cursor=previous, next_cursor=next_value, anchor_key=(f"generic:{anchor.removeprefix('generic:').strip()}" if anchor_resolution == "exact" else (first["row_key"] if first else None)), anchor_resolution=anchor_resolution), attribution=nlm_attribution, returned_count=len(rows), results=[DrugClassMemberCatalogRow(**row) for row in rows])

    @router.get("/procedures/taxonomy", response_model=ProcedureTaxonomyResponse)
    async def procedure_taxonomy(
        q: str | None = Query(None, max_length=100),
        category: str | None = Query(None, max_length=8),
        subcategory: str | None = Query(None, max_length=8),
        limit: int = Query(50, ge=1, le=200),
    ):
        needle = " ".join((q or "").split()).lower()
        if q is not None and not needle:
            raise HTTPException(status_code=422, detail="Query must not be blank")
        clauses = ["lower(t.family_name) != 'no rbcs family'"]
        params: list = []
        if needle:
            clauses.append(
                "(lower(t.family_name) like ? or lower(t.subcategory_name) like ? "
                "or lower(t.category_name) like ? or lower(t.hcpcs_code) like ?)"
            )
            params.extend([f"%{needle}%"] * 4)
        if category:
            clauses.append("upper(t.category_id) = ?")
            params.append(category.strip().upper())
        if subcategory:
            clauses.append("upper(t.subcategory_id) = ?")
            params.append(subcategory.strip().upper())
        cursor = get_conn().execute(
            f"""
            with latest as (
              select max(data_year) data_year from utilization_procedure_dictionary
            ), families as (
              select t.family_id, any_value(t.family_name) family_name,
                     any_value(t.category_id) category_id,
                     any_value(t.category_name) category_name,
                     any_value(t.subcategory_id) subcategory_id,
                     any_value(t.subcategory_name) subcategory_name,
                     count(distinct d.hcpcs_code)::INTEGER available_code_count,
                     sum(d.total_services)::DOUBLE total_services,
                     sum(d.total_payments)::DOUBLE total_payments,
                     max(d.data_year)::INTEGER data_year
              from utilization_procedure_taxonomy t
              join utilization_procedure_dictionary d on d.hcpcs_code=t.hcpcs_code
              join latest l on d.data_year=l.data_year
              where {' and '.join(clauses)}
              group by t.family_id
            )
            select *, count(*) over () total_matches from families
            order by case when lower(family_name)=? then 0
                          when lower(family_name) like ? then 1 else 2 end,
                     total_services desc, family_name, family_id limit ?
            """,
            [*params, needle, f"{needle}%", limit],
        )
        rows = _rows(cursor)
        total = int(rows[0].pop("total_matches")) if rows else 0
        data_year = (
            int(rows[0].pop("data_year"))
            if rows
            else _latest_year(get_conn(), "utilization_procedure_dictionary")
        )
        for row in rows:
            row.pop("data_year", None)
        return ProcedureTaxonomyResponse(
            query=" ".join((q or "").split()) or None,
            data_year=data_year,
            total=total,
            returned_count=len(rows),
            results=[ProcedureFamilySummary(**row) for row in rows],
        )

    @router.get(
        "/procedures/families/{family_id}", response_model=ProcedureFamilyResponse
    )
    async def procedure_family(family_id: str):
        normalized = family_id.strip().upper()
        if not re.fullmatch(r"[A-Z0-9]{1,8}-[A-Z0-9]{1,8}", normalized):
            raise HTTPException(status_code=422, detail="Invalid RBCS family ID")
        enabled = _descriptions_enabled()
        cursor = get_conn().execute(
            f"""
            with latest as (
              select max(data_year) data_year from utilization_procedure_dictionary
            )
            select t.family_id, t.family_name, t.category_id, t.category_name,
                   t.subcategory_id, t.subcategory_name,
                   count(distinct d.hcpcs_code)::INTEGER available_code_count,
                   sum(d.total_services)::DOUBLE total_services,
                   sum(d.total_payments)::DOUBLE total_payments,
                   max(d.data_year)::INTEGER data_year
            from utilization_procedure_taxonomy t
            join utilization_procedure_dictionary d on d.hcpcs_code=t.hcpcs_code
            join latest l on d.data_year=l.data_year
            where t.family_id=?
            group by t.family_id, t.family_name, t.category_id, t.category_name,
                     t.subcategory_id, t.subcategory_name
            """,
            [normalized],
        )
        family_row = cursor.fetchone()
        if family_row is None:
            raise HTTPException(status_code=404, detail="RBCS family is unavailable")
        family = dict(zip([item[0] for item in cursor.description], family_row))
        data_year = int(family.pop("data_year"))
        member_cursor = get_conn().execute(
            f"""
            select d.hcpcs_code as "value",
                   {"d.hcpcs_description" if enabled else "cast(null as varchar)"} description,
                   coalesce(d.hcpcs_drug_ind='Y', false) is_drug_code,
                   d.physician_count, d.total_services, d.total_payments
            from utilization_procedure_taxonomy t
            join utilization_procedure_dictionary d on d.hcpcs_code=t.hcpcs_code
            where t.family_id=? and d.data_year=?
            order by d.hcpcs_code
            """,
            [normalized, data_year],
        )
        return ProcedureFamilyResponse(
            data_year=data_year,
            family=ProcedureFamilySummary(**family),
            members=[ProcedureOption(**row) for row in _rows(member_cursor)],
        )

    @router.get("/drugs/classes", response_model=DrugClassesResponse)
    async def drug_classes(
        q: str | None = Query(None, max_length=100),
        source: Literal["ATC", "FDASPL"] = "ATC",
        parent: str | None = Query(None, max_length=32),
        limit: int = Query(50, ge=1, le=200),
    ):
        needle = " ".join((q or "").split()).lower()
        if q is not None and not needle:
            raise HTTPException(status_code=422, detail="Query must not be blank")
        clauses = ["c.source=?"]
        params: list = [source]
        if parent:
            clauses.append("c.parent_class_id=?")
            params.append(parent.strip())
        if needle:
            clauses.append(
                "(lower(c.class_name) like ? or lower(c.class_id) like ? "
                "or exists (select 1 from utilization_drug_class_members qm "
                "where qm.source=c.source and "
                "(qm.class_id=c.class_id or "
                "(c.source='ATC' and qm.class_id like c.class_id || '%')) "
                "and (lower(qm.generic_name) like ? or lower(qm.concept_name) like ?)))"
            )
            params.extend([f"%{needle}%"] * 4)
        cursor = get_conn().execute(
            f"""
            with latest as (select max(data_year) data_year from utilization_drug_dictionary),
            classes as (
              select c.source, c.class_type, c.class_id, c.class_name,
                     c.parent_class_id, c.parent_class_name, c.hierarchy_level,
                     count(distinct m.generic_name)::INTEGER available_generic_count,
                     count(distinct m.class_id)::INTEGER descendant_class_count,
                     sum(d.total_claims)::BIGINT total_claims,
                     sum(d.total_drug_cost)::DOUBLE total_drug_cost,
                     max(d.data_year)::INTEGER data_year
              from utilization_drug_classes c
              join utilization_drug_class_members m on m.source=c.source
               and (m.class_id=c.class_id or (c.source='ATC' and m.class_id like c.class_id || '%'))
              join utilization_drug_dictionary d
                on lower(d.generic_name)=lower(m.generic_name)
              join latest l on d.data_year=l.data_year
              where {' and '.join(clauses)}
              group by c.source, c.class_type, c.class_id, c.class_name,
                       c.parent_class_id, c.parent_class_name, c.hierarchy_level
            )
            select *, count(*) over () total_matches from classes
            order by case when lower(class_name)=? then 0
                          when lower(class_name) like ? then 1 else 2 end,
                     case when ?='' then hierarchy_level else -hierarchy_level end,
                     total_claims desc, class_name, class_id limit ?
            """,
            [*params, needle, f"{needle}%", needle, limit],
        )
        rows = _rows(cursor)
        total = int(rows[0].pop("total_matches")) if rows else 0
        data_year = (
            int(rows[0].pop("data_year"))
            if rows
            else _latest_year(get_conn(), "utilization_drug_dictionary")
        )
        for row in rows:
            row.pop("data_year", None)
        return DrugClassesResponse(
            query=" ".join((q or "").split()) or None,
            source=source,
            data_year=data_year,
            attribution=nlm_attribution,
            total=total,
            returned_count=len(rows),
            results=[DrugClassSummary(**row) for row in rows],
        )

    @router.get(
        "/drugs/classes/{source}/{class_id}", response_model=DrugClassResponse
    )
    async def drug_class(source: Literal["ATC", "FDASPL"], class_id: str):
        normalized = class_id.strip()
        if not normalized or len(normalized) > 32:
            raise HTTPException(status_code=422, detail="Invalid drug class ID")
        summary_cursor = get_conn().execute(
            """
            with latest as (select max(data_year) data_year from utilization_drug_dictionary)
            select c.source, c.class_type, c.class_id, c.class_name,
                   c.parent_class_id, c.parent_class_name, c.hierarchy_level,
                   count(distinct m.generic_name)::INTEGER available_generic_count,
                   count(distinct m.class_id)::INTEGER descendant_class_count,
                   sum(d.total_claims)::BIGINT total_claims,
                   sum(d.total_drug_cost)::DOUBLE total_drug_cost,
                   max(d.data_year)::INTEGER data_year
            from utilization_drug_classes c
            join utilization_drug_class_members m on m.source=c.source
             and (m.class_id=c.class_id or (c.source='ATC' and m.class_id like c.class_id || '%'))
            join utilization_drug_dictionary d
              on lower(d.generic_name)=lower(m.generic_name)
            join latest l on d.data_year=l.data_year
            where c.source=? and c.class_id=?
            group by c.source, c.class_type, c.class_id, c.class_name,
                     c.parent_class_id, c.parent_class_name, c.hierarchy_level
            """,
            [source, normalized],
        )
        summary_row = summary_cursor.fetchone()
        if summary_row is None:
            raise HTTPException(status_code=404, detail="Drug class is unavailable")
        summary_data = dict(
            zip([column[0] for column in summary_cursor.description], summary_row)
        )
        summary_year = int(summary_data.pop("data_year"))
        summary = DrugClassSummary(**summary_data)
        cursor = get_conn().execute(
            """
            with latest as (select max(data_year) data_year from utilization_drug_dictionary)
            select m.generic_name generic,
                   list(distinct d.brand_name order by d.brand_name) brands,
                   sum(d.physician_count)::INTEGER physician_count,
                   sum(d.total_claims)::BIGINT claims,
                   sum(d.total_drug_cost)::DOUBLE drug_cost,
                   max(d.data_year)::INTEGER data_year
            from utilization_drug_class_members m
            join utilization_drug_dictionary d
              on lower(d.generic_name)=lower(m.generic_name)
            join latest l on d.data_year=l.data_year
            where m.source=? and (m.class_id=? or (?='ATC' and m.class_id like ? || '%'))
            group by m.generic_name order by claims desc, generic
            """,
            [source, normalized, source, normalized],
        )
        rows = _rows(cursor)
        data_year = int(rows[0].pop("data_year")) if rows else summary_year
        for row in rows:
            row.pop("data_year", None)
        return DrugClassResponse(
            data_year=data_year,
            attribution=nlm_attribution,
            drug_class=summary,
            members=[DrugClassMember(**row) for row in rows],
        )

    @router.get("/procedures/catalog", response_model=ProcedureCatalogResponse)
    async def procedure_catalog(
        q: str | None = Query(None, max_length=100),
        prefix: str | None = Query(None, max_length=10),
        offset: int = Query(0, ge=0),
        limit: int = Query(100, ge=1, le=200),
    ):
        """List the immutable HCPCS dictionary in sequential, pageable order."""
        enabled = _descriptions_enabled()
        normalized_query = " ".join((q or "").split())
        normalized_prefix = "".join((prefix or "").split()).upper()
        if q is not None and not normalized_query:
            raise HTTPException(status_code=422, detail="Query must not be blank")
        if prefix is not None and not normalized_prefix:
            raise HTTPException(status_code=422, detail="Prefix must not be blank")
        if normalized_prefix and not re.fullmatch(r"[A-Z0-9]{1,10}", normalized_prefix):
            raise HTTPException(status_code=422, detail="Invalid HCPCS prefix")

        clauses = ["d.data_year = latest.data_year"]
        params: list = []
        if normalized_prefix:
            clauses.append("upper(d.hcpcs_code) like ?")
            params.append(normalized_prefix + "%")
        if normalized_query:
            needle = normalized_query.lower()
            query_clauses = ["lower(d.hcpcs_code) like ?"]
            params.append("%" + needle + "%")
            if enabled:
                query_clauses.append("lower(d.hcpcs_description) like ?")
                params.append("%" + needle + "%")
            clauses.append("(" + " or ".join(query_clauses) + ")")

        total = int(
            get_conn()
            .execute(
                f"""
                with latest as (
                  select max(data_year) data_year from utilization_procedure_dictionary
                )
                select count(*)
                from utilization_procedure_dictionary d, latest
                where {' and '.join(clauses)}
                """,
                params,
            )
            .fetchone()[0]
        )
        cursor = get_conn().execute(
            f"""
            with latest as (
              select max(data_year) data_year from utilization_procedure_dictionary
            )
            select d.hcpcs_code as "value",
                   {"d.hcpcs_description" if enabled else "cast(null as varchar)"} description,
                   coalesce(d.hcpcs_drug_ind = 'Y', false) is_drug_code,
                   d.physician_count, d.total_services, d.total_payments, d.data_year
            from utilization_procedure_dictionary d, latest
            where {' and '.join(clauses)}
            order by upper(d.hcpcs_code), d.hcpcs_code
            limit ? offset ?
            """,
            [*params, limit, offset],
        )
        rows = _rows(cursor)
        data_year = (
            int(rows[0].pop("data_year"))
            if rows
            else _latest_year(get_conn(), "utilization_procedure_dictionary")
        )
        for row in rows:
            row.pop("data_year", None)
        returned_count = len(rows)
        return ProcedureCatalogResponse(
            query=normalized_query or None,
            prefix=normalized_prefix or None,
            data_year=data_year,
            descriptions_enabled=enabled,
            total=total,
            offset=offset,
            limit=limit,
            returned_count=returned_count,
            has_more=offset + returned_count < total,
            results=[ProcedureOption(**row) for row in rows],
        )

    @router.get("/drugs/catalog", response_model=DrugCatalogResponse)
    async def drug_catalog(
        q: str | None = Query(None, max_length=100),
        prefix: str | None = Query(None, max_length=100),
        offset: int = Query(0, ge=0),
        limit: int = Query(100, ge=1, le=200),
    ):
        """List the immutable Part D brand/generic dictionary with stable paging."""
        normalized_query = " ".join((q or "").split())
        normalized_prefix = " ".join((prefix or "").split())
        if q is not None and not normalized_query:
            raise HTTPException(status_code=422, detail="Query must not be blank")
        if prefix is not None and not normalized_prefix:
            raise HTTPException(status_code=422, detail="Prefix must not be blank")

        clauses = ["d.data_year = latest.data_year"]
        params: list = []
        if normalized_prefix:
            needle = normalized_prefix.lower() + "%"
            clauses.append("(lower(d.brand_name) like ? or lower(d.generic_name) like ?)")
            params.extend([needle, needle])
        if normalized_query:
            needle = "%" + normalized_query.lower() + "%"
            clauses.append("(lower(d.brand_name) like ? or lower(d.generic_name) like ?)")
            params.extend([needle, needle])

        total = int(
            get_conn()
            .execute(
                f"""
                with latest as (
                  select max(data_year) data_year from utilization_drug_dictionary
                )
                select count(*)
                from utilization_drug_dictionary d, latest
                where {' and '.join(clauses)}
                """,
                params,
            )
            .fetchone()[0]
        )
        cursor = get_conn().execute(
            f"""
            with latest as (select max(data_year) data_year from utilization_drug_dictionary)
            select d.brand_name brand, d.generic_name generic, d.physician_count,
                   d.total_claims claims, d.total_drug_cost drug_cost, d.data_year
            from utilization_drug_dictionary d, latest
            where {' and '.join(clauses)}
            order by lower(d.brand_name), lower(d.generic_name), d.brand_name, d.generic_name
            limit ? offset ?
            """,
            [*params, limit, offset],
        )
        rows = _rows(cursor)
        data_year = (
            int(rows[0].pop("data_year"))
            if rows
            else _latest_year(get_conn(), "utilization_drug_dictionary")
        )
        for row in rows:
            row.pop("data_year", None)
        returned_count = len(rows)
        return DrugCatalogResponse(
            query=normalized_query or None,
            prefix=normalized_prefix or None,
            data_year=data_year,
            total=total,
            offset=offset,
            limit=limit,
            returned_count=returned_count,
            has_more=offset + returned_count < total,
            results=[DrugOption(**row) for row in rows],
        )

    @router.get("/procedures/options", response_model=ProcedureOptionsResponse)
    async def procedure_options(
        q: str = Query(..., min_length=1, max_length=100),
        limit: int = Query(20, ge=1, le=50),
    ):
        enabled = _descriptions_enabled()
        needle = q.strip().lower()
        if not needle:
            raise HTTPException(status_code=422, detail="Query must not be blank")
        description_clause = "or lower(hcpcs_description) like ?" if enabled else ""
        params: list = [needle + "%", "%" + needle + "%"]
        if enabled:
            params.append("%" + needle + "%")
        params.append(limit)
        cursor = get_conn().execute(
            f"""
            with latest as (
              select max(data_year) data_year from utilization_procedure_dictionary
            )
            select hcpcs_code as "value",
                   {"hcpcs_description" if enabled else "cast(null as varchar)"} description,
                   coalesce(hcpcs_drug_ind = 'Y', false) is_drug_code,
                   physician_count, total_services, total_payments, d.data_year
            from utilization_procedure_dictionary d, latest
            where d.data_year = latest.data_year
              and (lower(hcpcs_code) like ? or lower(hcpcs_code) like ? {description_clause})
            order by case when lower(hcpcs_code) = ? then 0
                          when lower(hcpcs_code) like ? then 1 else 2 end,
                     physician_count desc, hcpcs_code
            limit ?
            """,
            [*params[:-1], needle, needle + "%", params[-1]],
        )
        rows = _rows(cursor)
        data_year = (
            rows[0].pop("data_year")
            if rows
            else _latest_year(get_conn(), "utilization_procedure_dictionary")
        )
        return ProcedureOptionsResponse(
            query=q.strip(),
            data_year=data_year,
            descriptions_enabled=enabled,
            results=[ProcedureOption(**row) for row in rows],
        )

    @router.get("/drugs/options", response_model=DrugOptionsResponse)
    async def drug_options(
        q: str = Query(..., min_length=1, max_length=100),
        limit: int = Query(20, ge=1, le=50),
    ):
        needle = q.strip().lower()
        if not needle:
            raise HTTPException(status_code=422, detail="Query must not be blank")
        cursor = get_conn().execute(
            """
            with latest as (select max(data_year) data_year from utilization_drug_dictionary)
            select brand_name brand, generic_name generic, physician_count,
                   total_claims claims, total_drug_cost drug_cost, d.data_year
            from utilization_drug_dictionary d, latest
            where d.data_year = latest.data_year
              and (lower(brand_name) like ? or lower(generic_name) like ?)
            order by case when lower(brand_name) = ? or lower(generic_name) = ? then 0
                          when lower(brand_name) like ? or lower(generic_name) like ? then 1
                          else 2 end,
                     physician_count desc, brand_name, generic_name
            limit ?
            """,
            [
                "%" + needle + "%",
                "%" + needle + "%",
                needle,
                needle,
                needle + "%",
                needle + "%",
                limit,
            ],
        )
        rows = _rows(cursor)
        data_year = (
            rows[0].pop("data_year")
            if rows
            else _latest_year(get_conn(), "utilization_drug_dictionary")
        )
        return DrugOptionsResponse(
            query=q.strip(),
            data_year=data_year,
            results=[DrugOption(**row) for row in rows],
        )

    @router.get("/procedures/search", response_model=ProcedureSearchResponse)
    async def procedure_search(
        hcpcs: list[str] | None = Query(None),
        specialty: list[str] | None = Query(None),
        city: str | None = Query(None, max_length=100),
        state: str | None = Query(None, max_length=2),
        zip: str | None = None,
        zips: str | None = None,
        lat: float | None = Query(None, ge=-90, le=90),
        lng: float | None = Query(None, ge=-180, le=180),
        radius_miles: float = Query(10, gt=0, le=MAX_RADIUS_MILES),
        min_services: float = Query(0, ge=0),
        sort: Literal["services", "payments", "beneficiaries"] = "services",
        limit: int = Query(100, ge=1, le=200),
    ):
        try:
            selected = _normalize_values(hcpcs, "HCPCS")
            if not selected:
                raise ValueError("Select at least one HCPCS code")
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        (
            geo_sql,
            geo_params,
            distance_sql,
            distance_params,
            radius_sql,
            radius_params,
        ) = _location_sql(
            city=city,
            state=state,
            zip_code=zip,
            zips=zips,
            lat=lat,
            lng=lng,
            radius_miles=radius_miles,
            specialties=specialty,
        )
        placeholders = ",".join(["?"] * len(selected))
        order = {
            "services": "selected_services",
            "payments": "selected_payments",
            "beneficiaries": "selected_beneficiaries",
        }[sort]
        cursor = get_conn().execute(
            f"""
            with latest as (select max(data_year) data_year from provider_service_detail),
            selected as (
              select s.npi, max(s.data_year) data_year,
                     sum(coalesce(s.tot_services, 0)) selected_services,
                     sum(coalesce(s.tot_services, 0) * coalesce(s.avg_medicare_payment, 0))
                       selected_payments,
                     sum(s.tot_beneficiaries) selected_beneficiaries
              from provider_service_detail s, latest
              where s.data_year = latest.data_year and s.hcpcs_code in ({placeholders})
              group by s.npi
            ), located as (
              select x.npi, trim(concat_ws(' ', p.first_name, p.last_name)) as "name",
                     p.first_name, p.last_name, p.credentials,
                     list_extract(p.specialties, 1) specialty,
                     p.address, p.city, p.state, p.zip5,
                     p.latitude lat, p.longitude lng, {distance_sql},
                     x.selected_services, x.selected_payments,
                     x.selected_beneficiaries, p.partb_services, p.partb_payments,
                     case when p.partb_services > 0
                       then x.selected_services / p.partb_services end selected_service_share,
                     case when p.partb_payments > 0
                       then x.selected_payments / p.partb_payments end selected_payment_share,
                     x.data_year
              from selected x
              join serving_practice_nppes_provider_sites p on p.npi = x.npi
              where {geo_sql}
            ), qualified as (
              select *, count(*) over () total_matches from located
              where selected_services >= ? {radius_sql}
            )
            select * from qualified order by {order} desc nulls last, npi limit ?
            """,
            [
                *selected,
                *distance_params,
                *geo_params,
                min_services,
                *radius_params,
                limit,
            ],
        )
        rows = _rows(cursor)
        total = int(rows[0].pop("total_matches")) if rows else 0
        data_year = (
            int(rows[0]["data_year"])
            if rows
            else _latest_year(get_conn(), "provider_service_detail")
        )
        npis = [row["npi"] for row in rows]
        breakdowns: dict[str, list[ProcedureBreakdown]] = {npi: [] for npi in npis}
        if npis:
            breakdown_cursor = get_conn().execute(
                f"""
                select npi, hcpcs_code as "value",
                       {"arg_max(hcpcs_description, coalesce(tot_services, 0))" if _descriptions_enabled() else "cast(null as varchar)"} description,
                       coalesce(bool_or(hcpcs_drug_ind = 'Y'), false) is_drug_code,
                       sum(coalesce(tot_services, 0)) services,
                       sum(coalesce(tot_services, 0) * coalesce(avg_medicare_payment, 0))
                         payments,
                       sum(tot_beneficiaries) beneficiaries
                from provider_service_detail
                where data_year = ? and npi in ({','.join(['?'] * len(npis))})
                  and hcpcs_code in ({placeholders})
                group by npi, hcpcs_code order by npi, services desc, hcpcs_code
                """,
                [data_year, *npis, *selected],
            )
            for raw in _rows(breakdown_cursor):
                npi = raw.pop("npi")
                breakdowns[npi].append(ProcedureBreakdown(**raw))
        results: list[ProcedureSearchResult] = []
        for row in rows:
            row.pop("data_year", None)
            row["matched_codes"] = breakdowns[row["npi"]]
            results.append(ProcedureSearchResult(**row))
        return ProcedureSearchResponse(
            data_year=data_year,
            selected_hcpcs=selected,
            total=total,
            returned_count=len(results),
            truncated=total > len(results),
            results=results,
        )

    @router.get("/drugs/search", response_model=DrugSearchResponse)
    async def drug_search(
        brands: list[str] | None = Query(None),
        generics: list[str] | None = Query(None),
        specialty: list[str] | None = Query(None),
        city: str | None = Query(None, max_length=100),
        state: str | None = Query(None, max_length=2),
        zip: str | None = None,
        zips: str | None = None,
        lat: float | None = Query(None, ge=-90, le=90),
        lng: float | None = Query(None, ge=-180, le=180),
        radius_miles: float = Query(10, gt=0, le=MAX_RADIUS_MILES),
        min_claims: int = Query(0, ge=0),
        sort: Literal["claims", "drug_cost"] = "claims",
        limit: int = Query(100, ge=1, le=200),
    ):
        try:
            selected_brands = _normalize_values(brands, "brand")
            selected_generics = _normalize_values(generics, "generic")
            if not selected_brands and not selected_generics:
                raise ValueError("Select at least one brand or generic drug")
            if len(selected_brands) + len(selected_generics) > MAX_BASKET_SIZE:
                raise ValueError(f"At most {MAX_BASKET_SIZE} drugs may be selected")
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        (
            geo_sql,
            geo_params,
            distance_sql,
            distance_params,
            radius_sql,
            radius_params,
        ) = _location_sql(
            city=city,
            state=state,
            zip_code=zip,
            zips=zips,
            lat=lat,
            lng=lng,
            radius_miles=radius_miles,
            specialties=specialty,
        )
        drug_clauses: list[str] = []
        drug_params: list[str] = []
        if selected_brands:
            drug_clauses.append(
                "d.brand_name in (" + ",".join(["?"] * len(selected_brands)) + ")"
            )
            drug_params.extend(selected_brands)
        if selected_generics:
            drug_clauses.append(
                "d.generic_name in (" + ",".join(["?"] * len(selected_generics)) + ")"
            )
            drug_params.extend(selected_generics)
        cursor = get_conn().execute(
            f"""
            with latest as (select max(data_year) data_year from provider_drug_detail),
            selected as (
              select d.npi, max(d.data_year) data_year,
                     sum(coalesce(d.tot_claims, 0)) selected_claims,
                     sum(coalesce(d.tot_drug_cost, 0)) selected_drug_cost
              from provider_drug_detail d, latest
              where d.data_year = latest.data_year and ({' or '.join(drug_clauses)})
              group by d.npi
            ), latest_metrics as (
              select npi, rx_total_claims
              from utilization_metrics
              qualify row_number() over (partition by npi order by metric_year desc) = 1
            ), located as (
              select x.npi, trim(concat_ws(' ', p.first_name, p.last_name)) as "name",
                     p.first_name, p.last_name, p.credentials,
                     list_extract(p.specialties, 1) specialty,
                     p.address, p.city, p.state, p.zip5,
                     p.latitude lat, p.longitude lng, {distance_sql},
                     x.selected_claims, x.selected_drug_cost,
                     m.rx_total_claims partd_claims, p.partd_drug_cost,
                     case when m.rx_total_claims > 0
                       then x.selected_claims / m.rx_total_claims end selected_claim_share,
                     case when p.partd_drug_cost > 0
                       then x.selected_drug_cost / p.partd_drug_cost end selected_cost_share,
                     x.data_year
              from selected x
              join serving_practice_nppes_provider_sites p on p.npi = x.npi
              left join latest_metrics m on m.npi = x.npi
              where {geo_sql}
            ), qualified as (
              select *, count(*) over () total_matches from located
              where selected_claims >= ? {radius_sql}
            )
            select * from qualified order by selected_{sort} desc nulls last, npi limit ?
            """,
            [
                *drug_params,
                *distance_params,
                *geo_params,
                min_claims,
                *radius_params,
                limit,
            ],
        )
        rows = _rows(cursor)
        total = int(rows[0].pop("total_matches")) if rows else 0
        data_year = (
            int(rows[0]["data_year"])
            if rows
            else _latest_year(get_conn(), "provider_drug_detail")
        )
        npis = [row["npi"] for row in rows]
        breakdowns: dict[str, list[DrugBreakdown]] = {npi: [] for npi in npis}
        if npis:
            breakdown_cursor = get_conn().execute(
                f"""
                select npi, brand_name brand, generic_name generic,
                       sum(coalesce(tot_claims, 0)) claims,
                       sum(coalesce(tot_drug_cost, 0)) drug_cost
                from provider_drug_detail d
                where data_year = ? and npi in ({','.join(['?'] * len(npis))})
                  and ({' or '.join(drug_clauses)})
                group by npi, brand_name, generic_name
                order by npi, claims desc, brand_name, generic_name
                """,
                [data_year, *npis, *drug_params],
            )
            for raw in _rows(breakdown_cursor):
                npi = raw.pop("npi")
                breakdowns[npi].append(DrugBreakdown(**raw))
        results: list[DrugSearchResult] = []
        for row in rows:
            row.pop("data_year", None)
            row["matched_drugs"] = breakdowns[row["npi"]]
            results.append(DrugSearchResult(**row))
        return DrugSearchResponse(
            data_year=data_year,
            selected_brands=selected_brands,
            selected_generics=selected_generics,
            total=total,
            returned_count=len(results),
            truncated=total > len(results),
            results=results,
        )

    return router
