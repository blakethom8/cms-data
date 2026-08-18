"""Code-first Medicare procedure and Part D utilization discovery."""

from __future__ import annotations

import math
import os
import re
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
MAX_BASKET_SIZE = 50
NPI_RE = re.compile(r"^\d{10}$")


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


def get_utilization_router(get_conn):
    router = APIRouter(prefix="/utilization", tags=["Medicare Utilization"])

    nlm_attribution = (
        "This product uses publicly available data from the U.S. National Library of Medicine "
        "(NLM), National Institutes of Health, Department of Health and Human Services; NLM is "
        "not responsible for the product and does not endorse or recommend this or any other "
        "product."
    )

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
        summary_response = await drug_classes(
            q=normalized, source=source, parent=None, limit=200
        )
        summary = next(
            (item for item in summary_response.results if item.class_id == normalized), None
        )
        if summary is None:
            raise HTTPException(status_code=404, detail="Drug class is unavailable")
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
        data_year = int(rows[0].pop("data_year")) if rows else summary_response.data_year
        for row in rows:
            row.pop("data_year", None)
        return DrugClassResponse(
            data_year=data_year,
            attribution=nlm_attribution,
            drug_class=summary,
            members=[DrugClassMember(**row) for row in rows],
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
