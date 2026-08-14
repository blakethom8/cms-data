"""
Doctor Clinical Profiles — per-NPI, multi-lens rep-facing profiles.

Five lenses assembled from validated queries (designed via warehouse review):
  panel      — patient population (scale, demographics, risk, chronic mix)
  clinical   — what they actually do (payment-weighted procedures, F/O split)
  prescribing— Part D persona (brand share, top drugs, specialty-tier flags)
  industry   — Open Payments (engagement tier, manufacturers, products,
               research PI role, ownership stakes)
  access     — where to find them (best-door ranked locations, group
               affiliations across DAC + reassignment, hospital
               affiliations, MIPS)

All SQL is whitelisted; the only inputs are an NPI (validated digits) and
search strings, always bound as parameters.
"""
import os
import re
from typing import Literal, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from open_payments_profile import industry_summary

CRED = '"Cred\t\t\t\t"'
TELE = '"Telehlth\t\t\t\t"'
ProviderProfileBackend = Literal["raw", "mart", "auto"]
PROFILE_MART_QUERY_COLUMNS = {
    "serving_provider_profile_headers": frozenset(
        {
            "npi", "name", "credentials", "specialty", "secondary_specialties",
            "city", "state", "med_school", "grad_year", "telehealth",
        }
    ),
    "serving_provider_profile_locations": frozenset(
        {
            "npi", "addr_key", "street", "suites", "city", "state", "zip5",
            "phone", "roster_size", "latitude", "longitude", "likely_flagship",
            "sources",
        }
    ),
    "serving_provider_profile_groups": frozenset(
        {
            "npi", "group_id", "group_name", "group_size", "n_addresses",
            "reassignment_size", "sources",
        }
    ),
}


def _profile_mart_is_available(conn) -> bool:
    """Return whether all three profile serving schemas are query-compatible."""
    available: dict[str, set[str]] = {
        table: set() for table in PROFILE_MART_QUERY_COLUMNS
    }
    rows = conn.execute(
        """
        select table_name, column_name
        from information_schema.columns
        where table_schema = 'main'
          and table_name in (
            'serving_provider_profile_headers',
            'serving_provider_profile_locations',
            'serving_provider_profile_groups'
          )
        """
    ).fetchall()
    for table, column in rows:
        available[str(table)].add(str(column))
    return all(
        required.issubset(available[table])
        for table, required in PROFILE_MART_QUERY_COLUMNS.items()
    )

# Curated demo exemplars (validated LA cardiologists with contrasting stories).
EXEMPLARS = [
    {"npi": "1811967433", "name": "Matthew Budoff, MD",
     "story": "KOL + trial PI: $407K industry (25 mfrs), $976K research, healthy prevention-focused panel"},
    {"npi": "1194759803", "name": "Stephen Corday, MD",
     "story": "Solo independent, office buy-and-bill (Leqvio), brand-heavy Rx — easiest door in LA"},
    {"npi": "1326205873", "name": "Christopher Chu, MD",
     "story": "Hospital rounder with a safety-net panel: 96% dual-eligible, HCC 3.07, 66% diabetic"},
    {"npi": "1043244296", "name": "Gary Reznik, MD",
     "story": "Mega-prescriber: 46K claims / $11.5M, 92% LIS, brand-loyal even where generics exist"},
    {"npi": "1780065508", "name": "Vasimahmed Lala, DO",
     "story": "Pure endovascular operator — 62% of $3.7M from leg atherectomy/stents; device target"},
    {"npi": "1881985521", "name": "Duc Do, MD",
     "story": "UCLA multi-site traveler — skip the flagship, catch him at the 33-clinician Torrance satellite"},
    {"npi": "1326164633", "name": "Sameer Amin, MD",
     "story": "System-locked: Kaiser SCPMG (9,573 clinicians), switchboard phone, no MIPS visibility"},
    {"npi": "1831159714", "name": "Jeffrey Goodman, MD",
     "story": "Lunch-only: 56 meals from 19 manufacturers, zero paid engagements — accessible, not an influencer"},
]

_mips_stats: dict = {}


def _npi(v: str) -> str:
    if not re.fullmatch(r"\d{10}", v):
        raise HTTPException(status_code=400, detail="NPI must be 10 digits")
    return v


def _rows(conn, sql: str, params: list) -> list[dict]:
    cur = conn.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _row(conn, sql: str, params: list) -> Optional[dict]:
    out = _rows(conn, sql, params)
    return out[0] if out else None


def _profile_header(
    conn, npi: str, *, backend: Literal["raw", "mart"] = "raw"
) -> dict | None:
    """NPPES-first provider identity enriched with Medicare attributes.

    NPPES defines who can have a profile; DAC is optional evidence that adds
    Medicare specialty, education, and telehealth fields. This keeps an
    NPPES-only clinician discoverable and profileable without pretending that
    missing Medicare rows mean the NPI does not exist.
    """
    if backend == "mart":
        return _row(conn, """
            select npi, name, credentials, specialty, secondary_specialties,
                   city, state, med_school, grad_year,
                   year(current_date) - grad_year years_in_practice,
                   telehealth
            from serving_provider_profile_headers
            where npi = ?
        """, [npi])
    return _row(conn, f"""
        with nppes as (
          select CAST(npi as varchar) npi,
                 trim(coalesce(first_name || ' ', '') || coalesce(last_name, '')) "name",
                 nullif(trim(credentials), '') credentials,
                 practice_city city, practice_state state, taxonomy_1
          from raw_nppes
          where CAST(npi as varchar) = ?),
        dac as (
          select CAST("NPI" as varchar) npi,
                 any_value("Provider First Name") || ' '
                   || any_value("Provider Last Name") "name",
                 nullif(trim(any_value({CRED})), '') credentials,
                 any_value(pri_spec) specialty,
                 any_value(sec_spec_all) secondary_specialties,
                 any_value("City/Town") city, any_value("State") state,
                 any_value(Med_sch) med_school, any_value(Grd_yr) grad_year,
                 max(case when {TELE} = 'Y' then 1 else 0 end) = 1 telehealth
          from raw_dac_national
          where CAST("NPI" as varchar) = ?
          group by "NPI")
        select coalesce(n.npi, d.npi) npi,
               coalesce(n."name", d."name") "name",
               coalesce(n.credentials, d.credentials) credentials,
               coalesce(
                 d.specialty,
                 t.classification
                   || coalesce(' (' || nullif(t.specialization, '') || ')', '')
               ) specialty,
               d.secondary_specialties,
               coalesce(n.city, d.city) city,
               coalesce(n.state, d.state) state,
               d.med_school, d.grad_year,
               year(current_date) - d.grad_year years_in_practice,
               d.telehealth
        from nppes n
        full outer join dac d on d.npi = n.npi
        left join nucc_taxonomy t on t.taxonomy_code = n.taxonomy_1
    """, [npi, npi])


def _affiliation_groups(
    conn, npi: str, *, backend: Literal["raw", "mart"] = "raw"
) -> list[dict]:
    """DAC billing groups merged with PECOS reassignment relationships.

    DAC contributes the door-bearing enrollment (name, roster size, address
    count); reassignment contributes every group the clinician can bill
    through. A reassignment-only group still matters to reps — it is a live
    relationship even though CMS publishes no door for it at this NPI's grain.
    ``sources`` states which file(s) assert each row so the UI can stay
    transparent about provenance.
    """
    if backend == "mart":
        return _rows(conn, """
            select group_id, group_name, group_size, n_addresses,
                   reassignment_size, sources
            from serving_provider_profile_groups
            where npi = ?
            order by (sources <> 'reassignment') desc,
                     coalesce(group_size, reassignment_size, 0) desc
        """, [npi])
    return _rows(conn, """
        with dac as (
          select org_pac_id group_id, any_value("Facility Name") group_name,
                 any_value(num_org_mem) group_size,
                 count(distinct upper(trim(adr_ln_1))) n_addresses
          from raw_dac_national
          where CAST("NPI" as varchar) = ? and org_pac_id is not null
          group by org_pac_id),
        reassign as (
          select CAST("Group PAC ID" as varchar) group_id,
                 any_value("Group Legal Business Name") group_name,
                 any_value("Group Reassignments and Physician Assistants") reassignment_size
          from raw_reassignment
          where CAST("Individual NPI" as varchar) = ?
          group by 1)
        select coalesce(d.group_id, r.group_id) group_id,
               coalesce(d.group_name, r.group_name) group_name,
               d.group_size, coalesce(d.n_addresses, 0) n_addresses,
               r.reassignment_size,
               case when d.group_id is not null and r.group_id is not null then 'dac + reassignment'
                    when d.group_id is not null then 'dac'
                    else 'reassignment' end sources
        from dac d full outer join reassign r on r.group_id = d.group_id
        order by (d.group_id is not null) desc,
                 coalesce(d.group_size, r.reassignment_size, 0) desc
    """, [npi, npi])


def _profile_locations(
    conn, npi: str, *, backend: Literal["raw", "mart"] = "raw"
) -> list[dict]:
    """Practice doors from DAC enrollment merged with the NPPES practice address.

    DAC remains the roster/org grain; NPPES contributes the registry practice
    address when it differs (or confirms the same door). ``sources`` is
    ``dac``, ``nppes``, or ``dac + nppes`` so Access can label provenance the
    same way group affiliations already do.
    """
    if backend == "mart":
        return _rows(conn, """
            select street, suites, city, state, zip5, phone, roster_size,
                   latitude lat, longitude lng, likely_flagship, sources
            from serving_provider_profile_locations
            where npi = ?
            order by coalesce(roster_size, 0) asc, addr_key
        """, [npi])
    return _rows(conn, """
        with dac as (
          select upper(trim(adr_ln_1)) || '|' || left(CAST("ZIP Code" as varchar),5) addr_key,
                 min(trim(adr_ln_1)) street,
                 list(distinct trim(adr_ln_2) order by trim(adr_ln_2))
                   filter (where nullif(trim(adr_ln_2), '') is not null) suites,
                 min("City/Town") city, min("State") state,
                 left(min(CAST("ZIP Code" as varchar)),5) zip5,
                 min(CAST("Telephone Number" as varchar)) phone,
                 min(org_pac_id) org_pac_id
          from raw_dac_national
          where CAST("NPI" as varchar) = ?
            and nullif(trim(adr_ln_1), '') is not null
          group by 1),
        nppes as (
          select upper(trim(practice_address_1)) || '|' || left(CAST(practice_zip as varchar),5) addr_key,
                 min(trim(practice_address_1)) street,
                 list(distinct trim(practice_address_2) order by trim(practice_address_2))
                   filter (where nullif(trim(practice_address_2), '') is not null) suites,
                 min(practice_city) city, min(practice_state) state,
                 left(min(CAST(practice_zip as varchar)),5) zip5,
                 min(CAST(practice_phone as varchar)) phone
          from raw_nppes
          where CAST(npi as varchar) = ?
            and nullif(trim(practice_address_1), '') is not null
          group by 1),
        doc as (
          select coalesce(d.addr_key, n.addr_key) addr_key,
                 coalesce(d.street, n.street) street,
                 coalesce(d.suites, n.suites) suites,
                 coalesce(d.city, n.city) city,
                 coalesce(d.state, n.state) state,
                 coalesce(d.zip5, n.zip5) zip5,
                 coalesce(d.phone, n.phone) phone,
                 d.org_pac_id,
                 case when d.addr_key is not null and n.addr_key is not null then 'dac + nppes'
                      when d.addr_key is not null then 'dac'
                      else 'nppes' end sources
          from dac d full outer join nppes n on n.addr_key = d.addr_key),
        roster as (
          select org_pac_id, upper(trim(adr_ln_1)) || '|' || left(CAST("ZIP Code" as varchar),5) addr_key,
                 count(distinct "NPI") roster_size
          from raw_dac_national
          where org_pac_id in (select distinct org_pac_id from doc where org_pac_id is not null)
          group by 1, 2)
        select doc.street, doc.suites, doc.city, doc.state, doc.zip5, doc.phone,
               r.roster_size, g.lat, g.lng,
               (r.roster_size = max(r.roster_size) over () and r.roster_size > 50) likely_flagship,
               doc.sources
        from doc
        left join roster r on r.org_pac_id = doc.org_pac_id and r.addr_key = doc.addr_key
        left join address_geocode g on g.addr_key = doc.addr_key
        order by coalesce(r.roster_size, 0) asc, doc.addr_key, doc.org_pac_id
    """, [npi, npi])


def _hospital_affiliations(conn, npi: str) -> list[dict]:
    """DAC facility affiliations resolved to hospital names where possible.

    Non-hospital facility types (and hospitals absent from the general-info
    file) keep their row with a null ``facility_name`` — the CCN is still a
    real affiliation the rep should see.
    """
    return _rows(conn, """
        select f.facility_type,
               CAST(f."Facility Affiliations Certification Number" as varchar) ccn,
               any_value(h."Facility Name") facility_name,
               any_value(h."City/Town") city, any_value(h."State") state
        from raw_dac_facility_affiliations f
        left join raw_hospital_general_info h
          on CAST(h."Facility ID" as varchar)
             = CAST(f."Facility Affiliations Certification Number" as varchar)
        where CAST(f."NPI" as varchar) = ?
        group by 1, 2
        order by f.facility_type, ccn
    """, [npi])


def _hospital_affiliations_bulk(conn, npis: list[str]) -> list[dict]:
    """Bounded batch form used by product consumers instead of arbitrary SQL."""

    placeholders = ", ".join("?" for _ in npis)
    return _rows(conn, f"""
        select CAST(f."NPI" as varchar) npi,
               f.facility_type,
               CAST(f."Facility Affiliations Certification Number" as varchar) ccn,
               any_value(h."Facility Name") AS "name",
               any_value(h."Address") address,
               any_value(h."City/Town") city,
               any_value(h."State") state,
               any_value(CAST(h."ZIP Code" as varchar)) zip5,
               any_value(h."Hospital Type") hospital_type
        from raw_dac_facility_affiliations f
        left join raw_hospital_general_info h
          on CAST(h."Facility ID" as varchar)
             = CAST(f."Facility Affiliations Certification Number" as varchar)
        where CAST(f."NPI" as varchar) in ({placeholders})
        group by 1, 2, 3
        order by npi, f.facility_type, ccn
    """, npis)


def _hospital_affiliation_npis(raw: str) -> list[str]:
    requested = list(dict.fromkeys(value.strip() for value in raw.split(",")))
    if not requested or len(requested) > 50 or any(
        not re.fullmatch(r"\d{10}", npi) for npi in requested
    ):
        raise HTTPException(
            status_code=422,
            detail="Provide between 1 and 50 comma-separated 10-digit NPIs",
        )
    return requested


def _hospital_affiliations_response(conn, raw_npis: str) -> dict:
    requested = _hospital_affiliation_npis(raw_npis)
    rows = _hospital_affiliations_bulk(conn, requested)
    providers: dict[str, list[dict]] = {npi: [] for npi in requested}
    for row in rows:
        npi = row.pop("npi")
        row["zip5"] = (str(row.get("zip5") or "")[:5] or None)
        providers[npi].append(row)
    return {"providers": providers}


class SearchHit(BaseModel):
    npi: str
    name: str
    credentials: str | None = None
    specialty: str | None = None
    city: str | None = None
    state: str | None = None
    group_name: str | None = None
    source: str = "nppes"  # "nppes" | "nppes + medicare" | rare DAC-only "medicare"
    match_score: float | None = None  # fuzzy similarity for NPPES name hits


class HospitalAffiliation(BaseModel):
    facility_type: str | None = None
    ccn: str
    name: str | None = None
    address: str | None = None
    city: str | None = None
    state: str | None = None
    zip5: str | None = None
    hospital_type: str | None = None


class HospitalAffiliationsResponse(BaseModel):
    providers: dict[str, list[HospitalAffiliation]]


# Registry-tier fuzzy thresholds (jaro-winkler); validated on misspelled
# real-world queries — genuine targets score >=0.92, noise tops out ~0.83.
_FUZZY_THRESHOLD_FULL = 0.85   # first + last name provided
_FUZZY_THRESHOLD_LAST = 0.88   # last name only


def _search_nppes(conn, parts: list[str], city: Optional[str], state: Optional[str],
                  limit: int) -> list[dict]:
    """Primary discovery against the full NPPES registry (everyone with an NPI).

    Medicare DAC is enrichment, never the discovery universe. Last name is
    weighted 0.7 vs first 0.3; stored last names are also compared with
    spaces/hyphens stripped so "EL ATTRACHE" / "EL-ATTRACHE" / "ELATTRACHE"
    all behave the same. Source provenance records whether a hit also has DAC
    evidence without changing its NPPES-first identity.

    City ranks but never filters (same doctrine as the DAC tier): practice
    city is a mailing-address value, so a better name match in a neighboring
    town must not be hidden by a metro-name query. City acts as a tiebreaker
    below the name score; state stays a hard scope.
    """
    first = parts[0] if len(parts) >= 2 else None
    last = "".join(parts[1:]) if len(parts) >= 2 else parts[0]

    scope_preds = ["CAST(n.entity_type AS VARCHAR) = '1'", "n.last_name is not null"]
    scope_params: list = []
    if state:
        scope_preds.append("n.practice_state = ?")
        scope_params.append(state.upper())

    stripped_last = 'replace(replace(upper(n.last_name), \' \', \'\'), \'-\', \'\')'
    if first:
        # greatest() also scores the whole query as a compound surname, so
        # "el attrache" (no first name) still finds EL ATTRACHE / ELATTRACHE.
        score_expr = f"""
            greatest(
                0.7 * jaro_winkler_similarity({stripped_last}, ?)
              + 0.3 * jaro_winkler_similarity(upper(coalesce(n.first_name, '')), ?),
                jaro_winkler_similarity({stripped_last}, ?)
            )"""
        score_params = [last, first, "".join(parts)]
        threshold = _FUZZY_THRESHOLD_FULL
    else:
        score_expr = f"jaro_winkler_similarity({stripped_last}, ?)"
        score_params = [last]
        threshold = _FUZZY_THRESHOLD_LAST

    if city:
        city_match_expr = "(upper(coalesce(n.practice_city, '')) = ?)"
        city_params: list = [city.upper()]
    else:
        city_match_expr = "false"
        city_params = []

    sql = f"""
        with scored as (
            select CAST(n.npi as varchar) npi,
                   coalesce(n.first_name || ' ', '') || n.last_name as "name",
                   n.credentials, n.practice_city city, n.practice_state state,
                   n.taxonomy_1, ({score_expr}) score,
                   {city_match_expr} city_match
            from raw_nppes n
            where {' and '.join(scope_preds)}
            order by score desc, city_match desc
            limit {limit}
        )
        select s.npi, s."name", s.credentials, s.city, s.state,
               round(s.score, 3) match_score,
               coalesce(
                   any_value(d.pri_spec),
                   any_value(t.classification
                             || coalesce(' (' || nullif(t.specialization, '') || ')', ''))
               ) specialty,
               any_value(d."Facility Name") group_name,
               count(d."NPI") > 0 in_dac
        from scored s
        left join nucc_taxonomy t on s.taxonomy_1 = t.taxonomy_code
        left join raw_dac_national d on CAST(d."NPI" as varchar) = s.npi
        where s.score >= {threshold}
        group by s.npi, s."name", s.credentials, s.city, s.state, s.score, s.city_match
        order by s.score desc, s.city_match desc"""
    rows = _rows(conn, sql, score_params + city_params + scope_params)
    for row in rows:
        row["source"] = "nppes + medicare" if row.pop("in_dac", False) else "nppes"
    return rows


def _search_npi(conn, npi: str, state: Optional[str]) -> list[dict]:
    """Resolve exact identity through NPPES, with DAC as enrichment/fallback."""
    state_predicate = " AND n.practice_state = ?" if state else ""
    rows = _rows(conn, f"""
        select CAST(n.npi as varchar) npi,
               trim(coalesce(n.first_name || ' ', '') || coalesce(n.last_name, '')) "name",
               nullif(trim(n.credentials), '') credentials,
               coalesce(
                 any_value(d.pri_spec),
                 any_value(t.classification
                   || coalesce(' (' || nullif(t.specialization, '') || ')', ''))
               ) specialty,
               n.practice_city city, n.practice_state state,
               any_value(d."Facility Name") group_name,
               case when count(d."NPI") > 0 then 'nppes + medicare'
                    else 'nppes' end AS "source"
        from raw_nppes n
        left join nucc_taxonomy t on t.taxonomy_code = n.taxonomy_1
        left join raw_dac_national d on CAST(d."NPI" as varchar) = CAST(n.npi as varchar)
        where CAST(n.npi as varchar) = ? {state_predicate}
        group by n.npi, n.first_name, n.last_name, n.credentials,
                 n.practice_city, n.practice_state
    """, [npi, state.upper()] if state else [npi])
    if rows:
        return rows
    state_predicate = ' AND "State" = ?' if state else ""
    return _rows(conn, f"""
        select CAST("NPI" as varchar) npi,
               any_value("Provider First Name") || ' '
                 || any_value("Provider Last Name") as "name",
               nullif(trim(any_value({CRED})), '') credentials,
               any_value(pri_spec) specialty, any_value("City/Town") city,
               any_value("State") state, any_value("Facility Name") group_name,
               'medicare' AS "source"
        from raw_dac_national
        where CAST("NPI" as varchar) = ? {state_predicate}
        group by "NPI"
    """, [npi, state.upper()] if state else [npi])


def get_profiles_router(
    get_conn, provider_profile_backend: ProviderProfileBackend | None = None
):
    router = APIRouter(prefix="/profiles", tags=["Doctor Profiles"])
    selected_profile_backend = provider_profile_backend or os.getenv(
        "PROVIDER_PROFILE_BACKEND", "raw"
    )
    if selected_profile_backend not in {"raw", "mart", "auto"}:
        raise ValueError("PROVIDER_PROFILE_BACKEND must be raw, mart, or auto")
    profile_mart_available: bool | None = None

    def resolve_profile_backend() -> Literal["raw", "mart"]:
        """Select the complete three-table profile capability as one unit."""
        nonlocal profile_mart_available
        if selected_profile_backend != "auto":
            return selected_profile_backend
        if profile_mart_available is None:
            profile_mart_available = _profile_mart_is_available(get_conn())
        return "mart" if profile_mart_available else "raw"

    @router.get("/exemplars")
    async def exemplars():
        return EXEMPLARS

    @router.get("/search", response_model=list[SearchHit])
    def search(q: str, city: Optional[str] = None, state: Optional[str] = None,
                     limit: int = 15):
        """Find doctors by name (last or 'first last'), optional city/state.

        NPPES is the discovery universe, including providers who never bill
        Medicare. DAC enriches matching NPIs with Medicare specialty and group
        context but never gates whether the provider can be found.

        State is a hard scope; city only boosts ranking. CMS/NPPES city is a
        mailing-address value, so metro queries ("Los Angeles") must not hide
        exact name matches recorded in a neighboring suburb ("Tarzana").
        """
        limit = max(1, min(limit, 30))
        parts = q.strip().upper().split()
        if not parts:
            return []
        conn = get_conn()
        exact_npi = q.strip()
        if re.fullmatch(r"\d{10}", exact_npi):
            rows = _search_npi(conn, exact_npi, state)
        else:
            rows = _search_nppes(conn, parts, city, state, limit)
        return [SearchHit(**{**r, "credentials": (r.get("credentials") or "").strip() or None})
                for r in rows]

    @router.get(
        "/hospital-affiliations", response_model=HospitalAffiliationsResponse
    )
    def hospital_affiliations(npis: str):
        """Return facility affiliations for at most 50 comma-separated NPIs."""
        return _hospital_affiliations_response(get_conn(), npis)

    @router.get("/{npi}")
    def profile(npi: str):
        npi = _npi(npi)
        conn = get_conn()
        profile_backend = resolve_profile_backend()
        out: dict = {"npi": npi}

        # ------ header / background (DAC + NPPES) ------
        out["header"] = _profile_header(conn, npi, backend=profile_backend)
        if not out["header"]:
            raise HTTPException(status_code=404, detail="NPI not found in NPPES or Doctors & Clinicians")

        # ------ 1. patient panel ------
        out["panel"] = _row(conn, """
            select Tot_Benes medicare_patients, Tot_Srvcs total_services,
                   round(Tot_Srvcs / nullif(Tot_Benes,0), 1) services_per_patient,
                   round(Tot_Mdcr_Alowd_Amt) medicare_allowed_amt,
                   round(Drug_Mdcr_Pymt_Amt) part_b_drug_payments,
                   Bene_Avg_Age avg_patient_age,
                   round(100.0*(coalesce(Bene_Age_75_84_Cnt,0)+coalesce(Bene_Age_GT_84_Cnt,0))
                         / nullif(Tot_Benes,0)) pct_age_75_plus,
                   round(100.0*Bene_Feml_Cnt/nullif(Tot_Benes,0)) pct_female,
                   round(100.0*Bene_Dual_Cnt/nullif(Tot_Benes,0)) pct_dual_eligible,
                   Bene_Avg_Risk_Scre avg_hcc_risk_score,
                   Bene_CC_PH_Hypertension_V2_Pct pct_hypertension,
                   Bene_CC_PH_Hyperlipidemia_V2_Pct pct_hyperlipidemia,
                   Bene_CC_PH_Diabetes_V2_Pct pct_diabetes,
                   Bene_CC_PH_IschemicHeart_V2_Pct pct_ischemic_heart,
                   Bene_CC_PH_HF_NonIHD_V2_Pct pct_heart_failure,
                   Bene_CC_PH_Afib_V2_Pct pct_afib,
                   Bene_CC_PH_CKD_V2_Pct pct_ckd,
                   Bene_CC_PH_COPD_V2_Pct pct_copd,
                   Bene_CC_BH_Depress_V1_Pct pct_depression
            from raw_physician_by_provider
            where CAST(Rndrng_NPI as varchar) = ? and Rndrng_Prvdr_Ent_Cd = 'I'
        """, [npi])

        # ------ 2. clinical focus ------
        out["clinical"] = _row(conn, """
            select any_value(Rndrng_Prvdr_Type) cms_specialty,
                   count(distinct HCPCS_Cd) distinct_codes,
                   sum(Tot_Srvcs) total_services,
                   round(sum(Tot_Srvcs*Avg_Mdcr_Pymt_Amt)) est_total_paid,
                   round(sum(Tot_Srvcs*Avg_Mdcr_Pymt_Amt) filter (where Place_Of_Srvc='F')
                         / nullif(sum(Tot_Srvcs*Avg_Mdcr_Pymt_Amt),0), 2) facility_paid_share,
                   round(sum(Tot_Srvcs*Avg_Mdcr_Pymt_Amt) filter (where HCPCS_Drug_Ind='Y')
                         / nullif(sum(Tot_Srvcs*Avg_Mdcr_Pymt_Amt),0), 2) drug_admin_paid_share,
                   round(sum(Tot_Srvcs*Avg_Mdcr_Pymt_Amt) filter (where HCPCS_Cd between '99091' and '99499')
                         / nullif(sum(Tot_Srvcs*Avg_Mdcr_Pymt_Amt),0), 2) em_paid_share
            from raw_physician_by_provider_and_service
            where CAST(Rndrng_NPI as varchar) = ?
        """, [npi])
        out["top_procedures"] = _rows(conn, """
            with svc as (
              select HCPCS_Cd, any_value(HCPCS_Desc) descr,
                     case when max(HCPCS_Drug_Ind)='Y' then 'drug_admin'
                          when HCPCS_Cd between '99091' and '99499' then 'evaluation_mgmt'
                          when HCPCS_Cd between '70000' and '79999' then 'imaging'
                          when HCPCS_Cd between '80000' and '89999' then 'lab_path'
                          when HCPCS_Cd between '90000' and '98999' then 'diagnostic_proc'
                          when HCPCS_Cd between '00100' and '69999' then 'surgical_proc'
                          else 'other' end category,
                     sum(Tot_Srvcs) services, max(Tot_Benes) patients,
                     round(sum(Tot_Srvcs*Avg_Mdcr_Pymt_Amt)) est_paid,
                     round(coalesce(sum(Tot_Srvcs) filter (where Place_Of_Srvc='F'),0)
                           / nullif(sum(Tot_Srvcs),0), 2) facility_share
              from raw_physician_by_provider_and_service
              where CAST(Rndrng_NPI as varchar) = ? group by HCPCS_Cd),
            tot as (select sum(est_paid) all_paid from svc)
            select s.HCPCS_Cd hcpcs, s.category, left(s.descr, 70) description,
                   s.services, s.patients, s.est_paid,
                   round(s.est_paid/nullif(t.all_paid,0), 2) pct_of_paid, s.facility_share
            from svc s cross join tot t order by s.est_paid desc limit 10
        """, [npi])

        # ------ 3. prescribing ------
        out["prescribing"] = _row(conn, """
            select Tot_Clms total_claims, Tot_Benes patients, round(Tot_Drug_Cst) total_cost,
                   round(Tot_Drug_Cst/nullif(Tot_Clms,0), 2) cost_per_claim,
                   round(Brnd_Tot_Clms*1.0/nullif(Brnd_Tot_Clms+Gnrc_Tot_Clms,0), 2) brand_claim_share,
                   round(Brnd_Tot_Drug_Cst/nullif(Tot_Drug_Cst,0), 2) brand_cost_share,
                   Opioid_Prscrbr_Rate opioid_rate_pct,
                   round(LIS_Tot_Clms*1.0/nullif(Tot_Clms,0), 2) lis_claim_share,
                   Bene_Avg_Age rx_panel_avg_age, Bene_Avg_Risk_Scre rx_panel_risk
            from raw_part_d_by_provider where CAST(PRSCRBR_NPI as varchar) = ?
        """, [npi])
        out["top_drugs"] = _rows(conn, """
            with rx as (
              select Brnd_Name brand, Gnrc_Name generic, Tot_Clms claims, Tot_Benes patients,
                     round(Tot_Drug_Cst) drug_cost,
                     round(Tot_Drug_Cst/nullif(Tot_Clms,0), 2) cost_per_claim,
                     round(Tot_Day_Suply*1.0/nullif(Tot_Clms,0)) days_per_claim,
                     (Tot_Drug_Cst/nullif(Tot_Clms,0)) >= 950 specialty_tier
              from raw_part_d_by_provider_and_drug where CAST(Prscrbr_NPI as varchar) = ?),
            tot as (select sum(drug_cost) all_cost from rx)
            select r.*, round(r.drug_cost/nullif(t.all_cost,0), 2) pct_of_cost
            from rx r cross join tot t order by r.drug_cost desc limit 10
        """, [npi])

        # ------ 4. industry (Open Payments) ------
        out["industry"] = industry_summary(conn, npi)
        out["industry_by_nature"] = _rows(conn, """
            select case
                when Nature_of_Payment_or_Transfer_of_Value = 'Food and Beverage' then 'Meals'
                when Nature_of_Payment_or_Transfer_of_Value = 'Travel and Lodging' then 'Travel'
                when Nature_of_Payment_or_Transfer_of_Value in ('Consulting Fee','Honoraria') then 'Consulting/Honoraria'
                when Nature_of_Payment_or_Transfer_of_Value like 'Compensation for serv%' then 'Speaking/Faculty'
                when Nature_of_Payment_or_Transfer_of_Value in ('Education','Gift','Entertainment','Charitable Contribution') then 'Education/Gifts'
                when Nature_of_Payment_or_Transfer_of_Value in ('Royalty or License','Acquisitions') then 'Royalties/IP'
                else 'Other' end nature_group,
                count(*) n_payments, round(sum(Total_Amount_of_Payment_USDollars)) usd
            from raw_open_payments_general
            where CAST(Covered_Recipient_NPI as varchar) = ?
            group by 1 order by usd desc
        """, [npi])
        out["industry_manufacturers"] = _rows(conn, """
            select Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name manufacturer,
                   round(sum(Total_Amount_of_Payment_USDollars)) usd, count(*) n_payments,
                   string_agg(distinct Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1,
                              ', ' order by Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1)
                       products
            from raw_open_payments_general
            where CAST(Covered_Recipient_NPI as varchar) = ?
            group by 1 order by usd desc limit 5
        """, [npi])
        out["research"] = _row(conn, """
            select count(*) research_rows,
                   round(sum(Total_Amount_of_Payment_USDollars)) research_usd,
                   count(distinct Name_of_Study) n_studies,
                   string_agg(distinct Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name,
                              '; ' order by Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name)
                       sponsors
            from raw_open_payments_research
            where CAST(Covered_Recipient_NPI as varchar) = ?
               or CAST(Principal_Investigator_1_NPI as varchar) = ?
               or CAST(Principal_Investigator_2_NPI as varchar) = ?
               or CAST(Principal_Investigator_3_NPI as varchar) = ?
               or CAST(Principal_Investigator_4_NPI as varchar) = ?
               or CAST(Principal_Investigator_5_NPI as varchar) = ?
        """, [npi] * 6)
        out["ownership"] = _row(conn, """
            select count(*) stakes, round(sum(Total_Amount_Invested_USDollars)) invested,
                   string_agg(distinct Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name,
                              '; ' order by Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name)
                       companies
            from raw_open_payments_ownership where CAST(Physician_NPI as varchar) = ?
        """, [npi])

        # ------ 5. access & affiliation ------
        out["locations"] = _profile_locations(conn, npi, backend=profile_backend)
        home_state = (out["header"] or {}).get("state")
        out["locations"].sort(key=lambda l: (
            0 if l.get("state") == home_state else 1,
            l.get("roster_size") if l.get("roster_size") is not None else 10**9,
            l.get("street") or "",
            l.get("zip5") or "",
        ))

        out["groups"] = _affiliation_groups(conn, npi, backend=profile_backend)
        out["hospital_affiliations"] = _hospital_affiliations(conn, npi)
        if not _mips_stats:
            s = _row(conn, """select median(final_MIPS_score) med,
                              quantile_cont(final_MIPS_score, 0.25) q25,
                              quantile_cont(final_MIPS_score, 0.75) q75
                              from raw_mips_performance""", [])
            _mips_stats.update(s or {})
        mips = _rows(conn, """
            select source, final_MIPS_score final_score, Quality_category_score quality_score,
                   Cost_category_score cost_score
            from raw_mips_performance where CAST("NPI" as varchar) = ?
            order by case source when 'individual' then 1 when 'group' then 2 else 3 end
        """, [npi])
        for m in mips:
            fs = m.get("final_score")
            if fs is not None and _mips_stats:
                m["interpretation"] = (
                    "TOP QUARTILE" if fs >= _mips_stats["q75"]
                    else "ABOVE MEDIAN" if fs >= _mips_stats["med"]
                    else "BELOW MEDIAN" if fs >= _mips_stats["q25"]
                    else "BOTTOM QUARTILE")
        out["mips"] = mips
        out["mips_national"] = _mips_stats

        return out

    return router
