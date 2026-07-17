"""
Medicare practice/site and NPPES-primary location search.

Rolls up the CMS Doctors & Clinicians table (`raw_dac_national`) into practice
*locations* — (building address × group) — for a given specialty + location.
Medicare-only (no Google Places coupling). Uses the `address_geocode` table for
lat/lng + proximity.

Each result is one practice at one site: the group's legal name, address, geo,
phone, the count of matching-specialty clinicians there, the group's national
size, and the specialty mix. A heuristic `billing_artifact` flag marks
admin/billing addresses (one group, implausibly many providers for a single
street) so the UI can de-emphasise them.

The explicit ``location_basis=nppes_primary`` mode instead places every
Medicare-participating NPI's national Part B/Part D totals once at the primary
practice location reported in NPPES. It never splits or repeats those totals
across DAC enrollment sites, and it does not claim the services were rendered
at that address. Roster and profile calls accept the same mode so drill-downs
resolve against the identical NPPES location membership.
"""
import math
import re
from typing import Literal, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/practices", tags=["Medicare Practices"])

# Search term -> pri_spec ILIKE patterns (CMS uses granular specialty labels).
SPECIALTY_MAP: dict[str, list[str]] = {
    "primary care": ["%family%", "%internal medicine%", "%general practice%", "%geriatric%"],
    "cardiology": ["%cardio%"],
    "orthopedics": ["%orthop%"],
    "orthopedic surgery": ["%orthop%"],
    "endocrinology": ["%endocrin%"],
    "oncology": ["%oncology%", "%hematology%"],
    "gastroenterology": ["%gastro%"],
    "neurology": ["%neurolog%"],
    "dermatology": ["%dermat%"],
    "urology": ["%urolog%"],
    "pulmonology": ["%pulmon%"],
    "nephrology": ["%nephro%"],
    "rheumatology": ["%rheumat%"],
    "ophthalmology": ["%ophthalmo%"],
    "psychiatry": ["%psychiat%"],
    "obgyn": ["%obstetri%", "%gynecolog%"],
    "ob/gyn": ["%obstetri%", "%gynecolog%"],
    "ob gyn": ["%obstetri%", "%gynecolog%"],
    "pcp": ["%family%", "%internal medicine%", "%general practice%", "%geriatric%"],
    "primary care physician": ["%family%", "%internal medicine%", "%general practice%", "%geriatric%"],
}

# Provider-noun suffixes -> strip to the specialty root so a search for
# "cardiologist" matches "CARDIOLOGY" / "cardiologists" / "pediatrician", etc.
# Checked longest-first.
_SPECIALTY_SUFFIXES = ("icians", "ician", "ists", "ist")


def specialty_patterns(term: str) -> list[str]:
    """Resolve a free-text specialty into a list of pri_spec ILIKE patterns.

    Tries the explicit map, then normalizes provider-noun forms
    ("cardiologist" -> "cardiolog"), then falls back to a substring match.
    """
    t = " ".join(term.lower().split())
    if t in SPECIALTY_MAP:
        return SPECIALTY_MAP[t]
    for suffix in _SPECIALTY_SUFFIXES:
        if t.endswith(suffix) and len(t) - len(suffix) >= 4:
            root = t[: -len(suffix)]
            return SPECIALTY_MAP.get(root, [f"%{root}%"])
    return [f"%{t}%"]


class PracticeResult(BaseModel):
    practice_name: Optional[str] = None
    org_pac_id: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip5: Optional[str] = None
    phone: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    providers_here: int
    group_size_national: Optional[int] = None
    specialties: list[str] = []
    distance_miles: Optional[float] = None
    billing_artifact: bool = False
    # Roster billing power: national per-NPI totals summed over the matching
    # clinicians at this site (not door-level billing — CMS has no site grain).
    partb_payments: Optional[float] = None
    partd_drug_cost: Optional[float] = None
    # For SOLO sites (no org affiliation in DAC) the practice has no legal
    # name — surface the clinician's name so the UI can say "Dr. X (independent)".
    solo_provider_name: Optional[str] = None
    # Geographic attribution used for this row. ``nppes_primary`` means every
    # clinician's national NPI totals are placed once at the primary practice
    # address reported in NPPES; it does not assert that services occurred there.
    location_basis: Literal["cms_enrollment", "nppes_primary"] = "cms_enrollment"


class PracticeSearchResponse(BaseModel):
    specialty: str
    matched_patterns: list[str]
    location: str
    total: int
    results: list[PracticeResult]
    location_basis: Literal["cms_enrollment", "nppes_primary"] = "cms_enrollment"


class SpecialtyListResponse(BaseModel):
    specialties: list[str]


# The DAC credentials column name has trailing tabs in the source file.
CRED_COL = '"Cred\t\t\t\t"'

ZIP5_RE = re.compile(r"^\d{5}$")


def parse_zip_codes(zips: Optional[str], zip_code: Optional[str]) -> list[str]:
    """Normalize a comma-delimited ZIP boundary without accepting prefixes."""
    raw = [item.strip() for item in (zips or "").split(",") if item.strip()]
    if not raw and zip_code:
        raw = [zip_code.strip()]
    normalized = list(dict.fromkeys(raw))
    if len(normalized) > 100:
        raise ValueError("At most 100 ZIP codes may be selected")
    if any(not ZIP5_RE.fullmatch(item) for item in normalized):
        raise ValueError("ZIP codes must be five digits")
    return normalized


class ProviderResult(BaseModel):
    npi: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    credentials: Optional[str] = None
    specialty: Optional[str] = None
    phone: Optional[str] = None
    medicare_services: Optional[float] = None
    medicare_beneficiaries: Optional[int] = None
    medicare_payments: Optional[float] = None
    open_payments_total: Optional[float] = None


class ProviderRosterResponse(BaseModel):
    practice_name: Optional[str] = None
    org_pac_id: Optional[str] = None
    total: int
    providers: list[ProviderResult]
    location_basis: Literal["cms_enrollment", "nppes_primary"] = "cms_enrollment"


class ProcedureRollup(BaseModel):
    hcpcs: str
    description: Optional[str] = None
    est_payments: Optional[float] = None
    services: Optional[float] = None
    beneficiaries: Optional[int] = None


class DrugRollup(BaseModel):
    brand: str
    generic: Optional[str] = None
    drug_cost: Optional[float] = None
    claims: Optional[int] = None


class ManufacturerRollup(BaseModel):
    name: str
    total: float


class SiteProfileResponse(BaseModel):
    """Medicare deep-dive rollup for one practice site (address × group).

    All figures are national per-NPI CMS totals summed over the site's roster —
    roster billing power, not door-level billing (CMS publishes no site grain).
    Part D by-drug rows under 11 claims are suppressed upstream, so top-drug
    figures are a floor.
    """

    practice_name: Optional[str] = None
    org_pac_id: Optional[str] = None
    address: Optional[str] = None
    roster_size: int = 0
    partb_payments: Optional[float] = None
    partb_services: Optional[float] = None
    partb_beneficiaries: Optional[int] = None
    partd_drug_cost: Optional[float] = None
    partd_claims: Optional[int] = None
    open_payments_total: Optional[float] = None
    open_payments_count: int = 0
    open_payments_recipients: int = 0
    top_procedures: list[ProcedureRollup] = []
    top_drugs: list[DrugRollup] = []
    top_manufacturers: list[ManufacturerRollup] = []
    location_basis: Literal["cms_enrollment", "nppes_primary"] = "cms_enrollment"


def _site_profile_for_roster(
    conn,
    roster_rows: list[tuple],
    *,
    street: str,
    org_pac_id: Optional[str],
    location_basis: Literal["cms_enrollment", "nppes_primary"],
) -> SiteProfileResponse:
    """Roll national NPI metrics over an already-resolved location roster."""
    npis = [str(row[0]) for row in roster_rows]
    if not npis:
        return SiteProfileResponse(
            address=street,
            org_pac_id=org_pac_id,
            location_basis=location_basis,
        )
    practice_name = next((row[1] for row in roster_rows if row[1]), None)
    placeholders = ", ".join(["?"] * len(npis))
    partb = conn.execute(
        f"""
        select sum("Tot_Mdcr_Pymt_Amt"), sum("Tot_Srvcs"), sum("Tot_Benes")
        from raw_physician_by_provider
        where cast("Rndrng_NPI" as varchar) in ({placeholders})
        """,
        npis,
    ).fetchone()
    partd = conn.execute(
        f"""
        select sum("Tot_Drug_Cst"), sum("Tot_Clms")
        from raw_part_d_by_provider
        where cast("PRSCRBR_NPI" as varchar) in ({placeholders})
        """,
        npis,
    ).fetchone()
    procs = conn.execute(
        f"""
        select "HCPCS_Cd", any_value("HCPCS_Desc"),
               sum("Tot_Srvcs" * "Avg_Mdcr_Pymt_Amt") est_pay,
               sum("Tot_Srvcs"), sum("Tot_Benes")
        from raw_physician_by_provider_and_service
        where cast("Rndrng_NPI" as varchar) in ({placeholders})
        group by 1 order by est_pay desc nulls last limit 6
        """,
        npis,
    ).fetchall()
    drugs = conn.execute(
        f"""
        select "Brnd_Name", any_value("Gnrc_Name"),
               sum("Tot_Drug_Cst") cst, sum("Tot_Clms")
        from raw_part_d_by_provider_and_drug
        where cast("PRSCRBR_NPI" as varchar) in ({placeholders})
        group by 1 order by cst desc nulls last limit 6
        """,
        npis,
    ).fetchall()
    op = conn.execute(
        f"""
        select sum("Total_Amount_of_Payment_USDollars"), count(*),
               count(distinct cast("Covered_Recipient_NPI" as varchar))
        from raw_open_payments_general
        where cast("Covered_Recipient_NPI" as varchar) in ({placeholders})
        """,
        npis,
    ).fetchone()
    mfrs = conn.execute(
        f"""
        select "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name",
               sum("Total_Amount_of_Payment_USDollars") total
        from raw_open_payments_general
        where cast("Covered_Recipient_NPI" as varchar) in ({placeholders})
        group by 1 order by total desc limit 5
        """,
        npis,
    ).fetchall()
    return SiteProfileResponse(
        practice_name=practice_name,
        org_pac_id=org_pac_id,
        address=street,
        roster_size=len(npis),
        partb_payments=partb[0],
        partb_services=partb[1],
        partb_beneficiaries=int(partb[2]) if partb[2] is not None else None,
        partd_drug_cost=partd[0],
        partd_claims=int(partd[1]) if partd[1] is not None else None,
        open_payments_total=op[0],
        open_payments_count=op[1] or 0,
        open_payments_recipients=op[2] or 0,
        top_procedures=[
            ProcedureRollup(
                hcpcs=str(row[0]),
                description=row[1],
                est_payments=row[2],
                services=row[3],
                beneficiaries=int(row[4]) if row[4] is not None else None,
            )
            for row in procs
            if row[0] is not None
        ],
        top_drugs=[
            DrugRollup(
                brand=str(row[0]),
                generic=row[1],
                drug_cost=row[2],
                claims=int(row[3]) if row[3] is not None else None,
            )
            for row in drugs
            if row[0] is not None
        ],
        top_manufacturers=[
            ManufacturerRollup(name=str(row[0]), total=row[1])
            for row in mfrs
            if row[0] is not None
        ],
        location_basis=location_basis,
    )


def get_practices_router(get_conn):
    @router.get("/specialties", response_model=SpecialtyListResponse)
    async def specialties():
        """Return every provider specialty currently present in CMS claims."""
        rows = get_conn().execute(
            """
            select distinct trim("Rndrng_Prvdr_Type") specialty
            from raw_physician_by_provider
            where "Rndrng_Prvdr_Type" is not null
              and trim("Rndrng_Prvdr_Type") <> ''
            order by specialty
            """
        ).fetchall()
        return SpecialtyListResponse(specialties=[str(row[0]) for row in rows])

    @router.get("/search", response_model=PracticeSearchResponse)
    async def search(
        specialty: str,
        city: Optional[str] = None,
        state: Optional[str] = None,
        zip: Optional[str] = None,
        zips: Optional[str] = None,
        lat: Optional[float] = None,
        lng: Optional[float] = None,
        radius_miles: float = 10.0,
        limit: int = 50,
        location_basis: Literal["cms_enrollment", "nppes_primary"] = "cms_enrollment",
    ):
        limit = max(1, min(limit, 200))
        patterns = specialty_patterns(specialty)
        try:
            selected_zips = parse_zip_codes(zips, zip)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        if location_basis == "nppes_primary":
            spec_pred = " OR ".join(['p."Rndrng_Prvdr_Type" ILIKE ?'] * len(patterns))
            params: list = list(patterns)
            loc_clauses: list[str] = []
            loc_desc: list[str] = []
            proximity = lat is not None and lng is not None
            if proximity:
                dlat = radius_miles / 69.0
                dlng = radius_miles / (
                    69.0 * max(0.1, abs(math.cos(math.radians(lat))))
                )
                loc_clauses.append("ge.lat between ? and ? and ge.lng between ? and ?")
                params += [lat - dlat, lat + dlat, lng - dlng, lng + dlng]
                loc_desc.append(f"{radius_miles}mi of ({lat:.4f},{lng:.4f})")
            if state:
                loc_clauses.append("upper(n.practice_state) = ?")
                params.append(state.upper())
                loc_desc.append(state.upper())
            if city:
                loc_clauses.append("upper(n.practice_city) = ?")
                params.append(city.upper())
                loc_desc.append(city)
            if selected_zips:
                placeholders = ", ".join(["?"] * len(selected_zips))
                loc_clauses.append(f"left(n.practice_zip, 5) in ({placeholders})")
                params.extend(selected_zips)
                loc_desc.append(", ".join(selected_zips))
            if not loc_clauses:
                loc_clauses.append("1=1")

            dist_expr = "NULL"
            if proximity:
                dist_expr = (
                    f"3959*2*asin(sqrt(pow(sin(radians(ge.lat-{lat})/2),2)"
                    f"+cos(radians({lat}))*cos(radians(ge.lat))*"
                    f"pow(sin(radians(ge.lng-({lng}))/2),2)))"
                )
            order = "dist nulls last" if proximity else "providers_here desc"
            sql = f"""
            with attributed as (
                select cast(p."Rndrng_NPI" as varchar) npi,
                       upper(trim(n.practice_address_1)) addr_norm,
                       left(n.practice_zip, 5) zip5,
                       n.practice_address_1 addr,
                       n.practice_city city,
                       n.practice_state state,
                       n.practice_phone phone,
                       p."Rndrng_Prvdr_Type" spec,
                       n.first_name fn,
                       n.last_name ln,
                       p."Tot_Mdcr_Pymt_Amt" partb_payments,
                       rx."Tot_Drug_Cst" partd_drug_cost
                from raw_physician_by_provider p
                join raw_nppes n
                  on cast(p."Rndrng_NPI" as varchar) = cast(n.npi as varchar)
                left join raw_part_d_by_provider rx
                  on cast(p."Rndrng_NPI" as varchar) = cast(rx."PRSCRBR_NPI" as varchar)
                left join address_geocode ge
                  on (upper(trim(n.practice_address_1)) || '|' || left(n.practice_zip, 5))
                     = ge.addr_key
                where ({spec_pred})
                  and ({' and '.join(loc_clauses)})
                  and n.deactivation_date is null
            ),
            sites as (
                select addr_norm || '|' || zip5 addr_key,
                       any_value(addr) address,
                       any_value(city) city,
                       any_value(state) state,
                       any_value(zip5) zip5,
                       any_value(phone) phone,
                       count(distinct npi) providers_here,
                       list(distinct spec) specialties,
                       sum(partb_payments) partb_payments,
                       sum(partd_drug_cost) partd_drug_cost,
                       min(trim(coalesce(fn, '') || ' ' || coalesce(ln, ''))) solo_name
                from attributed
                group by addr_norm || '|' || zip5
            )
            select null practice_name, null org_pac_id, s.address, s.city, s.state,
                   s.zip5, s.phone, ge.lat, ge.lng, null group_size_national,
                   s.providers_here, s.specialties, {dist_expr} dist,
                   s.partb_payments, s.partd_drug_cost, s.solo_name
            from sites s
            left join address_geocode ge on s.addr_key = ge.addr_key
            order by {order}
            limit {limit}
            """
            rows = get_conn().execute(sql, params).fetchall()
            results = [
                PracticeResult(
                    practice_name=row[0],
                    org_pac_id=row[1],
                    address=row[2],
                    city=row[3],
                    state=row[4],
                    zip5=row[5],
                    phone=row[6],
                    lat=row[7],
                    lng=row[8],
                    group_size_national=row[9],
                    providers_here=row[10],
                    specialties=[value for value in (row[11] or []) if value],
                    distance_miles=round(row[12], 2) if row[12] is not None else None,
                    billing_artifact=False,
                    partb_payments=row[13],
                    partd_drug_cost=row[14],
                    solo_provider_name=(row[15] or "").strip() or None,
                    location_basis="nppes_primary",
                )
                for row in rows
            ]
            return PracticeSearchResponse(
                specialty=specialty,
                matched_patterns=patterns,
                location=", ".join(loc_desc) or "anywhere",
                total=len(results),
                results=results,
                location_basis="nppes_primary",
            )

        spec_pred = " OR ".join(["pri_spec ILIKE ?"] * len(patterns))
        params: list = list(patterns)

        loc_clauses: list[str] = []
        loc_desc: list[str] = []
        proximity = lat is not None and lng is not None
        if proximity:
            # rough bounding box to keep the candidate set small before the geo join
            dlat = radius_miles / 69.0
            dlng = radius_miles / (69.0 * max(0.1, abs(math.cos(math.radians(lat)))))
            loc_clauses.append("ge.lat between ? and ? and ge.lng between ? and ?")
            params += [lat - dlat, lat + dlat, lng - dlng, lng + dlng]
            loc_desc.append(f"{radius_miles}mi of ({lat:.4f},{lng:.4f})")
        if state:
            loc_clauses.append('d."State" = ?')
            params.append(state.upper())
            loc_desc.append(state.upper())
        if city:
            loc_clauses.append('upper(d."City/Town") = ?')
            params.append(city.upper())
            loc_desc.append(city)
        if selected_zips:
            placeholders = ", ".join(["?"] * len(selected_zips))
            loc_clauses.append(
                f'left(CAST(d."ZIP Code" AS VARCHAR), 5) in ({placeholders})'
            )
            params.extend(selected_zips)
            loc_desc.append(", ".join(selected_zips))
        if not loc_clauses:
            loc_clauses.append("1=1")

        dist_expr = "NULL"
        if proximity:
            dist_expr = (
                f"3959*2*asin(sqrt(pow(sin(radians(ge.lat-{lat})/2),2)"
                f"+cos(radians({lat}))*cos(radians(ge.lat))*pow(sin(radians(ge.lng-({lng}))/2),2)))"
            )
        order = "dist nulls last" if proximity else "providers_here desc"

        sql = f"""
        with clin as (
            select d."NPI" npi,
                   upper(trim(d.adr_ln_1)) addr_norm,
                   left(CAST(d."ZIP Code" AS VARCHAR), 5) zip5,
                   any_value(d.org_pac_id) opac,
                   any_value(d."Facility Name") grp,
                   max(d.num_org_mem) gsize,
                   any_value(d.adr_ln_1) addr,
                   any_value(d."City/Town") city,
                   any_value(d."State") state,
                   any_value(CAST(d."Telephone Number" AS VARCHAR)) phone,
                   any_value(d.pri_spec) spec,
                   any_value(d."Provider First Name") fn,
                   any_value(d."Provider Last Name") ln
            from raw_dac_national d
            left join address_geocode ge
              on (upper(trim(d.adr_ln_1)) || '|' || left(CAST(d."ZIP Code" AS VARCHAR), 5)) = ge.addr_key
            where ({spec_pred}) and ({' and '.join(loc_clauses)})
            group by d."NPI", upper(trim(d.adr_ln_1)), left(CAST(d."ZIP Code" AS VARCHAR), 5)
        ),
        util as (
            select CAST("Rndrng_NPI" AS VARCHAR) npi, sum("Tot_Mdcr_Pymt_Amt") pay
            from raw_physician_by_provider
            where CAST("Rndrng_NPI" AS VARCHAR) in (select CAST(npi AS VARCHAR) from clin)
            group by 1
        ),
        rx as (
            select CAST("PRSCRBR_NPI" AS VARCHAR) npi, sum("Tot_Drug_Cst") cst
            from raw_part_d_by_provider
            where CAST("PRSCRBR_NPI" AS VARCHAR) in (select CAST(npi AS VARCHAR) from clin)
            group by 1
        ),
        sites as (
            select addr_norm || '|' || zip5 addr_key,
                   coalesce(cast(opac as varchar), 'SOLO') grp_key,
                   any_value(grp) practice_name,
                   any_value(cast(opac as varchar)) org_pac_id,
                   any_value(addr) address, any_value(city) city, any_value(state) state,
                   any_value(zip5) zip5, any_value(phone) phone, max(gsize) group_size_national,
                   count(distinct c.npi) providers_here,
                   list(distinct spec) specialties,
                   sum(u.pay) partb_payments,
                   sum(x.cst) partd_drug_cost,
                   min(case when opac is null
                            then trim(coalesce(fn, '') || ' ' || coalesce(ln, '')) end) solo_name
            from clin c
            left join util u on CAST(c.npi AS VARCHAR) = u.npi
            left join rx x on CAST(c.npi AS VARCHAR) = x.npi
            group by addr_norm || '|' || zip5, coalesce(cast(opac as varchar), 'SOLO')
        )
        select s.practice_name, s.org_pac_id, s.address, s.city, s.state, s.zip5, s.phone,
               ge.lat, ge.lng, s.group_size_national, s.providers_here, s.specialties,
               {dist_expr} dist,
               s.partb_payments, s.partd_drug_cost, s.solo_name
        from sites s
        left join address_geocode ge on s.addr_key = ge.addr_key
        order by {order}
        limit {limit}
        """

        conn = get_conn()
        rows = conn.execute(sql, params).fetchall()

        results = []
        for r in rows:
            providers_here = r[10]
            # Heuristic: a single org showing 20+ providers of ONE specialty at one street
            # is almost always a billing/admin address, not a clinic.
            billing = bool(r[1]) and providers_here >= 20
            results.append(
                PracticeResult(
                    practice_name=r[0], org_pac_id=r[1], address=r[2], city=r[3], state=r[4],
                    zip5=r[5], phone=r[6], lat=r[7], lng=r[8], group_size_national=r[9],
                    providers_here=providers_here, specialties=[s for s in (r[11] or []) if s],
                    distance_miles=round(r[12], 2) if r[12] is not None else None,
                    billing_artifact=billing,
                    partb_payments=r[13], partd_drug_cost=r[14],
                    solo_provider_name=(r[15] or "").strip() or None,
                )
            )

        return PracticeSearchResponse(
            specialty=specialty,
            matched_patterns=patterns,
            location=", ".join(loc_desc) or "anywhere",
            total=len(results),
            results=results,
            location_basis="cms_enrollment",
        )

    @router.get("/providers", response_model=ProviderRosterResponse)
    async def providers(
        street: str,
        zip: str,
        org_pac_id: Optional[str] = None,
        specialty: Optional[str] = None,
        limit: int = 200,
        location_basis: Literal["cms_enrollment", "nppes_primary"] = "cms_enrollment",
    ):
        """Individual-provider roster for one practice location (address × group)."""
        limit = max(1, min(limit, 500))

        if location_basis == "nppes_primary":
            params: list = [zip[:5], street]
            spec_pred = "1=1"
            if specialty:
                patterns = specialty_patterns(specialty)
                spec_pred = " OR ".join(['p."Rndrng_Prvdr_Type" ILIKE ?'] * len(patterns))
                params.extend(patterns)
            rows = get_conn().execute(
                f"""
                select cast(p."Rndrng_NPI" as varchar) npi,
                       n.first_name, n.last_name, n.credentials,
                       p."Rndrng_Prvdr_Type" specialty, n.practice_phone,
                       p."Tot_Srvcs", p."Tot_Benes", p."Tot_Mdcr_Pymt_Amt",
                       op.open_payments_total
                from raw_physician_by_provider p
                join raw_nppes n
                  on cast(p."Rndrng_NPI" as varchar) = cast(n.npi as varchar)
                left join (
                    select cast("Covered_Recipient_NPI" as varchar) npi,
                           sum("Total_Amount_of_Payment_USDollars") open_payments_total
                    from raw_open_payments_general
                    group by 1
                ) op on cast(p."Rndrng_NPI" as varchar) = op.npi
                where left(n.practice_zip, 5) = ?
                  and upper(trim(n.practice_address_1)) = upper(trim(?))
                  and n.deactivation_date is null
                  and ({spec_pred})
                order by p."Tot_Mdcr_Pymt_Amt" desc nulls last
                limit {limit}
                """,
                params,
            ).fetchall()
            people = [
                ProviderResult(
                    npi=row[0],
                    first_name=row[1],
                    last_name=row[2],
                    credentials=(row[3] or "").strip() or None,
                    specialty=row[4],
                    phone=row[5],
                    medicare_services=row[6],
                    medicare_beneficiaries=int(row[7]) if row[7] is not None else None,
                    medicare_payments=row[8],
                    open_payments_total=row[9],
                )
                for row in rows
            ]
            return ProviderRosterResponse(
                practice_name=None,
                org_pac_id=None,
                total=len(people),
                providers=people,
                location_basis="nppes_primary",
            )

        params: list = [zip[:5], street]

        org = (org_pac_id or "").strip()
        if org and org.upper() != "SOLO":
            org_pred = "CAST(d.org_pac_id AS VARCHAR) = ?"
            params.append(org)
        else:
            org_pred = "d.org_pac_id IS NULL"

        spec_pred = "1=1"
        if specialty:
            patterns = specialty_patterns(specialty)
            spec_pred = " OR ".join(["d.pri_spec ILIKE ?"] * len(patterns))
            params.extend(patterns)

        sql = f"""
        with roster as (
            select d."NPI" npi,
                   any_value(d."Provider First Name") first_name,
                   any_value(d."Provider Last Name") last_name,
                   any_value(d.{CRED_COL}) credentials,
                   any_value(d.pri_spec) specialty,
                   any_value(CAST(d."Telephone Number" AS VARCHAR)) phone,
                   any_value(d."Facility Name") practice_name
            from raw_dac_national d
            where left(CAST(d."ZIP Code" AS VARCHAR), 5) = ?
              and upper(trim(d.adr_ln_1)) = upper(trim(?))
              and {org_pred}
              and ({spec_pred})
            group by d."NPI"
        ),
        util as (
            select CAST("Rndrng_NPI" AS VARCHAR) npi, sum("Tot_Srvcs") srv,
                   sum("Tot_Benes") ben, sum("Tot_Mdcr_Pymt_Amt") pay
            from raw_physician_by_provider
            where CAST("Rndrng_NPI" AS VARCHAR) in (select CAST(npi AS VARCHAR) from roster)
            group by 1
        ),
        op as (
            select CAST("Covered_Recipient_NPI" AS VARCHAR) npi,
                   sum("Total_Amount_of_Payment_USDollars") optot
            from raw_open_payments_general
            where CAST("Covered_Recipient_NPI" AS VARCHAR) in (select CAST(npi AS VARCHAR) from roster)
            group by 1
        )
        select r.npi, r.first_name, r.last_name, r.credentials, r.specialty, r.phone,
               r.practice_name, u.srv, u.ben, u.pay, o.optot
        from roster r
        left join util u on CAST(r.npi AS VARCHAR) = u.npi
        left join op o on CAST(r.npi AS VARCHAR) = o.npi
        order by u.pay desc nulls last
        limit {limit}
        """

        conn = get_conn()
        rows = conn.execute(sql, params).fetchall()

        people = [
            ProviderResult(
                npi=str(r[0]),
                first_name=r[1],
                last_name=r[2],
                credentials=(r[3] or "").strip() or None,
                specialty=r[4],
                phone=r[5],
                medicare_services=r[7],
                medicare_beneficiaries=int(r[8]) if r[8] is not None else None,
                medicare_payments=r[9],
                open_payments_total=r[10],
            )
            for r in rows
        ]
        return ProviderRosterResponse(
            practice_name=rows[0][6] if rows else None,
            org_pac_id=org or None,
            total=len(people),
            providers=people,
            location_basis="cms_enrollment",
        )

    @router.get("/site-profile", response_model=SiteProfileResponse)
    async def site_profile(
        street: str,
        zip: str,
        org_pac_id: Optional[str] = None,
        specialty: Optional[str] = None,
        location_basis: Literal["cms_enrollment", "nppes_primary"] = "cms_enrollment",
    ):
        """Medicare deep-dive for one practice location (address × group).

        Rolls Part B utilization, Part D prescribing, and Open Payments up over
        the site's roster: totals plus top procedures (by estimated payment),
        top drugs (by drug cost), and top paying manufacturers.
        """
        params: list = [zip[:5], street]

        if location_basis == "nppes_primary":
            spec_pred = "1=1"
            if specialty:
                patterns = specialty_patterns(specialty)
                spec_pred = " OR ".join(['p."Rndrng_Prvdr_Type" ILIKE ?'] * len(patterns))
                params.extend(patterns)
            roster_rows = get_conn().execute(
                f"""
                select cast(p."Rndrng_NPI" as varchar) npi, null practice_name
                from raw_physician_by_provider p
                join raw_nppes n
                  on cast(p."Rndrng_NPI" as varchar) = cast(n.npi as varchar)
                where left(n.practice_zip, 5) = ?
                  and upper(trim(n.practice_address_1)) = upper(trim(?))
                  and n.deactivation_date is null
                  and ({spec_pred})
                group by 1
                """,
                params,
            ).fetchall()
            return _site_profile_for_roster(
                get_conn(),
                roster_rows,
                street=street,
                org_pac_id=None,
                location_basis="nppes_primary",
            )

        org = (org_pac_id or "").strip()
        if org and org.upper() != "SOLO":
            org_pred = "CAST(d.org_pac_id AS VARCHAR) = ?"
            params.append(org)
        else:
            org_pred = "d.org_pac_id IS NULL"

        spec_pred = "1=1"
        if specialty:
            patterns = specialty_patterns(specialty)
            spec_pred = " OR ".join(["d.pri_spec ILIKE ?"] * len(patterns))
            params.extend(patterns)

        conn = get_conn()
        roster_rows = conn.execute(
            f"""
            select CAST(d."NPI" AS VARCHAR) npi, any_value(d."Facility Name") practice_name
            from raw_dac_national d
            where left(CAST(d."ZIP Code" AS VARCHAR), 5) = ?
              and upper(trim(d.adr_ln_1)) = upper(trim(?))
              and {org_pred}
              and ({spec_pred})
            group by 1
            """,
            params,
        ).fetchall()
        return _site_profile_for_roster(
            conn,
            roster_rows,
            street=street,
            org_pac_id=org or None,
            location_basis="cms_enrollment",
        )

    return router
