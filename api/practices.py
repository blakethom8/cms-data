"""
Medicare practice/site search.

Rolls up the CMS Doctors & Clinicians table (`raw_dac_national`) into practice
*locations* — (building address × group) — for a given specialty + location.
Medicare-only (no Google Places coupling). Uses the `address_geocode` table for
lat/lng + proximity.

Each result is one practice at one site: the group's legal name, address, geo,
phone, the count of matching-specialty clinicians there, the group's national
size, and the specialty mix. A heuristic `billing_artifact` flag marks
admin/billing addresses (one group, implausibly many providers for a single
street) so the UI can de-emphasise them.
"""
import math
from typing import Optional

from fastapi import APIRouter
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
}


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


class PracticeSearchResponse(BaseModel):
    specialty: str
    matched_patterns: list[str]
    location: str
    total: int
    results: list[PracticeResult]


def get_practices_router(get_conn):
    @router.get("/search", response_model=PracticeSearchResponse)
    async def search(
        specialty: str,
        city: Optional[str] = None,
        state: Optional[str] = None,
        zip: Optional[str] = None,
        lat: Optional[float] = None,
        lng: Optional[float] = None,
        radius_miles: float = 10.0,
        limit: int = 50,
    ):
        limit = max(1, min(limit, 200))
        term = specialty.lower().strip()
        patterns = SPECIALTY_MAP.get(term, [f"%{term}%"])
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
        if zip:
            z = zip.strip()
            n = min(len(z), 5)
            loc_clauses.append(f'left(CAST(d."ZIP Code" AS VARCHAR), {n}) = ?')
            params.append(z[:n])
            loc_desc.append(z)
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
                   any_value(d.pri_spec) spec
            from raw_dac_national d
            left join address_geocode ge
              on (upper(trim(d.adr_ln_1)) || '|' || left(CAST(d."ZIP Code" AS VARCHAR), 5)) = ge.addr_key
            where ({spec_pred}) and ({' and '.join(loc_clauses)})
            group by d."NPI", upper(trim(d.adr_ln_1)), left(CAST(d."ZIP Code" AS VARCHAR), 5)
        ),
        sites as (
            select addr_norm || '|' || zip5 addr_key,
                   coalesce(cast(opac as varchar), 'SOLO') grp_key,
                   any_value(grp) practice_name,
                   any_value(cast(opac as varchar)) org_pac_id,
                   any_value(addr) address, any_value(city) city, any_value(state) state,
                   any_value(zip5) zip5, any_value(phone) phone, max(gsize) group_size_national,
                   count(distinct npi) providers_here,
                   list(distinct spec) specialties
            from clin
            group by addr_norm || '|' || zip5, coalesce(cast(opac as varchar), 'SOLO')
        )
        select s.practice_name, s.org_pac_id, s.address, s.city, s.state, s.zip5, s.phone,
               ge.lat, ge.lng, s.group_size_national, s.providers_here, s.specialties,
               {dist_expr} dist
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
                )
            )

        return PracticeSearchResponse(
            specialty=specialty,
            matched_patterns=patterns,
            location=", ".join(loc_desc) or "anywhere",
            total=len(results),
            results=results,
        )

    return router
