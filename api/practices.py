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
from pydantic import BaseModel, Field

CONTRACT_VERSION = 2
MAX_RADIUS_MILES = 250.0
MAX_SPECIALTIES = 20
METRIC_SCOPE = "national_npi_totals"

LocationBasis = Literal["cms_enrollment", "nppes_primary"]
PopulationScope = Literal["selected_specialties", "all_specialties"]
OrganizationScope = Literal["cms_address_pac", "nppes_primary_address"]
SiteClassification = Literal["solo", "shared_unaffiliated", "organization_context"]

# Search term -> pri_spec ILIKE patterns (CMS uses granular specialty labels).
SPECIALTY_MAP: dict[str, list[str]] = {
    "primary care": [
        "%family%",
        "%internal medicine%",
        "%general practice%",
        "%geriatric%",
    ],
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
    "primary care physician": [
        "%family%",
        "%internal medicine%",
        "%general practice%",
        "%geriatric%",
    ],
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


class OrganizationContext(BaseModel):
    """A non-additive CMS enrollment context attached to an NPPES site."""

    org_pac_id: str
    practice_name: Optional[str] = None
    affiliated_provider_count: int
    primary_address_match_count: int
    group_size_national: Optional[int] = None


class PracticeResult(BaseModel):
    contract_version: int = CONTRACT_VERSION
    site_id: str
    requested_specialties: list[str] = Field(default_factory=list)
    population_scope: PopulationScope = "selected_specialties"
    metric_scope: Literal["national_npi_totals"] = METRIC_SCOPE
    organization_scope: OrganizationScope
    organization_contexts: list[OrganizationContext] = Field(default_factory=list)
    unaffiliated_provider_count: int = 0
    site_classification: SiteClassification
    roster_npi_count: int
    practice_name: Optional[str] = None
    org_pac_id: Optional[str] = None
    address: str
    city: str
    state: str
    zip5: str
    phone: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    providers_here: int
    group_size_national: Optional[int] = None
    specialties: list[str] = Field(default_factory=list)
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
    location_basis: LocationBasis = "cms_enrollment"


class PracticeSearchResponse(BaseModel):
    contract_version: int = CONTRACT_VERSION
    specialty: str
    requested_specialties: list[str]
    population_scope: Literal["selected_specialties"] = "selected_specialties"
    metric_scope: Literal["national_npi_totals"] = METRIC_SCOPE
    matched_patterns: list[str]
    location: str
    total: int
    returned_count: int
    truncated: bool
    results: list[PracticeResult]
    location_basis: LocationBasis = "cms_enrollment"


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


def parse_specialties(
    specialties: Optional[str], specialty: Optional[str], *, required: bool
) -> list[str]:
    """Return a bounded, case-insensitively deduplicated specialty selection."""
    raw = [item.strip() for item in (specialties or "").split(",") if item.strip()]
    if specialty and specialty.strip():
        raw.append(specialty.strip())

    selected: list[str] = []
    seen: set[str] = set()
    for item in raw:
        normalized = " ".join(item.split())
        key = normalized.casefold()
        if key not in seen:
            seen.add(key)
            selected.append(normalized)

    if required and not selected:
        raise ValueError("At least one specialty must be selected")
    if len(selected) > MAX_SPECIALTIES:
        raise ValueError(f"At most {MAX_SPECIALTIES} specialties may be selected")
    if any(len(item) > 100 for item in selected):
        raise ValueError("Specialty names must be at most 100 characters")
    if any("%" in item or "_" in item for item in selected):
        raise ValueError("Specialty names cannot contain wildcard characters")
    return selected


def patterns_for_specialties(specialties: list[str]) -> list[str]:
    """Resolve a specialty union without repeating equivalent SQL patterns."""
    patterns: list[str] = []
    seen: set[str] = set()
    for specialty in specialties:
        for pattern in specialty_patterns(specialty):
            key = pattern.casefold()
            if key not in seen:
                seen.add(key)
                patterns.append(pattern)
    return patterns


def validate_proximity(
    lat: Optional[float], lng: Optional[float], radius_miles: float
) -> bool:
    """Validate a paired coordinate and a finite, positive, bounded radius."""
    if not math.isfinite(radius_miles) or not 0 < radius_miles <= MAX_RADIUS_MILES:
        raise ValueError(
            f"radius_miles must be greater than 0 and at most {MAX_RADIUS_MILES:g}"
        )
    if (lat is None) != (lng is None):
        raise ValueError("lat and lng must be provided together")
    if lat is not None and (not math.isfinite(lat) or not -90 <= lat <= 90):
        raise ValueError("lat must be between -90 and 90")
    if lng is not None and (not math.isfinite(lng) or not -180 <= lng <= 180):
        raise ValueError("lng must be between -180 and 180")
    return lat is not None


def normalize_street(street: str) -> str:
    return " ".join(street.upper().split())


def site_identifier(
    location_basis: LocationBasis,
    street: str,
    zip5: str,
    org_pac_id: Optional[str] = None,
) -> str:
    """Build the stable identifier used by search, roster, and profile routes."""
    address_key = f"{normalize_street(street)}|{zip5[:5]}"
    if location_basis == "nppes_primary":
        return f"nppes_primary:{address_key}"
    org_key = (org_pac_id or "SOLO").strip() or "SOLO"
    return f"cms_enrollment:{address_key}|{org_key}"


def validate_site_identifier(
    supplied: Optional[str],
    *,
    location_basis: LocationBasis,
    street: str,
    zip5: str,
    org_pac_id: Optional[str],
) -> str:
    expected = site_identifier(location_basis, street, zip5, org_pac_id)
    if supplied is not None and supplied != expected:
        raise ValueError("site_id does not match the requested site")
    return expected


def classify_site(
    provider_count: int, unaffiliated_count: int, context_count: int
) -> SiteClassification:
    if context_count:
        return "organization_context"
    if provider_count == 1:
        return "solo"
    return "shared_unaffiliated"


def organization_contexts_for_npis(
    conn, npis: list[str], *, street: str, zip5: str
) -> tuple[list[OrganizationContext], set[str], dict[str, list[OrganizationContext]]]:
    """Resolve PAC affiliations without using them as NPPES-primary site grain."""
    unique_npis = list(dict.fromkeys(str(npi) for npi in npis))
    if not unique_npis:
        return [], set(), {}

    placeholders = ", ".join(["?"] * len(unique_npis))
    rows = conn.execute(
        f"""
        select cast(d."NPI" as varchar) npi,
               nullif(trim(cast(d.org_pac_id as varchar)), '') org_pac_id,
               min(nullif(trim(d."Facility Name"), '')) practice_name,
               max(d.num_org_mem) group_size_national,
               max(case when upper(trim(d.adr_ln_1)) = upper(trim(?))
                              and left(cast(d."ZIP Code" as varchar), 5) = ?
                        then 1 else 0 end) primary_address_match
        from raw_dac_national d
        where cast(d."NPI" as varchar) in ({placeholders})
          and nullif(trim(cast(d.org_pac_id as varchar)), '') is not null
        group by 1, 2
        """,
        [street, zip5[:5], *unique_npis],
    ).fetchall()

    context_members: dict[str, dict] = {}
    by_npi: dict[str, list[OrganizationContext]] = {npi: [] for npi in unique_npis}
    affiliated_npis: set[str] = set()
    for npi, org_pac_id, practice_name, group_size, address_match in rows:
        npi_value = str(npi)
        pac_value = str(org_pac_id)
        affiliated_npis.add(npi_value)
        member_context = OrganizationContext(
            org_pac_id=pac_value,
            practice_name=practice_name,
            affiliated_provider_count=1,
            primary_address_match_count=int(address_match or 0),
            group_size_national=int(group_size) if group_size is not None else None,
        )
        by_npi.setdefault(npi_value, []).append(member_context)

        aggregate = context_members.setdefault(
            pac_value,
            {
                "practice_names": [],
                "members": set(),
                "address_matches": set(),
                "group_size_national": None,
            },
        )
        if practice_name:
            aggregate["practice_names"].append(str(practice_name))
        aggregate["members"].add(npi_value)
        if address_match:
            aggregate["address_matches"].add(npi_value)
        if group_size is not None:
            current = aggregate["group_size_national"]
            aggregate["group_size_national"] = max(current or 0, int(group_size))

    contexts = [
        OrganizationContext(
            org_pac_id=pac,
            practice_name=min(values["practice_names"])
            if values["practice_names"]
            else None,
            affiliated_provider_count=len(values["members"]),
            primary_address_match_count=len(values["address_matches"]),
            group_size_national=values["group_size_national"],
        )
        for pac, values in context_members.items()
    ]

    def context_order(item: OrganizationContext) -> tuple[int, int, int, str]:
        return (
            -item.primary_address_match_count,
            -item.affiliated_provider_count,
            -(item.group_size_national or 0),
            item.org_pac_id,
        )

    contexts.sort(key=context_order)
    for member_contexts in by_npi.values():
        member_contexts.sort(key=context_order)
    return contexts, affiliated_npis, by_npi


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
    organization_contexts: list[OrganizationContext] = Field(default_factory=list)


class ProviderRosterResponse(BaseModel):
    contract_version: int = CONTRACT_VERSION
    site_id: str
    requested_specialties: list[str] = Field(default_factory=list)
    population_scope: PopulationScope
    metric_scope: Literal["national_npi_totals"] = METRIC_SCOPE
    practice_name: Optional[str] = None
    org_pac_id: Optional[str] = None
    total: int
    roster_npi_count: int
    returned_count: int
    truncated: bool
    providers: list[ProviderResult]
    location_basis: LocationBasis = "cms_enrollment"


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

    contract_version: int = CONTRACT_VERSION
    site_id: str
    requested_specialties: list[str] = Field(default_factory=list)
    population_scope: PopulationScope
    metric_scope: Literal["national_npi_totals"] = METRIC_SCOPE
    organization_scope: OrganizationScope
    organization_contexts: list[OrganizationContext] = Field(default_factory=list)
    unaffiliated_provider_count: int = 0
    site_classification: SiteClassification
    practice_name: Optional[str] = None
    org_pac_id: Optional[str] = None
    address: Optional[str] = None
    roster_size: int = 0
    roster_npi_count: int = 0
    partb_payments: Optional[float] = None
    partb_services: Optional[float] = None
    partb_beneficiaries: Optional[int] = None
    partd_drug_cost: Optional[float] = None
    partd_claims: Optional[int] = None
    open_payments_total: Optional[float] = None
    open_payments_count: int = 0
    open_payments_recipients: int = 0
    top_procedures: list[ProcedureRollup] = Field(default_factory=list)
    top_drugs: list[DrugRollup] = Field(default_factory=list)
    top_manufacturers: list[ManufacturerRollup] = Field(default_factory=list)
    location_basis: LocationBasis = "cms_enrollment"


def _site_profile_for_roster(
    conn,
    roster_rows: list[tuple],
    *,
    street: str,
    org_pac_id: Optional[str],
    location_basis: LocationBasis,
    site_id: str,
    requested_specialties: list[str],
    population_scope: PopulationScope,
    organization_scope: OrganizationScope,
    organization_contexts: list[OrganizationContext],
    unaffiliated_provider_count: int,
    site_classification: SiteClassification,
) -> SiteProfileResponse:
    """Roll national NPI metrics over an already-resolved location roster."""
    npis = list(dict.fromkeys(str(row[0]) for row in roster_rows))
    if not npis:
        raise HTTPException(status_code=404, detail="Practice site not found")
    practice_name = min(
        (str(row[1]) for row in roster_rows if row[1]),
        default=None,
    )
    placeholders = ", ".join(["?"] * len(npis))
    partb = conn.execute(
        f"""
        with per_npi as (
            select cast("Rndrng_NPI" as varchar) npi,
                   max("Tot_Mdcr_Pymt_Amt") payments,
                   max("Tot_Srvcs") services,
                   max("Tot_Benes") beneficiaries
            from raw_physician_by_provider
            where cast("Rndrng_NPI" as varchar) in ({placeholders})
            group by 1
        )
        select sum(payments), sum(services), sum(beneficiaries) from per_npi
        """,
        npis,
    ).fetchone()
    partd = conn.execute(
        f"""
        with per_npi as (
            select cast("PRSCRBR_NPI" as varchar) npi,
                   max("Tot_Drug_Cst") drug_cost,
                   max("Tot_Clms") claims
            from raw_part_d_by_provider
            where cast("PRSCRBR_NPI" as varchar) in ({placeholders})
            group by 1
        )
        select sum(drug_cost), sum(claims) from per_npi
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
        site_id=site_id,
        requested_specialties=requested_specialties,
        population_scope=population_scope,
        organization_scope=organization_scope,
        organization_contexts=organization_contexts,
        unaffiliated_provider_count=unaffiliated_provider_count,
        site_classification=site_classification,
        practice_name=practice_name,
        org_pac_id=org_pac_id,
        address=street,
        roster_size=len(npis),
        roster_npi_count=len(npis),
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
    # Each application instance must close over its own connection factory. A
    # module-global router retains the first factory when tests or workers build
    # more than one app in the same process.
    router = APIRouter(prefix="/practices", tags=["Medicare Practices"])
    specialty_catalog: tuple[str, ...] | None = None

    def get_specialty_catalog() -> tuple[str, ...]:
        """Load and cache the normalized specialty catalog for this router."""
        nonlocal specialty_catalog
        if specialty_catalog is not None:
            return specialty_catalog

        try:
            # core_providers.provider_type is the normalized, indexed specialty
            # column. Keep functions off the predicate so DuckDB can use that
            # index, and normalize the small grouped result in Python once.
            rows = (
                get_conn()
                .execute(
                    """
                    select provider_type
                    from core_providers
                    where provider_type is not null
                      and provider_type <> ''
                    group by provider_type
                    order by provider_type
                    """
                )
                .fetchall()
            )
        except Exception as exc:
            raise HTTPException(
                status_code=503,
                detail="CMS specialty catalog is unavailable",
            ) from exc

        normalized: dict[str, str] = {}
        for row in rows:
            value = str(row[0]).strip()
            if value:
                normalized.setdefault(value.casefold(), value)
        if not normalized:
            raise HTTPException(
                status_code=503,
                detail="CMS specialty catalog is unavailable",
            )

        specialty_catalog = tuple(sorted(normalized.values(), key=str.casefold))
        return specialty_catalog

    @router.get("/specialties", response_model=SpecialtyListResponse)
    async def specialties():
        """Return the cached normalized provider-specialty catalog."""
        return SpecialtyListResponse(specialties=list(get_specialty_catalog()))

    @router.get("/capabilities")
    async def capabilities():
        """Describe the bounded v2 practices contract for proxy readiness checks."""
        # The proxy treats this endpoint as a deploy/readiness gate. Do not
        # advertise v2 until its required specialty catalog can be queried.
        get_specialty_catalog()
        return {
            "contract_version": CONTRACT_VERSION,
            "capabilities": [
                "multi_zip",
                "nppes_primary",
                "exact_radius",
                "multi_specialty",
                "practice_specialties",
                "scoped_metrics",
            ],
        }

    @router.get("/search", response_model=PracticeSearchResponse)
    async def search(
        specialty: Optional[str] = None,
        specialties: Optional[str] = None,
        city: Optional[str] = None,
        state: Optional[str] = None,
        zip: Optional[str] = None,
        zips: Optional[str] = None,
        lat: Optional[float] = None,
        lng: Optional[float] = None,
        radius_miles: float = 10.0,
        limit: int = 50,
        location_basis: LocationBasis = "cms_enrollment",
    ):
        limit = max(1, min(limit, 200))
        try:
            requested_specialties = parse_specialties(
                specialties, specialty, required=True
            )
            selected_zips = parse_zip_codes(zips, zip)
            proximity = validate_proximity(lat, lng, radius_miles)
            if not any((city, state, selected_zips, proximity)):
                raise ValueError("Choose a city, state, ZIP boundary, or radius origin")
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        patterns = patterns_for_specialties(requested_specialties)

        loc_clauses: list[str] = []
        loc_params: list = []
        loc_desc: list[str] = []
        if proximity:
            assert lat is not None and lng is not None
            dlat = radius_miles / 69.0
            dlng = radius_miles / (69.0 * max(0.1, abs(math.cos(math.radians(lat)))))
            loc_clauses.append("ge.lat between ? and ? and ge.lng between ? and ?")
            loc_params.extend([lat - dlat, lat + dlat, lng - dlng, lng + dlng])
            loc_desc.append(f"{radius_miles}mi of ({lat:.4f},{lng:.4f})")
        if state:
            state_column = (
                "n.state" if location_basis == "nppes_primary" else 'd."State"'
            )
            loc_clauses.append(f"upper(trim({state_column})) = ?")
            loc_params.append(state.upper().strip())
            loc_desc.append(state.upper().strip())
        if city:
            city_column = (
                "n.city" if location_basis == "nppes_primary" else 'd."City/Town"'
            )
            loc_clauses.append(f"upper(trim({city_column})) = ?")
            loc_params.append(city.upper().strip())
            loc_desc.append(city.strip())
        if selected_zips:
            placeholders = ", ".join(["?"] * len(selected_zips))
            zip_column = (
                "n.zip5"
                if location_basis == "nppes_primary"
                else 'left(cast(d."ZIP Code" as varchar), 5)'
            )
            loc_clauses.append(f"{zip_column} in ({placeholders})")
            loc_params.extend(selected_zips)
            loc_desc.append(", ".join(selected_zips))
        distance_expression = "cast(null as double)"
        distance_params: list = []
        distance_filter = ""
        if proximity:
            assert lat is not None and lng is not None
            distance_expression = """
                case when ge.lat is null or ge.lng is null then null else
                    3959.0 * 2.0 * asin(sqrt(least(1.0, greatest(0.0,
                        pow(sin(radians(ge.lat - ?) / 2.0), 2)
                        + cos(radians(?)) * cos(radians(ge.lat))
                        * pow(sin(radians(ge.lng - ?) / 2.0), 2)
                    ))))
                end
            """
            distance_params = [lat, lat, lng, radius_miles]
            distance_filter = "where distance_miles <= ?"

        if location_basis == "nppes_primary":
            spec_pred = " OR ".join(['p."Rndrng_Prvdr_Type" ILIKE ?'] * len(patterns))
            sql = f"""
            with claims as (
                select cast(p."Rndrng_NPI" as varchar) npi,
                       min(trim(p."Rndrng_Prvdr_Type")) spec,
                       max(p."Tot_Mdcr_Pymt_Amt") partb_payments
                from raw_physician_by_provider p
                where ({spec_pred})
                group by 1
            ),
            geocodes as (
                select addr_key, min(lat) lat, min(lng) lng
                from address_geocode group by 1
            ),
            ranked_nppes as (
                select cast(n.npi as varchar) npi,
                       upper(trim(n.practice_address_1)) addr_norm,
                       left(n.practice_zip, 5) zip5,
                       n.practice_address_1 address,
                       n.practice_city city,
                       n.practice_state state,
                       n.practice_phone phone,
                       n.first_name first_name,
                       n.last_name last_name,
                       row_number() over (
                           partition by cast(n.npi as varchar)
                           order by upper(trim(n.practice_address_1)),
                                    left(n.practice_zip, 5),
                                    upper(trim(coalesce(n.practice_city, ''))),
                                    upper(trim(coalesce(n.practice_state, '')))
                       ) row_number
                from raw_nppes n
                where n.deactivation_date is null
                  and nullif(trim(n.practice_address_1), '') is not null
                  and nullif(trim(n.practice_city), '') is not null
                  and regexp_matches(upper(trim(n.practice_state)), '^[A-Z]{{2}}$')
                  and regexp_matches(left(n.practice_zip, 5), '^[0-9]{{5}}$')
                  and cast(n.npi as varchar) in (select npi from claims)
            ),
            primary_locations as (
                select * from ranked_nppes where row_number = 1
            ),
            attributed_base as (
                select c.npi, n.addr_norm, n.zip5, n.address, n.city, n.state,
                       n.phone, n.first_name, n.last_name, c.spec,
                       c.partb_payments
                from claims c
                join primary_locations n on c.npi = n.npi
                left join geocodes ge on n.addr_norm || '|' || n.zip5 = ge.addr_key
                where ({" and ".join(loc_clauses)})
            ),
            rx as (
                select cast("PRSCRBR_NPI" as varchar) npi,
                       max("Tot_Drug_Cst") partd_drug_cost
                from raw_part_d_by_provider
                where cast("PRSCRBR_NPI" as varchar) in (
                    select npi from attributed_base
                )
                group by 1
            ),
            attributed as (
                select a.*, rx.partd_drug_cost
                from attributed_base a left join rx on a.npi = rx.npi
            ),
            sites as (
                select addr_norm || '|' || zip5 addr_key,
                       min(address) address,
                       min(city) city,
                       min(state) state,
                       min(zip5) zip5,
                       min(phone) phone,
                       count(*) providers_here,
                       list(distinct spec order by spec) specialties,
                       sum(partb_payments) partb_payments,
                       sum(partd_drug_cost) partd_drug_cost,
                       min(trim(coalesce(first_name, '') || ' ' ||
                                coalesce(last_name, ''))) solo_name
                from attributed
                group by addr_norm || '|' || zip5
            ),
            org_memberships as (
                select a.addr_norm || '|' || a.zip5 addr_key,
                       a.npi,
                       nullif(trim(cast(d.org_pac_id as varchar)), '') org_pac_id,
                       min(nullif(trim(d."Facility Name"), '')) practice_name,
                       max(d.num_org_mem) group_size_national,
                       max(case when upper(trim(d.adr_ln_1)) = a.addr_norm
                                      and left(cast(d."ZIP Code" as varchar), 5) = a.zip5
                                then 1 else 0 end) primary_address_match
                from attributed a
                join raw_dac_national d
                  on a.npi = cast(d."NPI" as varchar)
                where nullif(trim(cast(d.org_pac_id as varchar)), '') is not null
                group by 1, 2, 3
            ),
            org_stats as (
                select addr_key, org_pac_id,
                       min(practice_name) practice_name,
                       count(distinct npi) affiliated_provider_count,
                       count(distinct case when primary_address_match = 1 then npi end)
                           primary_address_match_count,
                       max(group_size_national) group_size_national
                from org_memberships
                group by 1, 2
            ),
            org_rollup as (
                select s.addr_key,
                       list(struct_pack(
                           org_pac_id := s.org_pac_id,
                           practice_name := s.practice_name,
                           affiliated_provider_count := s.affiliated_provider_count,
                           primary_address_match_count := s.primary_address_match_count,
                           group_size_national := s.group_size_national
                       ) order by s.primary_address_match_count desc,
                                  s.affiliated_provider_count desc,
                                  s.group_size_national desc nulls last,
                                  s.org_pac_id) organization_contexts,
                       t.affiliated_npis
                from org_stats s
                join (
                    select addr_key, count(distinct npi) affiliated_npis
                    from org_memberships group by 1
                ) t on s.addr_key = t.addr_key
                group by s.addr_key, t.affiliated_npis
            ),
            located as (
                select s.*, ge.lat, ge.lng,
                       {distance_expression} distance_miles,
                       o.organization_contexts,
                       coalesce(o.affiliated_npis, 0) affiliated_npis
                from sites s
                left join geocodes ge on s.addr_key = ge.addr_key
                left join org_rollup o on s.addr_key = o.addr_key
            )
            select addr_key, address, city, state, zip5, phone, lat, lng,
                   providers_here, specialties, distance_miles, partb_payments,
                   partd_drug_cost, solo_name, organization_contexts, affiliated_npis,
                   count(*) over() total_count
            from located
            {distance_filter}
            order by {"distance_miles, addr_key" if proximity else "providers_here desc, addr_key"}
            limit {limit}
            """
            params = [*patterns, *loc_params, *distance_params]
            rows = get_conn().execute(sql, params).fetchall()
            results: list[PracticeResult] = []
            for row in rows:
                providers_here = int(row[8])
                contexts = [
                    OrganizationContext.model_validate(context)
                    for context in (row[14] or [])
                ]
                unaffiliated = providers_here - int(row[15] or 0)
                classification = classify_site(
                    providers_here, unaffiliated, len(contexts)
                )
                results.append(
                    PracticeResult(
                        site_id=site_identifier(
                            "nppes_primary", str(row[1]), str(row[4])
                        ),
                        requested_specialties=requested_specialties,
                        organization_scope="nppes_primary_address",
                        organization_contexts=contexts,
                        unaffiliated_provider_count=unaffiliated,
                        site_classification=classification,
                        roster_npi_count=providers_here,
                        address=row[1],
                        city=row[2],
                        state=row[3],
                        zip5=row[4],
                        phone=row[5],
                        lat=row[6],
                        lng=row[7],
                        providers_here=providers_here,
                        specialties=[value for value in (row[9] or []) if value],
                        distance_miles=(
                            round(row[10], 2) if row[10] is not None else None
                        ),
                        partb_payments=row[11],
                        partd_drug_cost=row[12],
                        solo_provider_name=(
                            ((row[13] or "").strip() or None)
                            if classification == "solo"
                            else None
                        ),
                        location_basis="nppes_primary",
                    )
                )
            total_available = int(rows[0][16]) if rows else 0
            return PracticeSearchResponse(
                specialty=requested_specialties[0],
                requested_specialties=requested_specialties,
                matched_patterns=patterns,
                location=", ".join(loc_desc) or "anywhere",
                total=total_available,
                returned_count=len(results),
                truncated=len(results) < total_available,
                results=results,
                location_basis="nppes_primary",
            )

        spec_pred = " OR ".join(["d.pri_spec ILIKE ?"] * len(patterns))
        sql = f"""
        with geocodes as (
            select addr_key, min(lat) lat, min(lng) lng
            from address_geocode group by 1
        ),
        clinicians as (
            select cast(d."NPI" as varchar) npi,
                   upper(trim(d.adr_ln_1)) addr_norm,
                   left(cast(d."ZIP Code" as varchar), 5) zip5,
                   nullif(trim(cast(d.org_pac_id as varchar)), '') org_pac_id,
                   min(nullif(trim(d."Facility Name"), '')) practice_name,
                   max(d.num_org_mem) group_size_national,
                   min(d.adr_ln_1) address,
                   min(d."City/Town") city,
                   min(d."State") state,
                   min(cast(d."Telephone Number" as varchar)) phone,
                   min(trim(d.pri_spec)) spec,
                   min(d."Provider First Name") first_name,
                   min(d."Provider Last Name") last_name
            from raw_dac_national d
            left join geocodes ge
              on upper(trim(d.adr_ln_1)) || '|' ||
                 left(cast(d."ZIP Code" as varchar), 5) = ge.addr_key
            where ({spec_pred})
              and nullif(trim(d.adr_ln_1), '') is not null
              and nullif(trim(d."City/Town"), '') is not null
              and regexp_matches(upper(trim(d."State")), '^[A-Z]{{2}}$')
              and regexp_matches(left(cast(d."ZIP Code" as varchar), 5), '^[0-9]{{5}}$')
              and ({" and ".join(loc_clauses)})
            group by 1, 2, 3, 4
        ),
        utilization as (
            select cast("Rndrng_NPI" as varchar) npi,
                   max("Tot_Mdcr_Pymt_Amt") payments
            from raw_physician_by_provider
            where cast("Rndrng_NPI" as varchar) in (select npi from clinicians)
            group by 1
        ),
        rx as (
            select cast("PRSCRBR_NPI" as varchar) npi,
                   max("Tot_Drug_Cst") drug_cost
            from raw_part_d_by_provider
            where cast("PRSCRBR_NPI" as varchar) in (select npi from clinicians)
            group by 1
        ),
        sites as (
            select addr_norm || '|' || zip5 addr_key,
                   coalesce(org_pac_id, 'SOLO') group_key,
                   min(practice_name) practice_name,
                   min(org_pac_id) org_pac_id,
                   min(address) address,
                   min(city) city,
                   min(state) state,
                   min(zip5) zip5,
                   min(phone) phone,
                   max(group_size_national) group_size_national,
                   count(*) providers_here,
                   list(distinct spec order by spec) specialties,
                   sum(u.payments) partb_payments,
                   sum(rx.drug_cost) partd_drug_cost,
                   min(case when org_pac_id is null then
                       trim(coalesce(first_name, '') || ' ' || coalesce(last_name, ''))
                   end) solo_name
            from clinicians c
            left join utilization u on c.npi = u.npi
            left join rx on c.npi = rx.npi
            group by addr_norm || '|' || zip5, coalesce(org_pac_id, 'SOLO')
        ),
        located as (
            select s.*, ge.lat, ge.lng, {distance_expression} distance_miles
            from sites s left join geocodes ge on s.addr_key = ge.addr_key
        )
        select addr_key, group_key, practice_name, org_pac_id, address, city,
               state, zip5, phone, group_size_national, providers_here,
               specialties, lat, lng, distance_miles, partb_payments,
               partd_drug_cost, solo_name, count(*) over() total_count
        from located
        {distance_filter}
        order by {"distance_miles, addr_key, group_key" if proximity else "providers_here desc, addr_key, group_key"}
        limit {limit}
        """
        params = [*patterns, *loc_params, *distance_params]
        rows = get_conn().execute(sql, params).fetchall()
        results = []
        for row in rows:
            provider_count = int(row[10])
            org_pac_id = row[3]
            unaffiliated = 0 if org_pac_id else provider_count
            contexts = (
                [
                    OrganizationContext(
                        org_pac_id=str(org_pac_id),
                        practice_name=row[2],
                        affiliated_provider_count=provider_count,
                        primary_address_match_count=provider_count,
                        group_size_national=(
                            int(row[9]) if row[9] is not None else None
                        ),
                    )
                ]
                if org_pac_id
                else []
            )
            classification = classify_site(provider_count, unaffiliated, len(contexts))
            results.append(
                PracticeResult(
                    site_id=site_identifier(
                        "cms_enrollment", str(row[4]), str(row[7]), org_pac_id
                    ),
                    requested_specialties=requested_specialties,
                    organization_scope="cms_address_pac",
                    organization_contexts=contexts,
                    unaffiliated_provider_count=unaffiliated,
                    site_classification=classification,
                    roster_npi_count=provider_count,
                    practice_name=row[2],
                    org_pac_id=org_pac_id,
                    address=row[4],
                    city=row[5],
                    state=row[6],
                    zip5=row[7],
                    phone=row[8],
                    group_size_national=row[9],
                    providers_here=provider_count,
                    specialties=[value for value in (row[11] or []) if value],
                    lat=row[12],
                    lng=row[13],
                    distance_miles=(round(row[14], 2) if row[14] is not None else None),
                    billing_artifact=bool(org_pac_id) and provider_count >= 20,
                    partb_payments=row[15],
                    partd_drug_cost=row[16],
                    solo_provider_name=(
                        ((row[17] or "").strip() or None)
                        if classification == "solo"
                        else None
                    ),
                    location_basis="cms_enrollment",
                )
            )
        total_available = int(rows[0][18]) if rows else 0
        return PracticeSearchResponse(
            specialty=requested_specialties[0],
            requested_specialties=requested_specialties,
            matched_patterns=patterns,
            location=", ".join(loc_desc) or "anywhere",
            total=total_available,
            returned_count=len(results),
            truncated=len(results) < total_available,
            results=results,
            location_basis="cms_enrollment",
        )

    @router.get("/providers", response_model=ProviderRosterResponse)
    async def providers(
        street: str,
        zip: str,
        org_pac_id: Optional[str] = None,
        specialty: Optional[str] = None,
        specialties: Optional[str] = None,
        site_id: Optional[str] = None,
        limit: int = 200,
        location_basis: LocationBasis = "cms_enrollment",
    ):
        """Individual-provider roster for one practice location (address × group)."""
        limit = max(1, min(limit, 500))
        zip5 = zip.strip()
        org = (org_pac_id or "").strip()
        org_value = org if org and org.upper() != "SOLO" else None
        try:
            if not ZIP5_RE.fullmatch(zip5):
                raise ValueError("ZIP codes must be five digits")
            requested_specialties = parse_specialties(
                specialties, specialty, required=False
            )
            resolved_site_id = validate_site_identifier(
                site_id,
                location_basis=location_basis,
                street=street,
                zip5=zip5,
                org_pac_id=org_value,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        population_scope: PopulationScope = (
            "selected_specialties" if requested_specialties else "all_specialties"
        )
        patterns = patterns_for_specialties(requested_specialties)

        if location_basis == "nppes_primary":
            params: list = []
            spec_pred = "1=1"
            if patterns:
                spec_pred = " OR ".join(
                    ['p."Rndrng_Prvdr_Type" ILIKE ?'] * len(patterns)
                )
                params.extend(patterns)
            params.extend([zip5, street])
            conn = get_conn()
            rows = conn.execute(
                f"""
                with claims as (
                    select cast(p."Rndrng_NPI" as varchar) npi,
                           min(trim(p."Rndrng_Prvdr_Type")) specialty,
                           max(p."Tot_Srvcs") services,
                           max(p."Tot_Benes") beneficiaries,
                           max(p."Tot_Mdcr_Pymt_Amt") payments
                    from raw_physician_by_provider p
                    where ({spec_pred})
                    group by 1
                ),
                ranked_nppes as (
                    select cast(n.npi as varchar) npi,
                           n.first_name, n.last_name, n.credentials,
                           n.practice_phone, n.practice_address_1,
                           left(n.practice_zip, 5) zip5,
                           row_number() over (
                               partition by cast(n.npi as varchar)
                               order by upper(trim(n.practice_address_1)),
                                        left(n.practice_zip, 5),
                                        upper(trim(coalesce(n.practice_city, ''))),
                                        upper(trim(coalesce(n.practice_state, '')))
                           ) row_number
                    from raw_nppes n
                    where n.deactivation_date is null
                      and nullif(trim(n.practice_address_1), '') is not null
                      and nullif(trim(n.practice_city), '') is not null
                      and regexp_matches(upper(trim(n.practice_state)), '^[A-Z]{{2}}$')
                      and regexp_matches(left(n.practice_zip, 5), '^[0-9]{{5}}$')
                      and cast(n.npi as varchar) in (select npi from claims)
                ),
                primary_locations as (
                    select * from ranked_nppes where row_number = 1
                ),
                roster_base as (
                    select c.npi, n.first_name, n.last_name, n.credentials,
                           c.specialty, n.practice_phone, c.services,
                           c.beneficiaries, c.payments
                    from claims c
                    join primary_locations n on c.npi = n.npi
                    where n.zip5 = ?
                      and upper(trim(n.practice_address_1)) = upper(trim(?))
                ),
                open_payments as (
                    select cast("Covered_Recipient_NPI" as varchar) npi,
                           sum("Total_Amount_of_Payment_USDollars") open_payments_total
                    from raw_open_payments_general
                    where cast("Covered_Recipient_NPI" as varchar) in (
                        select npi from roster_base
                    )
                    group by 1
                ),
                roster as (
                    select r.*, op.open_payments_total
                    from roster_base r left join open_payments op on r.npi = op.npi
                )
                select *, count(*) over() total_count
                from roster
                order by payments desc nulls last, npi
                limit {limit}
                """,
                params,
            ).fetchall()
            context_rows = organization_contexts_for_npis(
                conn, [str(row[0]) for row in rows], street=street, zip5=zip5
            )[2]
            people = [
                ProviderResult(
                    npi=str(row[0]),
                    first_name=row[1],
                    last_name=row[2],
                    credentials=(row[3] or "").strip() or None,
                    specialty=row[4],
                    phone=row[5],
                    medicare_services=row[6],
                    medicare_beneficiaries=int(row[7]) if row[7] is not None else None,
                    medicare_payments=row[8],
                    open_payments_total=row[9],
                    organization_contexts=context_rows.get(str(row[0]), []),
                )
                for row in rows
            ]
            roster_npi_count = int(rows[0][10]) if rows else 0
            return ProviderRosterResponse(
                site_id=resolved_site_id,
                requested_specialties=requested_specialties,
                population_scope=population_scope,
                practice_name=None,
                org_pac_id=None,
                total=roster_npi_count,
                roster_npi_count=roster_npi_count,
                returned_count=len(people),
                truncated=len(people) < roster_npi_count,
                providers=people,
                location_basis="nppes_primary",
            )

        params = [zip5, street]
        if org_value:
            org_pred = "nullif(trim(CAST(d.org_pac_id AS VARCHAR)), '') = ?"
            params.append(org_value)
        else:
            org_pred = "nullif(trim(CAST(d.org_pac_id AS VARCHAR)), '') IS NULL"

        spec_pred = "1=1"
        if patterns:
            spec_pred = " OR ".join(["d.pri_spec ILIKE ?"] * len(patterns))
            params.extend(patterns)

        sql = f"""
        with roster as (
            select d."NPI" npi,
                   min(d."Provider First Name") first_name,
                   min(d."Provider Last Name") last_name,
                   min(d.{CRED_COL}) credentials,
                   min(d.pri_spec) specialty,
                   min(CAST(d."Telephone Number" AS VARCHAR)) phone,
                   min(nullif(trim(d."Facility Name"), '')) practice_name,
                   max(d.num_org_mem) group_size_national
            from raw_dac_national d
            where left(CAST(d."ZIP Code" AS VARCHAR), 5) = ?
              and upper(trim(d.adr_ln_1)) = upper(trim(?))
              and {org_pred}
              and ({spec_pred})
            group by d."NPI"
        ),
        util as (
            select CAST("Rndrng_NPI" AS VARCHAR) npi, max("Tot_Srvcs") srv,
                   max("Tot_Benes") ben, max("Tot_Mdcr_Pymt_Amt") pay
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
               r.practice_name, r.group_size_national, u.srv, u.ben, u.pay, o.optot,
               count(*) over() total_count
        from roster r
        left join util u on CAST(r.npi AS VARCHAR) = u.npi
        left join op o on CAST(r.npi AS VARCHAR) = o.npi
        order by u.pay desc nulls last, cast(r.npi as varchar)
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
                medicare_services=r[8],
                medicare_beneficiaries=int(r[9]) if r[9] is not None else None,
                medicare_payments=r[10],
                open_payments_total=r[11],
                organization_contexts=(
                    [
                        OrganizationContext(
                            org_pac_id=org_value,
                            practice_name=r[6],
                            affiliated_provider_count=1,
                            primary_address_match_count=1,
                            group_size_national=(
                                int(r[7]) if r[7] is not None else None
                            ),
                        )
                    ]
                    if org_value
                    else []
                ),
            )
            for r in rows
        ]
        roster_npi_count = int(rows[0][12]) if rows else 0
        practice_name = min(
            (str(row[6]) for row in rows if row[6]),
            default=None,
        )
        return ProviderRosterResponse(
            site_id=resolved_site_id,
            requested_specialties=requested_specialties,
            population_scope=population_scope,
            practice_name=practice_name,
            org_pac_id=org_value,
            total=roster_npi_count,
            roster_npi_count=roster_npi_count,
            returned_count=len(people),
            truncated=len(people) < roster_npi_count,
            providers=people,
            location_basis="cms_enrollment",
        )

    @router.get("/site-profile", response_model=SiteProfileResponse)
    async def site_profile(
        street: str,
        zip: str,
        org_pac_id: Optional[str] = None,
        specialty: Optional[str] = None,
        specialties: Optional[str] = None,
        site_id: Optional[str] = None,
        location_basis: LocationBasis = "cms_enrollment",
    ):
        """Medicare deep-dive for one practice location (address × group).

        Rolls Part B utilization, Part D prescribing, and Open Payments up over
        the site's roster: totals plus top procedures (by estimated payment),
        top drugs (by drug cost), and top paying manufacturers.
        """
        zip5 = zip.strip()
        org = (org_pac_id or "").strip()
        org_value = org if org and org.upper() != "SOLO" else None
        try:
            if not ZIP5_RE.fullmatch(zip5):
                raise ValueError("ZIP codes must be five digits")
            requested_specialties = parse_specialties(
                specialties, specialty, required=False
            )
            resolved_site_id = validate_site_identifier(
                site_id,
                location_basis=location_basis,
                street=street,
                zip5=zip5,
                org_pac_id=org_value,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        population_scope: PopulationScope = (
            "selected_specialties" if requested_specialties else "all_specialties"
        )
        patterns = patterns_for_specialties(requested_specialties)
        params: list = []

        if location_basis == "nppes_primary":
            spec_pred = "1=1"
            if patterns:
                spec_pred = " OR ".join(
                    ['p."Rndrng_Prvdr_Type" ILIKE ?'] * len(patterns)
                )
                params.extend(patterns)
            params.extend([zip5, street])
            conn = get_conn()
            roster_rows = conn.execute(
                f"""
                with claims as (
                    select cast(p."Rndrng_NPI" as varchar) npi
                    from raw_physician_by_provider p
                    where ({spec_pred})
                    group by 1
                ),
                ranked_nppes as (
                    select cast(n.npi as varchar) npi, n.practice_address_1,
                           left(n.practice_zip, 5) zip5,
                           row_number() over (
                               partition by cast(n.npi as varchar)
                               order by upper(trim(n.practice_address_1)),
                                        left(n.practice_zip, 5),
                                        upper(trim(coalesce(n.practice_city, ''))),
                                        upper(trim(coalesce(n.practice_state, '')))
                           ) row_number
                    from raw_nppes n
                    where n.deactivation_date is null
                      and nullif(trim(n.practice_address_1), '') is not null
                      and nullif(trim(n.practice_city), '') is not null
                      and regexp_matches(upper(trim(n.practice_state)), '^[A-Z]{{2}}$')
                      and regexp_matches(left(n.practice_zip, 5), '^[0-9]{{5}}$')
                      and cast(n.npi as varchar) in (select npi from claims)
                )
                select c.npi, null practice_name
                from claims c
                join ranked_nppes n on c.npi = n.npi and n.row_number = 1
                where n.zip5 = ?
                  and upper(trim(n.practice_address_1)) = upper(trim(?))
                order by c.npi
                """,
                params,
            ).fetchall()
            contexts, affiliated_npis, _ = organization_contexts_for_npis(
                conn,
                [str(row[0]) for row in roster_rows],
                street=street,
                zip5=zip5,
            )
            unaffiliated_count = len(roster_rows) - len(affiliated_npis)
            return _site_profile_for_roster(
                conn,
                roster_rows,
                street=street,
                org_pac_id=None,
                location_basis="nppes_primary",
                site_id=resolved_site_id,
                requested_specialties=requested_specialties,
                population_scope=population_scope,
                organization_scope="nppes_primary_address",
                organization_contexts=contexts,
                unaffiliated_provider_count=unaffiliated_count,
                site_classification=classify_site(
                    len(roster_rows), unaffiliated_count, len(contexts)
                ),
            )

        params = [zip5, street]
        if org_value:
            org_pred = "nullif(trim(CAST(d.org_pac_id AS VARCHAR)), '') = ?"
            params.append(org_value)
        else:
            org_pred = "nullif(trim(CAST(d.org_pac_id AS VARCHAR)), '') IS NULL"

        spec_pred = "1=1"
        if patterns:
            spec_pred = " OR ".join(["d.pri_spec ILIKE ?"] * len(patterns))
            params.extend(patterns)

        conn = get_conn()
        roster_rows = conn.execute(
            f"""
            select CAST(d."NPI" AS VARCHAR) npi,
                   min(nullif(trim(d."Facility Name"), '')) practice_name,
                   max(d.num_org_mem) group_size_national
            from raw_dac_national d
            where left(CAST(d."ZIP Code" AS VARCHAR), 5) = ?
              and upper(trim(d.adr_ln_1)) = upper(trim(?))
              and {org_pred}
              and ({spec_pred})
            group by 1
            order by 1
            """,
            params,
        ).fetchall()
        provider_count = len(roster_rows)
        contexts = (
            [
                OrganizationContext(
                    org_pac_id=org_value,
                    practice_name=(roster_rows[0][1] if roster_rows else None),
                    affiliated_provider_count=provider_count,
                    primary_address_match_count=provider_count,
                    group_size_national=(
                        max(int(row[2]) for row in roster_rows if row[2] is not None)
                        if any(row[2] is not None for row in roster_rows)
                        else None
                    ),
                )
            ]
            if org_value
            else []
        )
        unaffiliated_count = 0 if org_value else provider_count
        return _site_profile_for_roster(
            conn,
            roster_rows,
            street=street,
            org_pac_id=org_value,
            location_basis="cms_enrollment",
            site_id=resolved_site_id,
            requested_specialties=requested_specialties,
            population_scope=population_scope,
            organization_scope="cms_address_pac",
            organization_contexts=contexts,
            unaffiliated_provider_count=unaffiliated_count,
            site_classification=classify_site(
                provider_count, unaffiliated_count, len(contexts)
            ),
        )

    return router
