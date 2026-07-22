"""
Market snapshot: one bounded payload with organizations, sites, and providers.

Serves the linked-views CMS dashboard. For a specialty union inside one
geography (city/state, ZIP boundary, or radius), a single scan of
``raw_dac_national`` produces membership rows at the (NPI x org_pac_id x door)
grain; Python then assembles three cross-linked blocks:

- ``organizations[]`` — GROUP BY org_pac_id with distinct-NPI provider counts,
  door counts, and dollar rollups de-duplicated per NPI.
- ``sites[]``        — CMS enrollment doors (street|zip5 x org), same grain and
  ``site_id`` format as ``/practices/search`` with
  ``location_basis=cms_enrollment`` so roster/site-profile drill-downs work.
- ``providers[]``    — distinct NPIs with every in-boundary door
  (``site_ids[]``), org affiliations, and national Medicare totals.

Dollar semantics: Part B / Part D / Open Payments figures are national per-NPI
annual totals ("attributed"), never door-level revenue. Rollups sum each NPI
exactly once per org/site; an NPI enrolled at several doors still contributes
its full national total to each door's roster figure (disclosed roster billing
power), while org totals and grand totals de-duplicate by NPI.

The NPPES primary practice address is matched against enrollment doors by
normalized street|zip5; when the strings line up, that door is flagged and
listed first. Formatting drift between NPPES and DAC means the flag is
"where matchable", not guaranteed.
"""

import math
from typing import Literal, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from practices import (
    CRED_COL,
    MAX_RADIUS_MILES,
    METRIC_SCOPE,
    SiteClassification,
    classify_site,
    parse_specialties,
    parse_zip_codes,
    patterns_for_specialties,
    site_identifier,
    validate_proximity,
)

SNAPSHOT_CONTRACT_VERSION = 1

# One membership row is (NPI x org x door). 8k comfortably covers a large
# metro speciality union (LA cardiology is ~550) while bounding payload size.
MAX_MEMBERSHIP_ROWS = 8000

# Mirrors /practices/search: an org enrolling 20+ matching clinicians at one
# door is usually an administrative/billing address, not a physical roster.
BILLING_ARTIFACT_THRESHOLD = 20


class SnapshotOrganization(BaseModel):
    org_pac_id: str
    name: Optional[str] = None
    provider_count: int
    site_count: int
    group_size_national: Optional[int] = None
    partb_payments: Optional[float] = None
    partd_drug_cost: Optional[float] = None
    open_payments_total: Optional[float] = None


class SnapshotIndependent(BaseModel):
    """Rollup of matched clinicians with no org_pac_id (independent/solo)."""

    provider_count: int
    site_count: int
    partb_payments: Optional[float] = None
    partd_drug_cost: Optional[float] = None
    open_payments_total: Optional[float] = None


class SnapshotSite(BaseModel):
    site_id: str
    org_pac_id: Optional[str] = None
    practice_name: Optional[str] = None
    address: str
    city: str
    state: str
    zip5: str
    phone: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    providers_here: int
    group_size_national: Optional[int] = None
    site_classification: SiteClassification
    billing_artifact: bool = False
    solo_provider_name: Optional[str] = None
    distance_miles: Optional[float] = None
    # Roster billing power: national per-NPI totals summed over this door's
    # matching roster — not door-level billing (CMS publishes no site grain).
    partb_payments: Optional[float] = None
    partd_drug_cost: Optional[float] = None


class SnapshotProvider(BaseModel):
    npi: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    credentials: Optional[str] = None
    specialty: Optional[str] = None
    org_pac_ids: list[str] = Field(default_factory=list)
    # Every in-boundary enrollment door; the NPPES-primary-matching door (if
    # the address strings reconcile) is listed first.
    site_ids: list[str] = Field(default_factory=list)
    door_count: int = 0
    has_nppes_primary_door: bool = False
    partb_payments: Optional[float] = None
    partb_services: Optional[float] = None
    partb_beneficiaries: Optional[float] = None
    partd_drug_cost: Optional[float] = None
    open_payments_total: Optional[float] = None


class OrganizationSearchResult(BaseModel):
    org_pac_id: str
    name: Optional[str] = None
    provider_count: int
    site_count: int
    group_size_national: Optional[int] = None


class OrganizationSearchResponse(BaseModel):
    query: str
    results: list[OrganizationSearchResult]


class SnapshotTotals(BaseModel):
    organizations: int
    sites: int
    providers: int
    partb_payments: Optional[float] = None
    partd_drug_cost: Optional[float] = None


class MarketSnapshotResponse(BaseModel):
    contract_version: int = SNAPSHOT_CONTRACT_VERSION
    requested_specialties: list[str]
    matched_patterns: list[str]
    location: str
    location_basis: Literal["cms_enrollment"] = "cms_enrollment"
    population_scope: Literal["selected_specialties"] = "selected_specialties"
    metric_scope: Literal["national_npi_totals"] = METRIC_SCOPE
    # Present when the population was anchored on one CMS enrollment group
    # (the organization lens) rather than specialty × geography.
    anchor_org_pac_id: Optional[str] = None
    totals: SnapshotTotals
    organizations: list[SnapshotOrganization]
    independent: Optional[SnapshotIndependent] = None
    sites: list[SnapshotSite]
    providers: list[SnapshotProvider]


def _sum_or_none(values) -> Optional[float]:
    present = [v for v in values if v is not None]
    if not present:
        return None
    return round(float(sum(present)), 2)


def get_market_snapshot_router(get_conn):
    # Closure-per-app for the same reason as get_practices_router: tests build
    # several apps in one process and each must keep its own connection factory.
    router = APIRouter(prefix="/practices", tags=["Medicare Practices"])

    @router.get("/market-snapshot", response_model=MarketSnapshotResponse)
    async def market_snapshot(
        specialty: Optional[str] = None,
        specialties: Optional[str] = None,
        city: Optional[str] = None,
        state: Optional[str] = None,
        zip: Optional[str] = None,
        zips: Optional[str] = None,
        lat: Optional[float] = None,
        lng: Optional[float] = None,
        radius_miles: float = 10.0,
        org_pac_id: Optional[str] = None,
    ):
        # Two population anchors: specialty × geography (the territory lens),
        # or a CMS enrollment group (the organization lens). With an org
        # anchor, specialties become an optional filter and geography an
        # optional narrowing — omit both to see everywhere the org operates.
        anchor_org = (org_pac_id or "").strip() or None
        try:
            requested_specialties = parse_specialties(
                specialties, specialty, required=anchor_org is None
            )
            selected_zips = parse_zip_codes(zips, zip)
            proximity = validate_proximity(lat, lng, radius_miles)
            if anchor_org is None and not any((city, state, selected_zips, proximity)):
                raise ValueError("Choose a city, state, ZIP boundary, or radius origin")
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        patterns = patterns_for_specialties(requested_specialties)
        spec_pred = " OR ".join(["d.pri_spec ILIKE ?"] * len(patterns))

        loc_clauses: list[str] = []
        loc_params: list = []
        loc_desc: list[str] = []
        if state:
            loc_clauses.append('upper(trim(d."State")) = ?')
            loc_params.append(state.upper().strip())
            loc_desc.append(state.upper().strip())
        if city:
            loc_clauses.append('upper(trim(d."City/Town")) = ?')
            loc_params.append(city.upper().strip())
            loc_desc.append(city.strip())
        if selected_zips:
            placeholders = ", ".join(["?"] * len(selected_zips))
            loc_clauses.append(f'left(cast(d."ZIP Code" as varchar), 5) in ({placeholders})')
            loc_params.extend(selected_zips)
            loc_desc.append(", ".join(selected_zips))

        # Proximity bounds the *door geocode*, mirroring /practices/search:
        # membership rows whose door has no geocode fall out of a radius query.
        distance_select = "cast(null as double) as distance_miles"
        distance_filter = ""
        distance_params: list = []
        if proximity:
            assert lat is not None and lng is not None
            dlat = radius_miles / 69.0
            dlng = radius_miles / (69.0 * max(0.1, abs(math.cos(math.radians(lat)))))
            loc_desc.append(f"{radius_miles}mi of ({lat:.4f},{lng:.4f})")
            distance_select = """
                3959.0 * 2.0 * asin(sqrt(least(1.0, greatest(0.0,
                    pow(sin(radians(g.lat - ?) / 2.0), 2)
                    + cos(radians(?)) * cos(radians(g.lat))
                    * pow(sin(radians(g.lng - ?) / 2.0), 2)
                )))) as distance_miles
            """
            distance_params = [lat, lat, lng]
            distance_filter = (
                "where distance_miles is not null and distance_miles <= ? "
                "and lat between ? and ? and lng between ? and ?"
            )

        where_clauses: list[str] = []
        if patterns:
            where_clauses.append(f"({spec_pred})")
        if anchor_org is not None:
            where_clauses.append("nullif(trim(coalesce(d.org_pac_id, '')), '') = ?")
        where_clauses.extend(loc_clauses)
        where_sql = " AND ".join(where_clauses)

        anchor_params: list = [anchor_org] if anchor_org is not None else []
        if anchor_org is not None:
            loc_desc.append(f"org {anchor_org}")

        matched_cte = f"""
            matched AS (
                SELECT cast(d."NPI" AS varchar) AS npi,
                       nullif(trim(coalesce(d.org_pac_id, '')), '') AS org_pac_id,
                       upper(trim(d.adr_ln_1)) || '|' || left(cast(d."ZIP Code" AS varchar), 5) AS addr_key,
                       min(trim(d."Facility Name")) AS org_name,
                       max(d.num_org_mem) AS group_size_national,
                       min(trim(d.adr_ln_1)) AS street,
                       min(trim(d."City/Town")) AS city,
                       min(trim(d."State")) AS state,
                       min(left(cast(d."ZIP Code" AS varchar), 5)) AS zip5,
                       min(cast(d."Telephone Number" AS varchar)) AS phone,
                       min(trim(d."Provider First Name")) AS first_name,
                       min(trim(d."Provider Last Name")) AS last_name,
                       min(trim(d.{CRED_COL})) AS credentials,
                       min(trim(d.pri_spec)) AS specialty
                FROM raw_dac_national d
                WHERE {where_sql}
                GROUP BY 1, 2, 3
            )
        """
        base_params = list(patterns) + anchor_params + loc_params

        conn = get_conn()

        count_sql = f"WITH {matched_cte} SELECT count(*) FROM matched"
        try:
            membership_count = conn.execute(count_sql, base_params).fetchone()[0]
        except Exception as exc:
            raise HTTPException(
                status_code=503, detail="CMS warehouse is unavailable"
            ) from exc
        if membership_count > MAX_MEMBERSHIP_ROWS:
            raise HTTPException(
                status_code=422,
                detail=(
                    "Selection is too broad for a snapshot "
                    f"({membership_count} enrollment rows > {MAX_MEMBERSHIP_ROWS}). "
                    "Narrow the geography or specialty selection."
                ),
            )

        sql = f"""
            WITH {matched_cte},
            npis AS (SELECT DISTINCT npi FROM matched),
            money AS (
                SELECT n.npi,
                       max(pb."Tot_Mdcr_Pymt_Amt") AS partb_payments,
                       max(pb."Tot_Srvcs") AS partb_services,
                       max(pb."Tot_Benes") AS partb_beneficiaries
                FROM npis n
                LEFT JOIN raw_physician_by_provider pb
                    ON cast(pb."Rndrng_NPI" AS varchar) = n.npi
                GROUP BY 1
            ),
            partd AS (
                SELECT n.npi, max(pd."Tot_Drug_Cst") AS partd_drug_cost
                FROM npis n
                LEFT JOIN raw_part_d_by_provider pd
                    ON cast(pd."PRSCRBR_NPI" AS varchar) = n.npi
                GROUP BY 1
            ),
            openpay AS (
                SELECT n.npi,
                       sum(op."Total_Amount_of_Payment_USDollars") AS open_payments_total
                FROM npis n
                JOIN raw_open_payments_general op
                    ON cast(op."Covered_Recipient_NPI" AS varchar) = n.npi
                GROUP BY 1
            ),
            nppes_primary AS (
                SELECT cast(n.npi AS varchar) AS npi,
                       min(upper(trim(n.practice_address_1)) || '|' || left(n.practice_zip, 5))
                           AS nppes_addr_key
                FROM raw_nppes n
                WHERE cast(n.npi AS varchar) IN (SELECT npi FROM npis)
                GROUP BY 1
            ),
            geo AS (
                SELECT addr_key, min(lat) AS lat, min(lng) AS lng
                FROM address_geocode
                WHERE addr_key IN (SELECT DISTINCT addr_key FROM matched)
                GROUP BY 1
            ),
            joined AS (
                SELECT m.*, g.lat, g.lng,
                       mo.partb_payments, mo.partb_services, mo.partb_beneficiaries,
                       pd.partd_drug_cost, op.open_payments_total,
                       coalesce(np.nppes_addr_key = m.addr_key, false) AS is_nppes_primary,
                       {distance_select}
                FROM matched m
                LEFT JOIN geo g ON g.addr_key = m.addr_key
                LEFT JOIN money mo ON mo.npi = m.npi
                LEFT JOIN partd pd ON pd.npi = m.npi
                LEFT JOIN openpay op ON op.npi = m.npi
                LEFT JOIN nppes_primary np ON np.npi = m.npi
            )
            SELECT * FROM joined {distance_filter}
        """
        params = base_params + distance_params
        if proximity:
            assert lat is not None and lng is not None
            dlat = radius_miles / 69.0
            dlng = radius_miles / (69.0 * max(0.1, abs(math.cos(math.radians(lat)))))
            params += [radius_miles, lat - dlat, lat + dlat, lng - dlng, lng + dlng]

        try:
            cursor = conn.execute(sql, params)
            columns = [d[0] for d in cursor.description]
            # NB: the `zip` query parameter shadows the builtin in this scope.
            rows = [
                {columns[i]: r[i] for i in range(len(columns))}
                for r in cursor.fetchall()
            ]
        except Exception as exc:
            raise HTTPException(
                status_code=503, detail="CMS warehouse is unavailable"
            ) from exc

        # ---- assemble the three linked blocks ---------------------------------
        providers: dict[str, dict] = {}
        sites: dict[tuple[str, Optional[str]], dict] = {}
        org_npis: dict[str, set] = {}
        org_doors: dict[str, set] = {}
        org_meta: dict[str, dict] = {}
        independent_npis: set = set()
        independent_doors: set = set()

        for row in rows:
            npi = row["npi"]
            org = row["org_pac_id"]
            addr_key = row["addr_key"]
            has_door = bool(addr_key) and bool((row["street"] or "").strip())

            entry = providers.setdefault(
                npi,
                {
                    "npi": npi,
                    "first_name": row["first_name"],
                    "last_name": row["last_name"],
                    "credentials": row["credentials"] or None,
                    "specialty": row["specialty"],
                    "org_pac_ids": [],
                    "primary_site_ids": [],
                    "other_site_ids": [],
                    "has_nppes_primary_door": False,
                    "partb_payments": row["partb_payments"],
                    "partb_services": row["partb_services"],
                    "partb_beneficiaries": row["partb_beneficiaries"],
                    "partd_drug_cost": row["partd_drug_cost"],
                    "open_payments_total": row["open_payments_total"],
                },
            )
            if org and org not in entry["org_pac_ids"]:
                entry["org_pac_ids"].append(org)

            if org:
                org_npis.setdefault(org, set()).add(npi)
                meta = org_meta.setdefault(
                    org, {"name": None, "group_size_national": None}
                )
                if row["org_name"] and not meta["name"]:
                    meta["name"] = row["org_name"]
                if row["group_size_national"] is not None:
                    existing = meta["group_size_national"]
                    meta["group_size_national"] = (
                        row["group_size_national"]
                        if existing is None
                        else max(existing, row["group_size_national"])
                    )
            else:
                independent_npis.add(npi)

            if not has_door:
                continue

            site_key = (addr_key, org)
            site = sites.setdefault(
                site_key,
                {
                    "org_pac_id": org,
                    "practice_name": row["org_name"] if org else None,
                    "address": row["street"],
                    "city": row["city"],
                    "state": row["state"],
                    "zip5": row["zip5"],
                    "phone": row["phone"],
                    "lat": row["lat"],
                    "lng": row["lng"],
                    "group_size_national": row["group_size_national"] if org else None,
                    "distance_miles": row["distance_miles"],
                    "npis": set(),
                    "partb_values": [],
                    "partd_values": [],
                    "solo_names": [],
                },
            )
            if npi not in site["npis"]:
                site["npis"].add(npi)
                site["partb_values"].append(row["partb_payments"])
                site["partd_values"].append(row["partd_drug_cost"])
                name = " ".join(
                    part for part in (row["first_name"], row["last_name"]) if part
                )
                if name:
                    site["solo_names"].append(name)
            if row["distance_miles"] is not None and (
                site["distance_miles"] is None
                or row["distance_miles"] < site["distance_miles"]
            ):
                site["distance_miles"] = row["distance_miles"]

            site_id = site_identifier(
                "cms_enrollment", row["street"], row["zip5"], org
            )
            bucket = (
                "primary_site_ids" if row["is_nppes_primary"] else "other_site_ids"
            )
            if site_id not in entry["primary_site_ids"] + entry["other_site_ids"]:
                entry[bucket].append(site_id)
            if row["is_nppes_primary"]:
                entry["has_nppes_primary_door"] = True

            if org:
                org_doors.setdefault(org, set()).add(addr_key)
            else:
                independent_doors.add(addr_key)

        provider_models: list[SnapshotProvider] = []
        for entry in providers.values():
            site_ids = entry.pop("primary_site_ids") + entry.pop("other_site_ids")
            provider_models.append(
                SnapshotProvider(
                    **entry, site_ids=site_ids, door_count=len(site_ids)
                )
            )
        provider_models.sort(
            key=lambda p: (p.partb_payments is None, -(p.partb_payments or 0.0))
        )

        site_models: list[SnapshotSite] = []
        for (addr_key, org), site in sites.items():
            provider_count = len(site["npis"])
            classification = classify_site(
                provider_count,
                unaffiliated_count=0 if org else provider_count,
                context_count=1 if org else 0,
            )
            solo_name = (
                site["solo_names"][0]
                if classification == "solo" and site["solo_names"]
                else None
            )
            site_models.append(
                SnapshotSite(
                    site_id=site_identifier(
                        "cms_enrollment", site["address"], site["zip5"], org
                    ),
                    org_pac_id=org,
                    practice_name=site["practice_name"],
                    address=site["address"],
                    city=site["city"],
                    state=site["state"],
                    zip5=site["zip5"],
                    phone=site["phone"],
                    lat=site["lat"],
                    lng=site["lng"],
                    providers_here=provider_count,
                    group_size_national=site["group_size_national"],
                    site_classification=classification,
                    billing_artifact=bool(org)
                    and provider_count >= BILLING_ARTIFACT_THRESHOLD,
                    solo_provider_name=solo_name,
                    distance_miles=(
                        round(site["distance_miles"], 2)
                        if site["distance_miles"] is not None
                        else None
                    ),
                    partb_payments=_sum_or_none(site["partb_values"]),
                    partd_drug_cost=_sum_or_none(site["partd_values"]),
                )
            )
        site_models.sort(key=lambda s: -s.providers_here)

        by_npi = {p.npi: p for p in provider_models}
        org_models: list[SnapshotOrganization] = []
        for org, npis in org_npis.items():
            members = [by_npi[n] for n in npis]
            org_models.append(
                SnapshotOrganization(
                    org_pac_id=org,
                    name=org_meta[org]["name"],
                    provider_count=len(npis),
                    site_count=len(org_doors.get(org, set())),
                    group_size_national=org_meta[org]["group_size_national"],
                    partb_payments=_sum_or_none(m.partb_payments for m in members),
                    partd_drug_cost=_sum_or_none(m.partd_drug_cost for m in members),
                    open_payments_total=_sum_or_none(
                        m.open_payments_total for m in members
                    ),
                )
            )
        org_models.sort(key=lambda o: (-o.provider_count, o.name or ""))

        independent = None
        if independent_npis:
            members = [by_npi[n] for n in independent_npis]
            independent = SnapshotIndependent(
                provider_count=len(independent_npis),
                site_count=len(independent_doors),
                partb_payments=_sum_or_none(m.partb_payments for m in members),
                partd_drug_cost=_sum_or_none(m.partd_drug_cost for m in members),
                open_payments_total=_sum_or_none(
                    m.open_payments_total for m in members
                ),
            )

        totals = SnapshotTotals(
            organizations=len(org_models),
            sites=len(site_models),
            providers=len(provider_models),
            partb_payments=_sum_or_none(p.partb_payments for p in provider_models),
            partd_drug_cost=_sum_or_none(p.partd_drug_cost for p in provider_models),
        )

        return MarketSnapshotResponse(
            requested_specialties=requested_specialties,
            matched_patterns=patterns,
            location=" · ".join(loc_desc) if loc_desc else "",
            anchor_org_pac_id=anchor_org,
            totals=totals,
            organizations=org_models,
            independent=independent,
            sites=site_models,
            providers=provider_models,
        )

    @router.get("/organizations", response_model=OrganizationSearchResponse)
    async def organization_search(
        q: str,
        city: Optional[str] = None,
        state: Optional[str] = None,
        zip: Optional[str] = None,
        zips: Optional[str] = None,
        limit: int = 20,
    ):
        """Typeahead over CMS enrollment groups, optionally bounded to a geography."""
        needle = " ".join(q.split())
        if len(needle) < 2:
            raise HTTPException(
                status_code=422, detail="Type at least two characters to search organizations"
            )
        if len(needle) > 80:
            raise HTTPException(status_code=422, detail="Organization query is too long")
        if "%" in needle or "_" in needle:
            raise HTTPException(
                status_code=422, detail="Organization queries cannot contain wildcards"
            )
        try:
            selected_zips = parse_zip_codes(zips, zip)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        limit = max(1, min(limit, 50))

        clauses = [
            "nullif(trim(coalesce(d.org_pac_id, '')), '') IS NOT NULL",
            'd."Facility Name" ILIKE ?',
        ]
        params: list = [f"%{needle}%"]
        if state:
            clauses.append('upper(trim(d."State")) = ?')
            params.append(state.upper().strip())
        if city:
            clauses.append('upper(trim(d."City/Town")) = ?')
            params.append(city.upper().strip())
        if selected_zips:
            placeholders = ", ".join(["?"] * len(selected_zips))
            clauses.append(f'left(cast(d."ZIP Code" as varchar), 5) in ({placeholders})')
            params.extend(selected_zips)

        sql = f"""
            SELECT nullif(trim(coalesce(d.org_pac_id, '')), '') AS org_pac_id,
                   min(trim(d."Facility Name")) AS name,
                   count(DISTINCT cast(d."NPI" AS varchar)) AS provider_count,
                   count(DISTINCT CASE WHEN nullif(trim(d.adr_ln_1), '') IS NOT NULL
                       THEN upper(trim(d.adr_ln_1)) || '|' || left(cast(d."ZIP Code" AS varchar), 5)
                   END) AS site_count,
                   max(d.num_org_mem) AS group_size_national
            FROM raw_dac_national d
            WHERE {' AND '.join(clauses)}
            GROUP BY 1
            ORDER BY provider_count DESC, name
            LIMIT {limit}
        """
        try:
            rows = get_conn().execute(sql, params).fetchall()
        except Exception as exc:
            raise HTTPException(
                status_code=503, detail="CMS warehouse is unavailable"
            ) from exc

        return OrganizationSearchResponse(
            query=needle,
            results=[
                OrganizationSearchResult(
                    org_pac_id=row[0],
                    name=row[1],
                    provider_count=row[2],
                    site_count=row[3],
                    group_size_national=row[4],
                )
                for row in rows
                if row[0]
            ],
        )

    return router
