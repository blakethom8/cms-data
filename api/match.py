"""
NPI Match Engine — bridges Google Places results to CMS provider intelligence.

Matching strategy (cascading):
1. Exact: last_name + first_name + zip5
2. Fuzzy: last_name + first_name + city + state
3. Loose: last_name + city + state + specialty keyword
"""

import re
import logging
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/match", tags=["Match Engine"])


class PlaceInput(BaseModel):
    """A Google Places result to match against CMS."""
    name: str                          # e.g. "John Smith, MD - Cardiology"
    address: Optional[str] = None      # e.g. "123 Main St, Los Angeles, CA 90012"
    phone: Optional[str] = None
    types: Optional[list[str]] = None  # Google Places types
    specialty_hint: Optional[str] = None  # If the user searched by specialty


class MatchResult(BaseModel):
    npi: str
    first_name: str
    last_name: str
    provider_type: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip5: Optional[str] = None
    confidence: float  # 0-1
    match_method: str  # exact_name_zip, fuzzy_name_city, loose_name_specialty
    # CMS Intelligence
    total_medicare_payment: Optional[float] = None
    total_services: Optional[int] = None
    total_beneficiaries: Optional[int] = None
    mips_score: Optional[float] = None
    num_hospital_affiliations: Optional[int] = None
    open_payments_total: Optional[float] = None
    open_payments_count: Optional[int] = None
    top_services: Optional[list[dict]] = None


class MatchResponse(BaseModel):
    input_name: str
    matches: list[MatchResult]
    match_count: int


class BatchMatchRequest(BaseModel):
    places: list[PlaceInput]


class BatchMatchResponse(BaseModel):
    results: list[MatchResponse]


def parse_provider_name(raw_name: str) -> dict:
    """Extract first/last name from a Google Places provider name.
    
    Returns: {"first": str, "last": str, "first_alt": str, "last_alt": str}
    The alt versions try reversing first/last order for fuzzy matching.
    """
    # Strip "Dr." / "Dr " prefix
    name = re.sub(r'\bDr\.?\s+', '', raw_name, flags=re.IGNORECASE)
    
    # Handle possessives: "Dr. Friedman's Clinic" → extract "Friedman"
    possessive_match = re.search(r"([A-Z][a-z]+)'s\s+(Clinic|Office|Practice|Center)", name, re.IGNORECASE)
    if possessive_match:
        extracted_name = possessive_match.group(1)
        return {
            "first": "",
            "last": extracted_name,
            "first_alt": extracted_name,
            "last_alt": ""
        }
    
    # Strip common suffixes
    name = re.sub(r',?\s*(MD|DO|DDS|DMD|DPM|OD|PhD|NP|PA|PA-C|APRN|FNP|CNP|DNP|RN|PT|OT|DC|LCSW|PsyD)\b\.?', '', name, flags=re.IGNORECASE)
    # Strip practice/group suffixes
    name = re.sub(r'\s*[-–—]\s.*$', '', name)  # "John Smith - Cardiology" → "John Smith"
    name = re.sub(r'\s+(Medical|Health|Clinic|Group|Associates|Practice|Center|Office|Inc|LLC|PC|PLLC).*$', '', name, flags=re.IGNORECASE)
    name = name.strip().strip(',').strip()
    
    # Strip middle initials (single letters or "X.")
    parts = [p for p in name.split() if len(p.replace('.', '')) > 1]
    
    if len(parts) >= 2:
        # Try both orders: first-last AND last-first
        return {
            "first": parts[0],
            "last": parts[-1],
            "first_alt": parts[-1],  # reversed
            "last_alt": parts[0]     # reversed
        }
    elif len(parts) == 1:
        return {"first": "", "last": parts[0], "first_alt": parts[0], "last_alt": ""}
    return {"first": "", "last": "", "first_alt": "", "last_alt": ""}


def parse_address(address: str) -> dict:
    """Extract city, state, zip from an address string."""
    result = {"city": "", "state": "", "zip5": ""}
    if not address:
        return result
    
    # Try to find zip
    zip_match = re.search(r'\b(\d{5})(?:-\d{4})?\b', address)
    if zip_match:
        result["zip5"] = zip_match.group(1)
    
    # Try to find state (2-letter code)
    state_match = re.search(r'\b([A-Z]{2})\b(?:\s+\d{5})?', address)
    if state_match:
        result["state"] = state_match.group(1)
    
    # City is typically before state
    # Pattern: "City, ST ZIP" or "Street, City, ST ZIP"
    parts = address.split(',')
    if len(parts) >= 2:
        # City is usually the second-to-last part before state
        city_part = parts[-2].strip() if len(parts) >= 3 else parts[0].strip()
        # Remove any numbers (street addresses)
        city_clean = re.sub(r'^\d+\s+\S+\s+\S+\s*', '', city_part).strip()
        if city_clean:
            result["city"] = city_clean
        else:
            result["city"] = city_part
    
    return result


def get_match_router(get_conn):
    """Create router with database connection dependency."""

    @router.post("/single", response_model=MatchResponse)
    async def match_single(place: PlaceInput):
        """Match a single Google Places result to CMS providers."""
        return await _do_match(place, get_conn())

    @router.post("/batch", response_model=BatchMatchResponse)
    async def match_batch(req: BatchMatchRequest):
        """Match multiple Google Places results to CMS providers."""
        conn = get_conn()
        results = []
        for place in req.places[:50]:  # Cap at 50
            result = await _do_match(place, conn)
            results.append(result)
        return BatchMatchResponse(results=results)

    @router.get("/search")
    async def match_search(
        name: str,
        address: Optional[str] = None,
        specialty: Optional[str] = None
    ):
        """Simple GET endpoint for dashboard testing."""
        place = PlaceInput(name=name, address=address, specialty_hint=specialty)
        return await _do_match(place, get_conn())

    return router


async def _do_match(place: PlaceInput, conn) -> MatchResponse:
    """Core matching logic with enhanced multi-address and name-order matching."""
    parsed_name = parse_provider_name(place.name)
    parsed_addr = parse_address(place.address or "")
    
    first = parsed_name["first"]
    last = parsed_name["last"]
    first_alt = parsed_name.get("first_alt", "")
    last_alt = parsed_name.get("last_alt", "")
    city = parsed_addr["city"]
    state = parsed_addr["state"]
    zip5 = parsed_addr["zip5"]
    
    matches = []
    seen_npis = set()  # Prevent duplicates

    def try_name_combo(fname, lname, method_suffix=""):
        """Try a single first+last combination."""
        found = []
        
        # Strategy 1a: Exact match on last + first + zip (core_providers)
        if lname and fname and zip5:
            rows = conn.execute("""
                SELECT npi, first_name, last_org_name, provider_type, city, state, zip5
                FROM core_providers 
                WHERE LOWER(last_org_name) = LOWER(?) 
                  AND LOWER(first_name) LIKE LOWER(?) || '%'
                  AND zip5 = ?
                  AND entity_type_code = 'I'
                LIMIT 5
            """, [lname, fname, zip5]).fetchall()
            for r in rows:
                if r[0] not in seen_npis:
                    found.append(_build_match(r, f"exact_name_zip{method_suffix}", 0.95, conn))
                    seen_npis.add(r[0])
        
        # Strategy 1b: Multi-address fallback — check practice_locations
        if not found and lname and fname and zip5:
            rows = conn.execute("""
                SELECT DISTINCT cp.npi, cp.first_name, cp.last_org_name, cp.provider_type, 
                       cp.city, cp.state, cp.zip5
                FROM core_providers cp
                JOIN practice_locations pl ON CAST(cp.npi AS VARCHAR) = pl.npi
                WHERE LOWER(cp.last_org_name) = LOWER(?)
                  AND LOWER(cp.first_name) LIKE LOWER(?) || '%'
                  AND pl.zip5 = ?
                  AND cp.entity_type_code = 'I'
                LIMIT 5
            """, [lname, fname, zip5]).fetchall()
            for r in rows:
                if r[0] not in seen_npis:
                    found.append(_build_match(r, f"practice_location_zip{method_suffix}", 0.88, conn))
                    seen_npis.add(r[0])
        
        # Strategy 1c: Check raw_nppes practice address
        if not found and lname and fname and zip5:
            rows = conn.execute("""
                SELECT DISTINCT cp.npi, cp.first_name, cp.last_org_name, cp.provider_type,
                       cp.city, cp.state, cp.zip5
                FROM core_providers cp
                JOIN raw_nppes np ON CAST(cp.npi AS BIGINT) = np.npi
                WHERE LOWER(cp.last_org_name) = LOWER(?)
                  AND LOWER(cp.first_name) LIKE LOWER(?) || '%'
                  AND SUBSTRING(np.practice_zip, 1, 5) = ?
                  AND cp.entity_type_code = 'I'
                LIMIT 5
            """, [lname, fname, zip5]).fetchall()
            for r in rows:
                if r[0] not in seen_npis:
                    found.append(_build_match(r, f"nppes_practice_zip{method_suffix}", 0.85, conn))
                    seen_npis.add(r[0])
        
        # Strategy 2: Name + city + state (core_providers)
        if not found and lname and fname and city and state:
            rows = conn.execute("""
                SELECT npi, first_name, last_org_name, provider_type, city, state, zip5
                FROM core_providers 
                WHERE LOWER(last_org_name) = LOWER(?) 
                  AND LOWER(first_name) LIKE LOWER(?) || '%'
                  AND LOWER(city) = LOWER(?)
                  AND state = ?
                  AND entity_type_code = 'I'
                LIMIT 5
            """, [lname, fname, city, state]).fetchall()
            for r in rows:
                if r[0] not in seen_npis:
                    found.append(_build_match(r, f"fuzzy_name_city{method_suffix}", 0.80, conn))
                    seen_npis.add(r[0])
        
        return found

    # Try normal name order first
    if first and last:
        matches.extend(try_name_combo(first, last))
    
    # Try reversed name order if no strong matches yet
    if not matches and first_alt and last_alt and (first_alt != first or last_alt != last):
        matches.extend(try_name_combo(first_alt, last_alt, "_reversed"))

    # Strategy 3: Last name only + city + state (broader fallback)
    if not matches and last and state:
        params = [last, state]
        city_clause = ""
        if city:
            city_clause = "AND LOWER(city) = LOWER(?)"
            params.append(city)
        
        rows = conn.execute(f"""
            SELECT npi, first_name, last_org_name, provider_type, city, state, zip5
            FROM core_providers 
            WHERE LOWER(last_org_name) = LOWER(?) 
              AND state = ?
              {city_clause}
              AND entity_type_code = 'I'
            LIMIT 10
        """, params).fetchall()
        
        for r in rows:
            if r[0] not in seen_npis:
                conf = 0.50
                # Boost if first name partially matches
                if first and r[1] and r[1].lower().startswith(first[0].lower()):
                    conf = 0.60
                # Boost if specialty matches
                if place.specialty_hint and r[3] and place.specialty_hint.lower() in r[3].lower():
                    conf += 0.15
                matches.append(_build_match(r, "loose_name_state", conf, conn))
                seen_npis.add(r[0])

    # Sort by confidence
    matches.sort(key=lambda m: m.confidence, reverse=True)
    
    return MatchResponse(
        input_name=place.name,
        matches=matches[:5],
        match_count=len(matches)
    )


def _build_match(row, method: str, confidence: float, conn) -> MatchResult:
    """Build a MatchResult with CMS intelligence enrichment."""
    npi = str(row[0])
    
    result = MatchResult(
        npi=npi,
        first_name=row[1] or "",
        last_name=row[2] or "",
        provider_type=row[3],
        city=row[4],
        state=row[5],
        zip5=row[6],
        confidence=confidence,
        match_method=method,
    )
    
    # Enrich with utilization
    try:
        util = conn.execute("""
            SELECT tot_medicare_payment, tot_services, tot_unique_beneficiaries
            FROM utilization_metrics WHERE npi = ? LIMIT 1
        """, [npi]).fetchone()
        if util:
            result.total_medicare_payment = round(float(util[0]), 2) if util[0] else None
            result.total_services = int(util[1]) if util[1] else None
            result.total_beneficiaries = int(util[2]) if util[2] else None
    except:
        pass

    # Enrich with MIPS
    try:
        mips = conn.execute("""
            SELECT CAST(final_mips_score AS DOUBLE) 
            FROM raw_mips_performance WHERE npi = ? LIMIT 1
        """, [npi]).fetchone()
        if mips and mips[0]:
            result.mips_score = round(mips[0], 1)
    except:
        pass

    # Enrich with hospital affiliations
    try:
        affil = conn.execute("""
            SELECT COUNT(DISTINCT facility_ccn) 
            FROM raw_dac_facility_affiliations WHERE npi = ?
        """, [npi]).fetchone()
        if affil:
            result.num_hospital_affiliations = affil[0]
    except:
        pass

    # Enrich with Open Payments
    try:
        pay = conn.execute("""
            SELECT COUNT(*), ROUND(SUM("Total_Amount_of_Payment_USDollars"), 2)
            FROM raw_open_payments_general 
            WHERE CAST("Covered_Recipient_NPI" AS VARCHAR) = ?
        """, [npi]).fetchone()
        if pay and pay[0] > 0:
            result.open_payments_count = pay[0]
            result.open_payments_total = pay[1]
    except:
        pass

    # Top services
    try:
        svcs = conn.execute("""
            SELECT HCPCS_Desc, Tot_Srvcs, Avg_Mdcr_Pymt_Amt * Tot_Srvcs as total_payment
            FROM raw_physician_by_provider_and_service 
            WHERE Rndrng_NPI = ?
            ORDER BY Tot_Srvcs DESC LIMIT 5
        """, [npi]).fetchall()
        if svcs:
            result.top_services = [
                {"service": s[0], "count": int(s[1]) if s[1] else 0, "payment": round(float(s[2]),2) if s[2] else 0}
                for s in svcs
            ]
    except:
        pass

    return result
