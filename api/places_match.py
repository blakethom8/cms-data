"""
Integrated Places + CMS Match — the real user workflow.

1. Query Google Places for specialty + location
2. For each result, attempt NPI match against CMS
3. Return unified results: Places data + CMS intelligence
"""

import os
import re
import logging
import httpx
from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from match import parse_provider_name, parse_address, _do_match, PlaceInput, _build_match
from llm_match import llm_match_provider

logger = logging.getLogger(__name__)

GOOGLE_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY", "")

router = APIRouter(prefix="/search", tags=["Integrated Search"])


class PlaceResult(BaseModel):
    """A single Google Places result."""
    place_id: str
    name: str
    address: str
    lat: Optional[float] = None
    lng: Optional[float] = None
    rating: Optional[float] = None
    reviews_count: Optional[int] = None
    types: list[str] = []
    # Phone requires a details call — skip for now
    

class CMSMatch(BaseModel):
    """CMS match for a place result."""
    npi: str
    first_name: str
    last_name: str
    provider_type: Optional[str] = None
    confidence: float
    match_method: str
    # Intelligence
    total_medicare_payment: Optional[float] = None
    total_services: Optional[int] = None  
    total_beneficiaries: Optional[int] = None
    mips_score: Optional[float] = None
    num_hospital_affiliations: Optional[int] = None
    open_payments_total: Optional[float] = None
    open_payments_count: Optional[int] = None
    top_services: Optional[list[dict]] = None
    # LLM-specific
    llm_reasoning: Optional[str] = None


class EnrichedResult(BaseModel):
    """A Places result enriched with CMS data."""
    place: PlaceResult
    cms_match: Optional[CMSMatch] = None
    match_type: str  # "individual", "organization", "no_match"
    provider_roster: Optional[list[CMSMatch]] = None  # For organizations


class SearchResponse(BaseModel):
    query: str
    location: str
    total_places: int
    matched_count: int
    results: list[EnrichedResult]


def is_individual_name(name: str) -> bool:
    """Heuristic: does this look like a person's name vs an organization?"""
    # Org indicators
    org_words = ['center', 'clinic', 'hospital', 'medical group', 'associates', 
                 'institute', 'health', 'university', 'ucla', 'usc', 'cedars',
                 'providence', 'kaiser', 'department', 'practice', 'office of',
                 'foundation', 'network']
    name_lower = name.lower()
    if any(w in name_lower for w in org_words):
        return False
    
    # Person indicators: has credentials suffix
    if re.search(r',?\s*(MD|DO|DDS|DMD|DPM|OD|PhD|NP|PA|APRN|FNP|DC|DPT)\b', name, re.IGNORECASE):
        return True
    
    # Short name with 2-3 words = likely a person
    clean = re.sub(r',?\s*(MD|DO|DDS|DMD|DPM|OD|PhD|NP|PA)\b\.?', '', name, flags=re.IGNORECASE).strip()
    words = clean.split()
    if 2 <= len(words) <= 4 and all(w[0].isupper() for w in words if w):
        return True
    
    return False


def extract_doctor_from_org_name(org_name: str) -> Optional[str]:
    """Try to extract a doctor's last name from an org name like 'Dr. Friedman's Clinic'."""
    # Pattern: "Dr. [Name]'s ..."
    match = re.search(r"Dr\.?\s+([A-Z][a-z]+)", org_name, re.IGNORECASE)
    if match:
        return match.group(1)
    
    # Pattern: "[Name]'s Clinic/Office/Practice"
    match = re.search(r"([A-Z][a-z]+)'s\s+(Clinic|Office|Practice|Center)", org_name, re.IGNORECASE)
    if match:
        return match.group(1)
    
    return None


async def get_provider_roster(
    address: str,
    specialty_hint: str,
    conn
) -> list[CMSMatch]:
    """Find providers practicing at a given organization address."""
    parsed_addr = parse_address(address)
    zip5 = parsed_addr.get("zip5")
    city = parsed_addr.get("city")
    state = parsed_addr.get("state")
    
    if not zip5:
        return []
    
    roster = []
    
    # Find providers at this zip with matching specialty
    # Normalize: "endocrinologist" → "endocrin", "cardiologist" → "cardiol", etc.
    specialty_stem = ""
    if specialty_hint:
        s = specialty_hint.lower().strip()
        # Strip common suffixes to get stem
        for suffix in ['ologist', 'ists', 'ist', 'ogy', 'ics', 'ic', 'er', 'or', 'ian']:
            if s.endswith(suffix) and len(s) > len(suffix) + 3:
                s = s[:-len(suffix)]
                break
        specialty_stem = s
    
    specialty_filter = ""
    if specialty_stem:
        specialty_filter = "AND LOWER(provider_type) LIKE '%' || ? || '%'"
    
    try:
        params = [zip5]
        if specialty_stem:
            params.append(specialty_stem)
        
        query = f"""
            SELECT npi, first_name, last_org_name, provider_type, city, state, zip5
            FROM core_providers
            WHERE zip5 = ?
              AND entity_type_code = 'I'
              {specialty_filter}
            ORDER BY npi
            LIMIT 50
        """
        
        rows = conn.execute(query, params).fetchall()
        
        # Enrich each with utilization to rank by volume
        for r in rows:
            match_obj = _build_match(r, "org_roster", 0.65, conn)
            # Convert to CMSMatch
            cms_match = CMSMatch(
                npi=match_obj.npi,
                first_name=match_obj.first_name,
                last_name=match_obj.last_name,
                provider_type=match_obj.provider_type,
                confidence=match_obj.confidence,
                match_method=match_obj.match_method,
                total_medicare_payment=match_obj.total_medicare_payment,
                total_services=match_obj.total_services,
                total_beneficiaries=match_obj.total_beneficiaries,
                mips_score=match_obj.mips_score,
                num_hospital_affiliations=match_obj.num_hospital_affiliations,
                open_payments_total=match_obj.open_payments_total,
                open_payments_count=match_obj.open_payments_count,
                top_services=match_obj.top_services,
            )
            roster.append(cms_match)
        
        # Sort by Medicare payment volume (most active first)
        roster.sort(
            key=lambda m: m.total_medicare_payment or 0,
            reverse=True
        )
        
        return roster[:10]  # Top 10 providers by volume
        
    except Exception as e:
        logger.error(f"Error fetching provider roster: {e}")
        return []


def get_search_router(get_conn):
    """Create integrated search router."""

    @router.get("/places", response_model=SearchResponse)
    async def search_places(
        specialty: str = Query(..., description="Specialty to search (e.g. 'endocrinologist')"),
        location: str = Query(..., description="City/area (e.g. 'Santa Monica, CA')"),
    ):
        """
        Search Google Places for providers, then match each result against CMS data.
        This mirrors the real user workflow in Provider Search.
        """
        if not GOOGLE_API_KEY:
            raise HTTPException(status_code=500, detail="Google Places API key not configured")
        
        conn = get_conn()
        
        # Step 1: Google Places text search
        search_query = f"{specialty} {location}"
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                "https://maps.googleapis.com/maps/api/place/textsearch/json",
                params={"query": search_query, "type": "doctor", "key": GOOGLE_API_KEY}
            )
            gdata = resp.json()
        
        if gdata.get("status") not in ("OK", "ZERO_RESULTS"):
            raise HTTPException(status_code=502, detail=f"Google Places error: {gdata.get('status')}")
        
        raw_places = gdata.get("results", [])
        
        # Step 2: For each result, attempt CMS match
        results = []
        matched = 0
        
        for p in raw_places:
            place = PlaceResult(
                place_id=p.get("place_id", ""),
                name=p.get("name", ""),
                address=p.get("formatted_address", ""),
                lat=p.get("geometry", {}).get("location", {}).get("lat"),
                lng=p.get("geometry", {}).get("location", {}).get("lng"),
                rating=p.get("rating"),
                reviews_count=p.get("user_ratings_total"),
                types=p.get("types", []),
            )
            
            # Determine if this looks like an individual or organization
            is_individual = is_individual_name(place.name)
            
            cms_match = None
            match_type = "no_match"
            provider_roster = None
            
            if is_individual:
                # Try to match individual provider
                place_input = PlaceInput(
                    name=place.name,
                    address=place.address,
                    specialty_hint=specialty,
                )
                match_result = await _do_match(place_input, conn)
                
                if match_result.matches and match_result.matches[0].confidence >= 0.7:
                    # High-confidence rule-based match — use directly
                    best = match_result.matches[0]
                    cms_match = CMSMatch(
                        npi=best.npi,
                        first_name=best.first_name,
                        last_name=best.last_name,
                        provider_type=best.provider_type,
                        confidence=best.confidence,
                        match_method=best.match_method,
                        total_medicare_payment=best.total_medicare_payment,
                        total_services=best.total_services,
                        total_beneficiaries=best.total_beneficiaries,
                        mips_score=best.mips_score,
                        num_hospital_affiliations=best.num_hospital_affiliations,
                        open_payments_total=best.open_payments_total,
                        open_payments_count=best.open_payments_count,
                        top_services=best.top_services,
                    )
                    match_type = "individual"
                    matched += 1
                else:
                    # Low confidence or no matches — try LLM fallback
                    # If we have rule-based candidates, use those
                    candidates = []
                    if match_result.matches:
                        candidates = [
                            {
                                "npi": m.npi,
                                "first_name": m.first_name,
                                "last_name": m.last_name,
                                "city": getattr(m, 'city', ''),
                                "state": getattr(m, 'state', ''),
                                "zip5": getattr(m, 'zip5', ''),
                                "provider_type": m.provider_type,
                            }
                            for m in match_result.matches[:5]
                        ]
                    else:
                        # No rule-based matches — do a broad search for LLM to reason over
                        parsed = parse_provider_name(place.name)
                        parsed_addr = parse_address(place.address)
                        last_name = parsed.get("last", "") or parsed.get("last_name", "")
                        state = parsed_addr.get("state", "CA")
                        if last_name:
                            try:
                                broad_rows = conn.execute("""
                                    SELECT npi, first_name, last_org_name, provider_type, 
                                           city, state, zip5
                                    FROM core_providers
                                    WHERE LOWER(last_org_name) = LOWER(?)
                                      AND state = ?
                                      AND entity_type_code = 'I'
                                    LIMIT 10
                                """, [last_name, state]).fetchall()
                                candidates = [
                                    {
                                        "npi": str(r[0]),
                                        "first_name": r[1] or "",
                                        "last_name": r[2] or "",
                                        "provider_type": r[3] or "",
                                        "city": r[4] or "",
                                        "state": r[5] or "",
                                        "zip5": r[6] or "",
                                    }
                                    for r in broad_rows
                                ]
                            except Exception as e:
                                logger.error(f"Broad search error: {e}")
                    
                    if candidates:
                        llm_result = await llm_match_provider(
                            place.name,
                            place.address,
                            candidates,
                            conn
                        )
                        
                        if llm_result:
                            matched_npi = llm_result["npi"]
                            # Try to find enriched match from rule-based results first
                            found_match = None
                            if match_result.matches:
                                for m in match_result.matches:
                                    if m.npi == matched_npi:
                                        found_match = m
                                        break
                            
                            if found_match:
                                cms_match = CMSMatch(
                                    npi=found_match.npi,
                                    first_name=found_match.first_name,
                                    last_name=found_match.last_name,
                                    provider_type=found_match.provider_type,
                                    confidence=llm_result["confidence"],
                                    match_method="llm_match",
                                    total_medicare_payment=found_match.total_medicare_payment,
                                    total_services=found_match.total_services,
                                    total_beneficiaries=found_match.total_beneficiaries,
                                    mips_score=found_match.mips_score,
                                    num_hospital_affiliations=found_match.num_hospital_affiliations,
                                    open_payments_total=found_match.open_payments_total,
                                    open_payments_count=found_match.open_payments_count,
                                    top_services=found_match.top_services,
                                    llm_reasoning=llm_result["reasoning"],
                                )
                            else:
                                # LLM matched from broad search — build match from NPI
                                try:
                                    row = conn.execute("""
                                        SELECT npi, first_name, last_org_name, provider_type,
                                               city, state, zip5
                                        FROM core_providers WHERE CAST(npi AS VARCHAR) = ?
                                        LIMIT 1
                                    """, [matched_npi]).fetchone()
                                    if row:
                                        from match import _build_match
                                        match_obj = _build_match(row, "llm_broad", llm_result["confidence"], conn)
                                        cms_match = CMSMatch(
                                            npi=match_obj.npi,
                                            first_name=match_obj.first_name,
                                            last_name=match_obj.last_name,
                                            provider_type=match_obj.provider_type,
                                            confidence=llm_result["confidence"],
                                            match_method="llm_match_broad",
                                            total_medicare_payment=match_obj.total_medicare_payment,
                                            total_services=match_obj.total_services,
                                            total_beneficiaries=match_obj.total_beneficiaries,
                                            mips_score=match_obj.mips_score,
                                            num_hospital_affiliations=match_obj.num_hospital_affiliations,
                                            open_payments_total=match_obj.open_payments_total,
                                            open_payments_count=match_obj.open_payments_count,
                                            top_services=match_obj.top_services,
                                            llm_reasoning=llm_result["reasoning"],
                                        )
                                except Exception as e:
                                    logger.error(f"Error building LLM broad match: {e}")
                            
                            if cms_match:
                                match_type = "individual"
                                matched += 1
                    
                    # If LLM also failed but we had low-confidence rule-based, still show it
                    if not cms_match and match_result.matches and match_result.matches[0].confidence >= 0.5:
                        best = match_result.matches[0]
                        cms_match = CMSMatch(
                            npi=best.npi,
                            first_name=best.first_name,
                            last_name=best.last_name,
                            provider_type=best.provider_type,
                            confidence=best.confidence,
                            match_method=best.match_method,
                            total_medicare_payment=best.total_medicare_payment,
                            total_services=best.total_services,
                            total_beneficiaries=best.total_beneficiaries,
                            mips_score=best.mips_score,
                            num_hospital_affiliations=best.num_hospital_affiliations,
                            open_payments_total=best.open_payments_total,
                            open_payments_count=best.open_payments_count,
                            top_services=best.top_services,
                        )
                        match_type = "individual"
                        matched += 1
            else:
                # Organization — get provider roster
                match_type = "organization"
                
                # Try to extract doctor name from org name
                extracted_name = extract_doctor_from_org_name(place.name)
                if extracted_name:
                    # Try to find this specific doctor at the address
                    place_input = PlaceInput(
                        name=extracted_name,
                        address=place.address,
                        specialty_hint=specialty,
                    )
                    match_result = await _do_match(place_input, conn)
                    if match_result.matches and match_result.matches[0].confidence >= 0.6:
                        best = match_result.matches[0]
                        cms_match = CMSMatch(
                            npi=best.npi,
                            first_name=best.first_name,
                            last_name=best.last_name,
                            provider_type=best.provider_type,
                            confidence=best.confidence,
                            match_method=best.match_method + "_org_extracted",
                            total_medicare_payment=best.total_medicare_payment,
                            total_services=best.total_services,
                            total_beneficiaries=best.total_beneficiaries,
                            mips_score=best.mips_score,
                            num_hospital_affiliations=best.num_hospital_affiliations,
                            open_payments_total=best.open_payments_total,
                            open_payments_count=best.open_payments_count,
                            top_services=best.top_services,
                        )
                        matched += 1
                
                # Get provider roster at this address
                provider_roster = await get_provider_roster(
                    place.address,
                    specialty,
                    conn
                )
            
            results.append(EnrichedResult(
                place=place,
                cms_match=cms_match,
                match_type=match_type,
                provider_roster=provider_roster,
            ))
        
        return SearchResponse(
            query=specialty,
            location=location,
            total_places=len(results),
            matched_count=matched,
            results=results,
        )

    return router
