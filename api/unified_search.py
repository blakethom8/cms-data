"""
Unified Provider Search — Approach 3: Parallel CMS + Google Places merge.

1. Query CMS/NPPES for all providers matching specialty + geography
2. Query Google Places for org/practice listings  
3. Match Places orgs to CMS addresses → unpack into individual providers
4. Merge into unified result set with practice group detection
"""

import os
import re
import logging
import time
from typing import Optional
from collections import defaultdict

import httpx
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

logger = logging.getLogger(__name__)

GOOGLE_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY", "")
SEARCH_API_KEY = os.getenv("SEARCH_API_KEY", "")

router = APIRouter(prefix="/unified", tags=["Unified Search"])


# --- Models ---

class ProviderResult(BaseModel):
    npi: str
    first_name: str
    last_name: str
    provider_type: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip5: Optional[str] = None
    # CMS intelligence
    total_medicare_payment: Optional[float] = None
    total_services: Optional[int] = None
    total_beneficiaries: Optional[int] = None
    mips_score: Optional[float] = None
    open_payments_total: Optional[float] = None
    open_payments_count: Optional[int] = None
    num_hospital_affiliations: Optional[int] = None
    # Google Places enrichment (inherited from practice)
    practice_name: Optional[str] = None
    practice_place_id: Optional[str] = None
    practice_rating: Optional[float] = None
    practice_reviews: Optional[int] = None
    practice_address: Optional[str] = None
    # Grouping
    practice_group_key: Optional[str] = None  # normalized address for grouping
    sources: list[str] = []  # ["cms", "places", "web"]


class PracticeGroup(BaseModel):
    name: Optional[str] = None
    place_id: Optional[str] = None
    address: str
    rating: Optional[float] = None
    reviews: Optional[int] = None
    phone: Optional[str] = None
    website: Optional[str] = None
    hours: Optional[str] = None
    provider_count: int = 0
    total_medicare_payment: float = 0
    providers: list[ProviderResult] = []


class UnifiedSearchResponse(BaseModel):
    query: str
    location: str
    total_providers: int
    total_with_places: int
    total_practice_groups: int
    cms_provider_count: int
    places_result_count: int
    places_source: str = "none"  # "searchapi", "google_places", or "none"
    elapsed_ms: float
    practice_groups: list[PracticeGroup]
    ungrouped_providers: list[ProviderResult]


# --- Helpers ---

def normalize_address_key(addr: str) -> str:
    """Normalize an address for grouping (strip suite, lowercase, etc.)."""
    if not addr:
        return ""
    addr = addr.lower().strip()
    # Remove suite/ste/unit/# numbers
    addr = re.sub(r'\s*(suite|ste|unit|#|apt)\s*\S*', '', addr)
    # Remove trailing comma + zip
    addr = re.sub(r',?\s*\d{5}(-\d{4})?$', '', addr)
    # Normalize common abbreviations
    addr = addr.replace(' blvd', ' boulevard').replace(' st', ' street').replace(' ave', ' avenue').replace(' dr', ' drive')
    # Take just the street number + name (first part)
    addr = re.sub(r'\s+', ' ', addr).strip().rstrip(',')
    return addr


def normalize_for_match(addr: str) -> str:
    """Extract street number + first word for fuzzy matching."""
    if not addr:
        return ""
    addr = addr.lower().strip()
    # Extract street number
    m = re.match(r'(\d+)\s+(.+)', addr)
    if m:
        num = m.group(1)
        street = m.group(2).split()[0] if m.group(2) else ""
        return f"{num} {street}"
    return addr[:20]


def batch_enrich(npis: list[str], conn) -> dict:
    """Fetch CMS enrichment data for a batch of providers. Returns dict of npi -> enrichment."""
    if not npis:
        return {}
    
    result = {npi: {} for npi in npis}
    placeholders = ','.join(['?' for _ in npis])
    
    # Utilization (bulk)
    try:
        rows = conn.execute(f"""
            SELECT npi, tot_medicare_payment, tot_services, tot_unique_beneficiaries
            FROM utilization_metrics WHERE npi IN ({placeholders})
        """, npis).fetchall()
        for r in rows:
            npi_str = str(r[0])
            if npi_str in result:
                result[npi_str]['total_medicare_payment'] = r[1]
                result[npi_str]['total_services'] = int(r[2]) if r[2] else None
                result[npi_str]['total_beneficiaries'] = int(r[3]) if r[3] else None
    except Exception as e:
        logger.error(f"Batch utilization error: {e}")
    
    # MIPS (bulk)
    try:
        rows = conn.execute(f"""
            SELECT CAST(npi AS VARCHAR), CAST(final_mips_score AS DOUBLE)
            FROM raw_mips_performance WHERE CAST(npi AS VARCHAR) IN ({placeholders})
        """, npis).fetchall()
        for r in rows:
            if r[0] in result and r[1]:
                result[r[0]]['mips_score'] = round(r[1], 1)
    except Exception as e:
        logger.error(f"Batch MIPS error: {e}")
    
    # Open Payments (bulk aggregation)
    try:
        rows = conn.execute(f"""
            SELECT CAST("Covered_Recipient_NPI" AS VARCHAR), COUNT(*), SUM("Total_Amount_of_Payment_USDollars")
            FROM raw_open_payments_general
            WHERE CAST("Covered_Recipient_NPI" AS VARCHAR) IN ({placeholders})
            GROUP BY "Covered_Recipient_NPI"
        """, npis).fetchall()
        for r in rows:
            if r[0] in result and r[1] > 0:
                result[r[0]]['open_payments_count'] = r[1]
                result[r[0]]['open_payments_total'] = round(r[2], 2) if r[2] else None
    except Exception as e:
        logger.error(f"Batch open payments error: {e}")
    
    # Hospital affiliations (bulk)
    try:
        rows = conn.execute(f"""
            SELECT CAST(npi AS VARCHAR), COUNT(DISTINCT facility_ccn)
            FROM raw_dac_facility_affiliations
            WHERE CAST(npi AS VARCHAR) IN ({placeholders})
            GROUP BY npi
        """, npis).fetchall()
        for r in rows:
            if r[0] in result and r[1] > 0:
                result[r[0]]['num_hospital_affiliations'] = r[1]
    except Exception as e:
        logger.error(f"Batch affiliations error: {e}")
    
    return result


# --- Main Search ---

def get_unified_router(get_conn):
    """Create unified search router."""

    @router.get("/search", response_model=UnifiedSearchResponse)
    async def unified_search(
        specialty: str = Query(..., description="Specialty (e.g. 'primary care', 'endocrinologist')"),
        location: str = Query(..., description="City/area (e.g. 'Santa Monica, CA')"),
        radius_miles: float = Query(5, description="Search radius in miles"),
    ):
        t0 = time.perf_counter()
        conn = get_conn()
        
        # --- Step 1: Parse location to get zip codes ---
        # Extract city and state from location string
        loc_parts = [p.strip() for p in location.split(',')]
        city = loc_parts[0] if loc_parts else location
        state = loc_parts[1].strip()[:2].upper() if len(loc_parts) > 1 else 'CA'
        
        # Map specialty search terms to CMS provider_type patterns
        specialty_lower = specialty.lower().strip()
        specialty_patterns = []
        
        # Common mappings
        SPECIALTY_MAP = {
            'primary care': ['internal medicine', 'family practice', 'family medicine', 'general practice', 'primary care'],
            'pcp': ['internal medicine', 'family practice', 'family medicine', 'general practice'],
            'cardiologist': ['cardiol'],
            'endocrinologist': ['endocrinol'],
            'orthopedic': ['orthop'],
            'neurologist': ['neurol'],
            'gastroenterologist': ['gastroenterol'],
            'dermatologist': ['dermatol'],
            'oncologist': ['oncol', 'hematol'],
            'pulmonologist': ['pulmon'],
            'urologist': ['urol'],
            'nephrologist': ['nephrol'],
            'rheumatologist': ['rheumatol'],
            'psychiatrist': ['psychiatr'],
            'ob-gyn': ['obstetric', 'gynecol'],
            'obgyn': ['obstetric', 'gynecol'],
            'pediatrician': ['pediatric'],
            'ophthalmologist': ['ophthalmol'],
            'ent': ['otolaryng'],
            'surgeon': ['surgery', 'surgical'],
        }
        
        for key, patterns in SPECIALTY_MAP.items():
            if key in specialty_lower:
                specialty_patterns = patterns
                break
        
        if not specialty_patterns:
            # Default: use the search term as a stem
            stem = re.sub(r'(ologist|ists?|ogy|ics?|er|or|ian)$', '', specialty_lower)
            if len(stem) >= 4:
                specialty_patterns = [stem]
            else:
                specialty_patterns = [specialty_lower]
        
        # --- Step 2: Query CMS for providers ---
        # Find zip codes for the city
        try:
            zip_rows = conn.execute("""
                SELECT DISTINCT zip5 FROM core_providers
                WHERE LOWER(city) = LOWER(?) AND state = ? 
                LIMIT 50
            """, [city, state]).fetchall()
            zip_codes = [r[0] for r in zip_rows if r[0]]
        except:
            zip_codes = []
        
        if not zip_codes:
            # Fallback: try partial city match
            try:
                zip_rows = conn.execute("""
                    SELECT DISTINCT zip5 FROM core_providers
                    WHERE LOWER(city) LIKE LOWER(?) AND state = ?
                    LIMIT 50
                """, [f"%{city}%", state]).fetchall()
                zip_codes = [r[0] for r in zip_rows if r[0]]
            except:
                zip_codes = []
        
        # Build specialty filter
        spec_conditions = " OR ".join(["LOWER(provider_type) LIKE ?" for _ in specialty_patterns])
        spec_params = [f"%{p}%" for p in specialty_patterns]
        
        cms_providers = []
        if zip_codes:
            placeholders = ','.join(['?' for _ in zip_codes])
            query = f"""
                SELECT c.npi, c.first_name, c.last_org_name, c.provider_type,
                       c.street_address_1, c.city, c.state, c.zip5, c.entity_type_code
                FROM core_providers c
                WHERE c.zip5 IN ({placeholders})
                  AND c.entity_type_code = 'I'
                  AND ({spec_conditions})
                ORDER BY c.last_org_name, c.first_name
            """
            params = zip_codes + spec_params
            try:
                rows = conn.execute(query, params).fetchall()
                for r in rows:
                    cms_providers.append({
                        'npi': str(r[0]),
                        'first_name': r[1] or '',
                        'last_name': r[2] or '',
                        'provider_type': r[3] or '',
                        'address': r[4] or '',
                        'city': r[5] or '',
                        'state': r[6] or '',
                        'zip5': r[7] or '',
                    })
            except Exception as e:
                logger.error(f"CMS query error: {e}")
        
        # --- Step 3: Query Places via SearchAPI (preferred) or Google Places (fallback) ---
        places_results = []
        places_source = "none"
        
        if SEARCH_API_KEY:
            # Use SearchAPI.io Google Maps engine — richer data (phone, website, hours)
            search_query = f"{specialty} {location}"
            try:
                async with httpx.AsyncClient(timeout=15) as client:
                    # Fetch 2 pages in parallel for ~40 results
                    import asyncio
                    async def fetch_page(page):
                        resp = await client.get(
                            "https://www.searchapi.io/api/v1/search",
                            params={
                                "api_key": SEARCH_API_KEY,
                                "engine": "google_maps",
                                "q": search_query,
                                "type": "search",
                                "gl": "us",
                                "hl": "en",
                                "page": page,
                            }
                        )
                        return resp.json()
                    
                    pages = await asyncio.gather(
                        fetch_page(1), fetch_page(2),
                        return_exceptions=True
                    )
                    
                    seen_ids = set()
                    for page_data in pages:
                        if isinstance(page_data, Exception):
                            continue
                        local_results = page_data.get("local_results", []) or page_data.get("places", [])
                        for p in local_results:
                            pid = p.get("place_id") or p.get("data_id") or ""
                            if pid in seen_ids:
                                continue
                            seen_ids.add(pid)
                            
                            gps = p.get("gps_coordinates", {}) or {}
                            places_results.append({
                                'place_id': pid,
                                'name': p.get('title', ''),
                                'address': p.get('address', ''),
                                'rating': p.get('rating'),
                                'reviews': p.get('reviews'),
                                'phone': p.get('phone'),
                                'website': p.get('website'),
                                'hours': p.get('hours'),
                                'type': p.get('type'),
                                'lat': gps.get('latitude'),
                                'lng': gps.get('longitude'),
                            })
                    
                    places_source = "searchapi"
                    logger.info(f"SearchAPI returned {len(places_results)} places for '{search_query}'")
            except Exception as e:
                logger.error(f"SearchAPI error: {e}")
        
        if not places_results and GOOGLE_API_KEY:
            # Fallback to direct Google Places API
            search_query = f"{specialty} {location}"
            try:
                async with httpx.AsyncClient(timeout=10) as client:
                    resp = await client.get(
                        "https://maps.googleapis.com/maps/api/place/textsearch/json",
                        params={"query": search_query, "type": "doctor", "key": GOOGLE_API_KEY}
                    )
                    gdata = resp.json()
                
                if gdata.get("status") == "OK":
                    for p in gdata.get("results", []):
                        places_results.append({
                            'place_id': p.get('place_id', ''),
                            'name': p.get('name', ''),
                            'address': p.get('formatted_address', ''),
                            'rating': p.get('rating'),
                            'reviews': p.get('user_ratings_total'),
                            'lat': p.get('geometry', {}).get('location', {}).get('lat'),
                            'lng': p.get('geometry', {}).get('location', {}).get('lng'),
                        })
                    places_source = "google_places"
            except Exception as e:
                logger.error(f"Google Places error: {e}")
        
        # --- Step 4: Batch enrich all CMS providers ---
        all_npis = [p['npi'] for p in cms_providers]
        enrichment_map = batch_enrich(all_npis, conn)
        
        # --- Step 5: Address-based matching ---
        # Build address index from CMS providers
        addr_index = defaultdict(list)  # normalized_addr -> [provider_idx]
        for i, p in enumerate(cms_providers):
            key = normalize_for_match(p['address'])
            if key:
                addr_index[key].append(i)
        
        # Match Places results to CMS addresses, dedup by address key
        addr_to_place = {}  # addr_key -> best place (prefer one with most reviews)
        for pi, place in enumerate(places_results):
            place_addr_key = normalize_for_match(place['address'])
            
            matched_addr_keys = set()
            for cms_key, cms_idxs in addr_index.items():
                if place_addr_key and cms_key and place_addr_key[:6] == cms_key[:6]:
                    matched_addr_keys.add(cms_key)
            
            for ak in matched_addr_keys:
                existing = addr_to_place.get(ak)
                if not existing or (place.get('reviews') or 0) > (existing.get('reviews') or 0):
                    addr_to_place[ak] = place
        
        # --- Step 6: Build practice groups ---
        practice_groups = []
        grouped_cms_idxs = set()
        
        # Group CMS providers by address, attach Places data where available
        all_addr_groups = defaultdict(list)
        for i, p in enumerate(cms_providers):
            key = normalize_for_match(p['address'])
            if key:
                all_addr_groups[key].append(i)
        
        for addr_key, cms_idxs in all_addr_groups.items():
            if len(cms_idxs) < 2:
                continue  # Solo providers handled separately
            
            place = addr_to_place.get(addr_key)
            has_places = place is not None
            
            providers = []
            total_pay = 0
            sample_addr = cms_providers[cms_idxs[0]]
            
            for ci in cms_idxs:
                p = cms_providers[ci]
                enrichment = enrichment_map.get(p['npi'], {})
                
                prov = ProviderResult(
                    npi=p['npi'],
                    first_name=p['first_name'],
                    last_name=p['last_name'],
                    provider_type=p['provider_type'],
                    address=p['address'],
                    city=p['city'],
                    state=p['state'],
                    zip5=p['zip5'],
                    practice_name=place['name'] if place else None,
                    practice_place_id=place['place_id'] if place else None,
                    practice_rating=place['rating'] if place else None,
                    practice_reviews=place['reviews'] if place else None,
                    practice_address=place['address'] if place else None,
                    practice_group_key=addr_key,
                    sources=["cms", "places"] if has_places else ["cms"],
                    **enrichment,
                )
                providers.append(prov)
                total_pay += enrichment.get('total_medicare_payment') or 0
                grouped_cms_idxs.add(ci)
            
            providers.sort(key=lambda x: x.total_medicare_payment or 0, reverse=True)
            
            practice_groups.append(PracticeGroup(
                name=place['name'] if place else None,
                place_id=place['place_id'] if place else None,
                address=place['address'] if place else f"{sample_addr['address']}, {sample_addr['city']}, {sample_addr['state']} {sample_addr['zip5']}",
                rating=place['rating'] if place else None,
                reviews=place['reviews'] if place else None,
                phone=place.get('phone') if place else None,
                website=place.get('website') if place else None,
                hours=place.get('hours') if place else None,
                provider_count=len(providers),
                total_medicare_payment=round(total_pay, 2),
                providers=providers,
            ))
        
        # Sort practice groups by total Medicare payment
        practice_groups.sort(key=lambda g: g.total_medicare_payment, reverse=True)
        
        # Ungrouped (solo) providers
        ungrouped = []
        for i, p in enumerate(cms_providers):
            if i not in grouped_cms_idxs:
                enrichment = enrichment_map.get(p['npi'], {})
                prov = ProviderResult(
                    npi=p['npi'],
                    first_name=p['first_name'],
                    last_name=p['last_name'],
                    provider_type=p['provider_type'],
                    address=p['address'],
                    city=p['city'],
                    state=p['state'],
                    zip5=p['zip5'],
                    sources=["cms"],
                    **enrichment,
                )
                ungrouped.append(prov)
        
        ungrouped.sort(key=lambda x: x.total_medicare_payment or 0, reverse=True)
        
        total_with_places = sum(1 for g in practice_groups for p in g.providers if "places" in p.sources)
        
        elapsed = (time.perf_counter() - t0) * 1000
        
        return UnifiedSearchResponse(
            query=specialty,
            location=location,
            places_source=places_source,
            total_providers=len(cms_providers),
            total_with_places=total_with_places,
            total_practice_groups=len(practice_groups),
            cms_provider_count=len(cms_providers),
            places_result_count=len(places_results),
            elapsed_ms=round(elapsed, 2),
            practice_groups=practice_groups,
            ungrouped_providers=ungrouped,
        )

    return router
