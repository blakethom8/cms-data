# Provider Search Intelligence Platform
## Product Architecture Document

**Version:** 1.0  
**Date:** February 2025  
**Status:** Design Phase

---

## 1. Product Overview

### 1.1 What We're Building

A multi-layered provider intelligence platform that enables physician liaisons and healthcare professionals to discover and evaluate medical providers through enriched, cross-referenced data from public and proprietary sources.

**Target Users:** Physician liaisons, healthcare network managers, referral coordinators

**Core Use Case:** Search by specialty + geography (e.g., "endocrinologist Santa Monica") and receive comprehensive provider profiles including credentials, patient reviews, web presence, insurance panels, and organizational affiliations.

### 1.2 Three-Layer Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Search & Presentation Layer              │
│              (Specialty + Geography Queries)                │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│              Unified Provider Profile Cache                 │
│         (provider_profiles: The Integration Layer)          │
└─────────────────────────────────────────────────────────────┘
                              │
          ┌───────────────────┼───────────────────┐
          ▼                   ▼                   ▼
┌──────────────────┐ ┌─────────────────┐ ┌──────────────────┐
│  Layer 1:        │ │  Layer 2:       │ │  Layer 3:        │
│  Provider        │ │  Consumer       │ │  Web             │
│  Backbone        │ │  Intelligence   │ │  Intelligence    │
│                  │ │                 │ │                  │
│  • CMS/NPPES     │ │  • Google       │ │  • LLM-extracted │
│  • Client claims │ │    Places       │ │    website data  │
│  • NPI-keyed     │ │  • Ratings      │ │  • Affiliations  │
│  • Quarterly     │ │  • Reviews      │ │  • Insurance     │
│    refresh       │ │  • Hours        │ │  • Languages     │
│                  │ │  • Photos       │ │  • Accepting new │
│                  │ │                 │ │  • Org graph     │
└──────────────────┘ └─────────────────┘ └──────────────────┘
```

### 1.3 Product Tiers

| Feature | Free | Pro |
|---------|------|-----|
| **Search** | Basic specialty + geography | Advanced filters (insurance, languages, ratings) |
| **Results** | Top 10 providers | Unlimited results |
| **Provider Data** | CMS backbone + Google ratings | Full 3-layer enrichment |
| **Freshness** | 90-day cache | 7-day cache + on-demand refresh |
| **Export** | CSV (10 records) | CSV/Excel/API (unlimited) |
| **Client Data** | N/A | Claims integration (custom pricing) |
| **Search History** | Last 5 searches | Unlimited + saved searches |
| **API Access** | No | Yes (rate-limited) |

---

## 2. Data Architecture

### 2.1 Core Tables

#### `provider_backbone`
*Source of truth for provider identity and credentials*

```sql
CREATE TABLE provider_backbone (
    npi VARCHAR(10) PRIMARY KEY,
    
    -- Identity
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    middle_name VARCHAR(50),
    credential VARCHAR(20),
    
    -- Demographics
    gender CHAR(1),
    
    -- Location (primary practice)
    address_line1 VARCHAR(200),
    address_line2 VARCHAR(200),
    city VARCHAR(100),
    state CHAR(2),
    zip VARCHAR(10),
    latitude DECIMAL(10, 7),
    longitude DECIMAL(10, 7),
    
    -- Taxonomy (specialty)
    primary_taxonomy VARCHAR(20),
    primary_specialty VARCHAR(100),
    all_taxonomies JSONB, -- Array of all taxonomy codes
    
    -- Metadata
    enumeration_date DATE,
    last_updated_cms DATE,
    source_file VARCHAR(50),
    
    -- Indexes
    INDEX idx_specialty_geo (primary_specialty, state, city),
    INDEX idx_location (latitude, longitude)
);
```

#### `provider_profiles`
*The unified cache: cross-layer enrichment lives here*

```sql
CREATE TABLE provider_profiles (
    id SERIAL PRIMARY KEY,
    npi VARCHAR(10) UNIQUE NOT NULL REFERENCES provider_backbone(npi),
    
    -- Enrichment status
    enrichment_level SMALLINT DEFAULT 0, -- 0=none, 1=L1, 2=L1+L2, 3=full
    last_enriched_at TIMESTAMP,
    enrichment_priority SMALLINT, -- For background refresh queue
    
    -- Layer 2: Consumer Intelligence
    google_place_id VARCHAR(100),
    google_rating DECIMAL(2,1),
    google_review_count INTEGER,
    google_hours JSONB,
    google_phone VARCHAR(20),
    google_website VARCHAR(500),
    google_photos JSONB,
    google_last_updated TIMESTAMP,
    
    -- Layer 3: Web Intelligence
    practice_website VARCHAR(500),
    accepting_new_patients BOOLEAN,
    accepted_insurance JSONB, -- Array of payer names
    spoken_languages JSONB, -- Array of language codes
    practice_group_name VARCHAR(200),
    practice_group_id INTEGER, -- FK to practice_groups
    hospital_affiliations JSONB, -- Array of hospital names/IDs
    board_certifications JSONB,
    medical_school VARCHAR(200),
    web_bio TEXT,
    web_last_scraped TIMESTAMP,
    web_extraction_confidence DECIMAL(3,2), -- 0-1 confidence score
    
    -- Search optimization
    search_count INTEGER DEFAULT 0, -- Popularity metric
    last_searched_at TIMESTAMP,
    
    -- Cache invalidation
    needs_refresh BOOLEAN DEFAULT FALSE,
    
    -- Indexes
    INDEX idx_npi (npi),
    INDEX idx_enrichment (enrichment_level, needs_refresh),
    INDEX idx_popularity (search_count DESC, last_searched_at DESC),
    INDEX idx_google_place (google_place_id)
);
```

#### `practice_groups`
*Organizational entities extracted from web intelligence*

```sql
CREATE TABLE practice_groups (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) UNIQUE NOT NULL,
    website VARCHAR(500),
    parent_organization_id INTEGER REFERENCES practice_groups(id),
    
    -- Metadata
    provider_count INTEGER DEFAULT 0,
    first_discovered TIMESTAMP,
    last_verified TIMESTAMP,
    confidence_score DECIMAL(3,2),
    
    -- Location
    primary_city VARCHAR(100),
    primary_state CHAR(2),
    
    INDEX idx_name (name),
    INDEX idx_location (primary_state, primary_city)
);
```

#### `client_claims_data`
*Optional: client-specific claims intelligence*

```sql
CREATE TABLE client_claims_data (
    id SERIAL PRIMARY KEY,
    client_id INTEGER NOT NULL, -- FK to clients table
    npi VARCHAR(10) NOT NULL REFERENCES provider_backbone(npi),
    
    -- Claims intelligence
    claim_count INTEGER,
    total_charges DECIMAL(12,2),
    date_range_start DATE,
    date_range_end DATE,
    
    -- Client-specific metrics
    referral_count INTEGER,
    patient_satisfaction_score DECIMAL(3,2),
    avg_wait_days INTEGER,
    specialty_services JSONB,
    
    -- Metadata
    uploaded_at TIMESTAMP,
    last_updated TIMESTAMP,
    
    UNIQUE (client_id, npi),
    INDEX idx_client_npi (client_id, npi)
);
```

#### `enrichment_queue`
*Background job tracking for progressive enrichment*

```sql
CREATE TABLE enrichment_queue (
    id SERIAL PRIMARY KEY,
    npi VARCHAR(10) NOT NULL REFERENCES provider_backbone(npi),
    
    job_type VARCHAR(50), -- 'google_places', 'web_scrape', 'llm_extract'
    priority SMALLINT DEFAULT 5, -- 1=highest, 10=lowest
    
    status VARCHAR(20), -- 'queued', 'processing', 'completed', 'failed'
    attempts INTEGER DEFAULT 0,
    
    queued_at TIMESTAMP DEFAULT NOW(),
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    
    error_message TEXT,
    
    INDEX idx_queue_status (status, priority, queued_at),
    INDEX idx_npi_job (npi, job_type)
);
```

### 2.2 NPI as Universal Join Key

All layers connect via NPI (National Provider Identifier):

```
provider_backbone.npi (1:1) ← provider_profiles.npi
                                    ↓
                            (1:1) client_claims_data.npi
                            (N:1) practice_groups.id
                            (1:1) google_place_id → external API
```

**Why NPI?**
- Federally mandated unique identifier
- Present in all CMS data and most claims data
- Portable across systems and geographies
- Permanent (doesn't change when providers move)

---

## 3. Enrichment Pipeline: The Timing Model

### 3.1 The Hybrid Strategy

**Core Principle:** Pre-seed common searches, enrich on-demand for the long tail, cache everything.

```
┌────────────────────────────────────────────────────────────────┐
│                     ENRICHMENT STRATEGY                        │
└────────────────────────────────────────────────────────────────┘

Pre-Computed (Tier 1):
  • Top 50 metro areas × Top 20 specialties
  • ~1,000 geography+specialty combinations
  • ~50K-100K providers (8-10% of provider base)
  • Enriched to Level 3 (full stack)
  • Refresh: Weekly for Google, monthly for web
  • Coverage: ~80% of actual user searches

On-Demand (Tier 2):
  • First search triggers enrichment
  • Progressive loading (L1 instant → L2 @ 2s → L3 @ 5s)
  • Cache persists indefinitely
  • Covers: Long-tail geographies, rare specialties
  • User tolerance: 5-second initial load acceptable

Background Queue (Tier 3):
  • Refreshes stale cache entries
  • Re-enriches high-traffic providers every 7 days
  • Low-priority LLM extraction for uncommonly searched providers
  • Runs during off-peak hours
```

### 3.2 Pre-Seeding Matrix

**Target Coverage:** 1,000 specialty+geography combinations

| Geography Tier | Count | Example | Specialty Count |
|----------------|-------|---------|-----------------|
| **Major Metro** | 15 | Los Angeles, New York, Chicago | 30 specialties |
| **Mid-Size Cities** | 35 | Santa Monica, Berkeley, Pasadena | 20 specialties |
| **States (general)** | 50 | California, Texas, Florida | 10 specialties |

**Top 30 Specialties for Pre-Seeding:**
1. Internal Medicine
2. Family Medicine
3. Cardiology
4. Orthopedic Surgery
5. Dermatology
6. Gastroenterology
7. Obstetrics & Gynecology
8. Pediatrics
9. Psychiatry
10. Neurology
11. Ophthalmology
12. Anesthesiology
13. Radiology
14. Emergency Medicine
15. Physical Medicine & Rehabilitation
16. Endocrinology
17. Pulmonology
18. Rheumatology
19. Urology
20. Otolaryngology (ENT)
21. General Surgery
22. Oncology
23. Nephrology
24. Infectious Disease
25. Allergy & Immunology
26. Hematology
27. Pain Management
28. Sports Medicine
29. Plastic Surgery
30. Vascular Surgery

**Pre-Seeding Job:**
```python
# Nightly job: Pre-seed common searches
for geography in TOP_GEOGRAPHIES:
    for specialty in COMMON_SPECIALTIES:
        providers = search_backbone(specialty, geography, limit=200)
        for provider in providers:
            if not is_enriched(provider.npi, level=3):
                enqueue_enrichment(provider.npi, priority=3)
```

### 3.3 Cache Invalidation Strategy

| Data Layer | Refresh Frequency | Trigger |
|------------|-------------------|---------|
| **Provider Backbone (L1)** | 90 days | CMS quarterly release |
| **Google Places (L2)** | 7 days (high-traffic) / 30 days (low-traffic) | Background job |
| **Web Intelligence (L3)** | 30 days (high-traffic) / 90 days (low-traffic) | Background job or user-triggered refresh |
| **Client Claims** | On upload | Client data refresh |

**High-traffic definition:** `search_count > 5` OR `last_searched_at < 7 days ago`

**Refresh Logic:**
```python
def needs_refresh(profile):
    if profile.search_count > 10:  # Popular provider
        return days_since(profile.last_enriched_at) > 7
    elif profile.search_count > 2:  # Moderate traffic
        return days_since(profile.last_enriched_at) > 30
    else:  # Long tail
        return days_since(profile.last_enriched_at) > 90
```

### 3.4 Geographic Indexing

**Use PostGIS for location-based queries:**

```sql
-- Add geography column for fast radius searches
ALTER TABLE provider_backbone 
  ADD COLUMN location GEOGRAPHY(POINT, 4326);

UPDATE provider_backbone 
  SET location = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326);

CREATE INDEX idx_location_gist ON provider_backbone USING GIST(location);

-- Example query: Find endocrinologists within 10 miles of Santa Monica
SELECT pb.*, pp.google_rating
FROM provider_backbone pb
JOIN provider_profiles pp ON pb.npi = pp.npi
WHERE pb.primary_specialty = 'Endocrinology'
  AND ST_DWithin(
    pb.location,
    ST_SetSRID(ST_MakePoint(-118.4912, 34.0195), 4326)::geography,
    16093  -- 10 miles in meters
  )
ORDER BY pp.google_rating DESC NULLS LAST, pb.last_name;
```

---

## 4. Search Flow

### 4.1 Search: "endocrinologist Santa Monica"

#### Step 1: Query Parsing
```
Input: "endocrinologist Santa Monica"
↓
Parser extracts:
  - specialty: "Endocrinology" (fuzzy match + taxonomy lookup)
  - geography: "Santa Monica, CA" (geocode to lat/lng: 34.0195, -118.4912)
  - radius: 10 miles (default)
```

#### Step 2: Backbone Search
```sql
-- Fast search on indexed backbone
SELECT npi, first_name, last_name, address, city, state, zip
FROM provider_backbone
WHERE primary_specialty = 'Endocrinology'
  AND ST_DWithin(location, ST_MakePoint(-118.4912, 34.0195)::geography, 16093)
LIMIT 200;

-- Returns: 47 NPIs in ~50ms
```

#### Step 3: Cache Check
```sql
-- Check enrichment status for these NPIs
SELECT npi, enrichment_level, last_enriched_at, google_rating, practice_group_name
FROM provider_profiles
WHERE npi IN (...list of 47 NPIs...);

-- Result: 
--   • 32 providers: enrichment_level = 3 (full) → Return immediately
--   • 10 providers: enrichment_level = 2 (Google only) → Trigger L3 in background
--   • 5 providers: enrichment_level = 0 (none) → Enqueue urgent enrichment
```

#### Step 4: Progressive Response

**Tier 1: Immediate (< 100ms)**
```json
{
  "search_id": "abc123",
  "query": {
    "specialty": "Endocrinology",
    "location": "Santa Monica, CA",
    "radius_miles": 10
  },
  "results": {
    "total": 47,
    "enriched": 32,
    "enriching": 15
  },
  "providers": [
    {
      "npi": "1234567890",
      "name": "Jane Smith, MD",
      "specialty": "Endocrinology",
      "address": "123 Main St, Santa Monica, CA",
      "distance_miles": 1.2,
      "enrichment_status": "complete",
      "google_rating": 4.7,
      "google_review_count": 143,
      "accepting_new_patients": true,
      "practice_group": "Santa Monica Medical Group"
    },
    // ... 31 more fully enriched
    {
      "npi": "9876543210",
      "name": "John Doe, MD",
      "enrichment_status": "partial",
      "google_rating": null  // Will be enriched shortly
    }
    // ... 14 more pending enrichment
  ],
  "enrichment_in_progress": true
}
```

**Tier 2: Google Places Enrichment (2-4 seconds)**

*Background jobs start immediately for 15 un-enriched providers*

```python
# Parallel execution (5 concurrent workers)
async def enrich_google_places(npis):
    tasks = [fetch_google_place(npi) for npi in npis]
    results = await asyncio.gather(*tasks)
    
    for npi, place_data in results:
        update_profile(npi, level=2, data=place_data)
        notify_client(search_id, npi, update=place_data)

# Client receives WebSocket updates:
{
  "search_id": "abc123",
  "update_type": "enrichment",
  "npi": "9876543210",
  "data": {
    "google_rating": 4.5,
    "google_review_count": 89,
    "google_hours": {...},
    "google_phone": "(310) 555-0123"
  }
}
```

**Tier 3: Web Intelligence (5-10 seconds)**

*LLM extraction runs in background, lower priority*

```python
# For high-value providers (top search results)
async def enrich_web_intelligence(npi, priority='normal'):
    # 1. Find provider website
    website = find_provider_website(npi)  # From Google Places or search
    
    # 2. Scrape website
    html = scrape_url(website)
    
    # 3. LLM extraction
    extracted = llm_extract({
        'html': html,
        'provider_name': get_name(npi),
        'schema': WEB_INTELLIGENCE_SCHEMA
    })
    
    # 4. Update profile
    update_profile(npi, level=3, data=extracted)
    notify_client(search_id, npi, update=extracted)

# Client receives final update:
{
  "search_id": "abc123",
  "update_type": "enrichment",
  "npi": "9876543210",
  "data": {
    "accepting_new_patients": true,
    "accepted_insurance": ["Blue Shield", "Aetna", "United Healthcare"],
    "spoken_languages": ["en", "es"],
    "practice_group_name": "Westside Endocrine Associates",
    "hospital_affiliations": ["UCLA Medical Center", "Providence Saint John's"]
  }
}
```

### 4.2 Cache Hit Path (Optimized)

**Scenario:** User searches "cardiologist Los Angeles" (pre-seeded)

```
1. Parse query → specialty + geography [10ms]
2. Search backbone → 2,847 NPIs [50ms]
3. Fetch profiles (enrichment_level=3) → JOIN [30ms]
4. Sort by rating + distance [10ms]
5. Return top 200 → [< 100ms total]
```

**SQL (single query):**
```sql
SELECT 
    pb.npi,
    pb.first_name,
    pb.last_name,
    pb.credential,
    pb.address_line1,
    pb.city,
    pb.state,
    pb.zip,
    ST_Distance(pb.location, $1::geography) / 1609.34 AS distance_miles,
    pp.google_rating,
    pp.google_review_count,
    pp.google_phone,
    pp.accepting_new_patients,
    pp.practice_group_name,
    pp.accepted_insurance,
    pp.spoken_languages
FROM provider_backbone pb
JOIN provider_profiles pp ON pb.npi = pp.npi
WHERE pb.primary_specialty = $2
  AND ST_DWithin(pb.location, $1::geography, $3)
  AND pp.enrichment_level = 3
  AND pp.needs_refresh = FALSE
ORDER BY pp.google_rating DESC NULLS LAST, distance_miles ASC
LIMIT 200;
```

**Result:** Sub-100ms response, fully enriched data.

### 4.3 Cache Miss Path (Long Tail)

**Scenario:** User searches "endocrinologist Bakersfield" (not pre-seeded)

```
1. Parse query → specialty + geography [10ms]
2. Search backbone → 12 NPIs [30ms]
3. Check profiles → 0 enriched [20ms]
4. Return backbone data immediately [60ms total]
5. Enqueue urgent enrichment jobs (all 12 NPIs, priority=1)
6. Google Places enrichment → 2-3 seconds
7. Web intelligence enrichment → 5-8 seconds
8. Progressive updates via WebSocket
```

**User Experience:**
- **T+60ms:** See 12 providers with basic info (name, address, specialty)
- **T+2.5s:** Ratings, reviews, hours appear for all 12
- **T+6s:** Insurance, languages, affiliations appear
- **Subsequent searches:** Instant (cached)

---

## 5. Entity Resolution Strategy

### 5.1 The Matching Challenge

**Problem:** How do we know that:
- NPI `1234567890` (CMS: "Jane Smith, Endocrinology, 123 Main St")
- Google Place ID `ChIJ...` ("Dr. Jane Smith - Santa Monica Endocrine")
- Website mention ("Jane A. Smith, MD, FACE")

...all refer to the same provider?

### 5.2 Multi-Phase Resolution

#### Phase 1: Exact NPI Match
```python
# CMS data already has NPI → direct link
# Client claims data has NPI → direct link
# Easy: no ambiguity
```

#### Phase 2: Google Places Matching
```python
def match_google_place(npi):
    provider = get_provider(npi)
    
    # Build search query
    query = f"{provider.full_name} {provider.specialty} {provider.address}"
    
    # Google Places Text Search
    results = google_places_search(query, location=(lat, lng), radius=100)
    
    # Scoring system
    for place in results:
        score = 0
        
        # Name similarity (Levenshtein distance)
        if name_similarity(provider.full_name, place.name) > 0.85:
            score += 40
        
        # Address proximity
        if distance(provider.location, place.location) < 50:  # meters
            score += 30
        
        # Phone match (if available in CMS)
        if provider.phone and place.phone:
            if normalize_phone(provider.phone) == normalize_phone(place.phone):
                score += 20
        
        # Category match (doctor/health)
        if 'doctor' in place.types or 'health' in place.types:
            score += 10
        
        # Confidence threshold
        if score >= 70:
            return place.place_id, confidence=score/100
    
    return None, confidence=0
```

**Fallback:** If no confident match, try:
1. Direct website lookup from NPPES (if available)
2. Manual Google search: `"Dr. [Name]" "[City]" "[Specialty]"`
3. Mark as "needs_manual_review" for low-confidence matches

#### Phase 3: LLM-Assisted Entity Resolution

For ambiguous cases, use LLM reasoning:

```python
def llm_entity_resolution(npi, google_candidates):
    provider = get_provider(npi)
    
    prompt = f"""
    I need to determine if any of these Google Places results match this provider:
    
    Provider from CMS:
    - Name: {provider.full_name}
    - Specialty: {provider.specialty}
    - Address: {provider.address}
    - NPI: {provider.npi}
    
    Google Places Candidates:
    {json.dumps(google_candidates, indent=2)}
    
    For each candidate, assess:
    1. Name match quality (accounting for middle initials, credentials)
    2. Location proximity
    3. Specialty/category alignment
    4. Any red flags (wrong gender, clearly different person)
    
    Return JSON:
    {{
        "best_match_id": "ChIJ..." or null,
        "confidence": 0.0-1.0,
        "reasoning": "brief explanation"
    }}
    """
    
    response = llm_call(prompt, model='gpt-4o-mini')  # Fast + cheap
    return response
```

**Cost:** ~$0.001 per resolution, only for ambiguous cases (~10% of providers)

### 5.3 Web Mention Extraction

**Challenge:** Extract structured data from unstructured HTML/text.

**Example Website Content:**
```html
<div class="provider-card">
  <h2>Jane A. Smith, MD, FACE</h2>
  <p>Board-certified endocrinologist specializing in diabetes and thyroid disorders.</p>
  <p><strong>Accepting new patients:</strong> Yes</p>
  <p><strong>Insurance accepted:</strong> Blue Shield, Aetna, United Healthcare, Medicare</p>
  <p><strong>Languages:</strong> English, Spanish</p>
  <p>Dr. Smith is affiliated with UCLA Medical Center and Providence Saint John's Health Center.</p>
</div>
```

**LLM Extraction Prompt:**
```python
def extract_web_intelligence(html, provider_context):
    prompt = f"""
    Extract structured information about this healthcare provider from their website.
    
    Provider Context (for validation):
    - Name: {provider_context.name}
    - Specialty: {provider_context.specialty}
    
    Website HTML:
    {html[:10000]}  # Truncate to first 10K chars
    
    Extract and return JSON:
    {{
        "accepting_new_patients": true/false/null,
        "accepted_insurance": ["payer1", "payer2", ...] or null,
        "spoken_languages": ["en", "es", ...] or null,
        "practice_group_name": "string" or null,
        "hospital_affiliations": ["hospital1", ...] or null,
        "board_certifications": ["cert1", ...] or null,
        "medical_school": "string" or null,
        "bio_summary": "1-2 sentence summary" or null,
        "confidence": 0.0-1.0  # Overall extraction confidence
    }}
    
    Rules:
    - Only extract information explicitly stated
    - Normalize insurance names (e.g., "BCBS" → "Blue Cross Blue Shield")
    - Use ISO 639-1 codes for languages (e.g., "Spanish" → "es")
    - If unsure, use null
    - Set confidence based on clarity and completeness of source
    """
    
    response = llm_call(prompt, model='gpt-4o-mini', structured_output=True)
    return response
```

**Validation:**
```python
def validate_extraction(extracted, provider):
    # Sanity checks
    if extracted.specialty and extracted.specialty != provider.specialty:
        extracted.confidence *= 0.7  # Penalize mismatch
    
    if extracted.location and distance(extracted.location, provider.location) > 50_000:  # 50km
        extracted.confidence *= 0.5  # Likely wrong provider
    
    # Store with confidence score
    if extracted.confidence >= 0.7:
        save_extraction(provider.npi, extracted, confidence=extracted.confidence)
    else:
        flag_for_review(provider.npi, extracted)
```

### 5.4 Confidence Scoring

**Overall Profile Confidence:**
```python
def calculate_profile_confidence(profile):
    scores = []
    
    # Layer 1: Backbone (always 1.0 - source of truth)
    scores.append(1.0)
    
    # Layer 2: Google Places match
    if profile.google_place_id:
        scores.append(profile.google_match_confidence or 0.8)
    
    # Layer 3: Web intelligence
    if profile.web_extraction_confidence:
        scores.append(profile.web_extraction_confidence)
    
    # Weighted average (backbone = 50%, Google = 30%, Web = 20%)
    weights = [0.5, 0.3, 0.2][:len(scores)]
    confidence = sum(s * w for s, w in zip(scores, weights)) / sum(weights)
    
    return confidence
```

**Display to Users:**
```json
{
  "npi": "1234567890",
  "name": "Jane Smith, MD",
  "data_quality": {
    "overall": 0.92,
    "sources": {
      "cms_verified": true,
      "google_places_match": "high_confidence",
      "web_data_freshness": "30_days_old"
    }
  }
}
```

---

## 6. Web Intelligence Layer (The Moat)

### 6.1 What Gets Extracted

**High-Value Fields (extracted for every provider):**

| Field | Value to Users | Extraction Difficulty |
|-------|----------------|----------------------|
| **Accepting New Patients** | Critical for referrals | Easy (usually explicit) |
| **Insurance Accepted** | Filter/match with patient needs | Medium (list extraction) |
| **Languages Spoken** | Accessibility for diverse patients | Easy (usually listed) |
| **Practice Group Name** | Understand organizational context | Medium (entity extraction) |
| **Hospital Affiliations** | Referral pathways, quality proxy | Medium (list + entity resolution) |

**Medium-Value Fields (extracted opportunistically):**

- Board certifications (quality signal)
- Medical school / residency (prestige signal)
- Fellowship training (sub-specialization)
- Professional bio (human context)
- Patient testimonials (alternative to Google reviews)
- Office amenities (parking, accessibility)

**Graph Intelligence (advanced - future):**

- Practice group → parent organization → health system
- Provider → referral patterns → specialist network
- Provider → hospital → quality metrics

### 6.2 Practice Group / Org Affiliation Mapping

**The Insight:** Most competitors show individual providers. We show the **organizational graph**.

**Example Use Case:**
```
Search: "cardiologist Santa Monica"

Competitor View:
- Dr. A (individual)
- Dr. B (individual)  
- Dr. C (individual)

Our View:
- Santa Monica Heart Institute (7 providers)
  ├─ Dr. A (interventional cardiology)
  ├─ Dr. B (electrophysiology)
  └─ Dr. C (heart failure)
  
- UCLA Cardiology Group (12 providers)
- [Individual providers not in groups]
```

**Extraction Strategy:**
```python
def extract_practice_groups(website_html):
    # Look for organizational signals
    signals = [
        'practice', 'group', 'associates', 'medical center',
        'health system', 'clinic', 'physicians group'
    ]
    
    # LLM extraction
    prompt = f"""
    From this website, extract the practice/organizational structure:
    
    {website_html}
    
    Return:
    {{
        "practice_group_name": "official name",
        "parent_organization": "if part of larger system",
        "practice_type": "single_provider | group_practice | hospital_employed | academic",
        "provider_count_estimate": integer or null,
        "locations": ["city1", "city2", ...]
    }}
    """
    
    return llm_call(prompt)

# Build the graph
def build_org_graph():
    groups = {}
    
    for profile in get_all_enriched_profiles():
        if profile.practice_group_name:
            group_id = get_or_create_group(profile.practice_group_name)
            link_provider_to_group(profile.npi, group_id)
            
            if profile.parent_organization:
                parent_id = get_or_create_group(profile.parent_organization)
                link_child_to_parent(group_id, parent_id)
```

**Display:**
```json
{
  "search_results": {
    "individual_providers": 47,
    "practice_groups": [
      {
        "id": 123,
        "name": "Santa Monica Heart Institute",
        "provider_count": 7,
        "specialties": ["Cardiology", "Interventional Cardiology"],
        "avg_rating": 4.6,
        "accepting_new_patients": true,
        "locations": ["Santa Monica", "West LA"],
        "providers": [
          {"npi": "1234567890", "name": "Dr. A", "subspecialty": "Interventional"},
          // ...
        ]
      }
    ]
  }
}
```

**The Moat:** Competitors can't easily replicate this because:
1. Requires LLM extraction at scale
2. Needs entity resolution across inconsistent naming
3. Graph construction is non-trivial
4. Ongoing maintenance as orgs merge/split/rename

### 6.3 Competitive Advantages

| Feature | Us | Healthgrades | Zocdoc | Doximity |
|---------|----|--------------| -------|----------|
| **CMS Backbone** | ✅ | ✅ | ✅ | ✅ |
| **Google Reviews** | ✅ | ✅ | ✅ | ❌ |
| **Insurance Panels** | ✅ (web-extracted) | ✅ (self-reported) | ✅ (self-reported) | ❌ |
| **Accepting New Patients** | ✅ (fresh, web-scraped) | ❌ (stale) | ✅ (if on platform) | ❌ |
| **Practice Group Mapping** | ✅ (automated) | ❌ | ❌ | ❌ |
| **Org Hierarchy** | ✅ | ❌ | ❌ | ❌ |
| **Client Claims Integration** | ✅ | ❌ | ❌ | ❌ (different market) |
| **No Self-Reporting Needed** | ✅ | ❌ | ❌ | ❌ |

**Key Differentiator:** We're the only platform that:
1. Doesn't require provider opt-in
2. Auto-discovers organizational structure
3. Integrates client proprietary data
4. Refreshes continuously from web sources

### 6.4 Refresh Cadence

| Data Type | High-Traffic Refresh | Low-Traffic Refresh | Trigger |
|-----------|---------------------|-------------------|---------|
| **Accepting New Patients** | 7 days | 30 days | Critical for referrals |
| **Insurance Panels** | 30 days | 90 days | Changes less frequently |
| **Practice Group** | 90 days | Never (until changed) | Relatively stable |
| **Affiliations** | 90 days | Never | Stable |
| **Bio/Description** | Never | Never | Low-priority |

**Smart Refresh:**
```python
def prioritize_refresh():
    # High-priority refresh queue
    refresh_queue = []
    
    # 1. Recently searched providers with stale Google data
    refresh_queue += get_providers(
        search_count > 5,
        days_since(google_last_updated) > 7
    )
    
    # 2. Providers with missing critical fields
    refresh_queue += get_providers(
        google_rating IS NULL,
        enrichment_level >= 2
    )
    
    # 3. Providers in pre-seed matrix with stale web data
    refresh_queue += get_providers(
        in_preseed_matrix = True,
        days_since(web_last_scraped) > 30
    )
    
    # Execute during off-peak hours
    schedule_background_jobs(refresh_queue, max_concurrent=10)
```

---

## 7. Client Data Integration

### 7.1 The Value Proposition

**Without Client Data:**
"Here are 47 endocrinologists in Santa Monica, sorted by Google rating."

**With Client Data:**
"Here are 47 endocrinologists in Santa Monica. **12 already accept your patients** (avg 8.3 referrals/month), **5 are high-performers** (>50 claims, <10 day wait), and **30 are untapped opportunities**."

### 7.2 Integration Model

**Client uploads claims data:**
```csv
npi,claim_count,total_charges,date_range_start,date_range_end,referral_count,avg_wait_days
1234567890,127,58493.21,2024-01-01,2024-12-31,23,7
9876543210,64,31245.89,2024-01-01,2024-12-31,11,12
...
```

**Ingestion Pipeline:**
```python
def ingest_client_claims(client_id, csv_file):
    records = parse_csv(csv_file)
    
    for record in records:
        # Validate NPI exists in backbone
        if not npi_exists(record.npi):
            log_warning(f"Unknown NPI: {record.npi}")
            continue
        
        # Upsert claims data
        upsert_client_claims(
            client_id=client_id,
            npi=record.npi,
            data=record,
            uploaded_at=now()
        )
        
        # Trigger enrichment if not already enriched
        if not is_enriched(record.npi, level=2):
            enqueue_enrichment(record.npi, priority=2)
```

### 7.3 Enhanced Search Results

**Search Query with Client Context:**
```sql
SELECT 
    pb.*,
    pp.google_rating,
    pp.accepting_new_patients,
    pp.practice_group_name,
    cc.claim_count,
    cc.referral_count,
    cc.avg_wait_days,
    -- Client-specific ranking
    CASE 
        WHEN cc.referral_count > 10 THEN 'established'
        WHEN cc.referral_count > 0 THEN 'emerging'
        ELSE 'untapped'
    END AS relationship_status
FROM provider_backbone pb
JOIN provider_profiles pp ON pb.npi = pp.npi
LEFT JOIN client_claims_data cc ON pb.npi = cc.npi AND cc.client_id = $client_id
WHERE pb.primary_specialty = $specialty
  AND ST_DWithin(pb.location, $location::geography, $radius)
ORDER BY 
    -- Prioritize existing relationships
    cc.referral_count DESC NULLS LAST,
    pp.google_rating DESC NULLS LAST,
    distance ASC;
```

**Enhanced Result Display:**
```json
{
  "npi": "1234567890",
  "name": "Jane Smith, MD",
  "google_rating": 4.7,
  "accepting_new_patients": true,
  "client_intelligence": {
    "relationship_status": "established",
    "referral_count_12mo": 23,
    "claim_volume": 127,
    "avg_wait_days": 7,
    "total_charges_12mo": 58493.21,
    "trend": "growing"  // Calculated from historical data
  },
  "recommendation": "High-value existing partner. Consider expanding referrals."
}
```

### 7.4 Client-Specific Features (Pro Tier)

**Feature 1: Relationship Mapping**
```
Visual dashboard showing:
- Established partners (green)
- Emerging relationships (yellow)
- Untapped opportunities (blue)
- Declining partners (red - decreasing referrals)
```

**Feature 2: Gap Analysis**
```
"You're underutilizing cardiology in West LA:
 • 47 cardiologists available
 • You only refer to 8
 • Top-rated providers with no referrals: [list]
"
```

**Feature 3: Competitive Intelligence**
```
"Compared to similar organizations:
 • Your avg provider rating: 4.3 ⭐
 • Industry benchmark: 4.6 ⭐
 • Opportunity: 12 providers rated >4.8 with no referrals
"
```

---

## 8. Cost Model

### 8.1 Pre-Compute vs On-Demand Economics

**Scenario 1: Pre-Compute Everything (7M NPIs)**

| Layer | Cost per NPI | Total Cost |
|-------|--------------|------------|
| Google Places Match | $0.03 | $210,000 |
| LLM Web Extraction | $0.01 | $70,000 |
| **Total Initial** | **$0.04** | **$280,000** |
| Quarterly Refresh | $0.04 | $280,000/quarter = **$1.12M/year** |

**Verdict:** Financially infeasible.

---

**Scenario 2: Pre-Seed Top 1,000 Combinations (~100K providers)**

| Layer | Cost per NPI | Total Cost |
|-------|--------------|------------|
| Google Places | $0.03 | $3,000 |
| LLM Extraction | $0.01 | $1,000 |
| **Total Initial** | **$0.04** | **$4,000** |
| Monthly Refresh (20% rotation) | $0.04 × 20K | $800/month = **$9,600/year** |

**Coverage:** ~80% of actual user searches (based on Pareto principle)

**Verdict:** Reasonable investment for great UX on common searches.

---

**Scenario 3: On-Demand Only (no pre-seeding)**

| Metric | Value |
|--------|-------|
| Avg search results | 50 providers |
| Enrichment needed (50% cache miss) | 25 providers |
| Cost per search | 25 × $0.04 = **$1.00** |
| Monthly searches (estimate) | 10,000 |
| **Monthly cost** | **$10,000** |

**User Experience:** 
- First search: 5-10 second wait ⚠️
- Repeat search: Instant ✅

**Verdict:** Cheaper, but poor first-impression UX. Not viable for competitive product.

---

**Scenario 4: Hybrid (Pre-Seed + On-Demand)**

| Component | Cost |
|-----------|------|
| Pre-seed (100K providers) | $4,000 initial + $800/month |
| On-demand (long tail, 20% of searches) | 2,000 searches × $1.00 = $2,000/month |
| **Total Ongoing** | **$2,800/month = $33,600/year** |

**Coverage:**
- 80% of searches: Instant (pre-seeded)
- 20% of searches: 5-second enrichment (cached after first search)

**Verdict:** ✅ Best balance of cost and UX. **Recommended approach.**

### 8.2 Cost Optimization Strategies

**1. LLM Model Selection**
```python
# Use cheaper models for extraction (structured output)
GPT_4O_MINI = 0.15/1M input tokens, 0.60/1M output tokens
# Avg extraction: ~5K input, ~500 output = $0.001/provider

# Reserve expensive models for ambiguous entity resolution only
GPT_4O = 2.50/1M input, 10.00/1M output
# Use only when confidence < 0.7 (10% of cases)
```

**2. Google Places Optimization**
```python
# Use Places API efficiently
# - Text Search: $0.032/request (can return multiple candidates)
# - Place Details: $0.017/request (get all fields in one call)

# Optimization: Batch candidates in single Text Search when possible
search_query = f"{name} {specialty} {city}"  # One search → multiple candidates
candidates = google_places_text_search(search_query)  # $0.032
best_match = score_candidates(candidates)  # Free (local logic)
details = google_places_details(best_match.place_id)  # $0.017
# Total: $0.049 (vs $0.032 × N searches)
```

**3. Caching Strategy**
```python
# Never re-enrich unnecessarily
# Cache invalidation only when:
# - Data is stale (age > threshold)
# - User explicitly requests refresh
# - External signal (e.g., practice moved, provider retired)

# Avoid common mistakes:
# ❌ Re-scraping website on every search
# ❌ Re-calling Google Places API when place_id is cached
# ✅ Use cached data until stale
```

**4. Selective Enrichment**
```python
# Don't enrich every provider in search results
# Prioritize top N results (e.g., top 20)

def selective_enrichment(search_results):
    top_results = search_results[:20]  # Only enrich what users will see
    
    for provider in top_results:
        if not is_enriched(provider.npi):
            enqueue_enrichment(provider.npi, priority=1)
    
    # Background enrich the rest (low priority)
    for provider in search_results[20:]:
        if not is_enriched(provider.npi):
            enqueue_enrichment(provider.npi, priority=10)
```

### 8.3 Revenue Model (for context)

**Free Tier:**
- Cost: ~$0 (only backbone data, no enrichment)
- Monetization: Lead generation for Pro tier

**Pro Tier: $199/month per user**
- Estimated usage: 500 searches/month
- Cost: ~$50/month (hybrid model, 80% cache hit rate)
- **Gross margin: 75%**

**Enterprise Tier: Custom pricing ($2K-$10K/month)**
- Includes client data integration
- Dedicated support
- API access
- Custom reporting
- **Gross margin: 80%** (scale economics, shared infrastructure)

**Break-Even Analysis:**
- Fixed costs: $5K/month (infrastructure, pre-seeding)
- Variable costs: $2,800/month (on-demand enrichment)
- **Break-even: 40 Pro users** ($199 × 40 = $7,960)

---

## 9. Implementation Phases

### Phase 1: MVP (Current → 4 weeks)

**Goal:** Prove core search functionality with single-layer enrichment.

**Deliverables:**
- [x] CMS/NPPES data pipeline (COMPLETE - on Hetzner server)
- [x] PostgreSQL database with provider_backbone table (COMPLETE)
- [x] Basic search API (specialty + geography) (COMPLETE)
- [ ] Google Places integration (L2 enrichment)
- [ ] provider_profiles table implementation
- [ ] Simple web UI for search
- [ ] Cache hit/miss logic
- [ ] Background enrichment queue (basic)

**Tech Stack:**
- PostgreSQL 15 with PostGIS
- Python 3.11 (FastAPI for API)
- Redis (job queue)
- React (minimal UI)

**Success Criteria:**
- Search "cardiologist Los Angeles" returns 100+ results in <200ms
- 50% of results have Google ratings (L2 enrichment)
- Cache persistence across searches

---

### Phase 2: Web Intelligence Layer (Weeks 5-8)

**Goal:** Add LLM-powered web extraction (L3) and entity resolution.

**Deliverables:**
- [ ] Web scraping infrastructure (respectful crawling, robots.txt)
- [ ] LLM extraction pipeline (GPT-4o-mini integration)
- [ ] Entity resolution logic (Google Place matching)
- [ ] practice_groups table and org mapping
- [ ] Confidence scoring system
- [ ] Progressive loading (WebSocket updates)

**New Features:**
- "Accepting new patients" filter
- "Insurance accepted" filter
- Practice group grouping in results
- Confidence indicators in UI

**Success Criteria:**
- 70% of searched providers have L3 enrichment within 10 seconds
- Entity resolution confidence >0.85 for 90% of matched providers
- Practice group mapping for >50% of providers

---

### Phase 3: Pre-Seeding & Optimization (Weeks 9-12)

**Goal:** Pre-seed common searches, optimize performance, refine UX.

**Deliverables:**
- [ ] Pre-seeding job (top 1,000 geo+specialty combos)
- [ ] Intelligent refresh scheduler
- [ ] Geographic indexing optimization
- [ ] Advanced search filters (ratings, distance, insurance)
- [ ] Search analytics (track popular queries)
- [ ] Cost monitoring dashboard

**Optimizations:**
- Sub-100ms response for pre-seeded searches
- Reduce on-demand enrichment cost by 50% (smarter caching)
- Implement pagination and infinite scroll

**Success Criteria:**
- 80% of searches return instant results (pre-seeded)
- Average search latency <150ms
- On-demand enrichment cost <$1,000/month

---

### Phase 4: Client Data Integration (Weeks 13-16)

**Goal:** Enable Pro tier features with client claims data.

**Deliverables:**
- [ ] client_claims_data table and ingestion pipeline
- [ ] CSV upload interface (with validation)
- [ ] Client-specific search rankings
- [ ] Relationship status indicators
- [ ] Gap analysis reports
- [ ] Export functionality (CSV, Excel)

**New Features:**
- "Your network" filter (providers you already use)
- "Untapped opportunities" view
- Referral volume trends
- Competitive benchmarking

**Success Criteria:**
- Client data ingestion <5 minutes for 10K NPIs
- Enhanced search results show client intelligence
- 3 pilot customers onboarded

---

### Phase 5: Scale & Polish (Weeks 17-20)

**Goal:** Production-ready platform, full feature set, marketing launch.

**Deliverables:**
- [ ] User authentication and multi-tenancy
- [ ] Billing integration (Stripe)
- [ ] API documentation and rate limiting
- [ ] Mobile-responsive UI
- [ ] Comprehensive test suite
- [ ] Monitoring and alerting (Sentry, Datadog)
- [ ] Documentation and onboarding flow

**Polish:**
- A/B test search result ranking algorithms
- Optimize LLM prompts for accuracy
- Implement feedback loop (user corrections)
- SEO optimization for organic discovery

**Success Criteria:**
- 10 paying customers
- 99.9% API uptime
- <1% error rate on enrichment jobs
- Positive user feedback (NPS >40)

---

### Current State (Hetzner Server)

**What's Built:**
```
~/cms-data/
├── data/
│   └── npidata_pfile_20240101-20240107.csv  # NPPES extract
├── scripts/
│   ├── ingest_cms_data.py  # CMS → PostgreSQL pipeline
│   └── search_api.py  # Basic FastAPI search endpoint
└── sql/
    └── schema.sql  # provider_backbone table definition

Database: PostgreSQL 15
  - provider_backbone: ~1.2M rows (Medicare providers)
  - Indexed on specialty + geography
  - Basic search working (no enrichment yet)
```

**Next Immediate Steps:**
1. ✅ Add PostGIS extension for geographic queries
2. ✅ Create provider_profiles table
3. ✅ Implement Google Places API integration
4. ✅ Build enrichment queue (Redis + worker)
5. ✅ Deploy simple React search UI

**Timeline:**
- Week 1: Google Places integration
- Week 2: provider_profiles + queue system
- Week 3: Web UI + progressive loading
- Week 4: Testing + refinement → MVP launch

---

## Appendices

### A. Technology Stack

| Layer | Technology | Rationale |
|-------|------------|-----------|
| **Database** | PostgreSQL 15 + PostGIS | Robust, spatial queries, JSON support |
| **Cache** | Redis | Fast, persistent, good for queues |
| **API** | FastAPI (Python) | Fast, async, auto-docs, type hints |
| **LLM** | OpenAI GPT-4o-mini | Cost-effective, structured output |
| **Job Queue** | Redis + RQ | Simple, Pythonic, reliable |
| **Frontend** | React + TailwindCSS | Fast dev, component reuse, responsive |
| **Deployment** | Hetzner VPS | Cost-effective, EU privacy compliance |
| **Monitoring** | Sentry + Uptime Robot | Error tracking, uptime monitoring |

### B. Data Sources

| Source | Purpose | Update Frequency | Access Method |
|--------|---------|------------------|---------------|
| **CMS NPPES** | Provider backbone (NPI, specialty, location) | Quarterly | Public download (CSV) |
| **Google Places** | Ratings, reviews, hours, photos | On-demand API | Places API (paid) |
| **Provider Websites** | Insurance, languages, affiliations | On-demand scraping | HTTP + LLM extraction |
| **Client Claims** | Referral patterns, volume | Client upload | CSV import |

### C. Key Metrics to Track

**Product Metrics:**
- Search volume (daily/weekly)
- Cache hit rate (target: >80%)
- Enrichment completion rate (target: >95%)
- Average search latency (target: <200ms)
- Provider coverage (% enriched to L3)

**Business Metrics:**
- Free → Pro conversion rate
- Customer acquisition cost (CAC)
- Monthly recurring revenue (MRR)
- Churn rate
- Net Promoter Score (NPS)

**Operational Metrics:**
- API costs (Google Places, LLM)
- Infrastructure costs
- Enrichment queue depth
- Error rates
- Database performance (query latency)

### D. Risk Mitigation

| Risk | Impact | Mitigation |
|------|--------|------------|
| **Google Places API cost overruns** | High | Rate limiting, caching, budget alerts |
| **LLM extraction accuracy** | Medium | Confidence scoring, human review queue |
| **Website scraping blocks** | Medium | Respectful crawling, user-agent rotation, fallbacks |
| **CMS data staleness** | Low | Quarterly refresh cycle, user-triggered updates |
| **Entity resolution errors** | Medium | Multi-phase matching, confidence thresholds |
| **Database performance** | High | Proper indexing, query optimization, read replicas |

---

## Conclusion

This architecture balances **speed, cost, and data quality** through a hybrid enrichment strategy:

1. **Pre-seed common searches** (top 1K geo+specialty combos) for instant results
2. **Enrich on-demand** for long-tail queries with progressive loading
3. **Cache aggressively** to avoid re-enrichment costs
4. **Refresh intelligently** based on search frequency and data staleness

**The moat** is the web intelligence layer: automated extraction of insurance panels, practice groups, and organizational affiliations that competitors can't replicate without significant LLM investment.

**Next decision point:** After Phase 2, evaluate whether practice group mapping provides sufficient differentiation to justify ongoing web scraping costs. If user feedback is strong, double down. If marginal, pivot to deeper client data integration features.

**Success looks like:** Physician liaisons preferring our platform because it's the only place they can see both public intelligence (Google ratings) and proprietary intelligence (their own referral patterns) in a single, fast interface.
