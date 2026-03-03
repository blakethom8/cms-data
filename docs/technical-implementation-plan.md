# Provider Intelligence Platform — Technical Implementation Plan

**Version:** 1.0
**Date:** February 16, 2026
**Status:** Implementation Planning

---

## Overview

This document details the technical implementation of the five-phase Provider Intelligence Pipeline. Each search request triggers an asynchronous pipeline that collects, harvests, reconciles, validates, and synthesizes provider data from multiple sources into a unified intelligence report.

**Key design principles:**
- **Async-first delivery:** Users submit a search, results are saved to their account when ready. Target: ~5-10 minutes per Full Intelligence search. Speed is secondary to depth and accuracy.
- **Multi-agent architecture:** Complex phases spawn parallel sub-agents/workers that operate independently and merge results.
- **Source-agnostic intelligence layer:** The reconciliation engine doesn't care where data comes from — CMS, NPPES, Google Places, web results, or client-uploaded datasets all flow through the same entity resolution pipeline.
- **Confidence-scored everything:** Every data point, match, and link carries a confidence score. The UI surfaces gaps honestly rather than hiding them.

---

## Pipeline Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        SEARCH REQUEST                            │
│  Input: specialty + geography + tier (quick/full/deep)           │
│  Output: job_id → user polls or gets notified when complete      │
└──────────────────────┬───────────────────────────────────────────┘
                       │
                       ▼
         ┌─────────────────────────┐
         │   JOB ORCHESTRATOR      │
         │                         │
         │  Creates job record     │
         │  Spawns phase workers   │
         │  Manages dependencies   │
         │  Tracks progress        │
         └─────────┬───────────────┘
                   │
     ┌─────────────┼─────────────────────────┐
     ▼             ▼                         ▼
  Phase 1       Phase 1                   Phase 1
  CMS/NPPES     Google Places             Web Search
  (parallel)    (parallel)                (parallel)
     │             │                         │
     └─────────────┼─────────────────────────┘
                   │
                   ▼
              Phase 2: Web Harvesting
              (parallel per domain)
                   │
                   ▼
              Phase 3: Reconciliation
              (entity resolution + linking)
                   │
                   ▼
              Phase 4: Validation & Enrichment
              (parallel per provider)
                   │
                   ▼
              Phase 5: LLM Synthesis
              (market briefing + provider cards)
                   │
                   ▼
         ┌─────────────────────────┐
         │   REPORT DELIVERY       │
         │                         │
         │  Save to user account   │
         │  Notify user            │
         │  Cache for future       │
         └─────────────────────────┘
```

---

## Phase 1: Parallel Discovery

**Duration:** 5-15 seconds
**Dependencies:** None (all sources queried simultaneously)
**Output:** Raw result sets seeding the three core tables

### 1A: CMS/NPPES Local Query

Query the pre-loaded DuckDB database on our Hetzner server.

**CMS Query Logic:**

```sql
-- Step 1: Resolve geography to zip codes
SELECT DISTINCT zip5
FROM core_providers
WHERE LOWER(city) = LOWER(:city)
AND state = :state_abbrev;

-- Step 2: Find providers matching specialty + geography
-- Use specialty stemming (e.g., "cardiologist" → "%cardiol%")
SELECT
    c.npi,
    c.first_name,
    c.last_org_name,
    c.provider_type,
    c.street_address_1,
    c.city,
    c.state,
    c.zip5,
    u.tot_medicare_payment,
    u.tot_services,
    u.tot_unique_beneficiaries,
    r."Group Legal Business Name" AS org_name,
    r."Group PAC ID" AS org_pac_id,
    d.num_org_mem AS org_member_count,
    d.grp_assgn AS group_assigned
FROM core_providers c
LEFT JOIN utilization_metrics u ON c.npi = u.npi
LEFT JOIN raw_reassignment r ON CAST(c.npi AS BIGINT) = r."Individual NPI"
LEFT JOIN raw_dac_national d ON CAST(c.npi AS VARCHAR) = CAST(d."NPI" AS VARCHAR)
WHERE c.zip5 IN (:resolved_zips)
AND (specialty_stem_match(c.provider_type, :specialty))
ORDER BY u.tot_medicare_payment DESC NULLS LAST
LIMIT 100;
```

**NPPES Query Logic:**

```sql
-- NPPES captures providers CMS misses:
-- concierge/DPC, newly-established, non-Medicare-billing
-- This is valuable — PPO-heavy concierge practices are high-value
-- liaison targets who need specialty referral partners

SELECT
    npi,
    first_name,
    last_name,
    credentials,
    practice_address_1,
    practice_city,
    practice_state,
    practice_zip,
    taxonomy_1,
    taxonomy_2,
    taxonomy_3,
    enumeration_date
FROM raw_nppes
WHERE LOWER(practice_city) = LOWER(:city)
AND practice_state = :state_abbrev
AND entity_type = '1'
AND (taxonomy_stem_match(taxonomy_1, :specialty)
     OR taxonomy_stem_match(taxonomy_2, :specialty)
     OR taxonomy_stem_match(taxonomy_3, :specialty))
LIMIT 100;
```

**Specialty Resolution:**

Maintain a mapping table that normalizes user input to both CMS `provider_type` patterns and NPPES taxonomy code prefixes:

```json
{
  "primary care": {
    "cms_patterns": ["internal medicine", "family practice", "family medicine", "general practice"],
    "taxonomy_prefixes": ["207Q", "207R", "208D", "363L"],
    "include_subtypes": true
  },
  "cardiologist": {
    "cms_patterns": ["cardiol"],
    "taxonomy_prefixes": ["207RC"],
    "include_subtypes": true
  }
}
```

The `include_subtypes` flag means "Interventional Cardiology" matches a "cardiologist" search. This map is extensible — new specialties can be added without code changes.

**Geography Resolution — Future Enhancement:**

Current: exact city name match → zip codes.
Planned: zip centroid + radius search. Store lat/lng per zip code, allow "within 5 miles of Beverly Hills" to capture providers at Cedars-Sinai (LA zip 90048) who serve Beverly Hills patients. This is important for areas where major hospitals sit on city boundaries.

**Output → `raw_cms_providers[]` and `raw_nppes_providers[]`**

### 1B: Google Places Search

Query SearchAPI's Google Maps engine for practice locations.

```
GET https://www.searchapi.io/api/v1/search
  ?engine=google_maps
  &q={specialty} near {city}, {state}
  &type=search
  &gl=us
  &hl=en
```

**Processing:**
- Extract: title, address, GPS coordinates, phone, website URL, rating, review count, place_id, type
- Normalize addresses (standardize St/Street, Ste/Suite, directionals)
- Flag place_type for filtering: keep medical-relevant types, tag but don't discard ambiguous ones

**Output → `raw_places[]`**

### 1C: Web Search

Query SearchAPI's Google Web engine for broader provider intelligence.

```
GET https://www.searchapi.io/api/v1/search
  ?engine=google
  &q={specialty} in {city}, {state}
  &num=20
```

**Why this matters:** Web results surface data that neither CMS nor Google Places captures:
- Health system directory pages listing providers by specialty
- Healthgrades/Vitals/WebMD profile pages with ratings and reviews
- News articles about practice openings, expansions, new hires
- Insurance provider directories
- Practice websites with "meet our team" pages

**Key extraction from web results:**
- URLs → domain analysis for health system affiliation signals
- Snippets → provider names, practice names mentioned
- Link patterns → identify which results are directories vs. individual profiles vs. news

**Output → `raw_web_results[]`**

### 1D: Client Data Integration (if provided)

If the client has uploaded supplementary data (their CRM export, internal provider lists, referral history), query it alongside the standard sources.

```
Client data could include:
- Existing relationship status ("met 2024-03", "no contact")
- Internal priority scores
- Referral volume from their own system
- Notes from previous outreach
```

This data flows into Phase 3 reconciliation as an additional source, enriching matched providers with client-specific context. The pipeline treats client data the same as any other source — it gets confidence-scored and reconciled.

**Output → `raw_client_data[]` (if applicable)**

---

## Phase 2: Web Harvesting & URL Intelligence

**Duration:** 30-120 seconds (parallelized across domains)
**Dependencies:** Phase 1 complete (needs Places URLs and Web result URLs)
**Triggered:** Full Intelligence and Deep Dive tiers only (skipped for Quick Search)

### 2A: URL Intelligence Layer (All Sites)

Before attempting to crawl any website, extract intelligence from the URLs themselves. This works on *every* result — including health system SPAs we can't render.

**Domain-based affiliation inference:**

```python
HEALTH_SYSTEM_DOMAINS = {
    "providence.org": "Providence",
    "uclahealth.org": "UCLA Health",
    "cedars-sinai.org": "Cedars-Sinai",
    "kp.org": "Kaiser Permanente",
    "sutterhealth.org": "Sutter Health",
    "dignityhealth.org": "Dignity Health / CommonSpirit",
    "sharp.com": "Sharp HealthCare",
    "memorialcare.org": "MemorialCare",
    "adventisthealth.org": "Adventist Health",
    "stanfordhealthcare.org": "Stanford Health Care",
    # ... extensible registry
}
```

**How it works:**
- A Google Places result for "Providence Saint John's Primary Care" returns `website: "https://www.providence.org/locations/..."`
- A Google Web result for "Dr. Jay Kahng" might return links to both `providence.org/doctors/jay-kahng` AND `healthgrades.com/physician/dr-jay-kahng`
- Even without crawling either page, we now know: this provider is affiliated with Providence, and has a Healthgrades profile

**URL path intelligence:**
- `/doctors/jay-kahng` → individual provider profile (harvestable)
- `/locations/saint-johns-primary-care` → location page (harvestable)
- `/find-a-doctor?specialty=internal-medicine&zip=90404` → directory search (harvestable)

**Aggregate URL signals per provider:**
```json
{
  "provider": "Jay Kahng, MD",
  "url_signals": [
    {"domain": "providence.org", "path_type": "provider_profile", "inferred_system": "Providence"},
    {"domain": "healthgrades.com", "path_type": "provider_profile", "inferred_system": null},
    {"domain": "vitals.com", "path_type": "provider_profile", "inferred_system": null}
  ],
  "affiliation_inference": {
    "system": "Providence",
    "confidence": 0.85,
    "evidence": "provider profile URL on providence.org"
  }
}
```

This is valuable even when we can't scrape the actual page content. A provider appearing on `cedars-sinai.org` is almost certainly affiliated with Cedars — that's a strong signal that doesn't require rendering JavaScript.

### 2B: Simple Site Harvesting

For sites that server-render their HTML (independent practices, smaller groups, Healthgrades, Vitals, WebMD, npidb.org), fetch and extract directly.

**Site classification:**

```
Tier 1 — Direct fetch (server-rendered HTML):
  - Independent practice WordPress/Squarespace sites
  - Healthgrades, Vitals, WebMD, Zocdoc profiles
  - npidb.org, npino.com (NPI lookup aggregators)
  - State medical board lookup pages
  - Smaller medical group websites

Tier 2 — Requires headless rendering:
  - Medium-complexity sites with partial JS rendering
  - Some medical group sites built on modern frameworks

Tier 3 — SPA / heavy JS (defer to URL intelligence only):
  - Providence, UCLA Health, Cedars-Sinai, Kaiser, Sutter
  - Major health system provider directories
  - Sites with aggressive bot protection
```

**Harvesting workflow per URL:**

```
1. Classify site tier (domain lookup → known tier, else probe with HEAD request)
2. If Tier 1:
   a. Fetch page content (HTTP GET, respect robots.txt)
   b. Extract readable content (html → markdown)
   c. Pass to LLM extraction (see prompts below)
3. If Tier 2:
   a. Render via headless browser (Playwright)
   b. Extract rendered DOM → markdown
   c. Pass to LLM extraction
4. If Tier 3:
   a. Use URL intelligence only (2A above)
   b. Check for alternate sources (Healthgrades, etc.) for the same provider
   c. Flag as "limited data — health system site not crawled"
```

**Parallelization strategy:**
- Group URLs by domain
- Process different domains in parallel (up to 10 concurrent)
- Serialize within same domain (1 request per 2 sec, respect rate limits)
- Spawn separate worker/agent per domain group for true parallelism

### 2C: LLM Extraction Prompts

For each successfully fetched page, extract structured data using targeted prompts.

**Provider Profile Extraction:**

```
System: You are extracting healthcare provider data from a webpage.
Return ONLY valid JSON. If a field is not found, use null.

Extract:
{
  "providers": [
    {
      "name": "full name as displayed",
      "credentials": "MD, DO, NP, PA, etc.",
      "specialty": "as listed on page",
      "phone": "direct line if shown",
      "profile_url": "link to individual profile if available",
      "photo_url": "headshot URL if available",
      "accepting_new_patients": true/false/null,
      "board_certifications": [],
      "education": [],
      "languages": [],
      "bio_summary": "2-3 sentence summary of their background/focus",
      "conditions_treated": [],
      "insurance_accepted": []
    }
  ],
  "page_type": "provider_profile|provider_directory|location_page|practice_homepage",
  "practice_name": "name of practice or clinic",
  "practice_address": "if shown"
}

User: [page content as markdown, truncated to ~4000 tokens]
```

**Practice/Location Intelligence Extraction:**

```
System: Extract business intelligence about this healthcare practice
from their website. This data will be used by physician liaisons
preparing for outreach meetings. Return ONLY valid JSON.

{
  "practice_name": "",
  "clinical_services": [],
  "conditions_treated": [],
  "special_programs": [],
  "care_philosophy": "how they describe their approach",
  "patient_focus": "who they primarily serve",
  "differentiators": [],
  "team_size": null,
  "recent_developments": [],
  "talking_points": "3-4 sentence briefing a liaison could use to prepare for a first meeting",
  "accepting_new_patients": true/false/null
}

User: [page content as markdown]
```

**Healthgrades/Vitals Profile Extraction:**

```
System: Extract provider data from this healthcare directory profile page.
Return ONLY valid JSON.

{
  "name": "",
  "credentials": "",
  "specialty": "",
  "rating": null,
  "review_count": null,
  "years_experience": null,
  "education": [],
  "board_certifications": [],
  "hospital_affiliations": [],
  "office_locations": [
    {"name": "", "address": "", "phone": ""}
  ],
  "insurance_accepted": [],
  "conditions_treated": [],
  "procedures_performed": []
}

User: [page content as markdown]
```

### 2D: Multi-Page Discovery (Simple Sites Only)

For Tier 1 sites where we successfully fetch the homepage, attempt to discover and fetch 1-2 additional high-value pages:

```
System: Given this healthcare practice homepage, identify the URLs
(from links on this page) that would contain:
1. A provider directory or "meet our team" page
2. A services or clinical capabilities page

Return JSON: {"provider_page_url": "..." or null, "services_page_url": "..." or null}
Only return URLs that are clearly visible in the page content.

User: [page content with links preserved]
```

Fetch discovered subpages and extract using the appropriate prompt above. Limit to 2 additional pages per site to control costs and time.

**Output → `harvested_profiles[]`, `harvested_locations[]`, `url_intelligence[]`**

---

## Phase 3: Reconciliation & Entity Resolution

**Duration:** 10-30 seconds
**Dependencies:** Phases 1 and 2 complete
**This is the core intelligence layer — where raw data becomes unified knowledge.**

### 3A: Within-Source Deduplication

**CMS Dedup:**
```
1. Group all CMS records by NPI
2. For each unique NPI:
   a. Keep core provider fields (name, specialty, address, metrics) from the record
      with the highest tot_medicare_payment (most representative affiliation)
   b. Collapse all (org_name, org_pac_id, org_member_count) into organizations[]
   c. Flag group_assigned: "Y" = system/group employed, "M" = independent/solo
   d. Sum or take max of utilization metrics if multiple records have different values
3. Output: one record per NPI with organizations[] array
```

**NPPES Dedup:**
```
1. Generally one record per NPI
2. Consolidate multiple taxonomy codes into specialties[]
3. Map taxonomy codes to human-readable labels via taxonomy lookup table
4. Output: one record per NPI with specialties[] array
```

**Web/Harvested Dedup:**
```
1. Normalize names: lowercase, strip titles (Dr./Dr), standardize credentials
2. Group by exact normalized_name + credentials
3. For near-matches, use Levenshtein distance < 2 on last name + first name initial match
4. Merge data from multiple pages for the same provider (Healthgrades + practice site)
5. Output: one record per unique provider with source_urls[]
```

### 3B: Cross-Source Entity Resolution

This is where the intelligence layer earns its keep. We're matching the same real-world provider across CMS, NPPES, Places, web results, and potentially client data.

**Match Cascade (ordered by confidence):**

```
Level 1 — NPI Join (confidence: 0.99)
  CMS record + NPPES record share the same NPI
  → Deterministic match. Merge credentials from NPPES, metrics from CMS.

Level 2 — NPI from Web Extraction (confidence: 0.95)
  Harvested profile page contains an NPI number
  → Join to CMS/NPPES records. High confidence — NPI on their own page.

Level 3 — Name + Address + Specialty (confidence: 0.80-0.90)
  Web-harvested provider name matches CMS/NPPES name
  AND practice address is within 0.25 miles of CMS/NPPES address
  AND specialty aligns (taxonomy maps to same category)
  → Scoring:
    - Exact name match: +0.30
    - Address within 0.1 mi: +0.30 / within 0.25 mi: +0.20
    - Specialty match: +0.20
    - Credentials match (MD=MD): +0.10
    - Same org inferred from URL: +0.10

Level 4 — Name + Organization (confidence: 0.70-0.85)
  Web-harvested provider on a health system site (e.g., providence.org)
  AND CMS record shows same org affiliation
  AND name matches
  → Confidence boosted by org alignment even if addresses differ
    (providers may practice at multiple locations within a system)

Level 5 — Name + Geography Only (confidence: 0.50-0.65)
  Name matches but no corroborating evidence
  → Flag as "possible match — needs validation"
  → Present to user with caveat

Level 6 — Unmatched (confidence: N/A)
  Provider found in only one source
  → Still valuable! A provider in NPPES but not CMS might be:
    - Concierge/DPC practice (no Medicare billing)
    - Newly established (not yet in CMS)
    - NP/PA billing under supervising physician
  → Tag with discovery_source and surface with context about why
    they might be absent from other sources
```

**Name Matching Logic:**

```python
def match_names(name_a, name_b):
    """
    Handle real-world name variations:
    - "Robert" vs "Bob" (common nickname map)
    - "Robert A. Merz" vs "Robert Merz" (middle initial)
    - "Merz, Robert" vs "Robert Merz" (inverted order)
    - "Robert Merz Jr." vs "Robert Merz" (suffix handling)

    Returns: (is_match: bool, confidence_modifier: float)
    """
    # Normalize: lowercase, strip suffixes (Jr, Sr, III), strip periods
    # Split into first/last components
    # Compare last name exactly (required)
    # Compare first name with nickname expansion
    # Penalize if middle initial present in one but not other (slight)
```

**Address Normalization & Proximity:**

```python
def normalize_address(raw_address):
    """
    Standardize:
    - "St" / "Street" / "St." → "St"
    - "Ste" / "Suite" / "#" → "Ste"
    - "Blvd" / "Boulevard" → "Blvd"
    - Strip unit/suite numbers for building-level comparison
    - Geocode via stored zip centroid for distance calculation
    """

def address_proximity(addr_a, addr_b):
    """
    Returns distance in miles between two addresses.
    Uses geocoded coordinates if available, else zip centroid fallback.
    """
```

### 3C: Provider-to-Place Linking

Connect individual providers to the physical locations where they practice.

**Evidence sources for links:**

| Evidence Type | Confidence Weight | Description |
|---|---|---|
| `website_extraction` | 0.90 | Provider listed on a Place's website |
| `exact_address_match` | 0.85 | Provider's CMS/NPPES address matches Place address exactly |
| `suite_address_match` | 0.80 | Same building, different suite number |
| `proximity_match` | 0.60 | Within 0.1 miles (same block / medical complex) |
| `org_affiliation` | 0.50 | Provider's CMS org matches Place's inferred health system |
| `web_co_occurrence` | 0.45 | Provider name appears in web result that also references the Place |

A provider-place link's final confidence is the max of all evidence sources (not summed — we want the strongest evidence to speak).

### 3D: Organization Graph Construction

```
1. Extract unique organizations from CMS data (org_name, pac_id)
2. Map Place website domains to health systems (domain registry from 2A)
3. Cluster Places by brand/system:
   - Same website domain → same system
   - Similar name + same city → possibly same group (flag for review)
4. Connect providers to organizations:
   - CMS org_pac_id → direct link
   - URL intelligence → inferred link (lower confidence)
5. Compute org-level metrics:
   - Total providers in geography
   - Total Medicare volume across providers
   - Number of locations
   - Specialties represented
```

### 3E: Unmatched Provider Analysis

Providers found in only one source get special treatment — they're not errors, they're *intelligence*.

```
CMS-only providers (not in web results):
  → Likely low web presence. May be older physicians, hospital-based,
    or practice under a large group's umbrella website.
  → Action: Note "no web presence found" — still valid target.

NPPES-only providers (not in CMS):
  → Likely non-Medicare-billing. Could be:
    - Concierge/DPC practice (HIGH value for liaisons targeting PPO patients)
    - Recently enumerated (new practice opening — opportunity signal)
    - NP/PA billing under supervisor
  → Action: Flag as "non-Medicare provider" with concierge/new practice hypothesis.
  → Enrichment: Check enumeration_date — if < 2 years, flag as "newly established."

Web-only providers (not in CMS or NPPES):
  → Could be mid-level providers, recently relocated, or data entry lag.
  → Action: Surface with lower confidence, note "found on web only."

Places with no linked providers:
  → The location exists but we couldn't identify individual providers there.
  → Action: Surface the location with practice-level data (services, phone, website)
    and note "individual providers not yet identified."
```

**Output → unified `providers[]`, `places[]`, `organizations[]`, `provider_places[]`, `provider_organizations[]` with confidence scores throughout**

---

## Phase 4: Validation & Enrichment

**Duration:** 10-30 seconds (parallelized per provider)
**Dependencies:** Phase 3 complete
**Scope:** Run on all providers for Quick Search validation; deeper checks for Full Intelligence

### 4A: NPPES Real-Time API Verification

For providers with NPIs, hit the live NPPES API to confirm current status:

```
GET https://npiregistry.cms.hhs.gov/api/?number={npi}&version=2.1
```

**Check for:**
- Address matches our records (flag discrepancies — provider may have moved)
- Status is active (deactivated NPIs = retired/deceased/surrendered license)
- Taxonomy codes still align with expected specialty
- Last update date (recent updates suggest active provider)

**Rate limiting:** NPPES API allows ~2-3 requests/second. Batch with 500ms delays. For 50 providers, this takes ~25 seconds — run in parallel with other 4x checks.

### 4B: Volume Tier Classification

Assign each provider a volume tier relative to their specialty in the geographic market:

```python
def assign_volume_tier(provider, all_providers_in_search):
    """
    Calculate percentile rank within this search's results.

    Tiers:
    - "high"   = top 25% by unique_beneficiaries
    - "medium" = 25th-75th percentile
    - "low"    = bottom 25%
    - "unknown" = no Medicare data (NPPES-only, web-only)

    For "unknown" providers, note WHY:
    - "non_medicare" = likely concierge/DPC/cash-pay
    - "newly_established" = enumeration_date < 2 years
    - "data_gap" = expected to have data but not found
    """
```

### 4C: Practice Type Classification

Classify each provider's practice model — critical for liaison targeting strategy:

```python
def classify_practice_type(provider):
    """
    Sources of evidence:
    - CMS group_assigned: "Y" = group/system, "M" = solo/independent
    - org_member_count: 1 = solo, 2-10 = small group, 10+ = large group/system
    - health_system affiliation: if linked to known system → "system_employed"
    - NPPES-only + no org: likely independent
    - Website signals: "concierge", "direct primary care", "membership" in content

    Classifications:
    - "system_employed" — provider in large health system (Providence, UCLA, etc.)
    - "large_group" — multi-provider group practice, not a named system
    - "small_group" — 2-10 provider practice
    - "solo_independent" — single provider practice
    - "concierge_dpc" — concierge or direct primary care model
    - "unknown"

    Liaison relevance:
    - solo_independent, small_group → most receptive to outreach
    - concierge_dpc → high-value PPO patients, needs referral partners
    - system_employed → harder to influence individually, approach via system relationships
    """
```

### 4D: Data Completeness Scoring

Rate how much we know about each provider — both for internal quality tracking and user transparency:

```python
def completeness_score(provider):
    """
    Fields and weights:
    - identity (name, NPI, credentials): 20%
    - location (verified address, phone): 15%
    - specialty (confirmed via 2+ sources): 10%
    - volume (Medicare metrics available): 15%
    - organization (affiliation known): 10%
    - web presence (profile found online): 10%
    - clinical detail (services, bio, certs): 10%
    - consumer signal (rating, reviews): 5%
    - contact info (phone, website): 5%

    Score 0.0 → 1.0. Surface in UI as "data richness" indicator.
    """
```

### 4E: Client Data Overlay (if applicable)

If client provided supplementary data in Phase 1D, merge it here:

```
For each matched provider:
  - Add relationship_status ("existing contact", "no history", "do not contact")
  - Add internal_priority if client has their own scoring
  - Add last_contact_date and contact_notes
  - Add referral_volume from client's own system

Flag conflicts:
  - Client shows provider at Address A, our data shows Address B
  - Client lists specialty X, CMS shows specialty Y
  → Surface discrepancies as "your records differ from public data"
```

**Output → enriched `providers[]` with volume_tier, practice_type, completeness_score, validation_flags, client_overlay**

---

## Phase 5: LLM Synthesis & Report Generation

**Duration:** 30-60 seconds
**Dependencies:** Phase 4 complete
**This phase transforms structured data into an actionable intelligence report.**

### 5A: Market Landscape Analysis

Send the full reconciled dataset to the LLM for market-level analysis.

**Prompt:**

```
System: You are a healthcare market analyst preparing a briefing for a
physician liaison. Analyze the provider data below and produce a market
landscape summary.

Your analysis should cover:

1. MARKET OVERVIEW
   - Total providers found, by source coverage
   - Breakdown by practice type (system-employed vs independent vs concierge)
   - Geographic clustering (where are providers concentrated?)

2. HEALTH SYSTEM LANDSCAPE
   - Which systems dominate this market? How many providers/locations each?
   - Independent practice presence — are there opportunities outside major systems?
   - Market concentration assessment

3. VOLUME DISTRIBUTION
   - High-volume providers (top referral sources in the area)
   - Distribution of Medicare volume across providers
   - Providers with no Medicare data and what that likely means

4. OPPORTUNITIES & GAPS
   - Newly established practices (enumeration < 2 years)
   - Concierge/DPC practices (PPO patient pools needing referral partners)
   - Practices emphasizing specific clinical programs
   - Under-served areas within the geography

5. DATA CONFIDENCE
   - How complete is our picture? What couldn't we verify?
   - Which providers have the richest vs thinnest data?

Format as a structured briefing with headers and bullet points.
Be specific — use names, numbers, and locations from the data.

User: [JSON of all reconciled providers, places, organizations]
```

### 5B: Provider Intelligence Cards

For each provider, generate a synthesis card that combines all available data into a liaison-ready briefing.

**Prompt (batched — 5-10 providers per call):**

```
System: Generate provider intelligence cards for a physician liaison.
Each card should synthesize all available data into an actionable briefing.

For each provider, produce:

{
  "npi": "",
  "display_name": "",
  "one_liner": "Single sentence positioning this provider for a liaison",
  "outreach_priority": "high|medium|low",
  "priority_rationale": "Why this priority level — be specific",

  "practice_context": {
    "type": "system_employed|independent|concierge_dpc|etc",
    "organization": "",
    "health_system": "",
    "location_summary": "Where they practice, in plain language"
  },

  "key_metrics": {
    "medicare_volume_tier": "",
    "unique_beneficiaries": null,
    "google_rating": null,
    "review_count": null,
    "years_in_practice": null
  },

  "outreach_intelligence": {
    "why_target": "What makes this provider worth meeting",
    "approach_strategy": "How to approach — referral value prop, shared patients, etc.",
    "talking_points": ["point 1", "point 2", "point 3"],
    "watch_outs": "Anything the liaison should be aware of"
  },

  "clinical_focus": {
    "specialties": [],
    "services": [],
    "patient_population": "",
    "differentiators": []
  },

  "data_confidence": {
    "overall": 0.0-1.0,
    "sources": ["cms", "nppes", "web", "healthgrades"],
    "gaps": ["what we couldn't find"]
  }
}

User: [JSON array of provider records with all enrichment data]
```

### 5C: Competitive Intelligence Summary

```
System: Produce a competitive intelligence summary showing the
organizational landscape for this market.

For each major organization/health system present:
- Number of providers and locations in the search area
- Total Medicare volume (aggregate)
- Key specialties represented
- Notable providers (highest volume, most reviewed)

Then summarize: market concentration, system vs independent balance,
and which organizations represent the best and most accessible
outreach opportunities.

User: [organizations + provider_organizations + aggregated metrics]
```

### 5D: Report Assembly

Combine all synthesis outputs into the final report structure:

```json
{
  "report_id": "uuid",
  "search_query": {
    "specialty": "primary care",
    "location": "Santa Monica, CA",
    "tier": "full_intelligence"
  },
  "generated_at": "2026-02-16T17:00:00Z",
  "pipeline_duration_seconds": 285,

  "executive_summary": "LLM-generated market overview (from 5A)",

  "market_landscape": {
    "total_providers": 45,
    "by_source": {"cms": 32, "nppes": 41, "web": 28, "places": 18},
    "by_practice_type": {"system_employed": 22, "independent": 12, "concierge": 3, "unknown": 8},
    "health_systems": [
      {"name": "Providence", "providers": 12, "locations": 3, "total_beneficiaries": 8500},
      {"name": "UCLA Health", "providers": 8, "locations": 2, "total_beneficiaries": 6200}
    ]
  },

  "providers": [
    {
      "// full provider intelligence card from 5B": "..."
    }
  ],

  "competitive_intelligence": "from 5C",

  "places": [
    {
      "// place records with linked providers": "..."
    }
  ],

  "data_quality": {
    "high_confidence_providers": 28,
    "medium_confidence_providers": 12,
    "low_confidence_providers": 5,
    "unmatched_cms": 4,
    "unmatched_nppes": 9,
    "unmatched_web": 3,
    "sites_not_crawled": ["cedars-sinai.org", "kp.org"],
    "notes": "3 providers found on Cedars-Sinai URLs but site could not be crawled. Affiliation inferred from URL domain."
  },

  "methodology": {
    "sources_queried": ["cms_duckdb", "nppes_duckdb", "google_places", "google_web"],
    "sites_harvested": 14,
    "sites_skipped": 6,
    "llm_calls": 22,
    "cache_hits": 3
  }
}
```

---

## Multi-Agent Processing Architecture

For Full Intelligence and Deep Dive tiers, the pipeline benefits from spawning parallel agents/workers.

### Agent Topology

```
                    ┌───────────────────┐
                    │  ORCHESTRATOR     │
                    │                   │
                    │  Owns the job     │
                    │  Spawns workers   │
                    │  Merges results   │
                    │  Runs Phase 3-5   │
                    └─────┬─────────────┘
                          │
            ┌─────────────┼─────────────────┐
            │             │                 │
            ▼             ▼                 ▼
    ┌──────────────┐ ┌──────────┐ ┌──────────────┐
    │ DISCOVERY    │ │ DISCOVERY│ │ DISCOVERY    │
    │ AGENT        │ │ AGENT    │ │ AGENT        │
    │              │ │          │ │              │
    │ CMS + NPPES  │ │ Places   │ │ Web Search   │
    │ queries      │ │ API call │ │ + URL intel   │
    └──────┬───────┘ └────┬─────┘ └──────┬───────┘
           │              │              │
           └──────────────┼──────────────┘
                          │
                          ▼
         ┌────────────────────────────────┐
         │  HARVEST COORDINATOR           │
         │                                │
         │  Groups URLs by domain         │
         │  Spawns per-domain workers     │
         └───────┬──────────┬─────────────┘
                 │          │
        ┌────────▼──┐  ┌───▼────────┐
        │ DOMAIN    │  │ DOMAIN     │    (up to 10 parallel)
        │ WORKER    │  │ WORKER     │
        │           │  │            │
        │ Fetch +   │  │ Fetch +    │
        │ Extract   │  │ Extract    │
        │ pages for │  │ pages for  │
        │ domain A  │  │ domain B   │
        └───────────┘  └────────────┘
```

### Orchestrator Responsibilities

```python
class PipelineOrchestrator:
    """
    Manages the full pipeline for a single search job.
    """

    async def run(self, job):
        # Phase 1: Parallel discovery (spawn 3 agents, await all)
        cms_result, places_result, web_result = await asyncio.gather(
            self.discover_cms(job),
            self.discover_places(job),
            self.discover_web(job)
        )

        # Phase 2: Web harvesting (spawn N domain workers)
        all_urls = self.collect_urls(places_result, web_result)
        domain_groups = self.group_by_domain(all_urls)
        harvest_results = await asyncio.gather(
            *[self.harvest_domain(domain, urls) for domain, urls in domain_groups.items()]
        )

        # Phase 3: Reconciliation (single agent — needs full picture)
        unified = await self.reconcile(cms_result, places_result, web_result, harvest_results)

        # Phase 4: Validation (parallel per provider)
        enriched = await asyncio.gather(
            *[self.validate_provider(p) for p in unified.providers]
        )

        # Phase 5: Synthesis (2-3 LLM calls)
        report = await self.synthesize(enriched, unified)

        # Save and notify
        await self.save_report(job, report)
        await self.notify_user(job)
```

### Job Tracking Schema

```sql
CREATE TABLE search_jobs (
    job_id UUID PRIMARY KEY,
    user_id UUID REFERENCES users(id),
    specialty TEXT NOT NULL,
    location TEXT NOT NULL,
    tier TEXT NOT NULL,  -- 'quick', 'full', 'deep'
    status TEXT DEFAULT 'queued',  -- queued, running, phase_1, phase_2, ..., complete, failed
    progress JSONB DEFAULT '{}',  -- {"phase": 1, "detail": "querying CMS..."}
    created_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    report JSONB,  -- final report JSON
    cost_cents INTEGER,  -- actual COGS for this job
    error TEXT
);

CREATE TABLE harvest_cache (
    url_hash TEXT PRIMARY KEY,
    url TEXT NOT NULL,
    fetched_at TIMESTAMPTZ DEFAULT NOW(),
    raw_content TEXT,  -- stored for re-extraction
    extracted_data JSONB,  -- cached LLM extraction output
    extraction_model TEXT,  -- which model/prompt version
    expires_at TIMESTAMPTZ  -- fetched_at + 7 days
);
```

---

## Implementation Priorities

### Sprint 1: Pipeline Foundation (Week 1-2)
- [ ] Job orchestrator with status tracking
- [ ] Phase 1 workers (CMS, NPPES, Places, Web — all already partially built)
- [ ] Specialty resolution mapping (expanded from current compare dashboard)
- [ ] Geography resolution with state normalization
- [ ] Basic report output (JSON → stored in Supabase)

### Sprint 2: Entity Resolution (Week 3-4)
- [ ] Within-source deduplication (CMS NPI grouping, name normalization)
- [ ] Cross-source matching cascade (NPI join → name+address → name+org)
- [ ] Provider-to-Place linking with confidence scores
- [ ] Organization graph construction
- [ ] Unmatched provider analysis and classification

### Sprint 3: Web Harvesting (Week 5-6)
- [ ] URL intelligence layer (domain → health system mapping)
- [ ] Simple site fetcher (Tier 1 sites)
- [ ] LLM extraction prompts (provider profiles, practice info)
- [ ] Harvest cache implementation
- [ ] Multi-page discovery for simple sites

### Sprint 4: Synthesis & Report (Week 7-8)
- [ ] Market landscape LLM synthesis
- [ ] Provider intelligence card generation
- [ ] Competitive intelligence summary
- [ ] Report assembly and storage
- [ ] User notification on completion

### Sprint 5: UI & Delivery (Week 9-10)
- [ ] User accounts and search history
- [ ] Report viewer UI (browse providers, filter, sort)
- [ ] Map view of places with linked providers
- [ ] Export options (PDF, CSV, JSON)
- [ ] Payment integration

---

## Cost Estimates Per Search

| Component | Quick | Full Intelligence | Deep Dive |
|---|---|---|---|
| CMS/NPPES query (local DuckDB) | ~$0.00 | ~$0.00 | ~$0.00 |
| Google Places API (SearchAPI) | $0.03 | $0.03 | $0.03 |
| Google Web Search (SearchAPI) | $0.01 | $0.01 | $0.01 |
| Web harvesting (fetch/render) | — | $0.02-0.05 | $0.02-0.05 |
| LLM extraction (~15-20 pages) | — | $0.15-0.30 | $0.15-0.30 |
| LLM reconciliation | $0.02 | $0.05-0.10 | $0.05-0.10 |
| LLM synthesis (market + cards) | — | $0.10-0.20 | $0.15-0.30 |
| NPPES API validation | $0.00 | $0.00 | $0.00 |
| Client data overlay | — | — | $0.02 |
| **Total COGS** | **~$0.06** | **~$0.36-0.68** | **~$0.42-0.81** |
| **Target Price** | **$3** | **$10** | **$25** |
| **Gross Margin** | **~98%** | **~93-96%** | **~97%** |

*LLM costs assume Claude Sonnet for extraction and Haiku for simpler tasks. Costs will decrease as models get cheaper and caching increases.*

---

*This plan is designed to be built iteratively — each sprint produces a functional layer that improves the output. Sprint 1 alone produces a Quick Search tier that's shippable.*
