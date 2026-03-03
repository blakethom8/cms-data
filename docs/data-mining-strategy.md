# Provider Data Mining Strategy

## Overview

Building a comprehensive provider intelligence repository by combining CMS Medicare data with external sources. Target: **8M+ NPIs** with enriched profiles for medical sales, pharma, and device targeting.

---

## Data Sources

### 1. CMS Public Data Catalog (Phase 1 - COMPLETE)

**Status:** ✅ Implemented  
**Scale:** ~1.2M Medicare-billing providers  
**Method:** CSV bulk download + API  
**Cost:** Free  

**Datasets:**
- Physician by Provider (foundational)
- Physician by Provider and Service (HCPCS drill-down)
- Part D Prescribers (by provider + by drug)
- Reassignment (group practice mapping)
- Hospital Enrollments
- Quality Payment Program (MIPS scores)
- PECOS Enrollment
- DME Referrals
- Order & Referring Eligibility

**What we get:**
- Utilization metrics (services, beneficiaries, payments)
- Prescribing volume and patterns
- Chronic condition prevalence
- Practice affiliations (via reassignment)
- Quality scores (MIPS)
- DME referral patterns

**Limitations:**
- Medicare-only (excludes Medicaid-only, cash-only, non-billing providers)
- No gender, detailed taxonomy codes
- No industry payment history
- Hospital affiliations are inferred (name matching), not direct

---

### 2. NPPES (National Plan & Provider Enumeration System)

**Status:** 🔴 Not implemented  
**Scale:** ~8M NPIs (all US healthcare providers)  
**Source:** https://download.cms.gov/nppes/NPI_Files.html  
**Method:** Bulk CSV download (9GB compressed, ~25GB uncompressed)  
**Refresh:** Monthly  
**Cost:** Free  

**What we get:**
- Complete NPI master file (Type 1 individuals + Type 2 organizations)
- **Gender** (critical for sales targeting)
- **Taxonomy codes** (more granular than Medicare specialty)
- Multiple practice locations per NPI
- Other identifiers (state license numbers, DEA numbers where disclosed)
- Enumeration date (when NPI was issued)
- Deactivation date (inactive providers)
- Credential text
- Sole proprietor flag

**Why we need it:**
1. **Coverage gap:** CMS data only covers Medicare-billing providers (~1.2M). NPPES has all active NPIs (~5-6M individuals).
2. **Gender for targeting:** Sales reps filter by gender for specific products (e.g., women's health).
3. **Taxonomy precision:** Taxonomy codes are more specific than Medicare provider_type. Example: "208D00000X" = General Practice vs. "207R00000X" = Internal Medicine.
4. **Non-Medicare providers:** Pediatricians, Medicaid-only providers, cash-only practices — not in CMS utilization data but exist in NPPES.

**Schema additions:**
```sql
ALTER TABLE core_providers ADD COLUMN gender TEXT;
ALTER TABLE core_providers ADD COLUMN primary_taxonomy TEXT;
ALTER TABLE core_providers ADD COLUMN enumeration_date DATE;
ALTER TABLE core_providers ADD COLUMN deactivation_date DATE;
ALTER TABLE core_providers ADD COLUMN sole_proprietor BOOLEAN;
```

**Join strategy:**
- Load NPPES into `raw_nppes` in DuckDB
- Filter to Type 1 (individuals)
- LEFT JOIN from `core_providers` on NPI → enrich existing records
- INSERT new NPIs not in CMS data (flag as `medicare_biller = FALSE`)
- New providers have no utilization metrics but are searchable by location/specialty

**Processing:**
- DuckDB can handle 25GB in-memory on modern hardware (64GB+ RAM)
- If memory-constrained: stream-load via `read_csv_auto()` with filters
- Index on NPI for fast lookups

**Cost:** $0  
**Risk:** 9GB download (30+ min on slow connections). Mitigate: automate via cron, resume-capable download.

---

### 3. Open Payments (Sunshine Act)

**Status:** 🔴 Not implemented  
**Scale:** ~1M provider-payment records annually  
**Source:** https://openpaymentsdata.cms.gov/  
**Method:** Bulk CSV download (2GB+ per year)  
**Refresh:** Annual (published by June 30 each year for prior year)  
**Cost:** Free  

**What we get:**
- Industry-to-provider payments (pharma, device manufacturers, group purchasing orgs)
- Payment amount, date, nature of payment (consulting, speaking fees, meals, travel, research, ownership)
- Paying entity name
- Multi-year history (2013-present)

**Why we need it:**
1. **Targeting refinement:** Providers who already accept industry payments are statistically more receptive to sales outreach.
2. **Competitive intelligence:** See which reps/companies are engaging which providers.
3. **Relationship mapping:** Identify key opinion leaders (KOLs) — high-payment recipients are often influential.

**Schema additions:**
```sql
CREATE TABLE industry_relationships (
    npi TEXT NOT NULL,
    payment_year INTEGER,
    total_payment_amount NUMERIC,
    payment_count INTEGER,
    top_payer_name TEXT,
    top_payment_nature TEXT,
    has_ownership_interest BOOLEAN,
    kol_flag BOOLEAN,  -- derived: total_payments > $10K
    PRIMARY KEY (npi, payment_year)
);
```

**Targeting score adjustment:**
When Open Payments is integrated, the targeting formula shifts:
```
targeting_score = (claims_volume_pctile * 0.70) + (industry_payments_pctile * 0.30)
```

Providers with higher industry engagement get higher scores.

**Processing:**
- Load all years (2013-present) into `raw_open_payments`
- Aggregate by NPI + year → total amount, count, top payer
- Flag KOLs (>$10K total payments)
- Join to `core_providers` on NPI

**Cost:** $0  
**Risk:** Multi-year dataset is large. Mitigate: Load incrementally (most recent 3 years first), older years optional.

---

### 4. Doctors & Clinicians (Medicare.gov Provider Data)

**Status:** 🔴 Not implemented  
**Scale:** ~1M providers  
**Source:** https://data.medicare.gov/provider-data/dataset/Doctors-and-Clinicians/mj5m-pzi6  
**Method:** CSV download or API  
**Refresh:** Quarterly  
**Cost:** Free  

**What we get:**
- **Direct hospital affiliations** (not inferred via name matching)
- Group practice affiliations
- ACO participation

**Why we need it:**
Current hospital affiliation strategy is name + state matching (reassignment → hospital_enrollments). This is error-prone. The Doctors & Clinicians dataset provides explicit affiliation flags.

**Schema additions:**
```sql
ALTER TABLE hospital_affiliations ADD COLUMN affiliation_method TEXT;
-- Values: 'explicit' (from Doctors & Clinicians) or 'inferred' (from reassignment)

ALTER TABLE core_providers ADD COLUMN aco_participant BOOLEAN;
ALTER TABLE core_providers ADD COLUMN aco_name TEXT;
```

**Processing:**
- Load into `raw_doctors_clinicians`
- Join on NPI → update `hospital_affiliations` with `affiliation_method = 'explicit'`
- Keep inferred affiliations but flag as lower confidence

**Cost:** $0

---

### 5. NPI Registry API (Real-Time Lookups)

**Status:** 🟡 Optional (not bulk data)  
**Scale:** Individual NPI lookups  
**Source:** https://npiregistry.cms.gov/api/  
**Method:** REST API  
**Rate limit:** 1,200 requests per 5 minutes (~240/min)  
**Cost:** Free  

**Use case:**
Real-time verification of a single NPI when bulk data is stale or user wants live status.

**Implementation:**
- Add `pipeline/npi_lookup.py` for API client
- Cache results in DuckDB with `last_verified` timestamp
- Use for:
  - "Is this NPI still active?"
  - "Get the most recent address for this NPI"
  - User-submitted NPIs not in our bulk data

**Cost:** $0  
**Risk:** Rate limits. Mitigate: Queue + batch requests, respect 240/min limit.

---

### 6. State Medical Boards (Phase 3 - Advanced)

**Status:** 🔴 Future consideration  
**Scale:** Varies by state  
**Source:** 50+ state medical board websites  
**Method:** Web scraping (no unified API)  
**Cost:** Development time + scraping infrastructure  

**What we get:**
- License status (active, suspended, revoked)
- Disciplinary actions
- Board certifications
- Medical school, residency

**Why it matters:**
- **Compliance:** Sales reps should not contact providers with revoked licenses.
- **Board certification:** Filter by board-certified vs. non-certified for premium product targeting.

**Challenges:**
- No unified API — each state has different formats
- Anti-scraping measures (CAPTCHAs, rate limits)
- Data quality varies wildly

**Recommendation:** Defer to Phase 3. Focus on CMS + NPPES + Open Payments first (all free, bulk downloads, high ROI).

---

### 7. Google Places API (Already Integrated in Provider Search)

**Status:** ✅ Implemented (in provider-search repo)  
**Cost:** $0.032 per text search request  
**Use case:** Real-time place search for practice location details (phone, website, hours, reviews)

**Not a bulk data source** — used at query time, not for batch enrichment.

---

## Architecture

### Storage

**Database:** DuckDB (embedded analytical database)

**Why DuckDB:**
- Handles 25GB+ datasets in-memory (NPPES fits comfortably)
- Columnar storage (fast aggregations)
- No server required (single-file database)
- Supports `read_csv_auto()` for direct CSV querying without loading into memory
- SQL interface (portable, testable)

**Schema layers:**
1. **Raw layer:** `raw_*` tables (mirrors source CSVs, minimal transformation)
2. **Core layer:** `core_providers`, `utilization_metrics`, `practice_locations`, etc. (cleaned, joined, NPI-deduplicated)
3. **Derived layer:** `targeting_scores`, `provider_service_detail` (computed metrics)

### Processing Pipeline

**Language:** Python 3.14+ (current venv)  
**Dependencies:** `duckdb`, `requests`, `pandas` (minimal usage), `tqdm` (progress bars)

**Modules:**
- `pipeline/acquire.py` — download CSVs, call APIs, handle retries
- `pipeline/load.py` — load CSVs into DuckDB raw tables
- `pipeline/transform.py` — clean, join, deduplicate → core tables
- `pipeline/scoring.py` — compute targeting percentiles, composite scores
- `pipeline/dedup.py` — handle Type 1/Type 2 NPI conflicts
- `pipeline/config.py` — dataset URLs, column mappings

**New modules needed:**
- `pipeline/nppes.py` — handle 9GB download, decompress, load, enrich `core_providers`
- `pipeline/open_payments.py` — download multi-year data, aggregate, create `industry_relationships`
- `pipeline/doctors_clinicians.py` — load and join explicit hospital affiliations

**Execution:**
```bash
# Full pipeline (run weekly)
python -m pipeline.acquire        # download all CSVs
python -m pipeline.load           # load into DuckDB raw layer
python -m pipeline.transform      # create core tables
python -m pipeline.scoring        # compute targeting scores

# NPPES enrichment (run monthly)
python -m pipeline.nppes          # download, load, enrich core_providers

# Open Payments (run annually)
python -m pipeline.open_payments  # download, aggregate, join
```

**Orchestration:**
- Phase 1: Manual execution (good for development)
- Phase 2: cron job or Airflow DAG (for production weekly refreshes)

---

## Cost Analysis

### Bulk Data Acquisition

| Source | Size | Download Time | Storage | Processing Time | Cost |
|--------|------|---------------|---------|-----------------|------|
| CMS Public Data (10 datasets) | ~5GB | 20 min | 5GB | 10 min | $0 |
| NPPES | 9GB → 25GB uncompressed | 30 min | 25GB | 30 min | $0 |
| Open Payments (3 years) | 6GB | 20 min | 6GB | 15 min | $0 |
| Doctors & Clinicians | 500MB | 5 min | 500MB | 5 min | $0 |
| **Total** | **~36GB** | **75 min** | **36GB** | **60 min** | **$0** |

**Infrastructure cost:**
- Local development: $0 (use Blake's MacBook Pro, 64GB RAM recommended)
- Cloud VM (if needed): Hetzner CX52 (8 vCPU, 64GB RAM, 240GB SSD) = ~$50/month

---

### API Costs (Optional Real-Time Lookups)

| API | Use Case | Rate Limit | Cost per Request | Monthly Cost (1000 lookups) |
|-----|----------|-----------|------------------|---------------------------|
| NPI Registry API | Real-time NPI verification | 1,200 per 5 min | $0 | $0 |
| Google Places | Practice details (phone, reviews) | Pay-per-use | $0.032 | $32 |

**Recommendation:** Avoid API calls for bulk enrichment. Use APIs only for:
1. Real-time user queries (provider search app)
2. Selective verification (e.g., check 100 high-value NPIs weekly)

---

### LLM/Agent Costs (For Classification or Enrichment)

**Scenario:** Use Claude/GPT to classify 1M providers into custom categories (e.g., "interventional cardiologist" vs. "general cardiologist")

**Estimate:**
- 1M providers × 200 tokens per prompt = 200M tokens
- Claude Sonnet 3.5: $3 per 1M input tokens → **$600**
- Claude Haiku: $0.25 per 1M input tokens → **$50**

**Mitigation strategies:**
1. **Rule-based first:** Use HCPCS codes, taxonomy codes, and specialty fields to classify 80% of providers deterministically.
2. **LLM for edge cases:** Only send ambiguous providers to LLM (reduces volume by 80%).
3. **Batch processing:** Embed 100 providers per prompt (reduces API calls by 100x).
4. **Cheaper model:** Use Haiku for classification (12x cheaper than Sonnet).

**Adjusted cost:** $600 → $60 (80% rule-based) → $6 (Haiku) → $0.60 (batch 100 per prompt)

**Recommendation:** LLM enrichment is feasible if done strategically. Budget ~$100 for experimental classification.

---

## Execution Plan

### Phase 1: Baseline (COMPLETE)
- ✅ CMS Public Data Catalog (10 datasets)
- ✅ DuckDB pipeline
- ✅ Targeting scores
- ✅ Query templates

**Output:** 1.2M Medicare providers with utilization, prescribing, quality, practice affiliations

---

### Phase 2: Scale to 8M Providers (NEXT)

**Goal:** Cover all US providers, not just Medicare billers

**Tasks:**
1. **NPPES integration** (highest priority)
   - Download 9GB NPPES file
   - Load into `raw_nppes`
   - Enrich `core_providers` with gender, taxonomy, enumeration date
   - Insert non-Medicare NPIs (flag as `medicare_biller = FALSE`)
   - **Deliverable:** 8M searchable providers (5M without utilization data, 1.2M with)

2. **Open Payments integration**
   - Download 2013-2024 data (focus on recent 3 years first)
   - Aggregate by NPI + year → `industry_relationships` table
   - Flag KOLs (>$10K payments)
   - Adjust targeting score formula
   - **Deliverable:** Industry payment history for ~1M providers

3. **Doctors & Clinicians integration**
   - Download quarterly file
   - Join explicit hospital affiliations
   - Add ACO participation flags
   - **Deliverable:** Improved hospital affiliation accuracy

**Timeline:** 2-3 weeks (assuming 1-2 days per data source + testing)

**Blockers:** None (all data is free and publicly available)

---

### Phase 3: Advanced Enrichment (FUTURE)

**Optional enhancements:**
1. **State medical board data** (license verification, disciplinary actions)
   - Requires web scraping infrastructure
   - 50+ state websites, no unified API
   - Defer until Phase 2 is complete

2. **Social media profiles** (Twitter, LinkedIn)
   - Name-matching is unreliable without ground truth
   - Defer unless there's a strong user need

3. **Clinical trial participation** (ClinicalTrials.gov)
   - API available
   - Use case: Identify researchers/KOLs for pharmaceutical targeting
   - Low priority (niche use case)

---

## Key Decisions

### 1. Bulk Download vs. API

**Decision:** Prioritize bulk downloads over APIs.

**Rationale:**
- Bulk = predictable cost ($0), one-time processing
- APIs = per-request cost, rate limits, slower for millions of records

**When to use APIs:**
- Real-time user queries (Google Places in provider search)
- Selective verification (NPI Registry for user-submitted NPIs)

---

### 2. DuckDB vs. PostgreSQL

**Decision:** Stick with DuckDB for Phase 1-2.

**Rationale:**
- DuckDB handles 25GB+ analytical workloads easily
- No server overhead (single-file database)
- Columnar storage = fast aggregations
- If we need multi-user access later, migrate to PostgreSQL

**Migration path:** DuckDB → PostgreSQL is straightforward (both SQL-based).

---

### 3. LLM Enrichment: Yes or No?

**Decision:** Yes, but strategically.

**Use LLMs for:**
- Classifying ambiguous specialty descriptions (after rule-based filtering)
- Parsing unstructured text fields (e.g., credentials)
- Generating provider summaries for sales reps

**Do NOT use LLMs for:**
- Bulk classification of 8M providers (too expensive)
- Tasks that can be solved with regex/rules

**Budget:** ~$100 for experimental LLM enrichment.

---

### 4. Refresh Cadence

| Data Source | Refresh Frequency | Rationale |
|------------|------------------|-----------|
| CMS Utilization | Annual | CMS publishes once per year |
| NPPES | Monthly | NPI master file updates monthly |
| Open Payments | Annual | Published June 30 each year |
| Doctors & Clinicians | Quarterly | Medicare.gov updates quarterly |

**Recommendation:**
- **Weekly refresh:** CMS data (lightweight, API-based)
- **Monthly refresh:** NPPES (automate download via cron)
- **Annual refresh:** Open Payments (June/July)

---

## Success Metrics

### Coverage
- **Phase 1:** 1.2M Medicare providers ✅
- **Phase 2 goal:** 8M total providers (all active US NPIs)
- **NPPES coverage:** 100% of enumerated NPIs
- **Open Payments coverage:** ~1M providers with payment history

### Data Quality
- **NPI deduplication:** 0 duplicate NPIs in `core_providers`
- **Gender coverage:** >95% (from NPPES)
- **Taxonomy coverage:** >90% (from NPPES)
- **Hospital affiliation accuracy:** >80% (improved with Doctors & Clinicians)

### Performance
- **Full pipeline runtime:** <2 hours (including NPPES + Open Payments)
- **Query response time:** <5 seconds for complex multi-table joins
- **Storage efficiency:** <50GB total (raw + core + derived tables)

---

## Next Steps

1. **Review this strategy** with Blake → align on priorities
2. **Create `pipeline/nppes.py`** (highest ROI: 8M providers for $0)
3. **Create `pipeline/open_payments.py`** (industry payment history)
4. **Update schema** (`ALTER TABLE` scripts for gender, taxonomy, industry_relationships)
5. **Test on subset** (California providers only → validate before full load)
6. **Automate weekly refresh** (cron job or Airflow)

---

## Questions for Blake

1. **Priority order:** NPPES → Open Payments → Doctors & Clinicians? Or different sequence?
2. **LLM enrichment:** Worth budgeting $100 for experimental classification?
3. **Hosting:** Local MacBook Pro or Hetzner VPS for production?
4. **Target completion date:** 2-3 weeks realistic for Phase 2?
5. **State medical boards:** Defer to Phase 3, or explore sooner?
