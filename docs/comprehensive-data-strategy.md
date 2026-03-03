# Comprehensive Provider Data Strategy: Sources, Costs, Legal Terms & LLM Augmentation

**Last Updated:** 2026-02-15  
**Scope:** Scale from 1.2M Medicare providers → 8M+ US providers with web-crawled enrichment

---

## Executive Summary

This document defines a complete data acquisition, processing, and augmentation strategy for building a commercial provider intelligence platform. We combine:

1. **Public CMS datasets** (free, 1.2M providers with utilization/prescribing data)
2. **NPPES master file** (free, 8M NPIs with demographics)
3. **Open Payments** (free, industry payment history)
4. **Doctors & Clinicians** (free, hospital affiliations)
5. **Web crawling** (high-value specialties, practice websites, social profiles)
6. **LLM augmentation** (provider summaries, specialty classification, insights)

**Total data volume:** ~50GB raw + ~100GB processed  
**Total cost:** ~$54/month infrastructure + ~$500-$1,500 one-time LLM enrichment  
**Legal status:** All public data sources permit commercial use with attribution  

---

## 1. Data Sources: Size, Download Strategy & Legal Terms

### 1.1 CMS Public Data Catalog (Phase 1 - COMPLETE)

#### Datasets

| Dataset | UUID | Size | Rows | Update Frequency |
|---------|------|------|------|------------------|
| Physician by Provider | 8889d81e | 500 MB | 1.2M | Annual |
| Physician by Provider & Service | 92396110 | 2 GB | 10M | Annual |
| Part D by Provider | 14d8e8a9 | 300 MB | 1.2M | Annual |
| Part D by Provider & Drug | 9552739e | 3 GB | 25M | Annual |
| Reassignment (Group Practices) | e1f1fa9a | 100 MB | 1.5M | Annual |
| Hospital Enrollments | f6f6505c | 5 MB | 8K | Quarterly (API) |
| QPP Experience (MIPS) | 7adb8b1b | 200 MB | 1M | Annual |
| PECOS Enrollment | 2457ea29 | 300 MB | 1.5M | Annual |
| DME by Referring Provider | f8603e5b | 150 MB | 500K | Annual |
| Order & Referring | c99b5865 | 50 MB | 1.5M | Annual |
| **Total** | | **~6 GB** | **~43M rows** | |

#### Download Strategy

**Method:** CSV bulk download via HTTPS  
**Source:** https://data.cms.gov/  
**API alternative:** data.cms.gov API (paginated, 5000 rows/request) — use only for hospital_enrollments  

**Implementation:**
```python
# pipeline/acquire.py (existing)
import requests
from tqdm import tqdm

def download_cms_dataset(config: DatasetConfig):
    url = config.csv_url or f"{CMS_API_BASE}/{config.uuid}/data.csv"
    response = requests.get(url, stream=True)
    total_size = int(response.headers.get('content-length', 0))
    
    with open(config.csv_path, 'wb') as f:
        with tqdm(total=total_size, unit='B', unit_scale=True) as pbar:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                pbar.update(len(chunk))
```

**Parallelization:** Download 3-4 files simultaneously (Hetzner has 20TB monthly bandwidth)

**Resume capability:** Use `Range` headers for partial downloads if interrupted

#### Legal Terms

**License:** [CMS Data Use Agreement](https://data.cms.gov/about)  
**Commercial use:** ✅ Permitted  
**Attribution:** Required ("Data provided by Centers for Medicare & Medicaid Services")  
**Redistribution:** ✅ Allowed (must include attribution)  
**Restrictions:** None for aggregated data; PII redaction already applied by CMS  

**Key clause:**
> "CMS provides these data for public use and redistribution without fee, provided proper attribution is given."

**Compliance:**
- Add footer to provider search app: "Provider data sourced from CMS Public Data Catalog"
- Include attribution in API responses (optional `data_sources` field)

---

### 1.2 NPPES (National Plan & Provider Enumeration System)

#### Dataset Details

| Attribute | Value |
|-----------|-------|
| **Size (compressed)** | 9 GB (ZIP) |
| **Size (uncompressed)** | 25 GB (CSV) |
| **Rows** | ~8M NPIs (5-6M Type 1 individuals, 2-3M Type 2 organizations) |
| **Columns** | 329 columns (we need ~30) |
| **Update frequency** | Monthly (first Sunday of each month) |
| **Download URL** | https://download.cms.gov/nppes/NPI_Files.html |

#### What We Get

- **NPI** (unique identifier)
- **Gender** (Male/Female/Not Specified)
- **Taxonomy codes** (up to 15 per NPI) — more granular than Medicare specialty
- **Credentials** (MD, DO, NP, PA, PhD, etc.)
- **Practice locations** (up to 3 addresses per NPI)
- **Other identifiers** (state license numbers, DEA if disclosed)
- **Enumeration date** (when NPI was issued)
- **Deactivation date** (if inactive)
- **Sole proprietor flag**

#### Download Strategy

**Method:** Bulk ZIP download → decompress → load into DuckDB  

**Steps:**
1. Download `npidata_pfile_20260101-20260131.zip` (~9GB, 20-30 min on Hetzner)
2. Decompress to `data/raw/nppes.csv` (~25GB, 5 min)
3. Filter to Type 1 (individuals) during load → reduces to ~5-6M rows
4. DuckDB `read_csv_auto()` can query directly from CSV without loading into memory

**Implementation:**
```python
# pipeline/nppes.py (to be created)
import requests
import zipfile
from pathlib import Path

NPPES_URL = "https://download.cms.gov/nppes/NPPES_Data_Dissemination_January_2026.zip"

def download_nppes(output_dir: Path):
    zip_path = output_dir / "nppes.zip"
    csv_path = output_dir / "nppes.csv"
    
    # Download with resume support
    if not zip_path.exists():
        print("Downloading NPPES (9GB, ~30 min)...")
        response = requests.get(NPPES_URL, stream=True)
        with open(zip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024*1024):
                f.write(chunk)
    
    # Decompress
    print("Decompressing NPPES (5 min)...")
    with zipfile.ZipFile(zip_path, 'r') as z:
        z.extractall(output_dir)
    
    return csv_path

def enrich_core_providers(db_path: Path, nppes_csv: Path):
    import duckdb
    con = duckdb.connect(str(db_path))
    
    # Load NPPES (filtered to Type 1)
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_nppes AS
        SELECT 
            NPI as npi,
            "Provider Gender Code" as gender,
            "Provider First Name" as first_name,
            "Provider Last Name (Legal Name)" as last_name,
            "Healthcare Provider Taxonomy Code_1" as primary_taxonomy,
            "Provider Enumeration Date" as enumeration_date,
            "NPI Deactivation Date" as deactivation_date,
            "Is Sole Proprietor" as sole_proprietor
        FROM read_csv_auto(?, header=true)
        WHERE "Entity Type Code" = '1'  -- Type 1 only
    """, [str(nppes_csv)])
    
    # Enrich core_providers
    con.execute("""
        UPDATE core_providers
        SET 
            gender = raw_nppes.gender,
            primary_taxonomy = raw_nppes.primary_taxonomy,
            enumeration_date = raw_nppes.enumeration_date::DATE,
            deactivation_date = raw_nppes.deactivation_date::DATE,
            sole_proprietor = raw_nppes.sole_proprietor = 'Y'
        FROM raw_nppes
        WHERE core_providers.npi = raw_nppes.npi
    """)
    
    # Insert non-Medicare NPIs
    con.execute("""
        INSERT INTO core_providers (npi, first_name, last_name, gender, primary_taxonomy, medicare_biller)
        SELECT npi, first_name, last_name, gender, primary_taxonomy, FALSE
        FROM raw_nppes
        WHERE npi NOT IN (SELECT npi FROM core_providers)
    """)
    
    print(f"Enriched {con.execute('SELECT COUNT(*) FROM core_providers').fetchone()[0]} providers")
```

**Compute time:**
- Download: 20-30 min
- Decompress: 5 min
- Load into DuckDB: 15 min
- Enrich + insert new NPIs: 20 min
- **Total:** ~60 min

**Storage:**
- ZIP: 9 GB
- CSV: 25 GB
- DuckDB (indexed): 10 GB
- **Total:** ~44 GB (can delete CSV after load)

#### Legal Terms

**License:** [NPPES Data Dissemination Notice](https://download.cms.gov/nppes/NPI_Files.html)  
**Commercial use:** ✅ Permitted  
**Attribution:** Not required, but recommended  
**Redistribution:** ✅ Allowed  
**Restrictions:** None  

**Key clause:**
> "The NPI database is intended for public access and use... There are no restrictions on the use or disclosure of information contained in the NPI database."

**Compliance:** No action required (but add to attribution footer for completeness)

---

### 1.3 Open Payments (Sunshine Act)

#### Dataset Details

| Attribute | Value |
|-----------|-------|
| **Size per year** | ~2 GB (compressed CSV) |
| **Total (2013-2024)** | ~24 GB |
| **Rows per year** | ~10M payment records |
| **Update frequency** | Annual (published June 30) |
| **Download URL** | https://openpaymentsdata.cms.gov/ |

#### What We Get

- **Provider NPI** (links to our core_providers)
- **Paying entity** (pharma/device company name)
- **Payment amount** (total dollars)
- **Payment date**
- **Nature of payment** (consulting, speaking, meals, research, ownership)
- **Product category** (drug/device/medical supply)

#### Download Strategy

**Method:** Annual bulk CSV download per year  

**Priority years:** 2022, 2023, 2024 (most recent 3 years) → 6 GB  
**Optional:** Full history 2013-2021 → additional 18 GB  

**Implementation:**
```python
# pipeline/open_payments.py (to be created)
import requests
from pathlib import Path

OPEN_PAYMENTS_BASE = "https://download.cms.gov/openpayments"

def download_open_payments_year(year: int, output_dir: Path):
    url = f"{OPEN_PAYMENTS_BASE}/OP_DTL_GNRL_PGYR{year}_P06302025.zip"
    zip_path = output_dir / f"open_payments_{year}.zip"
    
    print(f"Downloading Open Payments {year} (~2GB)...")
    response = requests.get(url, stream=True)
    with open(zip_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=1024*1024):
            f.write(chunk)
    
    # Decompress
    with zipfile.ZipFile(zip_path, 'r') as z:
        z.extractall(output_dir)

def aggregate_by_npi(db_path: Path, years: list[int]):
    import duckdb
    con = duckdb.connect(str(db_path))
    
    for year in years:
        csv_path = f"data/raw/OP_DTL_GNRL_PGYR{year}_P06302025.csv"
        
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS raw_open_payments_{year} AS
            SELECT 
                "Covered_Recipient_NPI" as npi,
                "Total_Amount_of_Payment_USDollars" as payment_amount,
                "Date_of_Payment" as payment_date,
                "Nature_of_Payment_or_Transfer_of_Value" as payment_nature,
                "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name" as payer_name
            FROM read_csv_auto('{csv_path}', header=true)
            WHERE "Covered_Recipient_NPI" IS NOT NULL
        """)
    
    # Aggregate all years
    con.execute("""
        CREATE TABLE IF NOT EXISTS industry_relationships AS
        SELECT 
            npi,
            COUNT(*) as payment_count,
            SUM(payment_amount) as total_payment_amount,
            MAX(payment_date) as last_payment_date,
            ARRAY_AGG(DISTINCT payer_name ORDER BY payment_amount DESC LIMIT 3) as top_payers,
            SUM(payment_amount) > 10000 as kol_flag
        FROM (
            SELECT * FROM raw_open_payments_2022
            UNION ALL
            SELECT * FROM raw_open_payments_2023
            UNION ALL
            SELECT * FROM raw_open_payments_2024
        )
        GROUP BY npi
    """)
    
    print(f"Aggregated {con.execute('SELECT COUNT(*) FROM industry_relationships').fetchone()[0]} provider payment records")
```

**Compute time:**
- Download (3 years): 15-20 min
- Decompress: 5 min
- Load + aggregate: 30 min
- **Total:** ~50 min

**Storage:**
- ZIP (3 years): 6 GB
- CSV: 12 GB
- DuckDB aggregated: 500 MB
- **Total:** ~18.5 GB (can delete CSVs after aggregation)

#### Legal Terms

**License:** [Open Payments Data Use Agreement](https://openpaymentsdata.cms.gov/about)  
**Commercial use:** ✅ Permitted  
**Attribution:** Required  
**Redistribution:** ✅ Allowed  
**Restrictions:** Must display CMS disclaimer about data accuracy  

**Key clause:**
> "These data are made available under the Open Government Directive for public use."

**Compliance:**
- Add footer: "Industry payment data provided by CMS Open Payments"
- Include disclaimer: "Payment data self-reported by manufacturers; CMS does not verify accuracy"

---

### 1.4 Doctors & Clinicians (Medicare.gov Provider Data)

#### Dataset Details

| Attribute | Value |
|-----------|-------|
| **Size** | 500 MB (CSV) |
| **Rows** | ~1M providers |
| **Update frequency** | Quarterly |
| **Download URL** | https://data.medicare.gov/provider-data/dataset/Doctors-and-Clinicians/mj5m-pzi6 |

#### What We Get

- **Explicit hospital affiliations** (not inferred via name matching)
- **Group practice affiliations**
- **ACO participation** (Accountable Care Organization)

#### Download Strategy

**Method:** CSV download or API (paginated)  
**Recommendation:** CSV bulk download (faster)

**Compute time:**
- Download: 5 min
- Load + join: 10 min
- **Total:** 15 min

**Storage:** 500 MB

#### Legal Terms

**License:** [Medicare.gov Terms of Use](https://data.medicare.gov/terms-of-use)  
**Commercial use:** ✅ Permitted  
**Attribution:** Required ("Source: data.medicare.gov")  
**Redistribution:** ✅ Allowed  
**Restrictions:** None  

**Compliance:** Add to attribution footer

---

### 1.5 Web Crawling: High-Value Specialty Providers

#### Target Specialties for Deep Enrichment

**Criteria for "high-value":**
- High Medicare payment volume (top 10% in specialty)
- High prescribing volume (pharma targets)
- High DME referrals (device targets)
- MIPS score outliers (quality improvement targets)

**Top specialties for enrichment:**

| Specialty | Est. Count (Top 10%) | Rationale |
|-----------|---------------------|-----------|
| Cardiology | 5,000 | High-value procedures, device sales |
| Orthopedic Surgery | 4,000 | Joint replacements, DME, implants |
| Oncology | 3,000 | High drug costs, clinical trials |
| Gastroenterology | 2,500 | High procedure volume |
| Ophthalmology | 2,500 | Cataract surgeries, device sales |
| Neurology | 2,000 | Specialty drugs, diagnostics |
| Urology | 2,000 | Procedures, devices |
| Dermatology | 2,000 | Cosmetic + medical, brand drugs |
| **Total** | **~25,000** | |

#### Data Sources to Crawl

**Per provider (automated):**
1. **Practice website** (from Google Places API → already integrated)
   - Services offered
   - Staff bios
   - Insurance accepted
   - Patient reviews (aggregate from Healthgrades, Vitals, RateMDs)
   
2. **Professional profiles:**
   - Healthgrades: ratings, board certifications, conditions treated
   - Doximity: publications, clinical focus
   - LinkedIn: education, career history
   - Hospital affiliations (verify against our data)

3. **Clinical trials** (ClinicalTrials.gov API):
   - Active trials as PI (principal investigator)
   - Research focus areas

#### Web Crawling Strategy

**Tools:**
- **Playwright** (browser automation for JS-heavy sites)
- **BeautifulSoup** (HTML parsing)
- **Scrapy** (optional, for large-scale crawling)

**Architecture:**
```python
# pipeline/crawlers/provider_web_crawler.py

from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import asyncio
import json

async def crawl_provider_profile(npi: str, practice_website: str, output_dir: Path):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        # Crawl practice website
        await page.goto(practice_website, timeout=30000)
        html = await page.content()
        
        # Parse with BeautifulSoup
        soup = BeautifulSoup(html, 'html.parser')
        
        # Extract structured data
        profile = {
            'npi': npi,
            'website': practice_website,
            'services': extract_services(soup),
            'staff': extract_staff(soup),
            'insurance': extract_insurance(soup),
            'hours': extract_hours(soup),
            'crawled_at': datetime.now().isoformat()
        }
        
        # Save raw HTML + parsed JSON
        (output_dir / 'html' / f'{npi}.html').write_text(html)
        (output_dir / 'parsed' / f'{npi}.json').write_text(json.dumps(profile))
        
        await browser.close()

# Parallel crawling (8 workers)
async def crawl_batch(npis: list[str], websites: list[str]):
    tasks = [crawl_provider_profile(npi, url) for npi, url in zip(npis, websites)]
    await asyncio.gather(*tasks, return_exceptions=True)
```

**Rate limiting:**
- 1 request per 2 seconds per domain (respect robots.txt)
- 8 parallel workers (one per Hetzner vCPU)
- Random user-agent rotation
- Exponential backoff on errors

**Compute time (25,000 providers):**
- 2 seconds per provider × 25,000 = 50,000 seconds
- With 8 parallel workers: 50,000 / 8 = 6,250 seconds = **~2 hours**
- Add retries + delays: **~3-4 hours total**

**Storage:**
- Raw HTML: 25,000 × 100 KB = 2.5 GB
- Parsed JSON: 25,000 × 10 KB = 250 MB
- **Total:** ~2.75 GB

#### Legal Considerations

**Robots.txt compliance:** ✅ Required  
**Terms of service:** Review each site (Healthgrades, Doximity, etc.)  
**Rate limiting:** ✅ Implement (avoid DDOS-like behavior)  
**User-agent:** Identify as "ProviderSearchBot/1.0 (contact@example.com)"  

**Healthgrades:** TOS prohibit automated scraping → **use their API** (paid, ~$0.10/lookup)  
**Doximity:** TOS prohibit scraping → **skip** or use manual LinkedIn search  
**Public websites:** Generally allowed if robots.txt permits  

**Recommendation:** Focus on practice websites (public, no TOS restrictions) + ClinicalTrials.gov API (free)

---

## 2. LLM Augmentation Strategy

### 2.1 Use Cases

| Use Case | Input | Output | Model | Cost per Provider |
|----------|-------|--------|-------|-------------------|
| Provider summary | Utilization + prescribing + crawled bio | 2-3 sentence summary | Claude Haiku | $0.0002 |
| Specialty classification | Provider type + HCPCS codes + taxonomy | Granular specialty label | Claude Haiku | $0.0001 |
| Practice insights | Crawled website + reviews | Key differentiators | Claude Sonnet | $0.001 |
| Competitive positioning | Industry payments + market data | Sales strategy notes | Claude Sonnet | $0.002 |

### 2.2 Provider Summary Generation

**Goal:** Generate concise, sales-ready summaries for high-value providers

**Example input:**
```json
{
  "npi": "1234567890",
  "name": "Dr. Jane Smith",
  "specialty": "Cardiology",
  "utilization": {
    "tot_services": 8500,
    "tot_beneficiaries": 1200,
    "tot_medicare_payment": 2400000
  },
  "prescribing": {
    "rx_total_claims": 3500,
    "top_drugs": ["Eliquis", "Plavix", "Lipitor"]
  },
  "quality": {
    "mips_score": 92
  },
  "practice": {
    "group_name": "Bay Area Cardiology",
    "hospital_affiliations": ["Stanford Health"]
  }
}
```

**LLM prompt:**
```
Generate a 2-3 sentence provider summary for sales targeting. Focus on:
- Volume (high/medium/low compared to specialty peers)
- Clinical focus (procedures, conditions, prescribing patterns)
- Practice context (solo vs. group, hospital affiliations)

Provider data: {JSON}

Output format:
Dr. [Name] is a [high/medium/low]-volume [specialty] at [practice], seeing ~[N] Medicare patients annually. [Key clinical focus]. [Notable affiliations or quality metrics].
```

**Example output:**
> Dr. Jane Smith is a high-volume cardiologist at Bay Area Cardiology, seeing ~1,200 Medicare patients annually and performing 8,500+ services ($2.4M Medicare payments). Prescribing focus on anticoagulation (Eliquis, Plavix) suggests interventional cardiology practice. Affiliated with Stanford Health; MIPS score 92 (top 10%).

**Cost calculation:**
- Input: ~500 tokens (JSON data)
- Output: ~100 tokens (summary)
- Claude Haiku: $0.25 per 1M input tokens + $1.25 per 1M output tokens
- Cost per provider: ~$0.0002

**Total cost (25,000 high-value providers):**
- 25,000 × $0.0002 = **$5**

**Bulk optimization:**
- Batch 50 providers per prompt → reduce API calls by 50x
- Cost: $5 / 50 = **$0.10** 🎉

### 2.3 Specialty Classification

**Goal:** Map Medicare "provider_type" + NPPES taxonomy → granular specialty labels

**Example:**
- Medicare: "Internal Medicine"
- Taxonomy: "207R00000X"
- HCPCS: High volume of CPT 93306 (echocardiogram)
- → LLM classification: **"Cardiovascular Medicine (non-interventional)"**

**Prompt:**
```
Classify this provider's specialty based on taxonomy code, Medicare provider type, and top procedures.

Taxonomy: {taxonomy_code}
Medicare specialty: {provider_type}
Top 5 procedures: {hcpcs_codes}

Choose from: [list of 200+ granular specialties]

Output only the specialty label.
```

**Cost calculation:**
- Input: 200 tokens
- Output: 10 tokens
- Haiku: $0.0001 per provider

**Total cost (8M providers):**
- 8M × $0.0001 = **$800**

**Optimization:**
- Rule-based first: Use taxonomy + HCPCS matching for 80% of providers (free)
- LLM for ambiguous 20%: 1.6M × $0.0001 = **$160**
- Batch 100 per prompt: **$1.60** 🎉

### 2.4 Practice Insights (High-Value Only)

**Goal:** Synthesize crawled website + reviews into sales talking points

**Input:**
```json
{
  "website_text": "We specialize in minimally invasive cardiac procedures...",
  "services": ["Angioplasty", "Stent placement", "Cardiac catheterization"],
  "reviews_summary": "Patients praise short wait times and thorough explanations",
  "insurance": ["Medicare", "Blue Shield", "Aetna"]
}
```

**Prompt:**
```
Generate 3 sales talking points for a device rep targeting this cardiology practice.

Practice data: {JSON}

Output:
1. [Clinical focus insight]
2. [Patient experience differentiator]
3. [Practice operations note]
```

**Example output:**
> 1. Focus on minimally invasive techniques — strong fit for next-gen stent systems
> 2. High patient satisfaction around communication — responsive to educational materials
> 3. Broad insurance mix suggests no narrow network constraints

**Cost per provider:**
- Input: 1,000 tokens (website + reviews)
- Output: 150 tokens
- Claude Sonnet: $0.001

**Total cost (25,000 high-value providers):**
- 25,000 × $0.001 = **$25**

### 2.5 Total LLM Cost Estimate

| Task | Providers | Cost per | Optimization | Total |
|------|-----------|----------|--------------|-------|
| Provider summaries | 25,000 | $0.0002 | Batch 50 | $0.10 |
| Specialty classification | 1.6M (20%) | $0.0001 | Batch 100 | $1.60 |
| Practice insights | 25,000 | $0.001 | Batch 10 | $2.50 |
| **Total** | | | | **$4.20** |

**One-time enrichment:** $4.20 (shockingly cheap!)

**Annual refresh:** $4.20 per year (re-run after data updates)

**Recommendation:** Even without batching optimizations, total cost is <$1,000 — well within budget. Prioritize quality over cost savings here.

---

## 3. Infrastructure & Compute Resources

### 3.1 Hetzner Server: CX52

| Resource | Spec | Utilization | Cost |
|----------|------|-------------|------|
| vCPUs | 8 dedicated | 80% during pipeline runs | |
| RAM | 64 GB | Peak 50GB (NPPES load) | |
| Storage | 240 GB SSD | 150 GB used (raw + processed + backups) | |
| Network | 20 TB/month | ~50 GB/month (downloads + Supabase sync) | |
| **Total** | | | **€46/month (~$50)** |

**Compute time breakdown:**

| Task | Frequency | Duration | CPU | RAM |
|------|-----------|----------|-----|-----|
| CMS download | Weekly | 20 min | 20% | 2 GB |
| CMS transform | Weekly | 30 min | 80% | 8 GB |
| NPPES download | Monthly | 30 min | 20% | 5 GB |
| NPPES enrich | Monthly | 30 min | 80% | 50 GB |
| Open Payments download | Annual | 20 min | 20% | 2 GB |
| Open Payments aggregate | Annual | 30 min | 80% | 10 GB |
| Web crawl (25K providers) | Quarterly | 4 hours | 80% | 4 GB |
| LLM enrichment (25K) | Quarterly | 1 hour | 20% | 2 GB |
| Supabase sync | Daily | 15 min | 40% | 4 GB |

**Peak load:** NPPES monthly refresh (80% CPU, 50GB RAM for 1 hour)

**Idle time:** 95%+ (cron jobs run a few hours per week)

**Scaling considerations:**
- Current: Single CX52 handles everything
- Future (>50K web crawls): Add CX42 worker node for parallel crawling
- Future (>100M LLM calls): Switch to cloud GPU (Runpod, Lambda Labs)

### 3.2 Supabase

**Plan:** Pro ($25/month)  
**Database size:** ~20 GB (core tables only, no raw data)  
**API requests:** ~10K/day (provider search queries)  
**Storage:** 8 GB (PostgreSQL)  
**Bandwidth:** ~10 GB/month (API responses)  

**Schema:**
- `core_providers` (8M rows, 5 GB)
- `utilization_metrics` (1.2M rows, 2 GB)
- `practice_locations` (2M rows, 500 MB)
- `hospital_affiliations` (500K rows, 200 MB)
- `provider_quality_scores` (1M rows, 300 MB)
- `industry_relationships` (1M rows, 500 MB)
- `provider_summaries` (25K rows, 10 MB) — LLM-generated

**Indexes:**
- `core_providers(npi)` — primary key
- `core_providers(specialty, state)` — common filter
- `utilization_metrics(npi)` — foreign key
- Full-text search on `provider_summaries.summary_text`

**Estimated query performance:**
- "Find cardiologists in California": <500ms
- "Get full provider profile by NPI": <100ms
- "Search providers by name": <1s (full-text search)

---

## 4. Cost Summary

### 4.1 Infrastructure (Monthly)

| Component | Cost |
|-----------|------|
| Hetzner CX52 | $50 |
| Supabase Pro | $25 |
| Hetzner Storage Box (backups) | $4 |
| **Total** | **$79/month** |

### 4.2 Data Acquisition (One-Time + Recurring)

| Source | Frequency | Cost |
|--------|-----------|------|
| CMS data | Weekly | $0 |
| NPPES | Monthly | $0 |
| Open Payments | Annual | $0 |
| Doctors & Clinicians | Quarterly | $0 |
| Web crawling | Quarterly | $0 (bandwidth included) |
| **Total** | | **$0** |

### 4.3 LLM Enrichment (One-Time + Recurring)

| Task | Frequency | Cost |
|------|-----------|------|
| Initial enrichment (25K providers) | One-time | $4 |
| New provider enrichment (5K/year) | Annual | $1 |
| Re-summarize after data refresh (25K) | Annual | $4 |
| **Total Year 1** | | **$9** |

### 4.4 Total Cost

| Category | Year 1 | Ongoing (Annual) |
|----------|--------|------------------|
| Infrastructure | $948 | $948 |
| Data | $0 | $0 |
| LLM | $9 | $5 |
| **Total** | **$957** | **$953/year** |

**ROI calculation:**
- Cost: ~$80/month
- Value: 8M searchable providers + 25K deep profiles
- Break-even: 1-2 paying customers at $50-100/month each

---

## 5. Legal & Compliance Summary

### 5.1 Data Use Permissions

| Source | Commercial Use | Attribution | Redistribution | Restrictions |
|--------|---------------|-------------|----------------|--------------|
| CMS Public Data | ✅ | Required | ✅ | None |
| NPPES | ✅ | Optional | ✅ | None |
| Open Payments | ✅ | Required + disclaimer | ✅ | Display accuracy disclaimer |
| Doctors & Clinicians | ✅ | Required | ✅ | None |
| Web crawling (public sites) | ✅ | Site-specific | ❌ | Respect robots.txt, TOS |

### 5.2 Attribution Requirements

**Required footer for provider search app:**

> Provider data sourced from:
> - Centers for Medicare & Medicaid Services (CMS)
> - CMS National Plan & Provider Enumeration System (NPPES)
> - CMS Open Payments Program
> - Medicare.gov Doctors & Clinicians
> 
> Industry payment data self-reported by manufacturers; CMS does not verify accuracy.

### 5.3 HIPAA Considerations

**Good news:** All data sources are **de-identified** and publicly available. No PHI (Protected Health Information) is included.

**No HIPAA compliance required** for aggregated utilization/prescribing data at the provider level.

**If adding patient-level data in future:** HIPAA Business Associate Agreement (BAA) required. Not applicable for Phase 1-2.

### 5.4 Terms of Service for Crawling

| Site | Scraping Allowed | API Available | Cost |
|------|------------------|---------------|------|
| Practice websites | ✅ (if robots.txt allows) | No | $0 |
| Healthgrades | ❌ (TOS prohibit) | ✅ (paid) | ~$0.10/lookup |
| Doximity | ❌ (TOS prohibit) | ❌ | N/A |
| ClinicalTrials.gov | ✅ | ✅ (free API) | $0 |
| LinkedIn | ❌ (TOS prohibit) | ✅ (paid, enterprise) | ~$500/month |

**Recommendation:**
- Crawl practice websites only (public, no TOS restrictions)
- Use ClinicalTrials.gov API (free)
- Skip Healthgrades/Doximity (not worth $0.10/lookup for 25K providers = $2,500)
- LinkedIn: manual search only (not worth $500/month for Phase 1)

---

## 6. Execution Timeline

### Phase 1: CMS Baseline (COMPLETE)
**Duration:** 2 weeks  
**Status:** ✅ Done  
**Deliverables:** 1.2M providers, 10 CMS datasets, DuckDB pipeline

---

### Phase 2A: NPPES Integration (NEXT)
**Duration:** 1 week  
**Tasks:**
- [ ] Create `pipeline/nppes.py`
- [ ] Download NPPES (9GB)
- [ ] Enrich `core_providers` with gender, taxonomy
- [ ] Insert 6M non-Medicare NPIs
- [ ] Test queries on 8M total providers

**Deliverables:** 8M searchable providers

---

### Phase 2B: Open Payments
**Duration:** 1 week  
**Tasks:**
- [ ] Create `pipeline/open_payments.py`
- [ ] Download 2022-2024 data (6GB)
- [ ] Aggregate by NPI → `industry_relationships`
- [ ] Adjust targeting score formula
- [ ] Flag KOLs (>$10K payments)

**Deliverables:** Industry payment history for 1M providers

---

### Phase 2C: Doctors & Clinicians
**Duration:** 3 days  
**Tasks:**
- [ ] Download quarterly file (500MB)
- [ ] Join explicit hospital affiliations
- [ ] Add ACO participation flags
- [ ] Update `hospital_affiliations` table

**Deliverables:** Improved hospital affiliation accuracy

---

### Phase 3: Web Crawling + LLM Enrichment
**Duration:** 2 weeks  
**Tasks:**
- [ ] Create `pipeline/crawlers/provider_web_crawler.py`
- [ ] Identify 25K high-value providers (top 10% per specialty)
- [ ] Crawl practice websites (4 hours, 8 parallel workers)
- [ ] Extract structured data (services, staff, reviews)
- [ ] Generate LLM summaries (1 hour, batched)
- [ ] Create `provider_summaries` table in Supabase

**Deliverables:** 25K deep profiles with sales-ready summaries

---

### Phase 4: Production Automation
**Duration:** 1 week  
**Tasks:**
- [ ] Set up Hetzner server (CX52)
- [ ] Configure cron jobs (weekly/monthly/annual refreshes)
- [ ] Automate Supabase sync
- [ ] Set up monitoring (disk, logs, errors)
- [ ] Create backup scripts

**Deliverables:** Fully automated data pipeline

---

## 7. Risks & Mitigations

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| NPPES download failure (9GB) | High | Medium | Resume-capable download, retry logic |
| DuckDB OOM on 64GB RAM | High | Low | Stream-load CSVs, delete intermediates |
| Website crawling rate limits | Medium | High | 2 sec/request, respect robots.txt, exponential backoff |
| Healthgrades/Doximity TOS violation | High | Medium | Skip scraping, use APIs or manual search |
| Supabase storage limit (8GB free) | Medium | Low | Upgrade to Pro ($25/month, 8GB → 500GB) |
| LLM API rate limits | Low | Low | Claude rate limit: 4,000 RPM (requests/min) — batching keeps us under limit |
| Data freshness (annual CMS updates) | Low | High | Clearly label data vintage in UI ("Data as of 2024") |

---

## 8. Next Steps

1. **Review this strategy** with Blake → confirm priorities
2. **Provision Hetzner CX52** (~30 min setup)
3. **Create missing pipeline modules:**
   - `pipeline/nppes.py` (NPPES download + enrich)
   - `pipeline/open_payments.py` (Open Payments download + aggregate)
   - `pipeline/export.py` (DuckDB → CSV for Supabase)
   - `pipeline/crawlers/provider_web_crawler.py` (web scraping)
4. **Execute Phase 2A** (NPPES integration, 1 week)
5. **Test Supabase sync** (export 8M providers, measure performance)
6. **Execute Phase 2B-C** (Open Payments + Doctors & Clinicians, 1.5 weeks)
7. **Execute Phase 3** (web crawling + LLM enrichment, 2 weeks)
8. **Automate production pipeline** (cron jobs, monitoring, 1 week)

**Total timeline:** 6 weeks from start to fully automated production system

---

## Questions for Blake

1. **Phase 2 priority:** NPPES → Open Payments → Doctors & Clinicians? Or different order?
2. **Web crawling scope:** 25K high-value providers sufficient, or target more?
3. **LLM enrichment:** Run all 3 tasks (summaries + classification + insights), or prioritize 1-2?
4. **Healthgrades API:** Worth $2,500 (25K × $0.10) for review data, or skip?
5. **Timeline:** 6 weeks realistic, or need faster/slower?
6. **Legal review:** Comfortable with attribution requirements, or need lawyer review?

Ready to start building!
