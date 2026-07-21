# CMS Healthcare Data Pipeline

Infrastructure for ingesting, transforming, and serving 90M+ rows of CMS public healthcare data for provider intelligence and market analysis.

**Production API:** [http://5.78.148.70:8080](http://5.78.148.70:8080)

---

## Overview

This pipeline consolidates fragmented CMS public datasets into a unified data warehouse, enabling:
- Provider intelligence (Medicare volume, prescribing patterns, quality scores)
- Healthcare market analysis (network mapping, referral patterns)
- Entity resolution across disparate data sources
- API access for downstream applications

For the current repository boundaries, refresh policies, promotion model, and implementation order,
see [`docs/data-platform-operating-model.md`](docs/data-platform-operating-model.md). Some older
examples below describe planned directories or commands; verify operational behavior against the
current `pipeline/`, `schema/`, and `api/` code.

The product and data contract for weekly NPPES change detection is documented in
[`docs/new-provider-radar.md`](docs/new-provider-radar.md).

### Read-only source status

The data-platform status command discovers publisher releases without downloading bulk files or
opening DuckDB:

```bash
# Live publisher metadata
.venv/bin/python -m pipeline.data_platform status

# Machine-readable output
.venv/bin/python -m pipeline.data_platform status --json

# Checked-in metadata fixtures; no network access
.venv/bin/python -m pipeline.data_platform status --offline --json
```

By default the command reads `data/manifests.json` if it exists, but it never creates or updates the
file. A source is `current` only when a validated, actively promoted manifest proves that its
publisher version matches live discovery. Missing or ambiguous provenance remains `unknown`.
Exit codes are `0` for all current, `1` for any stale or unknown source, and `2` for publisher
unavailability or a discovery-contract error.

### Immutable staged acquisition

All registered CMS CSV, NPPES V2, Open Payments, and AACT sources use the immutable acquisition
path. A dry run performs live publisher discovery but does not download or write anything:

```bash
.venv/bin/python -m pipeline.data_platform acquire cms_hospital_enrollments --dry-run
```

An actual acquisition writes only below the selected staging data root. Source-specific transfer
and expansion ceilings apply. CSV sources are schema- and row-validated; ZIP sources are checked for
safe member paths, encryption, required members, CRC integrity, compressed and uncompressed size,
and member-list fingerprint. Every run records bytes, SHA-256, source period, publisher version,
retrieval time, and code commit. The resulting manifest remains `not_promoted`; acquisition never
opens DuckDB or changes an active release pointer.

```bash
.venv/bin/python -m pipeline.data_platform acquire cms_hospital_enrollments \
  --data-root data --json
```

### Versioned staging warehouses

A validated source run can be loaded into a complete candidate copied from a checksum-verified
warehouse backup. The environment flag is intentionally restricted to `staging`:

```bash
.venv/bin/python -m pipeline.data_platform build-release \
  --environment staging \
  --source-run-id <validated-run-id> \
  --backup-manifest <verified-backup-manifest.json> \
  --data-root data --json

.venv/bin/python -m pipeline.data_platform promote \
  --environment staging \
  --warehouse-release-id <warehouse-release-id> \
  --data-root data --json

.venv/bin/python -m pipeline.data_platform rollback \
  --environment staging \
  --data-root data --json
```

The complete DuckDB candidate uses the ten CMS runs plus monthly and weekly NPPES and all three Open
Payments categories. Repeat `--source-run-id` exactly once for each of those 15 validated runs:

```bash
.venv/bin/python -m pipeline.data_platform build-platform-release \
  --environment staging \
  --backup-manifest <verified-backup-manifest.json> \
  --data-root data \
  --source-run-id <run-1> --source-run-id <run-2> \
  --source-run-id <...remaining-13-runs> --json

.venv/bin/python -m pipeline.data_platform prepare-aact-release \
  --environment staging \
  --source-run-id <validated-aact-run-id> \
  --data-root data --output-root <immutable-artifact-root> --json

.venv/bin/python -m pipeline.data_platform stage-aact-database \
  --environment staging \
  --release-manifest <immutable-aact-release.json> \
  --restore-log <new-absolute-log-path> \
  --evidence <new-absolute-evidence-path> --json
```

The builder never opens `DUCKDB_PATH`. It copies the verified backup to a new partial candidate,
strictly replaces publisher-shaped raw tables, rebuilds CMS-derived tables, applies the monthly
NPPES baseline followed by the weekly overlay and Radar events, rebuilds Open Payments aggregates,
records release-wide exact smoke counts, computes the completed database checksum, and atomically
renames the candidate. AACT remains PostgreSQL-backed; its command prepares a sealed `postgres.dmp`
and dictionary release. The staging restore command creates a new release-derived PostgreSQL
database, validates it through the read-only role, and has no drop, rename, or promotion operation.
Promotion changes only the
staging symlink and records a recoverable journal. The staging CLI has no production environment
option; production serving releases use the separate ledger described below.

Focused data-platform tests run from the API test directory so they are included in the repository's
complete suite:

```bash
cd api && ../.venv/bin/python -m pytest \
  test_data_platform.py test_acquisition.py test_archive_acquisition.py \
  test_archive_sources.py test_releases.py \
  test_production.py test_production_smoke.py -q
cd api && ../.venv/bin/python -m pytest -q
```

### Production release bundle

Production serving state is managed separately from staging. One atomic `release-current` symlink
selects a versioned bundle whose internal links name immutable code, runtime, and DuckDB artifacts.
The manager validates and journals that selection without opening DuckDB or reading API secrets.
The separate cutover command performs the one approved restart and automatically selects, restarts,
and verifies the predecessor if candidate smoke checks fail.

```bash
.venv/bin/python -m pipeline.production status \
  --production-root /srv/cms-data-platform/production --json

.venv/bin/python -m pipeline.production prepare \
  --production-root /srv/cms-data-platform/production \
  --artifact-root /srv/cms-data-platform/production-artifacts \
  --data-root /srv/cms-data-platform/data \
  --code-path /srv/cms-data-platform/production-artifacts/code/<full-git-commit> \
  --runtime-path /srv/cms-data-platform/production-artifacts/runtimes/<runtime-id> \
  --warehouse-path /srv/cms-data-platform/production-artifacts/warehouses/<warehouse-id>/warehouse.duckdb \
  --warehouse-release-id <warehouse-release-id> \
  --dry-run --json
```

The one-time legacy bootstrap, activation, verification, rollback, systemd installation, and smoke
commands are approval-gated operational steps documented in
[`docs/production-promotion-runbook.md`](docs/production-promotion-runbook.md). Production promotion
switches the one bundle pointer; it never overwrites the active or rollback database file in place.

The production freshness monitor is deliberately separate from refresh execution. Its daily systemd
timer performs only primary-publisher metadata discovery and resolves installed provenance from the
manifest snapshot belonging to the selected deployment:

```bash
/srv/cms-data-platform/production/release-current/runtime/bin/python \
  -m pipeline.production_status_monitor \
  --production-root /srv/cms-data-platform/production --json
```

See [`deploy/systemd/README.md`](deploy/systemd/README.md) for installation and journal commands, and
the [operating model](docs/data-platform-operating-model.md#operational-refresh-gates) for the monthly
NPPES reconciliation, weekly incremental chain, daily targeted Registry API verification, and
source-specific promotion gates. The monitor never downloads a dataset or opens DuckDB.

**Total data:** 90M+ rows across 30+ tables (~5.5GB)

---

## Data Sources

| Dataset | Records | What it provides |
|---------|---------|------------------|
| **NPPES NPI Registry** | 8M providers | Demographics, addresses, taxonomies, affiliations |
| **Medicare Utilization (Physicians)** | 1.3M records | Patient volume, procedures, payments by provider |
| **Medicare Utilization (Hospitals)** | 3K hospitals | Inpatient/outpatient claims, DRG codes, payments |
| **Part D Prescribing** | 28M records | Drug prescriptions by provider, costs, patient counts |
| **Open Payments (General)** | 14.7M records | Industry payments to providers (speaking, consulting, etc.) |
| **Open Payments (Research)** | 1.1M records | Research payments and grants |
| **Doctors & Clinicians** | 2.7M records | National provider comparisons |
| **Facility Affiliations** | 1.6M records | Provider-hospital relationships |
| **Hospital Info** | 5.4K facilities | Hospital characteristics, ownership, bed counts |
| **MIPS Performance** | 541K records | Quality scores, performance metrics |

**All datasets are CMS public data** — no PHI, no HIPAA constraints.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  CMS Public Data Sources (data.cms.gov)                             │
│  • NPPES API                                                        │
│  • Bulk CSV Downloads (Medicare, Prescribing, Open Payments)       │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│  INGEST LAYER (Python scripts)                                      │
│  • Fetch CSVs from CMS bulk download endpoints                      │
│  • Validate schemas                                                 │
│  • Load raw data into DuckDB                                        │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│  TRANSFORM LAYER (SQL + Python)                                     │
│  • Normalize column names                                           │
│  • Deduplicate records                                              │
│  • Build composite tables (provider_master, enrichment_layer)       │
│  • Create indexes for fast lookups                                  │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│  DuckDB Warehouse (provider_searcher.duckdb)                        │
│  • 30+ tables, 90M+ rows, 5.5GB                                     │
│  • Read-optimized for analytics                                     │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│  SERVE LAYER (FastAPI + DuckDB)                                     │
│  • Read-only API (port 8080)                                        │
│  • Provider search, enrichment, market analysis                     │
│  • Interactive SQL query interface                                  │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Key Features

### Entity Resolution Matching Engine

Matches providers across disparate datasets using cascading logic:

1. **Exact matching:** NPI joins (when available)
2. **Fuzzy matching:** Name + ZIP code (95% confidence)
3. **Multi-address:** Try all provider addresses (handles relocations)
4. **LLM fallback:** Ambiguous cases resolved with reasoning

**Match rate:** 62% for Google Places → NPPES  
**Read more:** [Matching Logic](docs/MATCHING.md)

### Composite Scoring

Combines multiple signals into actionable scores:
- **Medicare Volume Score:** Patient counts + revenue
- **Prescribing Influence:** High-value drug prescriptions
- **Quality Score:** MIPS performance metrics
- **Industry Relationships:** Open Payments totals

**Use case:** Prioritize providers for outreach based on data-driven targeting.

### API Access

FastAPI service providing:
- Provider search by name, NPI, location
- Enrichment (join all CMS data for a given NPI)
- Market analysis queries (top providers by specialty/location)
- Interactive SQL interface

**Endpoint:** `http://5.78.148.70:8080`  
**Docs:** `http://5.78.148.70:8080/docs`

---

## Project Structure

```
cms-data/
├── ingest/                 # Data ingestion scripts
│   ├── nppes.py            # NPPES API client
│   ├── bulk_cms.py         # CSV downloads
│   └── open_payments.py    # Open Payments loader
├── transform/              # SQL transformations
│   ├── dedupe.sql          # Deduplication logic
│   ├── enrich.sql          # Join logic for enrichment
│   └── indexes.sql         # Performance indexes
├── api/                    # FastAPI service
│   ├── main.py             # API entry point
│   ├── routers/            # API endpoints
│   └── database.py         # DuckDB connection
├── dashboard/              # Web UI for exploration
│   └── index.html          # Interactive query interface
├── data/                   # DuckDB warehouse
│   └── provider_searcher.duckdb
└── docs/                   # Documentation
    ├── MATCHING.md         # Entity resolution logic
    └── DATA-SOURCES.md     # Dataset schemas
```

---

## Getting Started

### Prerequisites

- Python 3.11+
- DuckDB
- 10GB+ disk space (for raw + processed data)

### Setup

```bash
# Clone repo
git clone https://github.com/blakethom8/cms-data.git
cd cms-data

# Install dependencies
pip install -r requirements.txt

# Run ingest (downloads and loads all data)
python ingest/run_all.py

# Run transformations
python transform/run_all.py

# Start API
uvicorn api.main:app --host 0.0.0.0 --port 8080
```

### Quick Test

```bash
# Query NPPES
python -c "
import duckdb
db = duckdb.connect('data/provider_searcher.duckdb')
print(db.execute('SELECT COUNT(*) FROM nppes').fetchone())
"
# Expected: (8000000+,)
```

---

## Use Cases

### 1. Provider Intelligence

**Question:** "Which cardiologists in LA County have the highest Medicare volume?"

```sql
SELECT 
    n.npi,
    n.first_name || ' ' || n.last_name AS name,
    n.primary_taxonomy,
    u.total_medicare_patients,
    u.total_medicare_revenue
FROM nppes n
JOIN medicare_utilization u ON n.npi = u.npi
WHERE n.state = 'CA'
  AND n.city LIKE '%LOS ANGELES%'
  AND n.primary_taxonomy LIKE '%Cardio%'
ORDER BY u.total_medicare_patients DESC
LIMIT 20;
```

### 2. Prescribing Pattern Analysis

**Question:** "Who are the top Ozempic prescribers in California?"

```sql
SELECT 
    p.npi,
    n.first_name || ' ' || n.last_name AS name,
    p.drug_name,
    p.total_claim_count,
    p.total_drug_cost
FROM prescribing p
JOIN nppes n ON p.npi = n.npi
WHERE p.drug_name LIKE '%SEMAGLUTIDE%'
  AND n.state = 'CA'
ORDER BY p.total_claim_count DESC
LIMIT 20;
```

### 3. Market Share Analysis

**Question:** "What's the referral network for Cedars-Sinai?"

```sql
SELECT 
    f.provider_name,
    COUNT(DISTINCT f.npi) AS affiliated_providers,
    n.primary_taxonomy,
    COUNT(*) AS total_affiliations
FROM facility_affiliations f
JOIN nppes n ON f.npi = n.npi
WHERE f.facility_name LIKE '%CEDARS%SINAI%'
GROUP BY f.provider_name, n.primary_taxonomy
ORDER BY affiliated_providers DESC;
```

---

## API Examples

### Provider Search

```bash
curl "http://5.78.148.70:8080/providers/search?name=John+Smith&city=Pasadena&state=CA"
```

**Response:**
```json
{
  "results": [
    {
      "npi": "1234567890",
      "name": "John Smith",
      "credentials": "MD",
      "specialty": "Cardiology",
      "address": "123 Main St, Pasadena, CA 91101",
      "medicare_patients": 487,
      "medicare_revenue": 1200000,
      "mips_score": 87
    }
  ]
}
```

### Enrichment

```bash
curl "http://5.78.148.70:8080/providers/1234567890/enrich"
```

Returns all CMS data joined for that NPI (utilization, prescribing, quality, payments).

---

## Performance

**Data loading:** ~30 minutes (all datasets)  
**Query latency:** <100ms (indexed lookups)  
**Storage:** 5.5GB compressed  
**API throughput:** ~500 req/sec (read-only)

---

## Downstream Applications

This pipeline powers:
1. **[Provider Search](https://github.com/blakethom8/provider-search)** — Contact enrichment and intelligence layer
2. **Internal BD Tools** — Cedars-Sinai business development analytics
3. **Market Analysis** — Healthcare ecosystem mapping

---

## Roadmap

### ✅ Phase 1: Data Pipeline (Complete)
- [x] Ingest 10+ CMS datasets
- [x] DuckDB warehouse setup
- [x] Basic transformations
- [x] FastAPI read-only API

### 🚧 Phase 2: Intelligence Layer (In Progress)
- [x] Entity resolution matching
- [ ] LLM-powered matching for ambiguous cases
- [ ] Composite scoring models
- [ ] Specialty taxonomy mapping

### 📋 Phase 3: Advanced Features (Planned)
- [ ] Real-time CMS data updates (monthly refresh)
- [ ] Supabase export (for production apps)
- [ ] GraphQL API
- [ ] Data quality monitoring
- [ ] Machine learning match model

---

## Data Freshness

**NPPES:** Updated monthly (first week of each month)  
**Medicare Utilization:** Annual (released ~6 months after year-end)  
**Prescribing:** Annual (released ~6 months after year-end)  
**Open Payments:** Annual (released June)  
**MIPS:** Annual (released ~9 months after performance year)

**Current data:** As of February 2026

---

## License

Private — All rights reserved

**Note:** CMS data is public domain, but this pipeline and derived datasets are proprietary.

---

*Built to unlock healthcare intelligence at scale.*
