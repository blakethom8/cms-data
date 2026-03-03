# Provider Searcher: Data Strategy & Schema Rationale

## What We're Building

Provider Searcher is an analytics platform for medical sales reps and physician liaisons. The core use case: a cardiology device rep covering California needs to answer "Which cardiologists in my territory should I prioritize?" — ranked by volume, prescribing patterns, quality gaps, and practice affiliations.

Everything in Phase 1 is built from the **CMS Public Data Catalog**, a collection of 100+ publicly available Medicare datasets hosted on data.cms.gov. No private data, no scraping, no paid APIs.

---

## Why These 10 Datasets?

Out of 100+ CMS datasets, most are geographic aggregates (state/county-level stats), program-specific enrollment files, or cost reports. Only a handful provide **NPI-level** (individual provider) data — which is what we need for targeting specific physicians.

We selected 10 datasets that, when joined together on NPI, create a complete provider profile: who they are, where they practice, what they do, how much they bill, what they prescribe, and how they score on quality.

### The Foundational Dataset

**`medicare_physician_other_practitioners_by_provider`** (UUID: `8889d81e`)

This single file is the backbone of the entire system. For each of ~1.2 million Medicare-billing providers it contains:

- **Identity**: NPI, name, credentials, specialty
- **Location**: street address, city, state, ZIP, rural/urban classification (RUCA)
- **Volume**: total services, beneficiaries, submitted charges, Medicare payments
- **Patient mix**: average age, risk score, dual-eligible count
- **Chronic conditions**: 25+ disease prevalence rates (% of that provider's patients with diabetes, heart failure, COPD, etc.)

This replaces what would normally require the 9GB NPPES master file (which only has demographics, no utilization) AND a separate claims dataset. The tradeoff: it only covers Medicare-billing providers, not every NPI in the country. For our use case (medical sales targeting), Medicare providers are the primary audience.

**Entity type filtering**: This file contains both Individual (Type 1) and Organization (Type 2) NPIs. We filter to `entity_type_code = 'I'` for targeting, since sales reps visit individual physicians, not corporate entities. Type 2 records are used only to establish practice/hospital relationships.

### Procedure-Level Detail

**`medicare_physician_other_practitioners_by_provider_and_service`** (UUID: `92396110`)

The foundational dataset tells you a cardiologist billed $2M last year. This one tells you *what they did* — broken down by HCPCS procedure code and place of service (office vs. facility).

**Why it matters**: A device rep selling cardiac stents needs to find interventional cardiologists who perform catheterizations (CPT 93458-93461) and PCIs (92928), not general cardiologists who only do office visits and EKGs. This dataset makes that distinction possible.

**Size warning**: ~10M rows. We filter to only providers already in `core_providers` during transformation.

### Prescribing Data

**`medicare_part_d_prescribers_by_provider`** (UUID: `14d8e8a9`)

NPI-level prescribing summary: total claims, total drug cost, brand vs. generic split, opioid prescribing rate. Joined to utilization_metrics on NPI.

**Why it matters**: Pharma reps need to know prescribing volume and brand affinity. A physician writing 500 brand-name claims per year with a 70% brand ratio is a very different target than one writing 50 generic-only claims.

**`medicare_part_d_prescribers_by_provider_and_drug`** (UUID: `9552739e`)

NPI + specific drug name. The most granular prescribing data available.

**Why it matters**: "Find every physician in Texas prescribing Eliquis" is a real sales query. This dataset answers it directly. ~25M rows — the largest dataset in our pipeline, loaded only in Phase 2 or on-demand.

### Practice Relationships

**`revalidation_clinic_group_practice_reassignment`** (UUID: `e1f1fa9a`)

This is the **only dataset in the entire catalog that maps an individual NPI to a group practice**. When a physician joins a group practice, they "reassign" their billing rights. Each row is one such reassignment: individual NPI → group PAC ID + group name + group state + group size.

**Why it matters**: Sales reps don't just target individual physicians — they target practices. "Which cardiology groups in LA have 10+ physicians?" requires this dataset. It also reveals practice size (the `group reassignments and physician assistants` column counts members), which correlates with purchasing authority.

One NPI can appear in multiple rows (a physician affiliated with two groups), which is why `practice_locations` is a one-to-many table.

### Hospital Affiliations

**`hospital_enrollments`** (UUID: `f6f6505c`)

~8,000 rows of hospital enrollment data: hospital NPI, CCN (CMS Certification Number), name, address, and subgroup flags (acute care, psychiatric, rehabilitation, etc.).

**Why it matters**: By joining reassignment data (individual → group) against hospital enrollments (group name/state → hospital), we can infer which physicians are affiliated with which hospitals. The join is on organization name + state (not a direct key), so we flag these as `confidence_level = 'medium'`. It's imperfect but is the best available without external data.

### Quality Scores

**`quality_payment_program_experience`** (UUID: `7adb8b1b`)

MIPS (Merit-based Incentive Payment System) scores for ~1M clinicians: final score (0-100), payment adjustment percentage, and breakdowns by quality, promoting interoperability, improvement activities, and cost categories. Also includes practice metadata: rural status, HPSA (Health Professional Shortage Area) status, practice size, facility-based status.

**Why it matters for targeting**: A physician with a low MIPS score faces Medicare payment penalties and is more likely to be receptive to quality improvement products, EHR solutions, or care management tools. The `quality_opportunity` component of our targeting score is derived from this: `1.0 - (mips_score / 100)`, so low-MIPS providers score higher.

### Enrollment Validation

**`medicare_fee_for_service_public_provider_enrollment`** (UUID: `2457ea29`)

The PECOS (Provider Enrollment, Chain, and Ownership System) file. Authoritative source for enrollment status, enrollment ID, and the `multiple_npi_flag` (indicating data quality issues with NPI assignment).

**Why it matters**: Used to enrich `core_providers` with `pecos_enrollment_id` and `multiple_npi_flag`. Providers not in PECOS may have lapsed enrollment — useful for filtering out inactive providers.

### DME Referral Volume

**`medicare_dme_by_referring_provider`** (UUID: `f8603e5b`)

NPI-level DME (Durable Medical Equipment) referral data: how many DME claims, suppliers, and Medicare payments each referring physician generated.

**Why it matters**: Device and DME sales reps need to know which physicians are actually referring equipment orders. A high-volume orthopedic surgeon who refers 500 DME claims/year is a prime target for mobility device companies. This data feeds into `utilization_metrics.dme_total_claims` and `dme_medicare_payment`.

### Order & Referring Eligibility

**`order_and_referring`** (UUID: `c99b5865`)

Simple 8-column file: NPI + Y/N flags for Part B, DME, HHA (Home Health Agency), PMD, and Hospice ordering eligibility.

**Why it matters**: Prevents wasted effort. A DME rep should not target a physician who is not eligible to order DME. This is a quick filter applied in query templates.

---

## How the Tables Fit Together

Everything joins on **NPI** (National Provider Identifier), the universal key for healthcare providers in the US.

```
core_providers (NPI, name, specialty, address)
    │
    ├── utilization_metrics (Part B volume + Part D prescribing + DME referrals)
    │       ├── provider_service_detail (HCPCS procedure-level breakdown)
    │       └── provider_drug_detail (drug-level prescribing)
    │
    ├── practice_locations (which group practices this NPI belongs to)
    │
    ├── hospital_affiliations (inferred hospital links via reassignment→hospital join)
    │
    ├── provider_quality_scores (MIPS scores, rural/HPSA flags)
    │
    └── order_referring_eligibility (can this NPI order Part B / DME / HHA / Hospice?)
```

### Data Flow

1. **Raw CSVs** are downloaded from data.cms.gov (bulk download for large files, paginated API for hospital_enrollments)
2. **Loaded into DuckDB** as `raw_*` tables via `read_csv_auto()` — no pandas, no memory pressure
3. **Transformed** into the schema above: filtering to Type 1 NPIs, joining Part B + Part D + DME into a single utilization row, mapping reassignments to practice locations, inferring hospital affiliations
4. **Targeting scores** computed as within-specialty percentile rankings (see below)
5. **Query templates** provide pre-built SQL for cardiology, pharma, and DME targeting

---

## Targeting Score Logic

The targeting score is a 0-100 composite that ranks providers **within their specialty** (a cardiologist is compared to other cardiologists, not all providers).

| Component | Weight | Source | Rationale |
|-----------|--------|--------|-----------|
| Claims volume percentile | 40% | `tot_services` from Part B | Highest-volume providers have the most patients and the most influence on purchasing |
| Payment volume percentile | 25% | `tot_medicare_payment` | High-payment providers handle complex/expensive cases — better targets for premium products |
| Beneficiary reach percentile | 15% | `tot_unique_beneficiaries` | Broad patient base = more potential product utilization |
| Prescribing volume percentile | 10% | `rx_total_claims` from Part D | Relevant for pharma targeting; also a proxy for overall clinical activity |
| Quality opportunity | 10% | `1 - (mips_score / 100)` | Low MIPS = potential receptivity to quality improvement solutions |

Providers with no MIPS score receive a neutral 0.5 for the quality component (50th percentile assumption).

### Phase 2 Enhancement

When Open Payments data (Sunshine Act) is added, the formula shifts to incorporate industry payment history:
`targeting_score = (claims_volume_pctile * 0.7) + (industry_payments_pctile * 0.3)`

This captures existing industry engagement — providers who already accept industry payments are statistically more receptive to sales outreach.

---

## Type 1 / Type 2 NPI Deduplication

A single physician can appear in the data multiple ways:
- As an **Individual** (Type 1 NPI): their personal utilization record
- Under an **Organization** (Type 2 NPI): a group practice that bills on their behalf

If we counted both, we'd double-count their services. Our strategy:

1. `core_providers` includes **only Type 1** (`entity_type_code = 'I'`)
2. `utilization_metrics` pulls **only from Type 1 records**
3. Type 2 NPIs are used **only** in `practice_locations` and `hospital_affiliations` for organizational context
4. The reassignment dataset explicitly maps Type 1 → Type 2 via `individual npi` → `group pac id`
5. Providers who appear in reassignment but **not** in utilization data are flagged `bills_through_group_only = TRUE` — these bill exclusively through their group and have no individual-level metrics

---

## What's NOT in the Catalog (Phase 2 Gaps)

Three datasets referenced in early planning are **not available** in the CMS Public Data Catalog:

| Dataset | What It Would Add | External Source |
|---------|-------------------|-----------------|
| **NPPES** (National Plan & Provider Enumeration System) | Gender, taxonomy code, non-Medicare providers | https://download.cms.gov/nppes/NPI_Files.html (9GB bulk download) |
| **Open Payments** (Sunshine Act) | Industry-to-provider payment history | https://openpaymentsdata.cms.gov/ |
| **Doctors & Clinicians** facility affiliations | Direct hospital affiliation flags | https://data.medicare.gov/provider-data/ |

Phase 1 works without these. The physician_by_provider dataset covers ~1.2M Medicare-billing providers with utilization data that NPPES lacks. Hospital affiliations are inferred from reassignment joins. Open Payments has no catalog substitute — the `industry_relationships` table exists in the schema but is empty until Phase 2.

---

## Column Name Reference

CMS uses abbreviated column names that are not self-explanatory. Key translations:

| CMS Column | Our Column | Meaning |
|------------|-----------|---------|
| `rndrng_npi` | `npi` | Rendering provider NPI |
| `rndrng_prvdr_ent_cd` | `entity_type_code` | 'I' = Individual, 'O' = Organization |
| `rndrng_prvdr_type` | `provider_type` | Medicare specialty description |
| `tot_srvcs` | `tot_services` | Total services (line items billed) |
| `tot_benes` | `tot_unique_beneficiaries` | Unique Medicare beneficiaries seen |
| `tot_mdcr_pymt_amt` | `tot_medicare_payment` | Total Medicare payment amount |
| `prscrbr_npi` | `npi` | Prescriber NPI (Part D data) |
| `rfrg_npi` | `npi` | Referring provider NPI (DME data) |
| `bene_cc_ph_*_pct` | `cc_*_pct` | Chronic condition prevalence (physical health) |
| `bene_cc_bh_*_pct` | `cc_*_pct` | Chronic condition prevalence (behavioral health) |
| `group pac id` | `group_pac_id` | Group practice PAC identifier (reassignment) |
