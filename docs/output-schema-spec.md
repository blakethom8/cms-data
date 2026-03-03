# Output Schema Specification

**Last Updated:** 2026-02-16
**Purpose:** Define the exact output tables, columns, sources, and relationships for the Provider Intelligence Platform.

---

## Design Principles

1. **Three entity levels:** Provider, Organization, Location — each is a first-class entity
2. **Many-to-many relationships:** Providers belong to multiple orgs and practice at multiple locations
3. **Metrics are separated from identity:** So we can update utilization data without touching provider records
4. **All NPIs are VARCHAR(10):** No more BIGINT/VARCHAR mismatches — cast on ingest
5. **Medicare-enriched vs. NPPES-only:** Flag distinguishes providers with claims data from those with just registry info

---

## Entity Relationship Diagram

```
┌─────────────┐       ┌──────────────────┐       ┌─────────────┐
│  providers   │──M:N──│  provider_orgs   │──M:N──│organizations│
│  (per NPI)   │       │  (junction)      │       │ (per group) │
└──────┬───────┘       └──────────────────┘       └──────┬──────┘
       │                                                  │
       │  1:N                                        1:N  │
       ▼                                                  ▼
┌──────────────┐                                 ┌──────────────┐
│provider_metrics│                               │  org_metrics  │
│(utilization,  │                                │ (aggregated)  │
│ Rx, quality,  │                                └──────────────┘
│ industry $)   │
└──────────────┘
       │
       │  M:N
       ▼
┌──────────────┐
│  locations   │
│(unique addr) │
└──────────────┘
```

---

## Table 1: `providers`

**One row per NPI.** The master provider record. Contains identity + demographics only (no metrics).

| Column | Type | Source | Description | Example |
|--------|------|--------|-------------|---------|
| `npi` | VARCHAR(10) PK | NPPES + CMS | National Provider Identifier | `1234567890` |
| `first_name` | VARCHAR(100) | NPPES → CMS fallback | Legal first name | `Jane` |
| `last_name` | VARCHAR(255) | NPPES → CMS fallback | Legal last name / org name | `Smith` |
| `middle_initial` | VARCHAR(5) | NPPES → CMS | Middle initial | `A` |
| `name_prefix` | VARCHAR(20) | NPPES | Prefix (Dr., Mr.) | `Dr.` |
| `name_suffix` | VARCHAR(20) | NPPES | Suffix (Jr., III) | `Jr.` |
| `credentials` | VARCHAR(50) | NPPES → CMS | Credential string | `MD, FACC` |
| `gender` | VARCHAR(1) | NPPES | M/F | `F` |
| `entity_type` | VARCHAR(1) | CMS + NPPES | I=Individual, O=Organization | `I` |
| `provider_type` | VARCHAR(100) | CMS (Medicare specialty) | CMS specialty classification | `Cardiology` |
| `primary_taxonomy_code` | VARCHAR(15) | NPPES | Healthcare taxonomy code | `207RC0000X` |
| `primary_taxonomy_desc` | VARCHAR(255) | Derived (taxonomy lookup) | Human-readable taxonomy | `Cardiovascular Disease` |
| `enumeration_date` | DATE | NPPES | When NPI was issued | `2005-08-22` |
| `is_sole_proprietor` | BOOLEAN | NPPES | Solo practice flag | `false` |
| `medicare_participating` | BOOLEAN | CMS | Has Medicare claims data? | `true` |
| `ordering_eligible` | VARCHAR(5) | Order & Referring | PARTB/DME/HHA/HOSPICE flags | `Y` |
| `pecos_enrollment_id` | VARCHAR(20) | PECOS | Medicare enrollment ID | `I20200101000123` |
| `data_vintage` | VARCHAR(10) | Derived | Data year label | `2023` |
| `last_updated` | TIMESTAMP | System | Last record update | `2026-02-16T10:00:00` |

**Row count:** ~8M (1.2M Medicare + ~6.8M NPPES-only)

**Source priority:** When NPPES and CMS both have a field (e.g., name, credentials), prefer NPPES (more current, updated monthly) with CMS as fallback.

**Example row:**
```
npi: 1234567890
first_name: Jane
last_name: Smith
credentials: MD, FACC
gender: F
entity_type: I
provider_type: Cardiology
primary_taxonomy_code: 207RC0000X
primary_taxonomy_desc: Cardiovascular Disease
medicare_participating: true
```

---

## Table 2: `organizations`

**One row per organization.** Group practices, hospitals, health systems. Derived from reassignment data (group practices) and hospital enrollments (hospitals).

| Column | Type | Source | Description | Example |
|--------|------|--------|-------------|---------|
| `org_id` | VARCHAR(20) PK | PAC ID (groups) or NPI (hospitals) | Unique org identifier | `6901397892` |
| `org_type` | VARCHAR(20) | Derived | `group_practice` or `hospital` | `hospital` |
| `org_name` | VARCHAR(255) | Reassignment / Hospital Enrollments | Legal business name | `Cedars-Sinai Medical Center` |
| `doing_business_as` | VARCHAR(255) | Hospital Enrollments | DBA name | `Cedars-Sinai` |
| `org_npi` | VARCHAR(10) | Hospital Enrollments | Organization NPI (hospitals) | `1234567890` |
| `ccn` | VARCHAR(10) | Hospital Enrollments | CMS Certification Number | `050625` |
| `enrollment_id` | VARCHAR(20) | Reassignment / Hospital | PECOS enrollment ID | `O20020812000015` |
| `provider_count` | INTEGER | Reassignment (Group Reassignments) | # of affiliated providers | `450` |
| `address_line_1` | VARCHAR(255) | Hospital Enrollments | Street address | `8700 Beverly Blvd` |
| `address_line_2` | VARCHAR(255) | Hospital Enrollments | Suite/floor | `Suite 200` |
| `city` | VARCHAR(100) | Reassignment / Hospital | City | `Los Angeles` |
| `state` | VARCHAR(2) | Reassignment / Hospital | State code | `CA` |
| `zip_code` | VARCHAR(10) | Hospital Enrollments | ZIP | `90048` |
| `incorporation_state` | VARCHAR(2) | Hospital Enrollments | Where incorporated | `CA` |
| `org_structure` | VARCHAR(50) | Hospital Enrollments | LLC, Corp, Nonprofit, etc. | `NONPROFIT` |
| `proprietary_nonprofit` | VARCHAR(1) | Hospital Enrollments | P=Proprietary, N=Nonprofit | `N` |
| `hospital_subgroups` | VARCHAR(255) | Hospital Enrollments | Comma-separated: acute_care, psychiatric, etc. | `general, acute_care, rehabilitation` |
| `last_updated` | TIMESTAMP | System | Last record update | `2026-02-16` |

**Row count:** ~50K group practices + ~8K hospitals = ~58K

**Derivation notes:**
- Group practices come from `raw_reassignment` grouped by `Group PAC ID`
- Hospitals come from `raw_hospital_enrollments`
- A single org can appear in both (a hospital that's also a group practice)

**Example row:**
```
org_id: 0042392180
org_type: hospital
org_name: SOUTHERN TENNESSEE MEDICAL CENTER LLC
doing_business_as: HIGHPOINT HEALTH - WINCHESTER WITH ASCENSION SAINT THOMAS
org_npi: 1467408781
ccn: 440058
provider_count: 85
city: WINCHESTER
state: TN
hospital_subgroups: general, acute_care, rehabilitation
```

---

## Table 3: `locations`

**One row per unique practice address.** Deduplicated from provider addresses across all sources.

| Column | Type | Source | Description | Example |
|--------|------|--------|-------------|---------|
| `location_id` | SERIAL PK | System | Auto-increment ID | `1` |
| `address_line_1` | VARCHAR(255) | CMS / NPPES | Street address (normalized) | `8700 BEVERLY BLVD` |
| `address_line_2` | VARCHAR(255) | CMS / NPPES | Suite/floor | `STE 200` |
| `city` | VARCHAR(100) | CMS / NPPES | City | `LOS ANGELES` |
| `state` | VARCHAR(2) | CMS / NPPES | State code | `CA` |
| `zip5` | VARCHAR(5) | CMS / NPPES | 5-digit ZIP | `90048` |
| `zip9` | VARCHAR(10) | NPPES | Full ZIP+4 | `90048-1804` |
| `country` | VARCHAR(2) | CMS / NPPES | Country code | `US` |
| `ruca_code` | VARCHAR(10) | CMS (physician_by_provider) | Rural-Urban Commuting Area | `1.0` |
| `ruca_description` | VARCHAR(100) | Derived | Rural/Urban classification | `Metropolitan` |
| `latitude` | DOUBLE | Future (geocoding) | GPS latitude | `34.0753` |
| `longitude` | DOUBLE | Future (geocoding) | GPS longitude | `-118.3804` |
| `google_place_id` | VARCHAR(255) | Future (Places API) | Google Places reference | `ChIJ...` |
| `phone` | VARCHAR(20) | NPPES | Practice phone number | `3104231234` |
| `provider_count` | INTEGER | Derived | # of providers at this location | `12` |

**Row count:** ~2-3M unique addresses (estimated after dedup)

**Deduplication strategy:**
- Normalize addresses: uppercase, strip punctuation, standardize abbreviations (ST→ST, STREET→ST, SUITE→STE)
- Group by (normalized_address_line_1, city, state, zip5)
- Pick richest record (most metadata) as canonical

**Example row:**
```
location_id: 42391
address_line_1: 8700 BEVERLY BLVD
city: LOS ANGELES
state: CA
zip5: 90048
ruca_code: 1.0
ruca_description: Metropolitan
provider_count: 312
```

---

## Table 4: `provider_orgs` (Junction)

**Links providers to organizations.** Many-to-many: a provider can belong to multiple groups/hospitals.

| Column | Type | Source | Description | Example |
|--------|------|--------|-------------|---------|
| `npi` | VARCHAR(10) FK | Reassignment / Doctors & Clinicians | Provider NPI | `1234567890` |
| `org_id` | VARCHAR(20) FK | Reassignment / Hospital Enrollments | Organization identifier | `6901397892` |
| `relationship_type` | VARCHAR(50) | Derived | `reassignment`, `hospital_affiliation`, `aco_member` | `reassignment` |
| `specialty_at_org` | VARCHAR(100) | Reassignment | Specialty as listed in reassignment | `Cardiology` |
| `confidence` | VARCHAR(10) | Derived | `high` (direct match) or `medium` (name match) | `high` |
| `source` | VARCHAR(50) | Derived | Which dataset established this link | `reassignment` |

**Row count:** ~3.5M (from reassignment) + hospital affiliations + Doctors & Clinicians

**Derivation:**
- Reassignment: `Individual NPI` → `Group PAC ID` (direct, high confidence)
- Hospital affiliation: Group practice → hospital (via name+state match, medium confidence)
- Doctors & Clinicians: Explicit hospital affiliations + ACO participation (high confidence)

**Example row:**
```
npi: 1942452552
org_id: 6901397892
relationship_type: reassignment
specialty_at_org: Addiction Medicine
confidence: high
source: reassignment
```

---

## Table 5: `provider_locations` (Junction)

**Links providers to their practice locations.** Many-to-many.

| Column | Type | Source | Description | Example |
|--------|------|--------|-------------|---------|
| `npi` | VARCHAR(10) FK | CMS / NPPES | Provider NPI | `1234567890` |
| `location_id` | INTEGER FK | Derived | Location reference | `42391` |
| `is_primary` | BOOLEAN | Derived | Primary practice location? | `true` |
| `address_source` | VARCHAR(20) | Derived | `cms`, `nppes`, `both` | `both` |

**Row count:** ~8M+ (every provider has at least one location)

---

## Table 6: `provider_metrics`

**One row per provider per metric year.** All utilization, prescribing, quality, and industry payment data.

| Column | Type | Source | Description | Example |
|--------|------|--------|-------------|---------|
| **Identity** |
| `npi` | VARCHAR(10) FK | — | Provider NPI | `1234567890` |
| `metric_year` | INTEGER | — | Data year | `2023` |
| **Part B Utilization** |
| `tot_hcpcs_codes` | INTEGER | physician_by_provider | Unique procedure codes billed | `45` |
| `tot_services` | DECIMAL | physician_by_provider | Total services rendered | `8500.00` |
| `tot_beneficiaries` | INTEGER | physician_by_provider | Unique Medicare patients | `1200` |
| `tot_submitted_charges` | DECIMAL | physician_by_provider | Total charges submitted | `3200000.00` |
| `tot_medicare_allowed` | DECIMAL | physician_by_provider | Medicare allowed amount | `2600000.00` |
| `tot_medicare_payment` | DECIMAL | physician_by_provider | Medicare paid amount | `2400000.00` |
| `tot_medicare_standardized` | DECIMAL | physician_by_provider | Standardized payment (geographic adj.) | `2350000.00` |
| `drug_services` | DECIMAL | physician_by_provider | Drug-related services | `500.00` |
| `medical_services` | DECIMAL | physician_by_provider | Medical services | `8000.00` |
| **Prescribing (Part D)** |
| `rx_total_claims` | INTEGER | part_d_by_provider | Total Rx claims | `3500` |
| `rx_total_drug_cost` | DECIMAL | part_d_by_provider | Total drug cost | `850000.00` |
| `rx_brand_claims` | INTEGER | part_d_by_provider | Brand name claims | `1200` |
| `rx_generic_claims` | INTEGER | part_d_by_provider | Generic claims | `2300` |
| `rx_brand_pct` | DECIMAL | Derived | Brand / total claims | `34.3` |
| `rx_opioid_rate` | DECIMAL | part_d_by_provider | Opioid prescribing rate | `2.5` |
| **DME Referrals** |
| `dme_total_claims` | INTEGER | dme_by_referring_provider | DME referral claims | `150` |
| `dme_medicare_payment` | DECIMAL | dme_by_referring_provider | DME Medicare payment | `45000.00` |
| **Beneficiary Demographics** |
| `bene_avg_age` | DECIMAL | physician_by_provider | Avg patient age | `72.4` |
| `bene_avg_risk_score` | DECIMAL | physician_by_provider | Avg HCC risk score | `1.45` |
| `bene_dual_eligible_count` | INTEGER | physician_by_provider | Dual-eligible patients | `180` |
| **Chronic Conditions (% of patients)** |
| `cc_diabetes_pct` | DECIMAL | physician_by_provider | % with diabetes | `42.0` |
| `cc_hypertension_pct` | DECIMAL | physician_by_provider | % with hypertension | `68.0` |
| `cc_heart_failure_pct` | DECIMAL | physician_by_provider | % with heart failure | `22.0` |
| `cc_ckd_pct` | DECIMAL | physician_by_provider | % with chronic kidney disease | `35.0` |
| `cc_copd_pct` | DECIMAL | physician_by_provider | % with COPD | `18.0` |
| `cc_cancer_pct` | DECIMAL | physician_by_provider | % with cancer | `12.0` |
| `cc_depression_pct` | DECIMAL | physician_by_provider | % with depression | `25.0` |
| `cc_alzheimers_pct` | DECIMAL | physician_by_provider | % with Alzheimer's/dementia | `15.0` |
| `cc_atrial_fib_pct` | DECIMAL | physician_by_provider | % with atrial fibrillation | `18.0` |
| `cc_hyperlipidemia_pct` | DECIMAL | physician_by_provider | % with high cholesterol | `58.0` |
| `cc_ischemic_heart_pct` | DECIMAL | physician_by_provider | % with ischemic heart disease | `30.0` |
| `cc_osteoporosis_pct` | DECIMAL | physician_by_provider | % with osteoporosis | `10.0` |
| `cc_arthritis_pct` | DECIMAL | physician_by_provider | % with arthritis | `32.0` |
| `cc_stroke_tia_pct` | DECIMAL | physician_by_provider | % with stroke/TIA | `8.0` |
| **Quality (MIPS)** |
| `mips_score` | DECIMAL | qpp_experience | Final MIPS composite score (0-100) | `92.0` |
| `mips_payment_adj_pct` | DECIMAL | qpp_experience | Payment adjustment % | `1.68` |
| `quality_category_score` | DECIMAL | qpp_experience | Quality category (0-100) | `88.0` |
| `pi_category_score` | DECIMAL | qpp_experience | Promoting Interoperability (0-100) | `95.0` |
| `ia_category_score` | DECIMAL | qpp_experience | Improvement Activities (0-100) | `40.0` |
| `cost_category_score` | DECIMAL | qpp_experience | Cost category (0-100) | `50.0` |
| `practice_size_bucket` | VARCHAR(20) | qpp_experience | small/medium/large | `medium` |
| `rural_status` | BOOLEAN | qpp_experience | In rural area? | `false` |
| `hpsa_status` | BOOLEAN | qpp_experience | In health professional shortage area? | `false` |
| **Industry Payments (Open Payments)** |
| `industry_payment_total` | DECIMAL | Open Payments (aggregated) | Total $ received across all companies | `45000.00` |
| `industry_payment_count` | INTEGER | Open Payments (aggregated) | Total # of payments | `28` |
| `industry_company_count` | INTEGER | Open Payments (aggregated) | # of unique paying companies | `5` |
| `industry_top_payer` | VARCHAR(255) | Open Payments | Highest-paying company | `Medtronic` |
| `industry_top_payer_amount` | DECIMAL | Open Payments | $ from top payer | `22000.00` |
| `industry_payment_natures` | VARCHAR(500) | Open Payments | Types of payments received | `consulting; speaking; meals` |
| `kol_tier` | VARCHAR(10) | Derived | KOL classification | `tier_2` |

**Row count:** ~1.2M (only Medicare providers have metrics)

**KOL Tier derivation:**
- `tier_1`: >$100K total industry payments
- `tier_2`: >$50K
- `tier_3`: >$10K
- NULL: <$10K or no payments

**Example row:**
```
npi: 1234567890
metric_year: 2023
tot_services: 8500
tot_beneficiaries: 1200
tot_medicare_payment: 2400000
rx_total_claims: 3500
rx_brand_pct: 34.3
mips_score: 92.0
industry_payment_total: 45000
kol_tier: tier_2
```

---

## Table 7: `provider_services`

**Procedure-level detail.** One row per provider × HCPCS code × place of service.

| Column | Type | Source | Description | Example |
|--------|------|--------|-------------|---------|
| `npi` | VARCHAR(10) FK | physician_by_provider_and_service | Provider NPI | `1234567890` |
| `hcpcs_code` | VARCHAR(10) | physician_by_provider_and_service | CPT/HCPCS procedure code | `93306` |
| `hcpcs_description` | VARCHAR(255) | physician_by_provider_and_service | Procedure name | `Echocardiography` |
| `hcpcs_drug_indicator` | VARCHAR(1) | physician_by_provider_and_service | Y=drug, N=non-drug | `N` |
| `place_of_service` | VARCHAR(1) | physician_by_provider_and_service | F=Facility, O=Office | `O` |
| `tot_beneficiaries` | INTEGER | physician_by_provider_and_service | Patients for this service | `450` |
| `tot_services` | DECIMAL | physician_by_provider_and_service | Service count | `680.00` |
| `avg_medicare_payment` | DECIMAL | physician_by_provider_and_service | Avg Medicare payment per service | `125.50` |
| `data_year` | INTEGER | Derived | Data year | `2023` |

**Row count:** ~9.6M

---

## Table 8: `provider_drugs`

**Drug-level prescribing detail.** One row per provider × drug.

| Column | Type | Source | Description | Example |
|--------|------|--------|-------------|---------|
| `npi` | VARCHAR(10) FK | part_d_by_provider_and_drug | Provider NPI | `1234567890` |
| `brand_name` | VARCHAR(255) | part_d_by_provider_and_drug | Brand name | `Eliquis` |
| `generic_name` | VARCHAR(255) | part_d_by_provider_and_drug | Generic name | `Apixaban` |
| `tot_claims` | INTEGER | part_d_by_provider_and_drug | # claims for this drug | `280` |
| `tot_30day_fills` | DECIMAL | part_d_by_provider_and_drug | 30-day equivalent fills | `260.0` |
| `tot_drug_cost` | DECIMAL | part_d_by_provider_and_drug | Total drug cost | `145000.00` |
| `tot_beneficiaries` | INTEGER | part_d_by_provider_and_drug | # patients prescribed | `180` |
| `data_year` | INTEGER | Derived | Data year | `2023` |

**Row count:** ~26.8M

---

## Table 9: `industry_payments`

**Individual payment records from Open Payments.** One row per provider × company × year (aggregated from raw payment records).

| Column | Type | Source | Description | Example |
|--------|------|--------|-------------|---------|
| `npi` | VARCHAR(10) FK | Open Payments | Provider NPI | `1234567890` |
| `payment_year` | INTEGER | Open Payments | Year of payments | `2023` |
| `company_name` | VARCHAR(255) | Open Payments | Paying manufacturer | `Medtronic Inc` |
| `total_amount` | DECIMAL | Open Payments (SUM) | Total $ from this company this year | `22000.00` |
| `payment_count` | INTEGER | Open Payments (COUNT) | # of individual payments | `12` |
| `payment_natures` | VARCHAR(500) | Open Payments (DISTINCT) | Types of payments | `consulting; speaking` |
| `product_categories` | VARCHAR(500) | Open Payments (DISTINCT) | Drug/device/supply | `Device` |

**Row count:** ~5-10M (across 2022-2024)

---

## Table 10: `targeting_scores`

**Composite targeting score for sales prioritization.** One row per provider. Within-specialty percentiles.

| Column | Type | Source | Description | Example |
|--------|------|--------|-------------|---------|
| `npi` | VARCHAR(10) PK/FK | Derived | Provider NPI | `1234567890` |
| `provider_type` | VARCHAR(100) | providers | Specialty (for context) | `Cardiology` |
| `claims_pctile` | DECIMAL | Derived | Services percentile within specialty | `0.92` |
| `payment_pctile` | DECIMAL | Derived | Medicare payment percentile | `0.88` |
| `bene_pctile` | DECIMAL | Derived | Beneficiary count percentile | `0.85` |
| `rx_pctile` | DECIMAL | Derived | Prescribing volume percentile | `0.78` |
| `quality_opportunity` | DECIMAL | Derived | 1 - (MIPS/100); higher = more opportunity | `0.08` |
| `industry_pctile` | DECIMAL | Derived | Industry payment percentile (new) | `0.65` |
| `targeting_score` | DECIMAL | Derived | Weighted composite 0-100 | `87.3` |

**Scoring formula:**
```
targeting_score = 
    claims_pctile     × 30  +
    payment_pctile    × 20  +
    bene_pctile       × 15  +
    rx_pctile         × 10  +
    quality_opportunity × 10 +
    industry_pctile   × 15
```

**Row count:** ~1.2M (Medicare providers only — need claims data for scoring)

---

## Table 11: `org_metrics` (Aggregated)

**Organization-level statistics.** Aggregated from member providers.

| Column | Type | Source | Description | Example |
|--------|------|--------|-------------|---------|
| `org_id` | VARCHAR(20) PK/FK | organizations | Organization ID | `0042392180` |
| `total_providers` | INTEGER | Derived (COUNT) | # of affiliated providers | `450` |
| `total_medicare_payment` | DECIMAL | Derived (SUM) | Total Medicare payments across providers | `125000000` |
| `total_beneficiaries` | INTEGER | Derived (SUM DISTINCT est.) | Total unique patients (estimated) | `45000` |
| `avg_mips_score` | DECIMAL | Derived (AVG) | Average MIPS score | `78.5` |
| `specialty_mix` | VARCHAR(500) | Derived | Top specialties + counts | `Cardiology:45, Internal Medicine:120, ...` |
| `top_specialty` | VARCHAR(100) | Derived | Most common specialty | `Internal Medicine` |
| `total_industry_payments` | DECIMAL | Derived (SUM) | Total industry $ to all providers | `2500000` |
| `kol_count` | INTEGER | Derived (COUNT) | # of KOL-tier providers | `12` |

**Row count:** ~58K

---

## Data Flow Summary

```
RAW SOURCES                    →  OUTPUT TABLES
─────────────────────────────     ─────────────────────

physician_by_provider ─────────→  providers (identity)
                      ─────────→  provider_metrics (utilization)
                      ─────────→  locations (address)

NPPES ─────────────────────────→  providers (gender, taxonomy, 6M new)
                      ─────────→  locations (practice address)

reassignment ──────────────────→  organizations (group practices)
                      ─────────→  provider_orgs (NPI → group)

hospital_enrollments ──────────→  organizations (hospitals)

Doctors & Clinicians ──────────→  provider_orgs (hospital affiliations, ACO)

part_d_by_provider ────────────→  provider_metrics (Rx summary)
part_d_by_provider_and_drug ───→  provider_drugs (drug detail)

physician_by_provider_and_svc ─→  provider_services (procedure detail)

qpp_experience ────────────────→  provider_metrics (MIPS quality)

dme_by_referring_provider ─────→  provider_metrics (DME referrals)

order_and_referring ───────────→  providers (ordering eligibility)

pecos_enrollment ──────────────→  providers (enrollment ID)

Open Payments (2022-2024) ─────→  industry_payments (payment detail)
                          ─────→  provider_metrics (aggregated industry $)

DERIVED:
  provider_metrics ────────────→  targeting_scores
  provider_orgs + metrics ─────→  org_metrics
  all address sources ─────────→  locations (deduplicated)
```

---

## Transform Execution Order

Due to foreign key dependencies, transforms must run in this order:

1. **`locations`** — deduplicate addresses from all sources
2. **`providers`** — build from CMS + NPPES, all NPIs VARCHAR(10)
3. **`organizations`** — build from reassignment + hospital enrollments
4. **`provider_locations`** — link providers to locations
5. **`provider_orgs`** — link providers to organizations
6. **`provider_metrics`** — join Part B + Part D + DME + QPP + Open Payments
7. **`provider_services`** — procedure detail
8. **`provider_drugs`** — drug detail
9. **`industry_payments`** — Open Payments detail
10. **`targeting_scores`** — compute from provider_metrics
11. **`org_metrics`** — aggregate from provider_metrics via provider_orgs

---

## Open Questions

1. **Doctors & Clinicians data:** Once downloaded, need to map its columns to `provider_orgs` (hospital affiliations, ACO participation). Schema TBD pending column inspection.
2. **Address normalization:** How aggressive? Simple uppercase + abbreviation standardization, or use a geocoding service?
3. **Historical metrics:** Store multiple years (2022 + 2023) or only latest?
4. **Org deduplication:** Same hospital can appear as both a group practice (reassignment) and hospital (enrollments). Merge strategy needed.
5. **provider_type mapping:** Use CMS specialty (Medicare providers) vs. NPPES taxonomy (non-Medicare). Should we unify into a single taxonomy?
