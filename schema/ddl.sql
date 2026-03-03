-- Provider Searcher: DuckDB Schema
-- Phase 1 (MVP) tables built from CMS Public Data Catalog sources

------------------------------------------------------------
-- Table 1: core_providers
-- Source: medicare_physician_other_practitioners_by_provider
-- Enrichment: PECOS enrollment (Phase 1), NPPES (Phase 2)
------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core_providers (
    npi                     VARCHAR(10)   PRIMARY KEY,
    last_org_name           VARCHAR(255)  NOT NULL,
    first_name              VARCHAR(100),
    middle_initial          VARCHAR(5),
    credentials             VARCHAR(50),
    entity_type_code        VARCHAR(1)    NOT NULL,  -- 'I'=Individual, 'O'=Organization
    provider_type           VARCHAR(100),            -- Medicare specialty
    gender                  VARCHAR(1),              -- Phase 2: from NPPES
    primary_taxonomy_code   VARCHAR(15),             -- Phase 2: from NPPES
    street_address_1        VARCHAR(255),
    street_address_2        VARCHAR(255),
    city                    VARCHAR(100),
    state                   VARCHAR(2),
    zip5                    VARCHAR(5),
    country                 VARCHAR(2),
    ruca_code               VARCHAR(10),
    medicare_participating  VARCHAR(1),
    pecos_enrollment_id     VARCHAR(20),
    multiple_npi_flag       VARCHAR(1),
    bills_through_group_only BOOLEAN DEFAULT FALSE,
    data_year               INTEGER       NOT NULL,
    last_updated            TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_core_providers_state ON core_providers(state);
CREATE INDEX IF NOT EXISTS idx_core_providers_zip5 ON core_providers(zip5);
CREATE INDEX IF NOT EXISTS idx_core_providers_provider_type ON core_providers(provider_type);
CREATE INDEX IF NOT EXISTS idx_core_providers_entity_type ON core_providers(entity_type_code);
CREATE INDEX IF NOT EXISTS idx_core_providers_state_type ON core_providers(state, provider_type);


------------------------------------------------------------
-- Table 2: practice_locations
-- Source: revalidation_clinic_group_practice_reassignment
------------------------------------------------------------
CREATE SEQUENCE IF NOT EXISTS seq_location_id START 1;

CREATE TABLE IF NOT EXISTS practice_locations (
    location_id          INTEGER       PRIMARY KEY DEFAULT nextval('seq_location_id'),
    npi                  VARCHAR(10)   NOT NULL REFERENCES core_providers(npi),
    group_pac_id         VARCHAR(20),
    group_enrollment_id  VARCHAR(20),
    group_legal_name     VARCHAR(255),
    group_state          VARCHAR(2),
    group_practice_size  INTEGER,
    street_address_1     VARCHAR(255),
    city                 VARCHAR(100),
    state                VARCHAR(2),
    zip5                 VARCHAR(5),
    google_place_id      VARCHAR(255),   -- Phase 2
    latitude             DOUBLE,         -- Phase 2
    longitude            DOUBLE,         -- Phase 2
    is_primary_location  BOOLEAN       DEFAULT FALSE,
    location_type        VARCHAR(50),    -- 'office', 'hospital', 'asc'
    data_year            INTEGER       NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_practice_locations_npi ON practice_locations(npi);
CREATE INDEX IF NOT EXISTS idx_practice_locations_state ON practice_locations(state);
CREATE INDEX IF NOT EXISTS idx_practice_locations_group ON practice_locations(group_pac_id);


------------------------------------------------------------
-- Table 3: utilization_metrics
-- Sources: by_provider (Part B) + part_d_by_provider (Rx)
--          + dme_by_referring_provider
------------------------------------------------------------
CREATE TABLE IF NOT EXISTS utilization_metrics (
    npi                       VARCHAR(10) NOT NULL REFERENCES core_providers(npi),
    metric_year               INTEGER     NOT NULL,
    -- Part B summary
    tot_hcpcs_codes           INTEGER,
    tot_services              DECIMAL(15,2),
    tot_unique_beneficiaries  INTEGER,
    tot_submitted_charges     DECIMAL(15,2),
    tot_medicare_allowed      DECIMAL(15,2),
    tot_medicare_payment      DECIMAL(15,2),
    tot_medicare_standardized DECIMAL(15,2),
    drug_services             DECIMAL(15,2),
    medical_services          DECIMAL(15,2),
    -- Part D prescribing
    rx_total_claims           INTEGER,
    rx_total_drug_cost        DECIMAL(15,2),
    rx_brand_claims           INTEGER,
    rx_generic_claims         INTEGER,
    rx_opioid_prescriber_rate DECIMAL(5,2),
    -- DME referrals
    dme_total_claims          INTEGER,
    dme_medicare_payment      DECIMAL(15,2),
    -- Beneficiary demographics
    bene_avg_age              DECIMAL(5,2),
    bene_avg_risk_score       DECIMAL(5,3),
    bene_dual_eligible_count  INTEGER,
    -- Chronic condition prevalence (% of provider's patients)
    cc_diabetes_pct           DECIMAL(5,2),
    cc_hypertension_pct       DECIMAL(5,2),
    cc_heart_failure_pct      DECIMAL(5,2),
    cc_ckd_pct                DECIMAL(5,2),
    cc_copd_pct               DECIMAL(5,2),
    cc_cancer_pct             DECIMAL(5,2),
    cc_depression_pct         DECIMAL(5,2),
    cc_alzheimers_pct         DECIMAL(5,2),
    cc_atrial_fib_pct         DECIMAL(5,2),
    cc_hyperlipidemia_pct     DECIMAL(5,2),
    cc_ischemic_heart_pct     DECIMAL(5,2),
    cc_osteoporosis_pct       DECIMAL(5,2),
    cc_arthritis_pct          DECIMAL(5,2),
    cc_stroke_tia_pct         DECIMAL(5,2),
    PRIMARY KEY (npi, metric_year)
);

CREATE INDEX IF NOT EXISTS idx_utilization_npi ON utilization_metrics(npi);
CREATE INDEX IF NOT EXISTS idx_utilization_year ON utilization_metrics(metric_year);


------------------------------------------------------------
-- Table 4: industry_relationships (Phase 2 - Open Payments)
------------------------------------------------------------
CREATE TABLE IF NOT EXISTS industry_relationships (
    npi                     VARCHAR(10)  NOT NULL REFERENCES core_providers(npi),
    payment_year            INTEGER      NOT NULL,
    paying_company_name     VARCHAR(255) NOT NULL,
    total_amount_received   DECIMAL(15,2) NOT NULL,
    payment_count           INTEGER,
    nature_of_payments      VARCHAR(500),
    top_paying_company_flag BOOLEAN      DEFAULT FALSE,
    PRIMARY KEY (npi, payment_year, paying_company_name)
);


------------------------------------------------------------
-- Table 5: hospital_affiliations
-- Source: reassignment joined to hospital_enrollments
------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hospital_affiliations (
    npi                VARCHAR(10)  NOT NULL REFERENCES core_providers(npi),
    hospital_npi       VARCHAR(10)  NOT NULL,
    hospital_ccn       VARCHAR(10),
    hospital_name      VARCHAR(255),
    hospital_city      VARCHAR(100),
    hospital_state     VARCHAR(2),
    hospital_zip       VARCHAR(10),
    hospital_subgroup  VARCHAR(50),
    affiliation_source VARCHAR(50) NOT NULL,
    confidence_level   VARCHAR(10),
    group_pac_id       VARCHAR(20),
    data_year          INTEGER     NOT NULL,
    PRIMARY KEY (npi, hospital_npi)
);

CREATE INDEX IF NOT EXISTS idx_hospital_aff_npi ON hospital_affiliations(npi);
CREATE INDEX IF NOT EXISTS idx_hospital_aff_hosp ON hospital_affiliations(hospital_npi);
CREATE INDEX IF NOT EXISTS idx_hospital_aff_state ON hospital_affiliations(hospital_state);


------------------------------------------------------------
-- Supplementary Table: provider_service_detail
-- Source: by_provider_and_service (NPI + HCPCS)
------------------------------------------------------------
CREATE TABLE IF NOT EXISTS provider_service_detail (
    npi                VARCHAR(10)  NOT NULL REFERENCES core_providers(npi),
    hcpcs_code         VARCHAR(10)  NOT NULL,
    hcpcs_description  VARCHAR(255),
    hcpcs_drug_ind     VARCHAR(1),
    place_of_service   VARCHAR(1),   -- 'F'=Facility, 'O'=Office
    tot_beneficiaries  INTEGER,
    tot_services       DECIMAL(15,2),
    tot_bene_day_srvcs DECIMAL(15,2),
    avg_submitted_chrg DECIMAL(15,2),
    avg_medicare_allowed DECIMAL(15,2),
    avg_medicare_payment DECIMAL(15,2),
    avg_medicare_standardized DECIMAL(15,2),
    data_year          INTEGER      NOT NULL,
    PRIMARY KEY (npi, hcpcs_code, place_of_service, data_year)
);

CREATE INDEX IF NOT EXISTS idx_svc_detail_hcpcs ON provider_service_detail(hcpcs_code);
CREATE INDEX IF NOT EXISTS idx_svc_detail_npi ON provider_service_detail(npi);


------------------------------------------------------------
-- Supplementary Table: provider_drug_detail
-- Source: part_d_by_provider_and_drug (NPI + drug)
------------------------------------------------------------
CREATE TABLE IF NOT EXISTS provider_drug_detail (
    npi                VARCHAR(10)  NOT NULL REFERENCES core_providers(npi),
    brand_name         VARCHAR(255),
    generic_name       VARCHAR(255) NOT NULL,
    tot_claims         INTEGER,
    tot_30day_fills    DECIMAL(15,2),
    tot_day_supply     INTEGER,
    tot_drug_cost      DECIMAL(15,2),
    tot_beneficiaries  INTEGER,
    ge65_tot_claims    INTEGER,
    ge65_tot_drug_cost DECIMAL(15,2),
    ge65_tot_benes     INTEGER,
    data_year          INTEGER      NOT NULL,
    PRIMARY KEY (npi, generic_name, data_year)
);

CREATE INDEX IF NOT EXISTS idx_drug_detail_generic ON provider_drug_detail(generic_name);
CREATE INDEX IF NOT EXISTS idx_drug_detail_brand ON provider_drug_detail(brand_name);
CREATE INDEX IF NOT EXISTS idx_drug_detail_npi ON provider_drug_detail(npi);


------------------------------------------------------------
-- Supplementary Table: provider_quality_scores
-- Source: quality_payment_program_experience
------------------------------------------------------------
CREATE TABLE IF NOT EXISTS provider_quality_scores (
    npi                       VARCHAR(10)  PRIMARY KEY REFERENCES core_providers(npi),
    practice_state            VARCHAR(2),
    practice_size             VARCHAR(50),
    clinician_type            VARCHAR(100),
    clinician_specialty       VARCHAR(100),
    years_in_medicare         VARCHAR(50),
    participation_option      VARCHAR(50),
    small_practice_status     BOOLEAN,
    rural_status              BOOLEAN,
    hpsa_status               BOOLEAN,
    hospital_based_status     BOOLEAN,
    facility_based_status     BOOLEAN,
    dual_eligibility_ratio    DECIMAL(5,3),
    final_mips_score          DECIMAL(7,2),
    payment_adjustment_pct    DECIMAL(7,4),
    complex_patient_bonus     DECIMAL(7,4),
    quality_category_score    DECIMAL(7,2),
    quality_category_weight   DECIMAL(5,2),
    pi_category_score         DECIMAL(7,2),
    pi_category_weight        DECIMAL(5,2),
    ia_category_score         DECIMAL(7,2),
    ia_category_weight        DECIMAL(5,2),
    cost_category_score       DECIMAL(7,2),
    cost_category_weight      DECIMAL(5,2),
    data_year                 INTEGER      NOT NULL
);


------------------------------------------------------------
-- Reference Table: order_and_referring eligibility
-- Source: order_and_referring
------------------------------------------------------------
CREATE TABLE IF NOT EXISTS order_referring_eligibility (
    npi        VARCHAR(10)  PRIMARY KEY REFERENCES core_providers(npi),
    last_name  VARCHAR(255),
    first_name VARCHAR(100),
    partb      VARCHAR(1),  -- 'Y'/'N'
    dme        VARCHAR(1),
    hha        VARCHAR(1),
    pmd        VARCHAR(1),
    hospice    VARCHAR(1)
);
