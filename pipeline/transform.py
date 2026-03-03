"""Transform raw CMS tables into the Provider Searcher analytical schema.

All heavy lifting is done in DuckDB SQL for memory efficiency.
"""

import logging

import duckdb

logger = logging.getLogger(__name__)


def build_core_providers(con: duckdb.DuckDBPyConnection, data_year: int):
    """Populate core_providers from raw physician_by_provider + PECOS enrichment.

    Only Type 1 (Individual) providers are included for targeting.
    """
    logger.info("Building core_providers (data_year=%d)", data_year)

    con.execute("DELETE FROM core_providers WHERE data_year = ?", [data_year])

    con.execute("""
        INSERT INTO core_providers (
            npi, last_org_name, first_name, middle_initial, credentials,
            entity_type_code, provider_type, street_address_1, street_address_2,
            city, state, zip5, country, ruca_code, medicare_participating,
            pecos_enrollment_id, multiple_npi_flag, data_year
        )
        SELECT
            p.rndrng_npi,
            p.rndrng_prvdr_last_org_name,
            p.rndrng_prvdr_first_name,
            p.rndrng_prvdr_mi,
            p.rndrng_prvdr_crdntls,
            p.rndrng_prvdr_ent_cd,
            p.rndrng_prvdr_type,
            p.rndrng_prvdr_st1,
            p.rndrng_prvdr_st2,
            p.rndrng_prvdr_city,
            p.rndrng_prvdr_state_abrvtn,
            p.rndrng_prvdr_zip5,
            p.rndrng_prvdr_cntry,
            p.rndrng_prvdr_ruca,
            p.rndrng_prvdr_mdcr_prtcptg_ind,
            e.enrlmt_id,
            e.multiple_npi_flag,
            ?
        FROM raw_physician_by_provider p
        LEFT JOIN raw_pecos_enrollment e ON p.rndrng_npi = e.npi
        WHERE p.rndrng_prvdr_ent_cd = 'I'
    """, [data_year])

    count = con.execute("SELECT COUNT(*) FROM core_providers WHERE data_year = ?", [data_year]).fetchone()[0]
    logger.info("core_providers: %d individual providers loaded", count)
    return count


def build_utilization_metrics(con: duckdb.DuckDBPyConnection, data_year: int):
    """Populate utilization_metrics by joining Part B + Part D + DME data.

    All data is for Type 1 (Individual) NPIs only.
    """
    logger.info("Building utilization_metrics (data_year=%d)", data_year)

    con.execute("DELETE FROM utilization_metrics WHERE metric_year = ?", [data_year])

    con.execute("""
        INSERT INTO utilization_metrics (
            npi, metric_year,
            -- Part B
            tot_hcpcs_codes, tot_services, tot_unique_beneficiaries,
            tot_submitted_charges, tot_medicare_allowed, tot_medicare_payment,
            tot_medicare_standardized, drug_services, medical_services,
            -- Part D
            rx_total_claims, rx_total_drug_cost, rx_brand_claims,
            rx_generic_claims, rx_opioid_prescriber_rate,
            -- DME
            dme_total_claims, dme_medicare_payment,
            -- Bene demographics
            bene_avg_age, bene_avg_risk_score, bene_dual_eligible_count,
            -- Chronic conditions
            cc_diabetes_pct, cc_hypertension_pct, cc_heart_failure_pct,
            cc_ckd_pct, cc_copd_pct, cc_cancer_pct, cc_depression_pct,
            cc_alzheimers_pct, cc_atrial_fib_pct, cc_hyperlipidemia_pct,
            cc_ischemic_heart_pct, cc_osteoporosis_pct, cc_arthritis_pct,
            cc_stroke_tia_pct
        )
        SELECT
            p.rndrng_npi,
            ?,
            -- Part B
            TRY_CAST(p.tot_hcpcs_cds AS INTEGER),
            TRY_CAST(p.tot_srvcs AS DECIMAL(15,2)),
            TRY_CAST(p.tot_benes AS INTEGER),
            TRY_CAST(p.tot_sbmtd_chrg AS DECIMAL(15,2)),
            TRY_CAST(p.tot_mdcr_alowd_amt AS DECIMAL(15,2)),
            TRY_CAST(p.tot_mdcr_pymt_amt AS DECIMAL(15,2)),
            TRY_CAST(p.tot_mdcr_stdzd_amt AS DECIMAL(15,2)),
            TRY_CAST(p.drug_tot_srvcs AS DECIMAL(15,2)),
            TRY_CAST(p.med_tot_srvcs AS DECIMAL(15,2)),
            -- Part D
            TRY_CAST(d.tot_clms AS INTEGER),
            TRY_CAST(d.tot_drug_cst AS DECIMAL(15,2)),
            TRY_CAST(d.brnd_tot_clms AS INTEGER),
            TRY_CAST(d.gnrc_tot_clms AS INTEGER),
            TRY_CAST(d.opioid_prscrbr_rate AS DECIMAL(5,2)),
            -- DME
            TRY_CAST(dme.tot_suplr_clms AS INTEGER),
            TRY_CAST(dme.suplr_mdcr_pymt_amt AS DECIMAL(15,2)),
            -- Bene demographics
            TRY_CAST(p.bene_avg_age AS DECIMAL(5,2)),
            TRY_CAST(p.bene_avg_risk_scre AS DECIMAL(5,3)),
            TRY_CAST(p.bene_dual_cnt AS INTEGER),
            -- Chronic conditions
            TRY_CAST(p.bene_cc_ph_diabetes_v2_pct AS DECIMAL(5,2)),
            TRY_CAST(p.bene_cc_ph_hypertension_v2_pct AS DECIMAL(5,2)),
            TRY_CAST(p.bene_cc_ph_hf_nonihd_v2_pct AS DECIMAL(5,2)),
            TRY_CAST(p.bene_cc_ph_ckd_v2_pct AS DECIMAL(5,2)),
            TRY_CAST(p.bene_cc_ph_copd_v2_pct AS DECIMAL(5,2)),
            TRY_CAST(p.bene_cc_ph_cancer6_v2_pct AS DECIMAL(5,2)),
            TRY_CAST(p.bene_cc_bh_depress_v1_pct AS DECIMAL(5,2)),
            TRY_CAST(p.bene_cc_bh_alz_nonalzdem_v2_pct AS DECIMAL(5,2)),
            TRY_CAST(p.bene_cc_ph_afib_v2_pct AS DECIMAL(5,2)),
            TRY_CAST(p.bene_cc_ph_hyperlipidemia_v2_pct AS DECIMAL(5,2)),
            TRY_CAST(p.bene_cc_ph_ischemicheart_v2_pct AS DECIMAL(5,2)),
            TRY_CAST(p.bene_cc_ph_osteoporosis_v2_pct AS DECIMAL(5,2)),
            TRY_CAST(p.bene_cc_ph_arthritis_v2_pct AS DECIMAL(5,2)),
            TRY_CAST(p.bene_cc_ph_stroke_tia_v2_pct AS DECIMAL(5,2))
        FROM raw_physician_by_provider p
        LEFT JOIN raw_part_d_by_provider d ON p.rndrng_npi = d.prscrbr_npi
        LEFT JOIN raw_dme_by_referring_provider dme ON p.rndrng_npi = dme.rfrg_npi
        WHERE p.rndrng_prvdr_ent_cd = 'I'
    """, [data_year])

    count = con.execute("SELECT COUNT(*) FROM utilization_metrics WHERE metric_year = ?", [data_year]).fetchone()[0]
    logger.info("utilization_metrics: %d rows loaded", count)
    return count


def build_practice_locations(con: duckdb.DuckDBPyConnection, data_year: int):
    """Populate practice_locations from reassignment data.

    Each row represents one individual NPI's association with one group practice.
    """
    logger.info("Building practice_locations (data_year=%d)", data_year)

    con.execute("DELETE FROM practice_locations WHERE data_year = ?", [data_year])

    # Use quoted column names since reassignment data has spaces in column names
    con.execute("""
        INSERT INTO practice_locations (
            npi, group_pac_id, group_enrollment_id, group_legal_name,
            group_state, group_practice_size, state, data_year
        )
        SELECT
            r."individual npi",
            r."group pac id",
            r."group enrollment id",
            r."group legal business name",
            r."group state code",
            TRY_CAST(r."group reassignments and physician assistants" AS INTEGER),
            r."individual state code",
            ?
        FROM raw_reassignment r
        WHERE r."individual npi" IN (SELECT npi FROM core_providers)
    """, [data_year])

    count = con.execute("SELECT COUNT(*) FROM practice_locations WHERE data_year = ?", [data_year]).fetchone()[0]
    logger.info("practice_locations: %d rows loaded", count)

    # Mark primary location (largest group practice or first alphabetically)
    con.execute("""
        UPDATE practice_locations
        SET is_primary_location = TRUE
        WHERE location_id IN (
            SELECT location_id FROM (
                SELECT location_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY npi
                        ORDER BY COALESCE(group_practice_size, 0) DESC, group_legal_name
                    ) AS rn
                FROM practice_locations
                WHERE data_year = ?
            ) sub
            WHERE rn = 1
        )
    """, [data_year])

    return count


def build_hospital_affiliations(con: duckdb.DuckDBPyConnection, data_year: int):
    """Populate hospital_affiliations by joining reassignment -> hospital_enrollments.

    A provider is affiliated with a hospital when they reassign billing
    to a group practice that is also enrolled as a hospital.
    """
    logger.info("Building hospital_affiliations (data_year=%d)", data_year)

    con.execute("DELETE FROM hospital_affiliations WHERE data_year = ?", [data_year])

    # Determine hospital subgroup by checking subgroup flag columns
    con.execute("""
        INSERT INTO hospital_affiliations (
            npi, hospital_npi, hospital_ccn, hospital_name,
            hospital_city, hospital_state, hospital_zip, hospital_subgroup,
            affiliation_source, confidence_level, group_pac_id, data_year
        )
        SELECT DISTINCT
            r."individual npi" AS npi,
            h.npi AS hospital_npi,
            h.ccn AS hospital_ccn,
            h."organization name" AS hospital_name,
            h.city AS hospital_city,
            h.state AS hospital_state,
            h."zip code" AS hospital_zip,
            CASE
                WHEN h."subgroup - acute care" = 'Y' THEN 'acute_care'
                WHEN h."subgroup - psychiatric" = 'Y' THEN 'psychiatric'
                WHEN h."subgroup - rehabilitation" = 'Y' THEN 'rehabilitation'
                WHEN h."subgroup - long-term" = 'Y' THEN 'long_term'
                WHEN h."subgroup - childrens" = 'Y' THEN 'childrens'
                WHEN h."subgroup - specialty hospital" = 'Y' THEN 'specialty'
                ELSE 'general'
            END AS hospital_subgroup,
            'reassignment' AS affiliation_source,
            'medium' AS confidence_level,
            r."group pac id",
            ?
        FROM raw_reassignment r
        INNER JOIN raw_hospital_enrollments h
            ON r."group legal business name" = h."organization name"
            AND r."group state code" = h.state
        WHERE r."individual npi" IN (SELECT npi FROM core_providers)
    """, [data_year])

    count = con.execute("SELECT COUNT(*) FROM hospital_affiliations WHERE data_year = ?", [data_year]).fetchone()[0]
    logger.info("hospital_affiliations: %d rows loaded", count)
    return count


def build_provider_quality_scores(con: duckdb.DuckDBPyConnection, data_year: int):
    """Populate provider_quality_scores from QPP experience data."""
    logger.info("Building provider_quality_scores (data_year=%d)", data_year)

    con.execute("DELETE FROM provider_quality_scores WHERE data_year = ?", [data_year])

    con.execute("""
        INSERT INTO provider_quality_scores (
            npi, practice_state, practice_size, clinician_type, clinician_specialty,
            years_in_medicare, participation_option, small_practice_status,
            rural_status, hpsa_status, hospital_based_status, facility_based_status,
            dual_eligibility_ratio, final_mips_score, payment_adjustment_pct,
            complex_patient_bonus, quality_category_score, quality_category_weight,
            pi_category_score, pi_category_weight, ia_category_score,
            ia_category_weight, cost_category_score, cost_category_weight,
            data_year
        )
        SELECT
            q.npi,
            q."practice state or us territory",
            q."practice size",
            q."clinician type",
            q."clinician specialty",
            q."years in medicare",
            q."participation option",
            CASE WHEN LOWER(q."small practice status") = 'y' THEN TRUE ELSE FALSE END,
            CASE WHEN LOWER(q."rural status") = 'y' THEN TRUE ELSE FALSE END,
            CASE WHEN LOWER(q."health professional shortage area status") = 'y' THEN TRUE ELSE FALSE END,
            CASE WHEN LOWER(q."hospital-based status") = 'y' THEN TRUE ELSE FALSE END,
            CASE WHEN LOWER(q."facility-based status") = 'y' THEN TRUE ELSE FALSE END,
            TRY_CAST(q."dual eligibility ratio" AS DECIMAL(5,3)),
            TRY_CAST(q."final score" AS DECIMAL(7,2)),
            TRY_CAST(q."payment adjustment percentage" AS DECIMAL(7,4)),
            TRY_CAST(q."complex patient bonus" AS DECIMAL(7,4)),
            TRY_CAST(q."quality category score" AS DECIMAL(7,2)),
            TRY_CAST(q."quality category weight" AS DECIMAL(5,2)),
            TRY_CAST(q."promoting interoperability (pi) category score" AS DECIMAL(7,2)),
            TRY_CAST(q."promoting interoperability (pi) category weight" AS DECIMAL(5,2)),
            TRY_CAST(q."improvement activities (ia) category score" AS DECIMAL(7,2)),
            TRY_CAST(q."improvement activities (ia) category weight" AS DECIMAL(5,2)),
            TRY_CAST(q."cost category score" AS DECIMAL(7,2)),
            TRY_CAST(q."cost category weight" AS DECIMAL(5,2)),
            ?
        FROM raw_qpp_experience q
        WHERE q.npi IN (SELECT npi FROM core_providers)
    """, [data_year])

    count = con.execute("SELECT COUNT(*) FROM provider_quality_scores WHERE data_year = ?", [data_year]).fetchone()[0]
    logger.info("provider_quality_scores: %d rows loaded", count)
    return count


def build_provider_service_detail(con: duckdb.DuckDBPyConnection, data_year: int):
    """Populate provider_service_detail from physician_by_provider_and_service."""
    logger.info("Building provider_service_detail (data_year=%d)", data_year)

    con.execute("DELETE FROM provider_service_detail WHERE data_year = ?", [data_year])

    con.execute("""
        INSERT INTO provider_service_detail (
            npi, hcpcs_code, hcpcs_description, hcpcs_drug_ind, place_of_service,
            tot_beneficiaries, tot_services, tot_bene_day_srvcs,
            avg_submitted_chrg, avg_medicare_allowed, avg_medicare_payment,
            avg_medicare_standardized, data_year
        )
        SELECT
            s.rndrng_npi,
            s.hcpcs_cd,
            s.hcpcs_desc,
            s.hcpcs_drug_ind,
            s.place_of_srvc,
            TRY_CAST(s.tot_benes AS INTEGER),
            TRY_CAST(s.tot_srvcs AS DECIMAL(15,2)),
            TRY_CAST(s.tot_bene_day_srvcs AS DECIMAL(15,2)),
            TRY_CAST(s.avg_sbmtd_chrg AS DECIMAL(15,2)),
            TRY_CAST(s.avg_mdcr_alowd_amt AS DECIMAL(15,2)),
            TRY_CAST(s.avg_mdcr_pymt_amt AS DECIMAL(15,2)),
            TRY_CAST(s.avg_mdcr_stdzd_amt AS DECIMAL(15,2)),
            ?
        FROM raw_physician_by_provider_and_service s
        WHERE s.rndrng_prvdr_ent_cd = 'I'
          AND s.rndrng_npi IN (SELECT npi FROM core_providers)
    """, [data_year])

    count = con.execute("SELECT COUNT(*) FROM provider_service_detail WHERE data_year = ?", [data_year]).fetchone()[0]
    logger.info("provider_service_detail: %d rows loaded", count)
    return count


def build_order_referring_eligibility(con: duckdb.DuckDBPyConnection):
    """Populate order_referring_eligibility from order_and_referring data."""
    logger.info("Building order_referring_eligibility")

    con.execute("DELETE FROM order_referring_eligibility")

    con.execute("""
        INSERT INTO order_referring_eligibility (npi, last_name, first_name, partb, dme, hha, pmd, hospice)
        SELECT
            o.npi,
            o.last_name,
            o.first_name,
            o.partb,
            o.dme,
            o.hha,
            o.pmd,
            o.hospice
        FROM raw_order_and_referring o
        WHERE o.npi IN (SELECT npi FROM core_providers)
    """)

    count = con.execute("SELECT COUNT(*) FROM order_referring_eligibility").fetchone()[0]
    logger.info("order_referring_eligibility: %d rows loaded", count)
    return count


def transform_all(con: duckdb.DuckDBPyConnection, data_year: int) -> dict[str, int]:
    """Run all transforms in dependency order. Returns {table: row_count}."""
    results = {}

    # 1. Core providers first (other tables reference it)
    results["core_providers"] = build_core_providers(con, data_year)

    # 2. Tables that depend on core_providers (can conceptually run in parallel)
    results["utilization_metrics"] = build_utilization_metrics(con, data_year)
    results["practice_locations"] = build_practice_locations(con, data_year)
    results["hospital_affiliations"] = build_hospital_affiliations(con, data_year)
    results["provider_quality_scores"] = build_provider_quality_scores(con, data_year)
    results["order_referring_eligibility"] = build_order_referring_eligibility(con)

    # 3. Service detail (large table, run last)
    results["provider_service_detail"] = build_provider_service_detail(con, data_year)

    # 4. Dedup (must run after core_providers + utilization_metrics)
    from .dedup import flag_group_only_billers
    results["group_only_flagged"] = flag_group_only_billers(con, data_year)

    return results
