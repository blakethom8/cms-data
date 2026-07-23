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
            CAST(p.rndrng_npi AS VARCHAR),
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
        LEFT JOIN (
            SELECT * EXCLUDE (preferred)
            FROM (
                SELECT *, row_number() OVER (
                    PARTITION BY npi ORDER BY enrlmt_id NULLS LAST
                ) AS preferred
                FROM raw_pecos_enrollment
            )
            WHERE preferred = 1
        ) e ON p.rndrng_npi = e.npi
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
            CAST(p.rndrng_npi AS VARCHAR),
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
            CAST(r."individual npi" AS VARCHAR),
            r."group pac id",
            r."group enrollment id",
            r."group legal business name",
            r."group state code",
            TRY_CAST(r."group reassignments and physician assistants" AS INTEGER),
            r."individual state code",
            ?
        FROM raw_reassignment r
        WHERE CAST(r."individual npi" AS VARCHAR) IN (
            SELECT npi FROM core_providers
        )
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


def _ensure_pecos_relationship_tables(con: duckdb.DuckDBPyConnection) -> None:
    """Create the curated PPEF relationship tables in copied legacy warehouses."""
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS pecos_provider_organizations (
            relationship_key VARCHAR PRIMARY KEY,
            npi VARCHAR(10) NOT NULL,
            provider_enrollment_id VARCHAR(20) NOT NULL,
            receiving_enrollment_id VARCHAR(20) NOT NULL,
            receiving_npi VARCHAR(10),
            receiving_organization_name VARCHAR(255),
            receiving_entity_kind VARCHAR(30) NOT NULL,
            receiving_provider_type_code VARCHAR(30),
            receiving_provider_type_desc VARCHAR(255),
            receiving_state VARCHAR(2),
            source_data_period VARCHAR NOT NULL,
            relationship_source_run_id VARCHAR NOT NULL,
            enrollment_source_run_id VARCHAR NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_pecos_provider_org_npi
            ON pecos_provider_organizations(npi);
        CREATE INDEX IF NOT EXISTS idx_pecos_provider_org_receiving
            ON pecos_provider_organizations(receiving_enrollment_id);

        CREATE TABLE IF NOT EXISTS pecos_enrollment_practice_locations (
            location_key VARCHAR PRIMARY KEY,
            receiving_enrollment_id VARCHAR(20) NOT NULL,
            receiving_npi VARCHAR(10),
            receiving_organization_name VARCHAR(255),
            receiving_entity_kind VARCHAR(30) NOT NULL,
            city VARCHAR(100),
            state VARCHAR(2),
            zip_code VARCHAR(20),
            zip5 VARCHAR(5),
            source_data_period VARCHAR NOT NULL,
            location_source_run_id VARCHAR NOT NULL,
            enrollment_source_run_id VARCHAR NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_pecos_enrollment_location_receiving
            ON pecos_enrollment_practice_locations(receiving_enrollment_id);
        CREATE INDEX IF NOT EXISTS idx_pecos_enrollment_location_state
            ON pecos_enrollment_practice_locations(state);
        """
    )


def build_pecos_provider_relationships(
    con: duckdb.DuckDBPyConnection,
) -> dict[str, int]:
    """Build normalized provider-to-enrollment and enrollment-location bridges.

    These models describe benefit reassignment, not employment. Practice
    locations remain at enrollment-location grain and are not claim sites.
    """
    logger.info("Building curated PPEF provider relationship bridges")
    _ensure_pecos_relationship_tables(con)
    con.execute("DELETE FROM pecos_enrollment_practice_locations")
    con.execute("DELETE FROM pecos_provider_organizations")

    con.execute(
        """
        INSERT INTO pecos_provider_organizations (
            relationship_key, npi, provider_enrollment_id,
            receiving_enrollment_id, receiving_npi,
            receiving_organization_name, receiving_entity_kind,
            receiving_provider_type_code, receiving_provider_type_desc,
            receiving_state, source_data_period,
            relationship_source_run_id, enrollment_source_run_id
        )
        SELECT
            MD5(CONCAT_WS('|', r.REASGN_BNFT_ENRLMT_ID, r.RCV_BNFT_ENRLMT_ID)),
            CAST(provider.NPI AS VARCHAR),
            r.REASGN_BNFT_ENRLMT_ID,
            r.RCV_BNFT_ENRLMT_ID,
            CAST(receiver.NPI AS VARCHAR),
            NULLIF(TRIM(receiver.ORG_NAME), ''),
            CASE
                WHEN NULLIF(TRIM(receiver.ORG_NAME), '') IS NOT NULL
                    THEN 'organization'
                ELSE 'individual_or_unknown'
            END,
            receiver.PROVIDER_TYPE_CD,
            receiver.PROVIDER_TYPE_DESC,
            UPPER(receiver.STATE_CD),
            r.source_data_period,
            r.source_run_id,
            provider.source_run_id
        FROM raw_pecos_reassignment r
        JOIN raw_pecos_enrollment provider
          ON provider.ENRLMT_ID = r.REASGN_BNFT_ENRLMT_ID
        JOIN raw_pecos_enrollment receiver
          ON receiver.ENRLMT_ID = r.RCV_BNFT_ENRLMT_ID
        WHERE LENGTH(TRIM(CAST(provider.NPI AS VARCHAR))) = 10
          AND CAST(provider.NPI AS VARCHAR) ~ '^[0-9]{10}$'
        """
    )

    con.execute(
        """
        INSERT INTO pecos_enrollment_practice_locations (
            location_key, receiving_enrollment_id, receiving_npi,
            receiving_organization_name, receiving_entity_kind,
            city, state, zip_code, zip5, source_data_period,
            location_source_run_id, enrollment_source_run_id
        )
        SELECT
            MD5(CONCAT_WS('|', location.ENRLMT_ID,
                COALESCE(location.CITY_NAME, ''),
                COALESCE(location.STATE_CD, ''),
                COALESCE(location.ZIP_CD, ''))),
            location.ENRLMT_ID,
            CAST(receiver.NPI AS VARCHAR),
            NULLIF(TRIM(receiver.ORG_NAME), ''),
            CASE
                WHEN NULLIF(TRIM(receiver.ORG_NAME), '') IS NOT NULL
                    THEN 'organization'
                ELSE 'individual_or_unknown'
            END,
            location.CITY_NAME,
            UPPER(location.STATE_CD),
            location.ZIP_CD,
            LEFT(CAST(location.ZIP_CD AS VARCHAR), 5),
            location.source_data_period,
            location.source_run_id,
            receiver.source_run_id
        FROM raw_pecos_practice_location location
        JOIN raw_pecos_enrollment receiver
          ON receiver.ENRLMT_ID = location.ENRLMT_ID
        """
    )

    counts = {
        "pecos_provider_organizations": int(
            con.execute("SELECT COUNT(*) FROM pecos_provider_organizations").fetchone()[0]
        ),
        "pecos_enrollment_practice_locations": int(
            con.execute(
                "SELECT COUNT(*) FROM pecos_enrollment_practice_locations"
            ).fetchone()[0]
        ),
    }
    logger.info("Curated PPEF relationship counts: %s", counts)
    return counts


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
            CAST(r."individual npi" AS VARCHAR) AS npi,
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
        WHERE CAST(r."individual npi" AS VARCHAR) IN (
            SELECT npi FROM core_providers
        )
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
            CAST(q.npi AS VARCHAR),
            q."practice state or us territory",
            q."practice size",
            q."clinician type",
            q."clinician specialty",
            q."years in medicare",
            q."participation option",
            lower(trim(CAST(q."small practice status" AS VARCHAR)))
                IN ('y', 'yes', 'true', '1'),
            lower(trim(CAST(q."rural status" AS VARCHAR)))
                IN ('y', 'yes', 'true', '1'),
            lower(trim(CAST(q."health professional shortage area status" AS VARCHAR)))
                IN ('y', 'yes', 'true', '1'),
            lower(trim(CAST(q."hospital-based status" AS VARCHAR)))
                IN ('y', 'yes', 'true', '1'),
            lower(trim(CAST(q."facility-based status" AS VARCHAR)))
                IN ('y', 'yes', 'true', '1'),
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
        WHERE CAST(q.npi AS VARCHAR) IN (SELECT npi FROM core_providers)
          AND length(trim(CAST(q.npi AS VARCHAR))) = 10
        QUALIFY row_number() OVER (
            PARTITION BY CAST(q.npi AS VARCHAR)
            ORDER BY TRY_CAST(q."final score" AS DECIMAL(7,2)) DESC NULLS LAST,
                     q."provider key" NULLS LAST
        ) = 1
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
            CAST(s.rndrng_npi AS VARCHAR),
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
          AND CAST(s.rndrng_npi AS VARCHAR) IN (SELECT npi FROM core_providers)
        QUALIFY row_number() OVER (
            PARTITION BY CAST(s.rndrng_npi AS VARCHAR), s.hcpcs_cd, s.place_of_srvc
            ORDER BY TRY_CAST(s.tot_srvcs AS DECIMAL(15,2)) DESC NULLS LAST
        ) = 1
    """, [data_year])

    count = con.execute("SELECT COUNT(*) FROM provider_service_detail WHERE data_year = ?", [data_year]).fetchone()[0]
    logger.info("provider_service_detail: %d rows loaded", count)
    return count


def build_provider_drug_detail(con: duckdb.DuckDBPyConnection, data_year: int):
    """Populate provider_drug_detail with one row per NPI and generic drug."""
    logger.info("Building provider_drug_detail (data_year=%d)", data_year)
    con.execute("DELETE FROM provider_drug_detail WHERE data_year = ?", [data_year])
    con.execute(
        """
        INSERT INTO provider_drug_detail (
            npi, brand_name, generic_name, tot_claims, tot_30day_fills,
            tot_day_supply, tot_drug_cost, tot_beneficiaries, ge65_tot_claims,
            ge65_tot_drug_cost, ge65_tot_benes, data_year
        )
        SELECT
            CAST(d.prscrbr_npi AS VARCHAR),
            min(d.brnd_name),
            trim(d.gnrc_name),
            sum(TRY_CAST(d.tot_clms AS INTEGER)),
            sum(TRY_CAST(d.tot_30day_fills AS DECIMAL(15,2))),
            sum(TRY_CAST(d.tot_day_suply AS INTEGER)),
            sum(TRY_CAST(d.tot_drug_cst AS DECIMAL(15,2))),
            sum(TRY_CAST(d.tot_benes AS INTEGER)),
            sum(TRY_CAST(d.ge65_tot_clms AS INTEGER)),
            sum(TRY_CAST(d.ge65_tot_drug_cst AS DECIMAL(15,2))),
            sum(TRY_CAST(d.ge65_tot_benes AS INTEGER)),
            ?
        FROM raw_part_d_by_provider_and_drug d
        WHERE CAST(d.prscrbr_npi AS VARCHAR) IN (SELECT npi FROM core_providers)
          AND nullif(trim(d.gnrc_name), '') IS NOT NULL
        GROUP BY CAST(d.prscrbr_npi AS VARCHAR), trim(d.gnrc_name)
        """,
        [data_year],
    )
    count = con.execute(
        "SELECT COUNT(*) FROM provider_drug_detail WHERE data_year = ?", [data_year]
    ).fetchone()[0]
    logger.info("provider_drug_detail: %d rows loaded", count)
    return count


def build_order_referring_eligibility(con: duckdb.DuckDBPyConnection):
    """Populate order_referring_eligibility from order_and_referring data."""
    logger.info("Building order_referring_eligibility")

    con.execute("DELETE FROM order_referring_eligibility")

    con.execute("""
        INSERT INTO order_referring_eligibility (npi, last_name, first_name, partb, dme, hha, pmd, hospice)
        SELECT
            CAST(o.npi AS VARCHAR),
            o.last_name,
            o.first_name,
            o.partb,
            o.dme,
            o.hha,
            o.pmd,
            o.hospice
        FROM raw_order_and_referring o
        WHERE CAST(o.npi AS VARCHAR) IN (SELECT npi FROM core_providers)
        QUALIFY row_number() OVER (
            PARTITION BY CAST(o.npi AS VARCHAR) ORDER BY o.npi
        ) = 1
    """)

    count = con.execute("SELECT COUNT(*) FROM order_referring_eligibility").fetchone()[0]
    logger.info("order_referring_eligibility: %d rows loaded", count)
    return count


def transform_all(
    con: duckdb.DuckDBPyConnection,
    data_year: int,
    *,
    practice_year: int | None = None,
    quality_year: int | None = None,
    include_hospital_affiliations: bool = True,
) -> dict[str, int]:
    """Run all transforms in dependency order. Returns {table: row_count}."""
    results = {}

    # 1. Core providers first (other tables reference it)
    results["core_providers"] = build_core_providers(con, data_year)

    # 2. Tables that depend on core_providers (can conceptually run in parallel)
    results["utilization_metrics"] = build_utilization_metrics(con, data_year)
    results["practice_locations"] = build_practice_locations(
        con, practice_year or data_year
    )
    results.update(build_pecos_provider_relationships(con))
    if include_hospital_affiliations:
        results["hospital_affiliations"] = build_hospital_affiliations(
            con, practice_year or data_year
        )
    results["provider_quality_scores"] = build_provider_quality_scores(
        con, quality_year or data_year
    )
    results["order_referring_eligibility"] = build_order_referring_eligibility(con)

    # 3. Service detail (large table, run last)
    results["provider_service_detail"] = build_provider_service_detail(con, data_year)
    results["provider_drug_detail"] = build_provider_drug_detail(con, data_year)

    # 4. Dedup (must run after core_providers + utilization_metrics)
    from .dedup import flag_group_only_billers
    results["group_only_flagged"] = flag_group_only_billers(con, data_year)

    return results


def clear_refresh_targets(
    con: duckdb.DuckDBPyConnection,
    *,
    include_core_providers: bool = True,
) -> None:
    """Clear CMS-derived rows in foreign-key-safe order inside a candidate only.

    DuckDB cannot always delete referenced parent rows in the same transaction
    that deleted their children. Complete release builds therefore clear the
    dependent tables, commit, and delete ``core_providers`` separately.
    """
    for table in (
        "hospital_affiliations",
        "practice_locations",
        "utilization_metrics",
        "industry_relationships",
        "provider_service_detail",
        "provider_drug_detail",
        "provider_quality_scores",
        "order_referring_eligibility",
    ):
        con.execute(f"DELETE FROM {table}")
    if include_core_providers:
        con.execute("DELETE FROM core_providers")
