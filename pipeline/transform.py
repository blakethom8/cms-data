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


def _ensure_serving_practice_provider_sites_table(
    con: duckdb.DuckDBPyConnection,
) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS serving_practice_provider_sites (
            site_key VARCHAR(64) NOT NULL, addr_key VARCHAR NOT NULL,
            group_key VARCHAR NOT NULL, npi VARCHAR(10) NOT NULL,
            org_pac_id VARCHAR, practice_name VARCHAR(255),
            group_size_national INTEGER, address VARCHAR(255) NOT NULL,
            city VARCHAR(100) NOT NULL, state VARCHAR(2) NOT NULL,
            zip5 VARCHAR(5) NOT NULL, phone VARCHAR(30),
            specialties VARCHAR[] NOT NULL, first_name VARCHAR(100),
            last_name VARCHAR(255), latitude DOUBLE, longitude DOUBLE,
            partb_payments DOUBLE, partd_drug_cost DOUBLE,
            dac_source_data_periods VARCHAR[] NOT NULL,
            dac_source_run_ids VARCHAR[] NOT NULL,
            partb_source_data_periods VARCHAR[] NOT NULL,
            partb_source_run_ids VARCHAR[] NOT NULL,
            partd_source_data_periods VARCHAR[] NOT NULL,
            partd_source_run_ids VARCHAR[] NOT NULL,
            data_year INTEGER NOT NULL,
            PRIMARY KEY (site_key, npi)
        );
        CREATE INDEX IF NOT EXISTS idx_serving_practice_sites_state
            ON serving_practice_provider_sites(state);
        CREATE INDEX IF NOT EXISTS idx_serving_practice_sites_zip5
            ON serving_practice_provider_sites(zip5);
        CREATE INDEX IF NOT EXISTS idx_serving_practice_sites_npi
            ON serving_practice_provider_sites(npi);
        """
    )


def build_serving_practice_provider_sites(
    con: duckdb.DuckDBPyConnection, data_year: int
) -> int:
    """Build the CMS-enrollment practice-search serving grain.

    The table retains one row per normalized site, organization/solo key, and
    NPI. Specialty values remain a list so request-time specialty filtering has
    the same behavior as the raw DAC oracle without duplicating NPI totals.
    """
    required_columns = {
        "raw_dac_national": {
            "NPI", "Provider First Name", "Provider Last Name", "pri_spec",
            "Facility Name", "org_pac_id", "num_org_mem", "adr_ln_1",
            "ZIP Code", "City/Town", "State", "Telephone Number",
            "source_run_id", "source_data_period",
        },
        "raw_physician_by_provider": {
            "Rndrng_NPI", "Tot_Mdcr_Pymt_Amt", "source_run_id",
            "source_data_period",
        },
        "raw_part_d_by_provider": {
            "PRSCRBR_NPI", "Tot_Drug_Cst", "source_run_id",
            "source_data_period",
        },
        "address_geocode": {"addr_key", "lat", "lng"},
    }
    missing_tables = [
        table
        for table, columns in required_columns.items()
        if not _table_has_columns(con, table, columns)
    ]
    if missing_tables:
        raise ValueError(
            "Serving practice mart inputs are missing required provenance or fields: "
            + ", ".join(sorted(missing_tables))
        )

    _ensure_serving_practice_provider_sites_table(con)

    missing_dac_provenance = int(
        con.execute(
            """
            SELECT count(*)
            FROM raw_dac_national
            WHERE nullif(trim(adr_ln_1), '') IS NOT NULL
              AND nullif(trim("City/Town"), '') IS NOT NULL
              AND regexp_matches(upper(trim("State")), '^[A-Z]{2}$')
              AND regexp_matches(left(CAST("ZIP Code" AS VARCHAR), 5), '^[0-9]{5}$')
              AND nullif(trim(pri_spec), '') IS NOT NULL
              AND (nullif(trim(source_data_period), '') IS NULL
                   OR nullif(trim(source_run_id), '') IS NULL)
            """
        ).fetchone()[0]
    )
    if missing_dac_provenance:
        raise ValueError(
            "Serving practice mart found eligible DAC rows without source provenance: "
            f"{missing_dac_provenance}"
        )

    logger.info("Building serving_practice_provider_sites (data_year=%d)", data_year)
    con.execute("DELETE FROM serving_practice_provider_sites")
    con.execute(
        """
        INSERT INTO serving_practice_provider_sites
        WITH geocodes AS (
            SELECT addr_key, min(lat) latitude, min(lng) longitude
            FROM address_geocode
            GROUP BY addr_key
        ),
        clinicians AS (
            SELECT
                CAST(d."NPI" AS VARCHAR) npi,
                upper(trim(d.adr_ln_1)) addr_norm,
                left(CAST(d."ZIP Code" AS VARCHAR), 5) zip5,
                nullif(trim(CAST(d.org_pac_id AS VARCHAR)), '') org_pac_id,
                min(nullif(trim(d."Facility Name"), '')) practice_name,
                max(try_cast(d.num_org_mem AS INTEGER)) group_size_national,
                min(d.adr_ln_1) address,
                min(d."City/Town") city,
                min(d."State") state,
                min(CAST(d."Telephone Number" AS VARCHAR)) phone,
                list(distinct trim(d.pri_spec) order by trim(d.pri_spec))
                    FILTER (WHERE nullif(trim(d.pri_spec), '') IS NOT NULL) specialties,
                min(d."Provider First Name") first_name,
                min(d."Provider Last Name") last_name,
                list(distinct d.source_data_period order by d.source_data_period)
                    dac_source_data_periods,
                list(distinct d.source_run_id order by d.source_run_id)
                    dac_source_run_ids
            FROM raw_dac_national d
            WHERE nullif(trim(d.adr_ln_1), '') IS NOT NULL
              AND nullif(trim(d."City/Town"), '') IS NOT NULL
              AND regexp_matches(upper(trim(d."State")), '^[A-Z]{2}$')
              AND regexp_matches(left(CAST(d."ZIP Code" AS VARCHAR), 5), '^[0-9]{5}$')
              AND nullif(trim(d.pri_spec), '') IS NOT NULL
            GROUP BY 1, 2, 3, 4
        ),
        utilization AS (
            SELECT CAST("Rndrng_NPI" AS VARCHAR) npi,
                   max("Tot_Mdcr_Pymt_Amt") partb_payments,
                   list(distinct source_data_period order by source_data_period)
                       FILTER (WHERE nullif(trim(source_data_period), '') IS NOT NULL)
                       source_data_periods,
                   list(distinct source_run_id order by source_run_id)
                       FILTER (WHERE nullif(trim(source_run_id), '') IS NOT NULL)
                       source_run_ids
            FROM raw_physician_by_provider
            GROUP BY 1
        ),
        rx AS (
            SELECT CAST("PRSCRBR_NPI" AS VARCHAR) npi,
                   max("Tot_Drug_Cst") partd_drug_cost,
                   list(distinct source_data_period order by source_data_period)
                       FILTER (WHERE nullif(trim(source_data_period), '') IS NOT NULL)
                       source_data_periods,
                   list(distinct source_run_id order by source_run_id)
                       FILTER (WHERE nullif(trim(source_run_id), '') IS NOT NULL)
                       source_run_ids
            FROM raw_part_d_by_provider
            GROUP BY 1
        )
        SELECT
            md5(concat_ws('|', c.addr_norm, c.zip5,
                coalesce(c.org_pac_id, 'SOLO'))) site_key,
            c.addr_norm || '|' || c.zip5 addr_key,
            coalesce(c.org_pac_id, 'SOLO') group_key,
            c.npi,
            c.org_pac_id,
            c.practice_name,
            c.group_size_national,
            c.address,
            c.city,
            c.state,
            c.zip5,
            c.phone,
            c.specialties,
            c.first_name,
            c.last_name,
            g.latitude,
            g.longitude,
            u.partb_payments,
            rx.partd_drug_cost,
            c.dac_source_data_periods,
            c.dac_source_run_ids,
            coalesce(u.source_data_periods, []::VARCHAR[]),
            coalesce(u.source_run_ids, []::VARCHAR[]),
            coalesce(rx.source_data_periods, []::VARCHAR[]),
            coalesce(rx.source_run_ids, []::VARCHAR[]),
            ?
        FROM clinicians c
        LEFT JOIN geocodes g ON g.addr_key = c.addr_norm || '|' || c.zip5
        LEFT JOIN utilization u ON u.npi = c.npi
        LEFT JOIN rx ON rx.npi = c.npi
        """,
        [data_year],
    )
    count = int(
        con.execute("SELECT count(*) FROM serving_practice_provider_sites").fetchone()[0]
    )
    logger.info("serving_practice_provider_sites: %d rows loaded", count)
    return count


def _ensure_serving_practice_nppes_tables(
    con: duckdb.DuckDBPyConnection,
) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS serving_practice_nppes_provider_sites (
            npi VARCHAR(10) PRIMARY KEY, addr_key VARCHAR NOT NULL,
            addr_norm VARCHAR NOT NULL, address VARCHAR(255) NOT NULL,
            city VARCHAR(100) NOT NULL, state VARCHAR(2) NOT NULL,
            zip5 VARCHAR(5) NOT NULL, phone VARCHAR(30),
            first_name VARCHAR(100), last_name VARCHAR(255),
            credentials VARCHAR(50), specialties VARCHAR[] NOT NULL,
            latitude DOUBLE, longitude DOUBLE, partb_payments DOUBLE,
            partb_services DOUBLE, partb_beneficiaries DOUBLE,
            partd_drug_cost DOUBLE, nppes_source_data_period VARCHAR NOT NULL,
            nppes_source_run_id VARCHAR NOT NULL,
            partb_source_data_periods VARCHAR[] NOT NULL,
            partb_source_run_ids VARCHAR[] NOT NULL,
            partd_source_data_periods VARCHAR[] NOT NULL,
            partd_source_run_ids VARCHAR[] NOT NULL,
            data_year INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_serving_nppes_sites_state
            ON serving_practice_nppes_provider_sites(state);
        CREATE INDEX IF NOT EXISTS idx_serving_nppes_sites_zip5
            ON serving_practice_nppes_provider_sites(zip5);
        CREATE INDEX IF NOT EXISTS idx_serving_nppes_sites_addr
            ON serving_practice_nppes_provider_sites(addr_key);

        CREATE TABLE IF NOT EXISTS serving_practice_nppes_org_memberships (
            addr_key VARCHAR NOT NULL, npi VARCHAR(10) NOT NULL,
            org_pac_id VARCHAR NOT NULL, practice_name VARCHAR(255),
            group_size_national INTEGER, primary_address_match BOOLEAN NOT NULL,
            dac_source_data_periods VARCHAR[] NOT NULL,
            dac_source_run_ids VARCHAR[] NOT NULL,
            data_year INTEGER NOT NULL,
            PRIMARY KEY (addr_key, npi, org_pac_id)
        );
        CREATE INDEX IF NOT EXISTS idx_serving_nppes_memberships_npi
            ON serving_practice_nppes_org_memberships(npi);
        CREATE INDEX IF NOT EXISTS idx_serving_nppes_memberships_addr
            ON serving_practice_nppes_org_memberships(addr_key);
        """
    )


def build_serving_practice_nppes_tables(
    con: duckdb.DuckDBPyConnection, data_year: int
) -> dict[str, int]:
    """Build NPPES-primary provider sites and non-additive organization context.

    The provider table assigns each Medicare NPI's national totals to exactly
    one deterministic active NPPES practice address. The separate membership
    bridge preserves every CMS organization context without duplicating those
    provider totals.
    """
    required_columns = {
        "raw_nppes": {
            "npi",
            "first_name",
            "last_name",
            "credentials",
            "practice_address_1",
            "practice_city",
            "practice_state",
            "practice_zip",
            "practice_phone",
            "deactivation_date",
            "source_run_id",
            "source_data_period",
        },
        "raw_physician_by_provider": {
            "Rndrng_NPI",
            "Rndrng_Prvdr_Type",
            "Tot_Mdcr_Pymt_Amt",
            "Tot_Srvcs",
            "Tot_Benes",
            "source_run_id",
            "source_data_period",
        },
        "raw_part_d_by_provider": {
            "PRSCRBR_NPI",
            "Tot_Drug_Cst",
            "source_run_id",
            "source_data_period",
        },
        "raw_dac_national": {
            "NPI",
            "Facility Name",
            "org_pac_id",
            "num_org_mem",
            "adr_ln_1",
            "ZIP Code",
            "source_run_id",
            "source_data_period",
        },
        "address_geocode": {"addr_key", "lat", "lng"},
    }
    missing_tables = [
        table
        for table, columns in required_columns.items()
        if not _table_has_columns(con, table, columns)
    ]
    if missing_tables:
        raise ValueError(
            "NPPES practice serving inputs are missing required provenance or fields: "
            + ", ".join(sorted(missing_tables))
        )

    missing_partb_provenance = int(
        con.execute(
            """
            SELECT count(*)
            FROM raw_physician_by_provider
            WHERE nullif(trim(CAST("Rndrng_NPI" AS VARCHAR)), '') IS NOT NULL
              AND nullif(trim("Rndrng_Prvdr_Type"), '') IS NOT NULL
              AND (nullif(trim(source_data_period), '') IS NULL
                   OR nullif(trim(source_run_id), '') IS NULL)
            """
        ).fetchone()[0]
    )
    if missing_partb_provenance:
        raise ValueError(
            "NPPES practice serving mart found eligible Part B rows without source "
            f"provenance: {missing_partb_provenance}"
        )

    missing_partd_provenance = int(
        con.execute(
            """
            SELECT count(*)
            FROM raw_part_d_by_provider
            WHERE nullif(trim(CAST("PRSCRBR_NPI" AS VARCHAR)), '') IS NOT NULL
              AND "Tot_Drug_Cst" IS NOT NULL
              AND (nullif(trim(source_data_period), '') IS NULL
                   OR nullif(trim(source_run_id), '') IS NULL)
            """
        ).fetchone()[0]
    )
    if missing_partd_provenance:
        raise ValueError(
            "NPPES practice serving mart found eligible Part D rows without source "
            f"provenance: {missing_partd_provenance}"
        )

    missing_nppes_provenance = int(
        con.execute(
            """
            SELECT count(*)
            FROM raw_nppes
            WHERE deactivation_date IS NULL
              AND nullif(trim(practice_address_1), '') IS NOT NULL
              AND nullif(trim(practice_city), '') IS NOT NULL
              AND regexp_matches(upper(trim(practice_state)), '^[A-Z]{2}$')
              AND regexp_matches(left(practice_zip, 5), '^[0-9]{5}$')
              AND CAST(npi AS VARCHAR) IN (
                  SELECT CAST("Rndrng_NPI" AS VARCHAR)
                  FROM raw_physician_by_provider
              )
              AND (nullif(trim(source_data_period), '') IS NULL
                   OR nullif(trim(source_run_id), '') IS NULL)
            """
        ).fetchone()[0]
    )
    if missing_nppes_provenance:
        raise ValueError(
            "NPPES practice serving mart found eligible NPPES rows without source "
            f"provenance: {missing_nppes_provenance}"
        )

    missing_dac_provenance = int(
        con.execute(
            """
            SELECT count(*)
            FROM raw_dac_national
            WHERE nullif(trim(CAST(org_pac_id AS VARCHAR)), '') IS NOT NULL
              AND CAST("NPI" AS VARCHAR) IN (
                  SELECT CAST("Rndrng_NPI" AS VARCHAR)
                  FROM raw_physician_by_provider
              )
              AND (nullif(trim(source_data_period), '') IS NULL
                   OR nullif(trim(source_run_id), '') IS NULL)
            """
        ).fetchone()[0]
    )
    if missing_dac_provenance:
        raise ValueError(
            "NPPES practice serving mart found eligible DAC memberships without source "
            f"provenance: {missing_dac_provenance}"
        )

    _ensure_serving_practice_nppes_tables(con)
    logger.info("Building NPPES-primary practice serving tables (data_year=%d)", data_year)
    con.execute("DELETE FROM serving_practice_nppes_org_memberships")
    con.execute("DELETE FROM serving_practice_nppes_provider_sites")
    con.execute(
        """
        INSERT INTO serving_practice_nppes_provider_sites
        WITH claims AS (
            SELECT CAST(p."Rndrng_NPI" AS VARCHAR) npi,
                   list(
                       distinct trim(p."Rndrng_Prvdr_Type")
                       order by trim(p."Rndrng_Prvdr_Type")
                   ) specialties,
                   max(p."Tot_Mdcr_Pymt_Amt") partb_payments,
                   max(p."Tot_Srvcs") partb_services,
                   max(p."Tot_Benes") partb_beneficiaries,
                   list(distinct p.source_data_period order by p.source_data_period)
                       FILTER (WHERE nullif(trim(p.source_data_period), '') IS NOT NULL)
                       source_data_periods,
                   list(distinct p.source_run_id order by p.source_run_id)
                       FILTER (WHERE nullif(trim(p.source_run_id), '') IS NOT NULL)
                       source_run_ids
            FROM raw_physician_by_provider p
            WHERE nullif(trim(p."Rndrng_Prvdr_Type"), '') IS NOT NULL
            GROUP BY 1
        ),
        ranked_nppes AS (
            SELECT CAST(n.npi AS VARCHAR) npi,
                   upper(trim(n.practice_address_1)) addr_norm,
                   left(n.practice_zip, 5) zip5,
                   n.practice_address_1 address,
                   n.practice_city city,
                   n.practice_state state,
                   n.practice_phone phone,
                   n.first_name,
                   n.last_name,
                   n.credentials,
                   n.source_data_period,
                   n.source_run_id,
                   row_number() OVER (
                       PARTITION BY CAST(n.npi AS VARCHAR)
                       ORDER BY upper(trim(n.practice_address_1)),
                                left(n.practice_zip, 5),
                                upper(trim(coalesce(n.practice_city, ''))),
                                upper(trim(coalesce(n.practice_state, '')))
                   ) row_number
            FROM raw_nppes n
            WHERE n.deactivation_date IS NULL
              AND nullif(trim(n.practice_address_1), '') IS NOT NULL
              AND nullif(trim(n.practice_city), '') IS NOT NULL
              AND regexp_matches(upper(trim(n.practice_state)), '^[A-Z]{2}$')
              AND regexp_matches(left(n.practice_zip, 5), '^[0-9]{5}$')
              AND CAST(n.npi AS VARCHAR) IN (SELECT npi FROM claims)
        ),
        geocodes AS (
            SELECT addr_key, min(lat) latitude, min(lng) longitude
            FROM address_geocode
            GROUP BY addr_key
        ),
        rx AS (
            SELECT CAST("PRSCRBR_NPI" AS VARCHAR) npi,
                   max("Tot_Drug_Cst") partd_drug_cost,
                   list(distinct source_data_period order by source_data_period)
                       FILTER (WHERE nullif(trim(source_data_period), '') IS NOT NULL)
                       source_data_periods,
                   list(distinct source_run_id order by source_run_id)
                       FILTER (WHERE nullif(trim(source_run_id), '') IS NOT NULL)
                       source_run_ids
            FROM raw_part_d_by_provider
            GROUP BY 1
        )
        SELECT c.npi,
               n.addr_norm || '|' || n.zip5 addr_key,
               n.addr_norm,
               n.address,
               n.city,
               n.state,
               n.zip5,
               n.phone,
               n.first_name,
               n.last_name,
               n.credentials,
               c.specialties,
               g.latitude,
               g.longitude,
               c.partb_payments,
               c.partb_services,
               c.partb_beneficiaries,
               rx.partd_drug_cost,
               n.source_data_period,
               n.source_run_id,
               c.source_data_periods,
               c.source_run_ids,
               coalesce(rx.source_data_periods, []::VARCHAR[]),
               coalesce(rx.source_run_ids, []::VARCHAR[]),
               ?
        FROM claims c
        JOIN ranked_nppes n ON n.npi = c.npi AND n.row_number = 1
        LEFT JOIN geocodes g ON g.addr_key = n.addr_norm || '|' || n.zip5
        LEFT JOIN rx ON rx.npi = c.npi
        """,
        [data_year],
    )
    con.execute(
        """
        INSERT INTO serving_practice_nppes_org_memberships
        SELECT p.addr_key,
               p.npi,
               nullif(trim(CAST(d.org_pac_id AS VARCHAR)), '') org_pac_id,
               min(nullif(trim(d."Facility Name"), '')) practice_name,
               max(try_cast(d.num_org_mem AS INTEGER)) group_size_national,
               max(CASE WHEN upper(trim(d.adr_ln_1)) = p.addr_norm
                              AND left(CAST(d."ZIP Code" AS VARCHAR), 5) = p.zip5
                        THEN 1 ELSE 0 END) = 1 primary_address_match,
               list(distinct d.source_data_period order by d.source_data_period)
                   dac_source_data_periods,
               list(distinct d.source_run_id order by d.source_run_id)
                   dac_source_run_ids,
               ?
        FROM serving_practice_nppes_provider_sites p
        JOIN raw_dac_national d ON p.npi = CAST(d."NPI" AS VARCHAR)
        WHERE nullif(trim(CAST(d.org_pac_id AS VARCHAR)), '') IS NOT NULL
        GROUP BY p.addr_key, p.npi, nullif(trim(CAST(d.org_pac_id AS VARCHAR)), '')
        """,
        [data_year],
    )
    counts = {
        table: int(con.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
        for table in (
            "serving_practice_nppes_provider_sites",
            "serving_practice_nppes_org_memberships",
        )
    }
    logger.info(
        "NPPES-primary serving rows: providers=%d memberships=%d",
        counts["serving_practice_nppes_provider_sites"],
        counts["serving_practice_nppes_org_memberships"],
    )
    return counts


def _ensure_serving_provider_profile_core_tables(
    con: duckdb.DuckDBPyConnection,
) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS serving_provider_profile_headers (
            npi VARCHAR(10) PRIMARY KEY, name VARCHAR, credentials VARCHAR,
            specialty VARCHAR, secondary_specialties VARCHAR, city VARCHAR,
            state VARCHAR(2), med_school VARCHAR, grad_year INTEGER,
            telehealth BOOLEAN, nppes_source_data_periods VARCHAR[] NOT NULL,
            nppes_source_run_ids VARCHAR[] NOT NULL,
            dac_source_data_periods VARCHAR[] NOT NULL,
            dac_source_run_ids VARCHAR[] NOT NULL, data_year INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_serving_profile_headers_state
            ON serving_provider_profile_headers(state);

        CREATE TABLE IF NOT EXISTS serving_provider_profile_locations (
            npi VARCHAR(10) NOT NULL, addr_key VARCHAR NOT NULL,
            street VARCHAR, suites VARCHAR[], city VARCHAR, state VARCHAR(2),
            zip5 VARCHAR(5), phone VARCHAR, roster_size BIGINT,
            latitude DOUBLE, longitude DOUBLE, likely_flagship BOOLEAN,
            sources VARCHAR NOT NULL,
            nppes_source_data_periods VARCHAR[] NOT NULL,
            nppes_source_run_ids VARCHAR[] NOT NULL,
            dac_source_data_periods VARCHAR[] NOT NULL,
            dac_source_run_ids VARCHAR[] NOT NULL, data_year INTEGER NOT NULL,
            PRIMARY KEY (npi, addr_key)
        );
        CREATE INDEX IF NOT EXISTS idx_serving_profile_locations_npi
            ON serving_provider_profile_locations(npi);

        CREATE TABLE IF NOT EXISTS serving_provider_profile_groups (
            npi VARCHAR(10) NOT NULL, group_id VARCHAR NOT NULL,
            group_name VARCHAR, group_size INTEGER, n_addresses BIGINT NOT NULL,
            reassignment_size BIGINT, sources VARCHAR NOT NULL,
            dac_source_data_periods VARCHAR[] NOT NULL,
            dac_source_run_ids VARCHAR[] NOT NULL,
            reassignment_source_data_periods VARCHAR[] NOT NULL,
            reassignment_source_run_ids VARCHAR[] NOT NULL,
            data_year INTEGER NOT NULL, PRIMARY KEY (npi, group_id)
        );
        CREATE INDEX IF NOT EXISTS idx_serving_profile_groups_npi
            ON serving_provider_profile_groups(npi);
        """
    )


def build_serving_provider_profile_core_tables(
    con: duckdb.DuckDBPyConnection, data_year: int
) -> dict[str, int]:
    """Build the first provider-profile serving slice.

    Identity, merged practice doors, and organization contexts use separate
    grains so multi-address and multi-organization providers never duplicate
    one another. Hospital affiliations remain on their source-faithful raw
    path until those legacy inputs have managed source manifests.
    """
    required_columns = {
        "raw_nppes": {
            "npi", "first_name", "last_name", "credentials", "practice_address_1",
            "practice_address_2", "practice_city", "practice_state", "practice_zip",
            "practice_phone", "taxonomy_1", "source_run_id", "source_data_period",
        },
        "raw_dac_national": {
            "NPI", "Provider First Name", "Provider Last Name", "Cred\t\t\t\t",
            "pri_spec", "sec_spec_all", "City/Town", "State", "Med_sch", "Grd_yr",
            "Telehlth\t\t\t\t", "Facility Name", "org_pac_id", "num_org_mem",
            "adr_ln_1", "adr_ln_2", "ZIP Code", "Telephone Number",
            "source_run_id", "source_data_period",
        },
        "raw_reassignment": {
            "Individual NPI", "Group PAC ID", "Group Legal Business Name",
            "Group Reassignments and Physician Assistants", "source_run_id",
            "source_data_period",
        },
        "nucc_taxonomy": {"taxonomy_code", "classification", "specialization"},
        "address_geocode": {"addr_key", "lat", "lng"},
    }
    missing_tables = [
        table
        for table, columns in required_columns.items()
        if not _table_has_columns(con, table, columns)
    ]
    if missing_tables:
        raise ValueError(
            "Provider profile serving inputs are missing required provenance or fields: "
            + ", ".join(sorted(missing_tables))
        )

    provenance_checks = (
        (
            "NPPES",
            """
            SELECT count(*) FROM raw_nppes
            WHERE nullif(trim(CAST(npi AS VARCHAR)), '') IS NOT NULL
              AND (nullif(trim(source_data_period), '') IS NULL
                   OR nullif(trim(source_run_id), '') IS NULL)
            """,
        ),
        (
            "DAC",
            """
            SELECT count(*) FROM raw_dac_national
            WHERE nullif(trim(CAST("NPI" AS VARCHAR)), '') IS NOT NULL
              AND (nullif(trim(source_data_period), '') IS NULL
                   OR nullif(trim(source_run_id), '') IS NULL)
            """,
        ),
        (
            "reassignment",
            """
            SELECT count(*) FROM raw_reassignment
            WHERE nullif(trim(CAST("Individual NPI" AS VARCHAR)), '') IS NOT NULL
              AND nullif(trim(CAST("Group PAC ID" AS VARCHAR)), '') IS NOT NULL
              AND (nullif(trim(source_data_period), '') IS NULL
                   OR nullif(trim(source_run_id), '') IS NULL)
            """,
        ),
    )
    for label, sql in provenance_checks:
        missing = int(con.execute(sql).fetchone()[0])
        if missing:
            raise ValueError(
                "Provider profile serving mart found eligible "
                f"{label} rows without source provenance: {missing}"
            )

    _ensure_serving_provider_profile_core_tables(con)
    logger.info("Building provider profile core serving tables (data_year=%d)", data_year)
    con.execute("DELETE FROM serving_provider_profile_locations")
    con.execute("DELETE FROM serving_provider_profile_groups")
    con.execute("DELETE FROM serving_provider_profile_headers")

    con.execute(
        """
        INSERT INTO serving_provider_profile_headers
        WITH ranked_nppes AS (
            SELECT CAST(npi AS VARCHAR) npi,
                   trim(coalesce(first_name || ' ', '') || coalesce(last_name, '')) "name",
                   nullif(trim(credentials), '') credentials,
                   practice_city city, practice_state state, taxonomy_1,
                   source_data_period, source_run_id,
                   row_number() OVER (
                       PARTITION BY CAST(npi AS VARCHAR)
                       ORDER BY source_data_period DESC, source_run_id DESC,
                                coalesce(first_name, ''), coalesce(last_name, '')
                   ) row_number
            FROM raw_nppes
        ),
        nppes AS (
            SELECT npi, "name", credentials, city, state, taxonomy_1,
                   [source_data_period] source_data_periods,
                   [source_run_id] source_run_ids
            FROM ranked_nppes WHERE row_number = 1
        ),
        ranked_dac AS (
            SELECT CAST("NPI" AS VARCHAR) npi,
                   "Provider First Name" || ' ' || "Provider Last Name" "name",
                   nullif(trim("Cred\t\t\t\t"), '') credentials,
                   pri_spec specialty, sec_spec_all secondary_specialties,
                   "City/Town" city, "State" state,
                   Med_sch med_school, Grd_yr grad_year,
                   max(CASE WHEN "Telehlth\t\t\t\t" = 'Y' THEN 1 ELSE 0 END)
                       OVER (PARTITION BY "NPI") = 1 telehealth,
                   row_number() OVER (
                       PARTITION BY "NPI"
                       ORDER BY nullif(trim(pri_spec), '') IS NULL,
                                trim(pri_spec),
                                nullif(trim(sec_spec_all), '') IS NULL,
                                trim(sec_spec_all),
                                coalesce(trim("Provider First Name"), ''),
                                coalesce(trim("Provider Last Name"), ''),
                                coalesce(trim("City/Town"), ''),
                                coalesce(trim("State"), ''),
                                coalesce(trim(Med_sch), ''),
                                coalesce(Grd_yr, 0),
                                coalesce(trim("Cred\t\t\t\t"), '')
                   ) row_number
            FROM raw_dac_national
        ),
        dac_provenance AS (
            SELECT CAST("NPI" AS VARCHAR) npi,
                   list(distinct source_data_period order by source_data_period)
                       source_data_periods,
                   list(distinct source_run_id order by source_run_id) source_run_ids
            FROM raw_dac_national
            GROUP BY "NPI"
        ),
        dac AS (
            SELECT d.npi, d."name", d.credentials, d.specialty,
                   d.secondary_specialties, d.city, d.state, d.med_school,
                   d.grad_year, d.telehealth, p.source_data_periods,
                   p.source_run_ids
            FROM ranked_dac d
            JOIN dac_provenance p ON p.npi = d.npi
            WHERE d.row_number = 1
        ),
        taxonomy AS (
            SELECT taxonomy_code, min(classification) classification,
                   min(specialization) specialization
            FROM nucc_taxonomy GROUP BY taxonomy_code
        )
        SELECT coalesce(n.npi, d.npi) npi,
               coalesce(n."name", d."name") "name",
               coalesce(n.credentials, d.credentials) credentials,
               coalesce(
                   d.specialty,
                   t.classification
                       || coalesce(' (' || nullif(t.specialization, '') || ')', '')
               ) specialty,
               d.secondary_specialties,
               coalesce(n.city, d.city) city,
               coalesce(n.state, d.state) state,
               d.med_school, d.grad_year, d.telehealth,
               coalesce(n.source_data_periods, []::VARCHAR[]),
               coalesce(n.source_run_ids, []::VARCHAR[]),
               coalesce(d.source_data_periods, []::VARCHAR[]),
               coalesce(d.source_run_ids, []::VARCHAR[]),
               ?
        FROM nppes n
        FULL OUTER JOIN dac d ON d.npi = n.npi
        LEFT JOIN taxonomy t ON t.taxonomy_code = n.taxonomy_1
        WHERE regexp_matches(coalesce(n.npi, d.npi), '^[0-9]{10}$')
        """,
        [data_year],
    )

    con.execute(
        """
        INSERT INTO serving_provider_profile_locations
        WITH dac AS (
            SELECT CAST("NPI" AS VARCHAR) npi,
                   CASE WHEN "ZIP Code" IS NULL THEN '~MISSING_ZIP_DAC~'
                        ELSE upper(trim(adr_ln_1)) || '|'
                            || left(CAST("ZIP Code" AS VARCHAR), 5)
                   END addr_key,
                   min(trim(adr_ln_1)) street,
                   list(distinct trim(adr_ln_2) order by trim(adr_ln_2))
                       FILTER (WHERE nullif(trim(adr_ln_2), '') IS NOT NULL) suites,
                   min("City/Town") city, min("State") state,
                   left(min(CAST("ZIP Code" AS VARCHAR)), 5) zip5,
                   min(CAST("Telephone Number" AS VARCHAR)) phone,
                   min(nullif(trim(CAST(org_pac_id AS VARCHAR)), '')) org_pac_id,
                   list(distinct source_data_period order by source_data_period)
                       source_data_periods,
                   list(distinct source_run_id order by source_run_id) source_run_ids
            FROM raw_dac_national
            WHERE nullif(trim(adr_ln_1), '') IS NOT NULL
            GROUP BY 1, 2
        ),
        nppes AS (
            SELECT CAST(npi AS VARCHAR) npi,
                   CASE WHEN practice_zip IS NULL THEN '~MISSING_ZIP_NPPES~'
                        ELSE upper(trim(practice_address_1)) || '|'
                            || left(CAST(practice_zip AS VARCHAR), 5)
                   END addr_key,
                   min(trim(practice_address_1)) street,
                   list(distinct trim(practice_address_2) order by trim(practice_address_2))
                       FILTER (WHERE nullif(trim(practice_address_2), '') IS NOT NULL) suites,
                   min(practice_city) city, min(practice_state) state,
                   left(min(CAST(practice_zip AS VARCHAR)), 5) zip5,
                   min(CAST(practice_phone AS VARCHAR)) phone,
                   list(distinct source_data_period order by source_data_period)
                       source_data_periods,
                   list(distinct source_run_id order by source_run_id) source_run_ids
            FROM raw_nppes
            WHERE nullif(trim(practice_address_1), '') IS NOT NULL
            GROUP BY 1, 2
        ),
        doc AS (
            SELECT coalesce(d.npi, n.npi) npi,
                   coalesce(d.addr_key, n.addr_key) addr_key,
                   coalesce(d.street, n.street) street,
                   coalesce(d.suites, n.suites) suites,
                   coalesce(d.city, n.city) city,
                   coalesce(d.state, n.state) state,
                   coalesce(d.zip5, n.zip5) zip5,
                   coalesce(d.phone, n.phone) phone,
                   d.org_pac_id,
                   CASE WHEN d.addr_key IS NOT NULL AND n.addr_key IS NOT NULL
                            THEN 'dac + nppes'
                        WHEN d.addr_key IS NOT NULL THEN 'dac'
                        ELSE 'nppes' END sources,
                   coalesce(n.source_data_periods, []::VARCHAR[])
                       nppes_source_data_periods,
                   coalesce(n.source_run_ids, []::VARCHAR[]) nppes_source_run_ids,
                   coalesce(d.source_data_periods, []::VARCHAR[])
                       dac_source_data_periods,
                   coalesce(d.source_run_ids, []::VARCHAR[]) dac_source_run_ids
            FROM dac d
            FULL OUTER JOIN nppes n ON n.npi = d.npi AND n.addr_key = d.addr_key
        ),
        roster AS (
            SELECT nullif(trim(CAST(org_pac_id AS VARCHAR)), '') org_pac_id,
                   upper(trim(adr_ln_1)) || '|'
                       || left(CAST("ZIP Code" AS VARCHAR), 5) addr_key,
                   count(distinct "NPI") roster_size
            FROM raw_dac_national
            WHERE nullif(trim(CAST(org_pac_id AS VARCHAR)), '') IS NOT NULL
              AND nullif(trim(adr_ln_1), '') IS NOT NULL
            GROUP BY 1, 2
        ),
        geocodes AS (
            SELECT addr_key, min(lat) latitude, min(lng) longitude
            FROM address_geocode GROUP BY addr_key
        ),
        enriched AS (
            SELECT doc.*, r.roster_size, g.latitude, g.longitude
            FROM doc
            LEFT JOIN roster r
              ON r.org_pac_id = doc.org_pac_id AND r.addr_key = doc.addr_key
            LEFT JOIN geocodes g ON g.addr_key = doc.addr_key
            JOIN serving_provider_profile_headers h ON h.npi = doc.npi
        )
        SELECT npi, addr_key, street, suites, city, state, zip5, phone,
               roster_size, latitude, longitude,
               roster_size = max(roster_size) OVER (PARTITION BY npi)
                   AND roster_size > 50 likely_flagship,
               sources, nppes_source_data_periods, nppes_source_run_ids,
               dac_source_data_periods, dac_source_run_ids, ?
        FROM enriched
        """,
        [data_year],
    )

    con.execute(
        """
        INSERT INTO serving_provider_profile_groups
        WITH dac AS (
            SELECT CAST("NPI" AS VARCHAR) npi,
                   CAST(org_pac_id AS VARCHAR) group_id,
                   any_value("Facility Name") group_name,
                   any_value(num_org_mem) group_size,
                   count(distinct upper(trim(adr_ln_1))) n_addresses,
                   list(distinct source_data_period order by source_data_period)
                       source_data_periods,
                   list(distinct source_run_id order by source_run_id) source_run_ids
            FROM raw_dac_national
            WHERE org_pac_id IS NOT NULL
            GROUP BY 1, 2
        ),
        reassign AS (
            SELECT CAST("Individual NPI" AS VARCHAR) npi,
                   CAST("Group PAC ID" AS VARCHAR) group_id,
                   any_value("Group Legal Business Name") group_name,
                   any_value("Group Reassignments and Physician Assistants")
                       reassignment_size,
                   list(distinct source_data_period order by source_data_period)
                       source_data_periods,
                   list(distinct source_run_id order by source_run_id) source_run_ids
            FROM raw_reassignment
            WHERE "Group PAC ID" IS NOT NULL
            GROUP BY 1, 2
        )
        SELECT coalesce(d.npi, r.npi) npi,
               coalesce(d.group_id, r.group_id) group_id,
               coalesce(d.group_name, r.group_name) group_name,
               d.group_size, coalesce(d.n_addresses, 0) n_addresses,
               r.reassignment_size,
               CASE WHEN d.group_id IS NOT NULL AND r.group_id IS NOT NULL
                        THEN 'dac + reassignment'
                    WHEN d.group_id IS NOT NULL THEN 'dac'
                    ELSE 'reassignment' END sources,
               coalesce(d.source_data_periods, []::VARCHAR[]),
               coalesce(d.source_run_ids, []::VARCHAR[]),
               coalesce(r.source_data_periods, []::VARCHAR[]),
               coalesce(r.source_run_ids, []::VARCHAR[]),
               ?
        FROM dac d
        FULL OUTER JOIN reassign r
          ON r.npi = d.npi AND r.group_id = d.group_id
        JOIN serving_provider_profile_headers h ON h.npi = coalesce(d.npi, r.npi)
        """,
        [data_year],
    )

    counts = {
        table: int(con.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
        for table in (
            "serving_provider_profile_headers",
            "serving_provider_profile_locations",
            "serving_provider_profile_groups",
        )
    }
    logger.info(
        "Provider profile serving rows: headers=%d locations=%d groups=%d",
        counts["serving_provider_profile_headers"],
        counts["serving_provider_profile_locations"],
        counts["serving_provider_profile_groups"],
    )
    return counts


def _ensure_serving_provider_profile_claims_tables(
    con: duckdb.DuckDBPyConnection,
) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS serving_provider_profile_claims_summary (
            npi VARCHAR(10) PRIMARY KEY, has_panel BOOLEAN NOT NULL,
            medicare_patients BIGINT, panel_total_services DOUBLE,
            services_per_patient DOUBLE, medicare_allowed_amt DOUBLE,
            part_b_drug_payments DOUBLE, avg_patient_age BIGINT,
            pct_age_75_plus DOUBLE, pct_female DOUBLE,
            pct_dual_eligible DOUBLE, avg_hcc_risk_score DOUBLE,
            pct_hypertension BIGINT, pct_hyperlipidemia BIGINT,
            pct_diabetes BIGINT, pct_ischemic_heart BIGINT,
            pct_heart_failure BIGINT, pct_afib BIGINT, pct_ckd BIGINT,
            pct_copd BIGINT, pct_depression BIGINT,
            has_clinical BOOLEAN NOT NULL, cms_specialty VARCHAR,
            distinct_codes BIGINT NOT NULL, clinical_total_services DOUBLE,
            est_total_paid DOUBLE, facility_paid_share DOUBLE,
            drug_admin_paid_share DOUBLE, em_paid_share DOUBLE,
            has_prescribing BOOLEAN NOT NULL, total_claims BIGINT,
            prescribing_patients BIGINT, total_cost DOUBLE,
            cost_per_claim DOUBLE, brand_claim_share DOUBLE,
            brand_cost_share DOUBLE, opioid_rate_pct DOUBLE,
            lis_claim_share DOUBLE, rx_panel_avg_age DOUBLE,
            rx_panel_risk DOUBLE,
            part_b_provider_source_data_periods VARCHAR[] NOT NULL,
            part_b_provider_source_run_ids VARCHAR[] NOT NULL,
            part_b_service_source_data_periods VARCHAR[] NOT NULL,
            part_b_service_source_run_ids VARCHAR[] NOT NULL,
            part_d_provider_source_data_periods VARCHAR[] NOT NULL,
            part_d_provider_source_run_ids VARCHAR[] NOT NULL,
            data_year INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS serving_provider_profile_top_services (
            npi VARCHAR(10) NOT NULL, service_rank INTEGER NOT NULL,
            hcpcs VARCHAR NOT NULL, category VARCHAR NOT NULL,
            description VARCHAR, services DOUBLE, patients BIGINT,
            est_paid DOUBLE, pct_of_paid DOUBLE, facility_share DOUBLE,
            source_data_periods VARCHAR[] NOT NULL,
            source_run_ids VARCHAR[] NOT NULL, data_year INTEGER NOT NULL,
            PRIMARY KEY (npi, service_rank), UNIQUE (npi, hcpcs)
        );
        CREATE INDEX IF NOT EXISTS idx_serving_profile_top_services_npi
            ON serving_provider_profile_top_services(npi);

        CREATE TABLE IF NOT EXISTS serving_provider_profile_top_drugs (
            npi VARCHAR(10) NOT NULL, drug_rank INTEGER NOT NULL,
            brand VARCHAR, generic VARCHAR, claims BIGINT, patients BIGINT,
            drug_cost DOUBLE, cost_per_claim DOUBLE, days_per_claim DOUBLE,
            specialty_tier BOOLEAN, pct_of_cost DOUBLE,
            source_data_periods VARCHAR[] NOT NULL,
            source_run_ids VARCHAR[] NOT NULL, data_year INTEGER NOT NULL,
            PRIMARY KEY (npi, drug_rank)
        );
        CREATE INDEX IF NOT EXISTS idx_serving_profile_top_drugs_npi
            ON serving_provider_profile_top_drugs(npi);
        """
    )


def build_serving_provider_profile_claims_tables(
    con: duckdb.DuckDBPyConnection, data_year: int
) -> dict[str, int]:
    """Build response-exact profile utilization, service, and drug marts.

    The summary and both detail tables retain the profile endpoint's existing
    response grain. Deterministic tie-breakers make repeated builds stable,
    while separate source arrays keep Part B provider, Part B service, and
    Part D provider evidence distinguishable.
    """
    required_columns = {
        "raw_physician_by_provider": {
            "Rndrng_NPI", "Rndrng_Prvdr_Ent_Cd", "Tot_Benes", "Tot_Srvcs",
            "Tot_Mdcr_Alowd_Amt", "Drug_Mdcr_Pymt_Amt", "Bene_Avg_Age",
            "Bene_Age_75_84_Cnt", "Bene_Age_GT_84_Cnt", "Bene_Feml_Cnt",
            "Bene_Dual_Cnt", "Bene_Avg_Risk_Scre",
            "Bene_CC_PH_Hypertension_V2_Pct",
            "Bene_CC_PH_Hyperlipidemia_V2_Pct",
            "Bene_CC_PH_Diabetes_V2_Pct",
            "Bene_CC_PH_IschemicHeart_V2_Pct",
            "Bene_CC_PH_HF_NonIHD_V2_Pct", "Bene_CC_PH_Afib_V2_Pct",
            "Bene_CC_PH_CKD_V2_Pct", "Bene_CC_PH_COPD_V2_Pct",
            "Bene_CC_BH_Depress_V1_Pct", "source_run_id",
            "source_data_period",
        },
        "raw_physician_by_provider_and_service": {
            "Rndrng_NPI", "Rndrng_Prvdr_Type", "HCPCS_Cd", "HCPCS_Desc",
            "HCPCS_Drug_Ind", "Place_Of_Srvc", "Tot_Srvcs", "Tot_Benes",
            "Avg_Mdcr_Pymt_Amt", "source_run_id", "source_data_period",
        },
        "raw_part_d_by_provider": {
            "Prscrbr_NPI", "Tot_Clms", "Tot_Benes", "Tot_Drug_Cst",
            "Brnd_Tot_Clms", "Gnrc_Tot_Clms", "Brnd_Tot_Drug_Cst",
            "Opioid_Prscrbr_Rate", "LIS_Tot_Clms", "Bene_Avg_Age",
            "Bene_Avg_Risk_Scre", "source_run_id", "source_data_period",
        },
        "raw_part_d_by_provider_and_drug": {
            "Prscrbr_NPI", "Brnd_Name", "Gnrc_Name", "Tot_Clms",
            "Tot_Benes", "Tot_Drug_Cst", "Tot_Day_Suply", "source_run_id",
            "source_data_period",
        },
        "serving_provider_profile_headers": {"npi"},
    }
    missing_tables = [
        table
        for table, columns in required_columns.items()
        if not _table_has_columns(con, table, columns)
    ]
    if missing_tables:
        raise ValueError(
            "Provider profile claims inputs are missing required provenance or fields: "
            + ", ".join(sorted(missing_tables))
        )

    provenance_checks = (
        ("Part B provider", "raw_physician_by_provider", "Rndrng_NPI"),
        (
            "Part B service",
            "raw_physician_by_provider_and_service",
            "Rndrng_NPI",
        ),
        ("Part D provider", "raw_part_d_by_provider", "Prscrbr_NPI"),
        ("Part D drug", "raw_part_d_by_provider_and_drug", "Prscrbr_NPI"),
    )
    for label, table, npi_column in provenance_checks:
        missing = int(
            con.execute(
                f"""
                SELECT count(*) FROM {table}
                WHERE nullif(trim(CAST({npi_column} AS VARCHAR)), '') IS NOT NULL
                  AND (nullif(trim(source_data_period), '') IS NULL
                       OR nullif(trim(source_run_id), '') IS NULL)
                """
            ).fetchone()[0]
        )
        if missing:
            raise ValueError(
                "Provider profile claims mart found eligible "
                f"{label} rows without source provenance: {missing}"
            )

    uniqueness_checks = (
        (
            "Part B provider NPI",
            """
            SELECT count(*) FROM (
                SELECT CAST(Rndrng_NPI AS VARCHAR)
                FROM raw_physician_by_provider
                WHERE Rndrng_Prvdr_Ent_Cd = 'I'
                GROUP BY 1 HAVING count(*) > 1
            )
            """,
        ),
        (
            "Part B service NPI/HCPCS/place-of-service",
            """
            SELECT count(*) FROM (
                SELECT CAST(Rndrng_NPI AS VARCHAR), HCPCS_Cd, Place_Of_Srvc
                FROM raw_physician_by_provider_and_service
                GROUP BY 1, 2, 3 HAVING count(*) > 1
            )
            """,
        ),
        (
            "Part D provider NPI",
            """
            SELECT count(*) FROM (
                SELECT CAST(Prscrbr_NPI AS VARCHAR)
                FROM raw_part_d_by_provider
                GROUP BY 1 HAVING count(*) > 1
            )
            """,
        ),
        (
            "Part D drug NPI/brand/generic",
            """
            SELECT count(*) FROM (
                SELECT CAST(Prscrbr_NPI AS VARCHAR), Brnd_Name, Gnrc_Name
                FROM raw_part_d_by_provider_and_drug
                GROUP BY 1, 2, 3 HAVING count(*) > 1
            )
            """,
        ),
    )
    for label, sql in uniqueness_checks:
        duplicates = int(con.execute(sql).fetchone()[0])
        if duplicates:
            raise ValueError(
                f"Provider profile claims input violates {label} grain: "
                f"{duplicates} duplicate keys"
            )

    _ensure_serving_provider_profile_claims_tables(con)
    logger.info("Building provider profile claims serving tables (data_year=%d)", data_year)
    con.execute("DELETE FROM serving_provider_profile_top_services")
    con.execute("DELETE FROM serving_provider_profile_top_drugs")
    con.execute("DELETE FROM serving_provider_profile_claims_summary")

    con.execute(
        """
        INSERT INTO serving_provider_profile_claims_summary
        WITH panel AS (
            SELECT CAST(Rndrng_NPI AS VARCHAR) npi,
                   Tot_Benes medicare_patients,
                   Tot_Srvcs panel_total_services,
                   round(Tot_Srvcs / nullif(Tot_Benes, 0), 1) services_per_patient,
                   round(Tot_Mdcr_Alowd_Amt) medicare_allowed_amt,
                   round(Drug_Mdcr_Pymt_Amt) part_b_drug_payments,
                   Bene_Avg_Age avg_patient_age,
                   round(100.0 * (coalesce(Bene_Age_75_84_Cnt, 0)
                         + coalesce(Bene_Age_GT_84_Cnt, 0))
                         / nullif(Tot_Benes, 0)) pct_age_75_plus,
                   round(100.0 * Bene_Feml_Cnt / nullif(Tot_Benes, 0)) pct_female,
                   round(100.0 * Bene_Dual_Cnt / nullif(Tot_Benes, 0)) pct_dual_eligible,
                   Bene_Avg_Risk_Scre avg_hcc_risk_score,
                   Bene_CC_PH_Hypertension_V2_Pct pct_hypertension,
                   Bene_CC_PH_Hyperlipidemia_V2_Pct pct_hyperlipidemia,
                   Bene_CC_PH_Diabetes_V2_Pct pct_diabetes,
                   Bene_CC_PH_IschemicHeart_V2_Pct pct_ischemic_heart,
                   Bene_CC_PH_HF_NonIHD_V2_Pct pct_heart_failure,
                   Bene_CC_PH_Afib_V2_Pct pct_afib,
                   Bene_CC_PH_CKD_V2_Pct pct_ckd,
                   Bene_CC_PH_COPD_V2_Pct pct_copd,
                   Bene_CC_BH_Depress_V1_Pct pct_depression,
                   [source_data_period] source_data_periods,
                   [source_run_id] source_run_ids
            FROM raw_physician_by_provider
            WHERE Rndrng_Prvdr_Ent_Cd = 'I'
        ),
        clinical AS (
            SELECT CAST(Rndrng_NPI AS VARCHAR) npi,
                   min(Rndrng_Prvdr_Type) cms_specialty,
                   count(distinct HCPCS_Cd) distinct_codes,
                   sum(Tot_Srvcs) clinical_total_services,
                   round(sum(Tot_Srvcs * Avg_Mdcr_Pymt_Amt)) est_total_paid,
                   round(sum(Tot_Srvcs * Avg_Mdcr_Pymt_Amt)
                         FILTER (WHERE Place_Of_Srvc = 'F')
                         / nullif(sum(Tot_Srvcs * Avg_Mdcr_Pymt_Amt), 0), 2)
                       facility_paid_share,
                   round(sum(Tot_Srvcs * Avg_Mdcr_Pymt_Amt)
                         FILTER (WHERE HCPCS_Drug_Ind = 'Y')
                         / nullif(sum(Tot_Srvcs * Avg_Mdcr_Pymt_Amt), 0), 2)
                       drug_admin_paid_share,
                   round(sum(Tot_Srvcs * Avg_Mdcr_Pymt_Amt)
                         FILTER (WHERE HCPCS_Cd BETWEEN '99091' AND '99499')
                         / nullif(sum(Tot_Srvcs * Avg_Mdcr_Pymt_Amt), 0), 2)
                       em_paid_share,
                   list(distinct source_data_period order by source_data_period)
                       source_data_periods,
                   list(distinct source_run_id order by source_run_id) source_run_ids
            FROM raw_physician_by_provider_and_service
            GROUP BY 1
        ),
        prescribing AS (
            SELECT CAST(Prscrbr_NPI AS VARCHAR) npi,
                   Tot_Clms total_claims, Tot_Benes prescribing_patients,
                   round(Tot_Drug_Cst) total_cost,
                   round(Tot_Drug_Cst / nullif(Tot_Clms, 0), 2) cost_per_claim,
                   round(Brnd_Tot_Clms * 1.0
                         / nullif(Brnd_Tot_Clms + Gnrc_Tot_Clms, 0), 2)
                       brand_claim_share,
                   round(Brnd_Tot_Drug_Cst / nullif(Tot_Drug_Cst, 0), 2)
                       brand_cost_share,
                   Opioid_Prscrbr_Rate opioid_rate_pct,
                   round(LIS_Tot_Clms * 1.0 / nullif(Tot_Clms, 0), 2)
                       lis_claim_share,
                   Bene_Avg_Age rx_panel_avg_age,
                   Bene_Avg_Risk_Scre rx_panel_risk,
                   [source_data_period] source_data_periods,
                   [source_run_id] source_run_ids
            FROM raw_part_d_by_provider
        ),
        keys AS (
            SELECT npi FROM panel UNION SELECT npi FROM clinical
            UNION SELECT npi FROM prescribing
        )
        SELECT k.npi, p.npi IS NOT NULL,
               p.medicare_patients, p.panel_total_services,
               p.services_per_patient, p.medicare_allowed_amt,
               p.part_b_drug_payments, p.avg_patient_age, p.pct_age_75_plus,
               p.pct_female, p.pct_dual_eligible, p.avg_hcc_risk_score,
               p.pct_hypertension, p.pct_hyperlipidemia, p.pct_diabetes,
               p.pct_ischemic_heart, p.pct_heart_failure, p.pct_afib,
               p.pct_ckd, p.pct_copd, p.pct_depression,
               c.npi IS NOT NULL, c.cms_specialty,
               coalesce(c.distinct_codes, 0), c.clinical_total_services,
               c.est_total_paid, c.facility_paid_share,
               c.drug_admin_paid_share, c.em_paid_share,
               r.npi IS NOT NULL, r.total_claims, r.prescribing_patients,
               r.total_cost, r.cost_per_claim, r.brand_claim_share,
               r.brand_cost_share, r.opioid_rate_pct, r.lis_claim_share,
               r.rx_panel_avg_age, r.rx_panel_risk,
               coalesce(p.source_data_periods, []::VARCHAR[]),
               coalesce(p.source_run_ids, []::VARCHAR[]),
               coalesce(c.source_data_periods, []::VARCHAR[]),
               coalesce(c.source_run_ids, []::VARCHAR[]),
               coalesce(r.source_data_periods, []::VARCHAR[]),
               coalesce(r.source_run_ids, []::VARCHAR[]), ?
        FROM keys k
        JOIN serving_provider_profile_headers h ON h.npi = k.npi
        LEFT JOIN panel p ON p.npi = k.npi
        LEFT JOIN clinical c ON c.npi = k.npi
        LEFT JOIN prescribing r ON r.npi = k.npi
        """,
        [data_year],
    )

    con.execute(
        """
        INSERT INTO serving_provider_profile_top_services
        WITH service AS (
            SELECT CAST(Rndrng_NPI AS VARCHAR) npi, HCPCS_Cd hcpcs,
                   CASE WHEN max(HCPCS_Drug_Ind) = 'Y' THEN 'drug_admin'
                        WHEN HCPCS_Cd BETWEEN '99091' AND '99499'
                            THEN 'evaluation_mgmt'
                        WHEN HCPCS_Cd BETWEEN '70000' AND '79999' THEN 'imaging'
                        WHEN HCPCS_Cd BETWEEN '80000' AND '89999' THEN 'lab_path'
                        WHEN HCPCS_Cd BETWEEN '90000' AND '98999'
                            THEN 'diagnostic_proc'
                        WHEN HCPCS_Cd BETWEEN '00100' AND '69999'
                            THEN 'surgical_proc'
                        ELSE 'other' END category,
                   left(min(HCPCS_Desc), 70) description,
                   sum(Tot_Srvcs) services, max(Tot_Benes) patients,
                   round(sum(Tot_Srvcs * Avg_Mdcr_Pymt_Amt)) est_paid,
                   round(coalesce(sum(Tot_Srvcs)
                         FILTER (WHERE Place_Of_Srvc = 'F'), 0)
                         / nullif(sum(Tot_Srvcs), 0), 2) facility_share,
                   list(distinct source_data_period order by source_data_period)
                       source_data_periods,
                   list(distinct source_run_id order by source_run_id) source_run_ids
            FROM raw_physician_by_provider_and_service
            GROUP BY 1, 2
        ),
        ranked AS (
            SELECT s.*,
                   round(est_paid / nullif(sum(est_paid) OVER (PARTITION BY npi), 0), 2)
                       pct_of_paid,
                   row_number() OVER (
                       PARTITION BY npi ORDER BY est_paid DESC NULLS LAST, hcpcs
                   ) service_rank
            FROM service s
        )
        SELECT r.npi, CAST(r.service_rank AS INTEGER), r.hcpcs, r.category,
               r.description, r.services, r.patients, r.est_paid,
               r.pct_of_paid, r.facility_share, r.source_data_periods,
               r.source_run_ids, ?
        FROM ranked r
        JOIN serving_provider_profile_headers h ON h.npi = r.npi
        WHERE r.service_rank <= 10
        """,
        [data_year],
    )

    con.execute(
        """
        INSERT INTO serving_provider_profile_top_drugs
        WITH drug AS (
            SELECT CAST(Prscrbr_NPI AS VARCHAR) npi, Brnd_Name brand,
                   Gnrc_Name generic, Tot_Clms claims, Tot_Benes patients,
                   round(Tot_Drug_Cst) drug_cost,
                   round(Tot_Drug_Cst / nullif(Tot_Clms, 0), 2) cost_per_claim,
                   round(Tot_Day_Suply * 1.0 / nullif(Tot_Clms, 0))
                       days_per_claim,
                   (Tot_Drug_Cst / nullif(Tot_Clms, 0)) >= 950 specialty_tier,
                   [source_data_period] source_data_periods,
                   [source_run_id] source_run_ids
            FROM raw_part_d_by_provider_and_drug
        ),
        ranked AS (
            SELECT d.*,
                   round(drug_cost / nullif(sum(drug_cost)
                         OVER (PARTITION BY npi), 0), 2) pct_of_cost,
                   row_number() OVER (
                       PARTITION BY npi
                       ORDER BY drug_cost DESC NULLS LAST,
                                coalesce(brand, ''), coalesce(generic, '')
                   ) drug_rank
            FROM drug d
        )
        SELECT r.npi, CAST(r.drug_rank AS INTEGER), r.brand, r.generic,
               r.claims, r.patients, r.drug_cost, r.cost_per_claim,
               r.days_per_claim, r.specialty_tier, r.pct_of_cost,
               r.source_data_periods, r.source_run_ids, ?
        FROM ranked r
        JOIN serving_provider_profile_headers h ON h.npi = r.npi
        WHERE r.drug_rank <= 10
        """,
        [data_year],
    )

    counts = {
        table: int(con.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
        for table in (
            "serving_provider_profile_claims_summary",
            "serving_provider_profile_top_services",
            "serving_provider_profile_top_drugs",
        )
    }
    logger.info(
        "Provider profile claims serving rows: summaries=%d services=%d drugs=%d",
        counts["serving_provider_profile_claims_summary"],
        counts["serving_provider_profile_top_services"],
        counts["serving_provider_profile_top_drugs"],
    )
    return counts


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


def _table_has_columns(
    con: duckdb.DuckDBPyConnection, table: str, columns: set[str]
) -> bool:
    """Return whether a legacy/optional table exposes the required columns."""
    try:
        available = {
            str(row[1]).casefold()
            for row in con.execute(f"PRAGMA table_info('{table}')").fetchall()
        }
    except duckdb.CatalogException:
        return False
    return all(column.casefold() in available for column in columns)


def _ensure_provider_evidence_output_tables(con: duckdb.DuckDBPyConnection) -> None:
    """Create additive source-preserving evidence outputs for older warehouses."""
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS provider_address_evidence (
            evidence_key VARCHAR(64) PRIMARY KEY,
            npi VARCHAR(10) NOT NULL REFERENCES core_providers(npi),
            address_line_1 VARCHAR(255), address_line_2 VARCHAR(255),
            city VARCHAR(100), state VARCHAR(2), zip_code VARCHAR(20),
            country VARCHAR(10), address_id VARCHAR,
            address_granularity VARCHAR(30) NOT NULL,
            relationship_type VARCHAR(100) NOT NULL,
            evidence_kind VARCHAR(40) NOT NULL,
            source_tables VARCHAR(255) NOT NULL,
            source_data_period VARCHAR, source_run_id VARCHAR,
            source_data_periods VARCHAR[] NOT NULL,
            source_run_ids VARCHAR[] NOT NULL,
            data_year INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_provider_address_evidence_npi
            ON provider_address_evidence(npi);
        CREATE INDEX IF NOT EXISTS idx_provider_address_evidence_location
            ON provider_address_evidence(state, zip_code);

        CREATE TABLE IF NOT EXISTS provider_organization_evidence (
            evidence_key VARCHAR(64) PRIMARY KEY,
            npi VARCHAR(10) NOT NULL REFERENCES core_providers(npi),
            organization_identifier_type VARCHAR(60) NOT NULL,
            organization_identifier VARCHAR(255), organization_name VARCHAR(255),
            relationship_type VARCHAR(100) NOT NULL,
            evidence_kind VARCHAR(40) NOT NULL,
            confidence_level VARCHAR(20), source_tables VARCHAR(255) NOT NULL,
            source_data_period VARCHAR, source_run_id VARCHAR,
            source_data_periods VARCHAR[] NOT NULL,
            source_run_ids VARCHAR[] NOT NULL,
            data_year INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_provider_organization_evidence_npi
            ON provider_organization_evidence(npi);
        CREATE INDEX IF NOT EXISTS idx_provider_organization_evidence_identifier
            ON provider_organization_evidence(
                organization_identifier_type, organization_identifier
            );
        """
    )
    con.execute(
        """
        ALTER TABLE provider_address_evidence ADD COLUMN IF NOT EXISTS
            source_data_periods VARCHAR[] DEFAULT [];
        ALTER TABLE provider_address_evidence ADD COLUMN IF NOT EXISTS
            source_run_ids VARCHAR[] DEFAULT [];
        ALTER TABLE provider_organization_evidence ADD COLUMN IF NOT EXISTS
            source_data_periods VARCHAR[] DEFAULT [];
        ALTER TABLE provider_organization_evidence ADD COLUMN IF NOT EXISTS
            source_run_ids VARCHAR[] DEFAULT [];
        """
    )


def build_provider_evidence_outputs(
    con: duckdb.DuckDBPyConnection, data_year: int
) -> dict[str, int]:
    """Materialize source-preserving provider address and organization evidence.

    These tables make competing provider-location and provider-organization
    records reviewable in one place. They deliberately do not select a primary
    address or organization; relationship type and evidence kind remain visible
    for every row.
    """
    logger.info("Building provider address and organization evidence (data_year=%d)", data_year)
    _ensure_provider_evidence_output_tables(con)
    con.execute("DELETE FROM provider_address_evidence")
    con.execute("DELETE FROM provider_organization_evidence")

    if _table_has_columns(
        con,
        "raw_nppes",
        {
            "npi", "practice_address_1", "practice_address_2", "practice_city",
            "practice_state", "practice_zip", "practice_country", "source_run_id",
            "source_data_period",
        },
    ):
        con.execute(
            """
            INSERT INTO provider_address_evidence
            SELECT DISTINCT
                md5(concat_ws('|', 'nppes_registered_practice_address', CAST(r.npi AS VARCHAR),
                    coalesce(r.practice_address_1, ''), coalesce(r.practice_address_2, ''),
                    coalesce(r.practice_city, ''), coalesce(r.practice_state, ''),
                    coalesce(CAST(r.practice_zip AS VARCHAR), ''), r.source_data_period)),
                CAST(r.npi AS VARCHAR), nullif(trim(r.practice_address_1), ''),
                nullif(trim(r.practice_address_2), ''), nullif(trim(r.practice_city), ''),
                upper(nullif(trim(r.practice_state), '')), nullif(trim(CAST(r.practice_zip AS VARCHAR)), ''),
                nullif(trim(r.practice_country), ''), NULL, 'street_address',
                'registered_practice_address', 'publisher_asserted', 'raw_nppes',
                r.source_data_period, r.source_run_id,
                [r.source_data_period], [r.source_run_id],
                coalesce(try_cast(left(r.source_data_period, 4) AS INTEGER), ?)
            FROM raw_nppes r
            INNER JOIN core_providers c ON c.npi = CAST(r.npi AS VARCHAR)
            WHERE nullif(trim(r.practice_address_1), '') IS NOT NULL
               OR nullif(trim(r.practice_city), '') IS NOT NULL
            """,
            [data_year],
        )

    if _table_has_columns(
        con,
        "raw_dac_national",
        {
            "NPI", "adr_ln_1", "adr_ln_2", "City/Town", "State", "ZIP Code",
            "adrs_id", "source_run_id", "source_data_period",
        },
    ):
        con.execute(
            """
            INSERT INTO provider_address_evidence
            SELECT DISTINCT
                md5(concat_ws('|', 'dac_practice_address', CAST(d."NPI" AS VARCHAR),
                    coalesce(CAST(d.adrs_id AS VARCHAR), ''), coalesce(d.adr_ln_1, ''),
                    coalesce(d.adr_ln_2, ''), coalesce(d."City/Town", ''),
                    coalesce(d."State", ''), coalesce(CAST(d."ZIP Code" AS VARCHAR), ''),
                    d.source_data_period)),
                CAST(d."NPI" AS VARCHAR), nullif(trim(d.adr_ln_1), ''),
                nullif(trim(d.adr_ln_2), ''), nullif(trim(d."City/Town"), ''),
                upper(nullif(trim(d."State"), '')), nullif(trim(CAST(d."ZIP Code" AS VARCHAR)), ''),
                NULL, nullif(trim(CAST(d.adrs_id AS VARCHAR)), ''), 'street_address',
                'clinician_practice_address', 'publisher_asserted', 'raw_dac_national',
                d.source_data_period, d.source_run_id,
                [d.source_data_period], [d.source_run_id],
                coalesce(try_cast(left(d.source_data_period, 4) AS INTEGER), ?)
            FROM raw_dac_national d
            INNER JOIN core_providers c ON c.npi = CAST(d."NPI" AS VARCHAR)
            WHERE nullif(trim(d.adr_ln_1), '') IS NOT NULL
               OR nullif(trim(d."City/Town"), '') IS NOT NULL
            """,
            [data_year],
        )

    if _table_has_columns(
        con,
        "raw_physician_by_provider",
        {
            "Rndrng_NPI", "Rndrng_Prvdr_City", "Rndrng_Prvdr_State_Abrvtn",
            "Rndrng_Prvdr_Zip5", "source_run_id", "source_data_period",
        },
    ):
        con.execute(
            """
            INSERT INTO provider_address_evidence
            SELECT DISTINCT
                md5(concat_ws('|', 'medicare_rendering_location', CAST(p.Rndrng_NPI AS VARCHAR),
                    coalesce(p.Rndrng_Prvdr_City, ''), coalesce(p.Rndrng_Prvdr_State_Abrvtn, ''),
                    coalesce(CAST(p.Rndrng_Prvdr_Zip5 AS VARCHAR), ''), p.source_data_period)),
                CAST(p.Rndrng_NPI AS VARCHAR), NULL, NULL, nullif(trim(p.Rndrng_Prvdr_City), ''),
                upper(nullif(trim(p.Rndrng_Prvdr_State_Abrvtn), '')),
                nullif(trim(CAST(p.Rndrng_Prvdr_Zip5 AS VARCHAR)), ''), NULL, NULL,
                'city_state_zip', 'medicare_rendering_location', 'publisher_asserted',
                'raw_physician_by_provider', p.source_data_period, p.source_run_id,
                [p.source_data_period], [p.source_run_id],
                coalesce(try_cast(left(p.source_data_period, 4) AS INTEGER), ?)
            FROM raw_physician_by_provider p
            INNER JOIN core_providers c ON c.npi = CAST(p.Rndrng_NPI AS VARCHAR)
            WHERE nullif(trim(p.Rndrng_Prvdr_City), '') IS NOT NULL
               OR nullif(trim(p.Rndrng_Prvdr_State_Abrvtn), '') IS NOT NULL
            """,
            [data_year],
        )

    if _table_has_columns(
        con,
        "raw_open_payments_general",
        {
            "Covered_Recipient_NPI", "Recipient_Primary_Business_Street_Address_Line1",
            "Recipient_Primary_Business_Street_Address_Line2", "Recipient_City",
            "Recipient_State", "Recipient_Zip_Code", "source_run_id", "source_data_period",
        },
    ):
        con.execute(
            """
            INSERT INTO provider_address_evidence
            SELECT DISTINCT
                md5(concat_ws('|', 'open_payments_recipient_business_address',
                    CAST(o.Covered_Recipient_NPI AS VARCHAR),
                    coalesce(o.Recipient_Primary_Business_Street_Address_Line1, ''),
                    coalesce(o.Recipient_Primary_Business_Street_Address_Line2, ''),
                    coalesce(o.Recipient_City, ''), coalesce(o.Recipient_State, ''),
                    coalesce(CAST(o.Recipient_Zip_Code AS VARCHAR), ''), o.source_data_period)),
                CAST(o.Covered_Recipient_NPI AS VARCHAR),
                nullif(trim(o.Recipient_Primary_Business_Street_Address_Line1), ''),
                nullif(trim(o.Recipient_Primary_Business_Street_Address_Line2), ''),
                nullif(trim(o.Recipient_City), ''), upper(nullif(trim(o.Recipient_State), '')),
                nullif(trim(CAST(o.Recipient_Zip_Code AS VARCHAR)), ''), NULL, NULL,
                'street_address', 'payment_recipient_business_address', 'publisher_asserted',
                'raw_open_payments_general', o.source_data_period, o.source_run_id,
                [o.source_data_period], [o.source_run_id],
                coalesce(try_cast(left(o.source_data_period, 4) AS INTEGER), ?)
            FROM raw_open_payments_general o
            INNER JOIN core_providers c ON c.npi = CAST(o.Covered_Recipient_NPI AS VARCHAR)
            WHERE nullif(trim(o.Recipient_Primary_Business_Street_Address_Line1), '') IS NOT NULL
               OR nullif(trim(o.Recipient_City), '') IS NOT NULL
            """,
            [data_year],
        )

    if _table_has_columns(
        con,
        "pecos_provider_organizations",
        {
            "npi", "receiving_enrollment_id", "source_data_period",
            "relationship_source_run_id", "enrollment_source_run_id",
        },
    ) and _table_has_columns(
        con,
        "pecos_enrollment_practice_locations",
        {
            "receiving_enrollment_id", "city", "state", "zip_code", "location_key",
            "source_data_period", "location_source_run_id", "enrollment_source_run_id",
        },
    ):
        con.execute(
            """
            INSERT INTO provider_address_evidence
            SELECT DISTINCT
                md5(concat_ws('|', 'pecos_receiving_organization_location', p.npi,
                    l.location_key, p.source_data_period)),
                p.npi, NULL, NULL, nullif(trim(l.city), ''), upper(nullif(trim(l.state), '')),
                nullif(trim(l.zip_code), ''), NULL, l.location_key, 'city_state_zip',
                'receiving_organization_published_location', 'normalized_publisher_relationship',
                'raw_pecos_reassignment + raw_pecos_practice_location', l.source_data_period,
                l.location_source_run_id,
                list_sort(list_distinct([p.source_data_period, l.source_data_period])),
                list_sort(list_distinct([
                    p.relationship_source_run_id, p.enrollment_source_run_id,
                    l.location_source_run_id, l.enrollment_source_run_id
                ])),
                coalesce(try_cast(left(l.source_data_period, 4) AS INTEGER), ?)
            FROM pecos_provider_organizations p
            INNER JOIN pecos_enrollment_practice_locations l
              ON l.receiving_enrollment_id = p.receiving_enrollment_id
            INNER JOIN core_providers c ON c.npi = p.npi
            """,
            [data_year],
        )

    if _table_has_columns(
        con,
        "raw_dac_national",
        {"NPI", "org_pac_id", "Facility Name", "source_run_id", "source_data_period"},
    ):
        con.execute(
            """
            INSERT INTO provider_organization_evidence
            SELECT DISTINCT
                md5(concat_ws('|', 'dac_organization_pac', CAST(d."NPI" AS VARCHAR),
                    coalesce(CAST(d.org_pac_id AS VARCHAR), ''), d.source_data_period)),
                CAST(d."NPI" AS VARCHAR), 'organization_pac_id',
                nullif(trim(CAST(d.org_pac_id AS VARCHAR)), ''),
                nullif(trim(d."Facility Name"), ''), 'clinician_organization_address_association',
                'publisher_asserted', NULL, 'raw_dac_national', d.source_data_period,
                d.source_run_id, [d.source_data_period], [d.source_run_id],
                coalesce(try_cast(left(d.source_data_period, 4) AS INTEGER), ?)
            FROM raw_dac_national d
            INNER JOIN core_providers c ON c.npi = CAST(d."NPI" AS VARCHAR)
            WHERE nullif(trim(CAST(d.org_pac_id AS VARCHAR)), '') IS NOT NULL
            """,
            [data_year],
        )

    if _table_has_columns(
        con,
        "raw_reassignment",
        {
            "Individual NPI", "Group PAC ID", "Group Legal Business Name",
            "source_run_id", "source_data_period",
        },
    ):
        con.execute(
            """
            INSERT INTO provider_organization_evidence
            SELECT DISTINCT
                md5(concat_ws('|', 'reassignment_group_pac', CAST(r."Individual NPI" AS VARCHAR),
                    coalesce(CAST(r."Group PAC ID" AS VARCHAR), ''), r.source_data_period)),
                CAST(r."Individual NPI" AS VARCHAR), 'group_pac_id',
                nullif(trim(CAST(r."Group PAC ID" AS VARCHAR)), ''),
                nullif(trim(r."Group Legal Business Name"), ''),
                'benefit_reassignment_to_group', 'publisher_asserted', NULL,
                'raw_reassignment', r.source_data_period, r.source_run_id,
                [r.source_data_period], [r.source_run_id],
                coalesce(try_cast(left(r.source_data_period, 4) AS INTEGER), ?)
            FROM raw_reassignment r
            INNER JOIN core_providers c ON c.npi = CAST(r."Individual NPI" AS VARCHAR)
            WHERE nullif(trim(CAST(r."Group PAC ID" AS VARCHAR)), '') IS NOT NULL
            """,
            [data_year],
        )

    if _table_has_columns(
        con,
        "pecos_provider_organizations",
        {
            "npi", "receiving_enrollment_id", "receiving_organization_name",
            "source_data_period", "relationship_source_run_id", "enrollment_source_run_id",
        },
    ):
        con.execute(
            """
            INSERT INTO provider_organization_evidence
            SELECT DISTINCT
                md5(concat_ws('|', 'pecos_receiving_enrollment', p.npi,
                    p.receiving_enrollment_id, p.source_data_period)),
                p.npi, 'pecos_receiving_enrollment_id', p.receiving_enrollment_id,
                nullif(trim(p.receiving_organization_name), ''),
                'receiving_medicare_benefits_organization',
                'normalized_publisher_relationship', NULL,
                'raw_pecos_reassignment + raw_pecos_enrollment', p.source_data_period,
                p.relationship_source_run_id, [p.source_data_period],
                list_sort(list_distinct([
                    p.relationship_source_run_id, p.enrollment_source_run_id
                ])),
                coalesce(try_cast(left(p.source_data_period, 4) AS INTEGER), ?)
            FROM pecos_provider_organizations p
            INNER JOIN core_providers c ON c.npi = p.npi
            """,
            [data_year],
        )

    if _table_has_columns(
        con,
        "raw_dac_facility_affiliations",
        {
            "NPI", "Facility Affiliations Certification Number",
            "Facility Type Certification Number",
        },
    ):
        con.execute(
            """
            INSERT INTO provider_organization_evidence
            SELECT DISTINCT
                md5(concat_ws('|', 'dac_facility_certification', CAST(f."NPI" AS VARCHAR),
                    coalesce(f."Facility Affiliations Certification Number", ''),
                    coalesce(f."Facility Type Certification Number", ''))),
                CAST(f."NPI" AS VARCHAR), 'facility_certification_number',
                nullif(trim(coalesce(f."Facility Affiliations Certification Number",
                    f."Facility Type Certification Number")), ''), NULL,
                'clinician_facility_certification_affiliation', 'publisher_asserted', NULL,
                'raw_dac_facility_affiliations', NULL, NULL,
                []::VARCHAR[], []::VARCHAR[], ?
            FROM raw_dac_facility_affiliations f
            INNER JOIN core_providers c ON c.npi = CAST(f."NPI" AS VARCHAR)
            WHERE nullif(trim(coalesce(f."Facility Affiliations Certification Number",
                f."Facility Type Certification Number")), '') IS NOT NULL
            """,
            [data_year],
        )

    if _table_has_columns(
        con,
        "provider_hospital_evidence",
        {
            "npi", "hospital_npi", "hospital_name", "evidence_method",
            "confidence_level", "source_data_period", "data_year",
        },
    ):
        con.execute(
            """
            INSERT INTO provider_organization_evidence
            SELECT DISTINCT
                md5(concat_ws('|', 'provider_hospital_evidence', h.npi, h.hospital_npi,
                    h.evidence_method, coalesce(h.source_data_period, ''),
                    CAST(h.data_year AS VARCHAR))),
                h.npi, 'hospital_npi', h.hospital_npi, nullif(trim(h.hospital_name), ''),
                'hospital_association', 'derived_or_inferred', h.confidence_level,
                'provider_hospital_evidence', h.source_data_period, NULL,
                CASE WHEN h.source_data_period IS NULL THEN []::VARCHAR[]
                    ELSE [h.source_data_period] END,
                []::VARCHAR[], h.data_year
            FROM provider_hospital_evidence h
            INNER JOIN core_providers c ON c.npi = h.npi
            """
        )

    for table in ("provider_address_evidence", "provider_organization_evidence"):
        invalid_provenance = con.execute(
            f"""
            SELECT count(*) FROM {table}
            WHERE (source_data_period IS NOT NULL
                   AND NOT list_contains(source_data_periods, source_data_period))
               OR (source_run_id IS NOT NULL
                   AND NOT list_contains(source_run_ids, source_run_id))
            """
        ).fetchone()[0]
        if invalid_provenance:
            raise ValueError(
                f"{table} has {invalid_provenance} rows whose primary source is "
                "missing from complete provenance"
            )

    counts = {
        "provider_address_evidence": int(
            con.execute("SELECT count(*) FROM provider_address_evidence").fetchone()[0]
        ),
        "provider_organization_evidence": int(
            con.execute("SELECT count(*) FROM provider_organization_evidence").fetchone()[0]
        ),
    }
    logger.info("Provider evidence output counts: %s", counts)
    return counts


def _ensure_provider_hospital_evidence_table(con: duckdb.DuckDBPyConnection) -> None:
    """Create the additive evidence layer for warehouses predating this model."""
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS provider_hospital_evidence (
            evidence_key VARCHAR(64) PRIMARY KEY,
            npi VARCHAR(10) NOT NULL REFERENCES core_providers(npi),
            hospital_npi VARCHAR(10) NOT NULL,
            hospital_ccn VARCHAR(10),
            hospital_name VARCHAR(255),
            hospital_city VARCHAR(100),
            hospital_state VARCHAR(2),
            hospital_zip VARCHAR(10),
            evidence_method VARCHAR(80) NOT NULL,
            confidence_level VARCHAR(10) NOT NULL,
            group_pac_id VARCHAR(20),
            organization_pac_id VARCHAR(20),
            dac_address_id VARCHAR,
            provider_enrollment_id VARCHAR(20),
            receiving_enrollment_id VARCHAR(20),
            source_data_period VARCHAR,
            data_year INTEGER NOT NULL
        )
        """
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_provider_hospital_evidence_npi "
        "ON provider_hospital_evidence(npi)"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_provider_hospital_evidence_hospital "
        "ON provider_hospital_evidence(hospital_npi)"
    )


def build_provider_hospital_evidence(
    con: duckdb.DuckDBPyConnection, data_year: int
) -> int:
    """Build source-preserving provider-to-hospital relationship evidence.

    This deliberately retains direct PECOS receiving-organization matches,
    existing reassignment-based inference, and optional DAC name/address campus
    evidence as separate rows.  No method asserts employment, exclusivity, or a
    primary hospital.
    """
    logger.info("Building provider_hospital_evidence (data_year=%d)", data_year)
    _ensure_provider_hospital_evidence_table(con)
    con.execute("DELETE FROM provider_hospital_evidence")

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE provider_hospital_evidence_hospitals AS
        SELECT * EXCLUDE (preferred)
        FROM (
            SELECT h.*,
                row_number() OVER (
                    PARTITION BY h.npi
                    ORDER BY
                        CASE WHEN nullif(trim(h.ccn), '') IS NOT NULL THEN 0 ELSE 1 END,
                        h.ccn NULLS LAST,
                        h.enrollment_id NULLS LAST,
                        h.address_line_1 NULLS LAST
                ) AS preferred
            FROM raw_hospital_enrollments h
        )
        WHERE preferred = 1
        """
    )

    counts: dict[str, int] = {}
    if _table_has_columns(
        con,
        "pecos_provider_organizations",
        {
            "npi",
            "provider_enrollment_id",
            "receiving_enrollment_id",
            "receiving_npi",
            "source_data_period",
        },
    ):
        con.execute(
            """
            INSERT INTO provider_hospital_evidence
            SELECT DISTINCT
                md5(concat_ws('|', 'pecos_receiving_npi_match', p.npi,
                    p.provider_enrollment_id, p.receiving_enrollment_id, h.npi,
                    p.source_data_period)),
                p.npi,
                h.npi,
                nullif(trim(h.ccn), ''),
                nullif(trim(h.organization_name), ''),
                nullif(trim(h.city), ''),
                upper(nullif(trim(h.state), '')),
                nullif(trim(h.zip_code), ''),
                'pecos_receiving_npi_match',
                'high',
                NULL,
                NULL,
                NULL,
                p.provider_enrollment_id,
                p.receiving_enrollment_id,
                p.source_data_period,
                ?
            FROM pecos_provider_organizations p
            INNER JOIN provider_hospital_evidence_hospitals h
                ON CAST(p.receiving_npi AS VARCHAR) = CAST(h.npi AS VARCHAR)
            INNER JOIN core_providers c ON c.npi = p.npi
            WHERE nullif(trim(CAST(p.receiving_npi AS VARCHAR)), '') IS NOT NULL
            """,
            [data_year],
        )
        counts["pecos_receiving_npi_match"] = int(
            con.execute(
                """
                SELECT count(*) FROM provider_hospital_evidence
                WHERE evidence_method = 'pecos_receiving_npi_match'
                """
            ).fetchone()[0]
        )

    con.execute(
        """
        INSERT INTO provider_hospital_evidence
        SELECT DISTINCT
            md5(concat_ws('|', a.affiliation_source, a.npi, a.hospital_npi,
                coalesce(a.group_pac_id, ''), CAST(a.data_year AS VARCHAR))),
            a.npi,
            a.hospital_npi,
            a.hospital_ccn,
            a.hospital_name,
            a.hospital_city,
            a.hospital_state,
            a.hospital_zip,
            a.affiliation_source,
            a.confidence_level,
            a.group_pac_id,
            NULL,
            NULL,
            NULL,
            NULL,
            CAST(a.data_year AS VARCHAR),
            a.data_year
        FROM hospital_affiliations a
        INNER JOIN core_providers c ON c.npi = a.npi
        """
    )
    counts["reassignment"] = int(
        con.execute(
            """
            SELECT count(*) FROM provider_hospital_evidence
            WHERE evidence_method IN (
                'cms_reassignment_legal_name_state',
                'cms_reassignment_dba_name_state'
            )
            """
        ).fetchone()[0]
    )

    dac_columns = {
        "NPI",
        "org_pac_id",
        "Facility Name",
        "adrs_id",
        "adr_ln_1",
        "City/Town",
        "State",
        "ZIP Code",
    }
    if _table_has_columns(con, "raw_dac_national", dac_columns):
        con.execute(
            """
            CREATE OR REPLACE TEMP TABLE provider_hospital_evidence_dac_keys AS
            WITH names AS (
                SELECT
                    h.npi AS hospital_npi,
                    h.ccn AS hospital_ccn,
                    h.organization_name AS hospital_name,
                    h.city AS hospital_city,
                    h.state AS hospital_state,
                    h.zip_code AS hospital_zip,
                    regexp_replace(upper(trim(h.organization_name)), '[^A-Z0-9]', '', 'g')
                        AS match_name,
                    regexp_replace(upper(trim(h.address_line_1)), '[^A-Z0-9]', '', 'g')
                        AS match_address,
                    upper(trim(h.city)) AS match_city,
                    upper(trim(h.state)) AS match_state,
                    left(trim(h.zip_code), 5) AS match_zip5,
                    1 AS match_priority
                FROM provider_hospital_evidence_hospitals h
                WHERE nullif(trim(h.organization_name), '') IS NOT NULL

                UNION ALL

                SELECT
                    h.npi, h.ccn, h.organization_name, h.city, h.state, h.zip_code,
                    regexp_replace(upper(trim(h.doing_business_as_name)), '[^A-Z0-9]', '', 'g'),
                    regexp_replace(upper(trim(h.address_line_1)), '[^A-Z0-9]', '', 'g'),
                    upper(trim(h.city)), upper(trim(h.state)), left(trim(h.zip_code), 5),
                    2
                FROM provider_hospital_evidence_hospitals h
                WHERE nullif(trim(h.doing_business_as_name), '') IS NOT NULL
            ),
            candidate_counts AS (
                SELECT match_name, match_address, match_city, match_state, match_zip5,
                       count(DISTINCT hospital_npi) AS hospital_count
                FROM names
                WHERE match_name <> '' AND match_address <> '' AND match_state <> ''
                GROUP BY 1, 2, 3, 4, 5
            ),
            ranked AS (
                SELECT n.*, c.hospital_count,
                       row_number() OVER (
                           PARTITION BY n.match_name, n.match_address, n.match_city,
                               n.match_state, n.match_zip5
                           ORDER BY n.match_priority, n.hospital_npi
                       ) AS preferred
                FROM names n
                INNER JOIN candidate_counts c
                    USING (match_name, match_address, match_city, match_state, match_zip5)
            )
            SELECT * EXCLUDE (preferred)
            FROM ranked
            WHERE hospital_count = 1 AND preferred = 1
            """
        )
        con.execute(
            """
            INSERT INTO provider_hospital_evidence
            SELECT DISTINCT
                md5(concat_ws('|',
                    CASE k.match_priority
                        WHEN 1 THEN 'dac_hospital_organization_name_address'
                        ELSE 'dac_hospital_dba_name_address'
                    END,
                    CAST(d."NPI" AS VARCHAR), d.org_pac_id, d.adrs_id, k.hospital_npi)),
                CAST(d."NPI" AS VARCHAR),
                k.hospital_npi,
                nullif(trim(k.hospital_ccn), ''),
                nullif(trim(k.hospital_name), ''),
                nullif(trim(k.hospital_city), ''),
                upper(nullif(trim(k.hospital_state), '')),
                nullif(trim(k.hospital_zip), ''),
                CASE k.match_priority
                    WHEN 1 THEN 'dac_hospital_organization_name_address'
                    ELSE 'dac_hospital_dba_name_address'
                END,
                'medium',
                NULL,
                nullif(trim(CAST(d.org_pac_id AS VARCHAR)), ''),
                nullif(trim(CAST(d.adrs_id AS VARCHAR)), ''),
                NULL,
                NULL,
                NULL,
                ?
            FROM raw_dac_national d
            INNER JOIN core_providers c ON c.npi = CAST(d."NPI" AS VARCHAR)
            INNER JOIN provider_hospital_evidence_dac_keys k
                ON regexp_replace(upper(trim(d."Facility Name")), '[^A-Z0-9]', '', 'g')
                    = k.match_name
               AND regexp_replace(upper(trim(d.adr_ln_1)), '[^A-Z0-9]', '', 'g')
                    = k.match_address
               AND upper(trim(d."City/Town")) = k.match_city
               AND upper(trim(d."State")) = k.match_state
               AND left(trim(CAST(d."ZIP Code" AS VARCHAR)), 5) = k.match_zip5
            WHERE nullif(trim(CAST(d.org_pac_id AS VARCHAR)), '') IS NOT NULL
              AND nullif(trim(d."Facility Name"), '') IS NOT NULL
            """,
            [data_year],
        )
        counts["dac_name_address"] = int(
            con.execute(
                """
                SELECT count(*) FROM provider_hospital_evidence
                WHERE evidence_method LIKE 'dac_hospital_%_address'
                """
            ).fetchone()[0]
        )

    count = int(
        con.execute("SELECT count(*) FROM provider_hospital_evidence").fetchone()[0]
    )
    logger.info("provider_hospital_evidence: %d rows loaded (%s)", count, counts)
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
    """Populate provider_drug_detail at NPI, brand, generic, and year grain."""
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
            coalesce(nullif(trim(d.brnd_name), ''), trim(d.gnrc_name)),
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
        GROUP BY CAST(d.prscrbr_npi AS VARCHAR),
                 coalesce(nullif(trim(d.brnd_name), ''), trim(d.gnrc_name)),
                 trim(d.gnrc_name)
        """,
        [data_year],
    )
    count = con.execute(
        "SELECT COUNT(*) FROM provider_drug_detail WHERE data_year = ?", [data_year]
    ).fetchone()[0]
    logger.info("provider_drug_detail: %d rows loaded", count)
    return count


def build_utilization_dictionaries(
    con: duckdb.DuckDBPyConnection, data_year: int
) -> dict[str, int]:
    """Build compact HCPCS and drug option dictionaries from inverted facts."""
    logger.info("Building utilization dictionaries (data_year=%d)", data_year)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS utilization_procedure_dictionary (
            hcpcs_code VARCHAR(10) NOT NULL,
            hcpcs_description VARCHAR(255),
            hcpcs_drug_ind VARCHAR(1),
            physician_count INTEGER NOT NULL,
            total_services DOUBLE NOT NULL,
            total_payments DOUBLE NOT NULL,
            data_year INTEGER NOT NULL,
            PRIMARY KEY (hcpcs_code, data_year)
        );
        CREATE INDEX IF NOT EXISTS idx_utilization_procedure_code
            ON utilization_procedure_dictionary(hcpcs_code);
        CREATE TABLE IF NOT EXISTS utilization_drug_dictionary (
            brand_name VARCHAR(255) NOT NULL,
            generic_name VARCHAR(255) NOT NULL,
            physician_count INTEGER NOT NULL,
            total_claims BIGINT NOT NULL,
            total_drug_cost DOUBLE NOT NULL,
            data_year INTEGER NOT NULL,
            PRIMARY KEY (brand_name, generic_name, data_year)
        );
        CREATE INDEX IF NOT EXISTS idx_utilization_drug_brand
            ON utilization_drug_dictionary(brand_name);
        CREATE INDEX IF NOT EXISTS idx_utilization_drug_generic
            ON utilization_drug_dictionary(generic_name);
        """
    )
    con.execute(
        "DELETE FROM utilization_procedure_dictionary WHERE data_year = ?",
        [data_year],
    )
    con.execute("DELETE FROM utilization_drug_dictionary WHERE data_year = ?", [data_year])
    con.execute(
        """
        INSERT INTO utilization_procedure_dictionary
        SELECT hcpcs_code,
               arg_max(hcpcs_description, coalesce(tot_services, 0)),
               arg_max(hcpcs_drug_ind, coalesce(tot_services, 0)),
               count(distinct npi),
               coalesce(sum(tot_services), 0),
               coalesce(sum(tot_services * avg_medicare_payment), 0),
               data_year
        FROM provider_service_detail
        WHERE data_year = ?
        GROUP BY hcpcs_code, data_year
        """,
        [data_year],
    )
    con.execute(
        """
        INSERT INTO utilization_drug_dictionary
        SELECT brand_name, generic_name, count(distinct npi),
               coalesce(sum(tot_claims), 0), coalesce(sum(tot_drug_cost), 0), data_year
        FROM provider_drug_detail
        WHERE data_year = ?
        GROUP BY brand_name, generic_name, data_year
        """,
        [data_year],
    )
    counts = {
        "utilization_procedure_dictionary": con.execute(
            "SELECT count(*) FROM utilization_procedure_dictionary WHERE data_year = ?",
            [data_year],
        ).fetchone()[0],
        "utilization_drug_dictionary": con.execute(
            "SELECT count(*) FROM utilization_drug_dictionary WHERE data_year = ?",
            [data_year],
        ).fetchone()[0],
    }
    logger.info("Utilization dictionaries built: %s", counts)
    return counts


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
    include_provider_evidence_outputs: bool = True,
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
        results["provider_hospital_evidence"] = build_provider_hospital_evidence(
            con, practice_year or data_year
        )
    if include_provider_evidence_outputs:
        results.update(
            build_provider_evidence_outputs(con, practice_year or data_year)
        )
    results["provider_quality_scores"] = build_provider_quality_scores(
        con, quality_year or data_year
    )
    results["order_referring_eligibility"] = build_order_referring_eligibility(con)

    # 3. Service detail (large table, run last)
    results["provider_service_detail"] = build_provider_service_detail(con, data_year)
    results["provider_drug_detail"] = build_provider_drug_detail(con, data_year)
    results.update(build_utilization_dictionaries(con, data_year))

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
        "provider_address_evidence",
        "provider_organization_evidence",
        "provider_hospital_evidence",
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
