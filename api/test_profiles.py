import duckdb

from open_payments_profile import industry_summary


def _connection() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(":memory:")
    connection.execute(
        """
        create table raw_open_payments_general (
          Covered_Recipient_NPI varchar,
          Covered_Recipient_Profile_ID varchar,
          Program_Year integer,
          Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name varchar,
          Nature_of_Payment_or_Transfer_of_Value varchar,
          Total_Amount_of_Payment_USDollars double
        )
        """
    )
    return connection


def test_industry_summary_exposes_verified_open_payments_profile() -> None:
    connection = _connection()
    connection.executemany(
        "insert into raw_open_payments_general values (?, ?, ?, ?, ?, ?)",
        [
            ("1053572107", "92854", 2024, "Acme Pharma", "Food and Beverage", 20),
            ("1053572107", "92854", 2025, "Acme Pharma", "Consulting Fee", 5000),
        ],
    )

    summary = industry_summary(connection, "1053572107")

    assert summary["program_year"] == 2025
    assert summary["open_payments_profile_id"] == "92854"
    assert summary["open_payments_url"] == (
        "https://openpaymentsdata.cms.gov/physician/92854"
    )
    assert summary["payment_count"] == 2
    assert summary["tier_label"] == "Paid speaker/advisor"


def test_industry_summary_suppresses_ambiguous_profile_mapping() -> None:
    connection = _connection()
    connection.executemany(
        "insert into raw_open_payments_general values (?, ?, ?, ?, ?, ?)",
        [
            ("1053572107", "92854", 2025, "Acme Pharma", "Food and Beverage", 20),
            ("1053572107", "99999", 2025, "Acme Pharma", "Food and Beverage", 30),
        ],
    )

    summary = industry_summary(connection, "1053572107")

    assert summary["open_payments_profile_id"] is None
    assert summary["open_payments_url"] is None


def test_industry_summary_returns_no_link_when_no_payments_exist() -> None:
    connection = _connection()

    summary = industry_summary(connection, "1111111111")

    assert summary["payment_count"] == 0
    assert summary["program_year"] is None
    assert summary["open_payments_profile_id"] is None
    assert summary["open_payments_url"] is None
    assert summary["tier_label"] == "No industry contact"
