"""Name-search ranking: city must boost, never filter (state stays a scope).

CMS/NPPES city is a mailing-address value — an exact name match recorded in
Tarzana must survive a "Los Angeles" query instead of losing to worse fuzzy
names that happen to sit inside the metro city proper.
"""
import duckdb

from profiles import CRED, _search_npi, _search_nppes


def _connection() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(":memory:")
    connection.execute(
        f"""
        create table raw_dac_national (
          "NPI" varchar,
          "Provider First Name" varchar,
          "Provider Last Name" varchar,
          {CRED} varchar,
          pri_spec varchar,
          "City/Town" varchar,
          "State" varchar,
          "Facility Name" varchar
        )
        """
    )
    connection.execute(
        """
        create table raw_nppes (
          npi varchar,
          entity_type integer,
          first_name varchar,
          last_name varchar,
          credentials varchar,
          practice_city varchar,
          practice_state varchar,
          taxonomy_1 varchar
        )
        """
    )
    connection.execute(
        "create table nucc_taxonomy (taxonomy_code varchar, classification varchar, specialization varchar)"
    )
    return connection


def test_nppes_city_boosts_but_exact_name_still_wins_elsewhere() -> None:
    connection = _connection()
    connection.execute(
        "insert into raw_nppes values "
        "('1710390513', 1, 'LAUREN', 'DESTEFANO', 'MD', 'TARZANA', 'CA', null), "
        "('1154889061', 1, 'MARTINIANA', 'LAURETA', null, 'LOS ANGELES', 'CA', null)"
    )

    rows = _search_nppes(connection, ["LAUREN", "DESTEFANO"], "Los Angeles", "CA", 15)

    assert rows, "exact name match outside the queried city must not be filtered out"
    assert rows[0]["npi"] == "1710390513"


def test_nppes_city_match_breaks_score_ties() -> None:
    connection = _connection()
    connection.execute(
        "insert into raw_nppes values "
        "('1000000004', 1, 'JANE', 'SMITH', 'MD', 'TARZANA', 'CA', null), "
        "('1000000005', 1, 'JANE', 'SMITH', 'MD', 'LOS ANGELES', 'CA', null)"
    )

    rows = _search_nppes(connection, ["JANE", "SMITH"], "Los Angeles", "CA", 15)

    assert [row["npi"] for row in rows] == ["1000000005", "1000000004"]


def test_nppes_name_search_is_enriched_by_medicare() -> None:
    connection = _connection()
    connection.execute(
        "insert into raw_nppes values "
        "('1396877080', 1, 'ALICIA', 'TERANDO', 'M.D.', 'PASADENA', 'CA', '2086X0206X')"
    )
    connection.execute(
        "insert into raw_dac_national values "
        "('1396877080', 'ALICIA', 'TERANDO', 'MD', 'SURGICAL ONCOLOGY', "
        "'PASADENA', 'CA', 'CEDARS-SINAI MEDICAL CARE FOUNDATION')"
    )

    rows = _search_nppes(connection, ["ALICIA", "TERANDO"], "Los Angeles", "CA", 15)

    assert rows[0]["npi"] == "1396877080"
    assert rows[0]["specialty"] == "SURGICAL ONCOLOGY"
    assert rows[0]["group_name"] == "CEDARS-SINAI MEDICAL CARE FOUNDATION"
    assert rows[0]["source"] == "nppes + medicare"


def test_exact_npi_search_uses_nppes_source() -> None:
    connection = _connection()
    connection.execute(
        "insert into raw_nppes values "
        "('1881985521', 1, 'DUC', 'DO', 'MD', 'LOS ANGELES', 'CA', null)"
    )

    rows = _search_npi(connection, "1881985521", "CA")

    assert rows == [
        {
            "npi": "1881985521",
            "name": "DUC DO",
            "credentials": "MD",
            "specialty": None,
            "city": "LOS ANGELES",
            "state": "CA",
            "group_name": None,
            "source": "nppes",
        }
    ]


def test_exact_npi_search_uses_nppes_identity_and_medicare_enrichment() -> None:
    connection = _connection()
    connection.execute(
        "insert into raw_nppes values "
        "('1154580017', 1, 'TREVAN', 'FISCHER', 'M.D.', 'SANTA MONICA', 'CA', null)"
    )
    connection.execute(
        "insert into raw_dac_national values "
        "('1154580017', 'TREVAN', 'FISCHER', 'MD', 'GENERAL SURGERY', "
        "'LOS ANGELES', 'CA', 'CEDARS-SINAI MEDICAL CARE FOUNDATION')"
    )

    rows = _search_npi(connection, "1154580017", "CA")

    assert rows == [
        {
            "npi": "1154580017",
            "name": "TREVAN FISCHER",
            "credentials": "M.D.",
            "specialty": "GENERAL SURGERY",
            "city": "SANTA MONICA",
            "state": "CA",
            "group_name": "CEDARS-SINAI MEDICAL CARE FOUNDATION",
            "source": "nppes + medicare",
        }
    ]


def test_exact_npi_search_falls_back_to_dac_when_nppes_is_absent() -> None:
    connection = _connection()
    connection.execute(
        "insert into raw_dac_national values "
        "('1154580017', 'TREVAN', 'FISCHER', 'MD', 'GENERAL SURGERY', "
        "'LOS ANGELES', 'CA', 'CEDARS-SINAI MEDICAL CARE FOUNDATION')"
    )

    rows = _search_npi(connection, "1154580017", "CA")

    assert rows[0]["source"] == "medicare"
