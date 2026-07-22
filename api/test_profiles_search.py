"""Name-search ranking: city must boost, never filter (state stays a scope).

CMS/NPPES city is a mailing-address value — an exact name match recorded in
Tarzana must survive a "Los Angeles" query instead of losing to worse fuzzy
names that happen to sit inside the metro city proper.
"""
import duckdb

from profiles import CRED, _search_dac, _search_registry


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


def test_dac_city_mismatch_does_not_hide_exact_name_match() -> None:
    connection = _connection()
    connection.execute(
        "insert into raw_dac_national values "
        "('1710390513', 'LAUREN', 'DESTEFANO', 'MD', 'SURGICAL ONCOLOGY', "
        "'TARZANA', 'CA', 'CEDARS-SINAI MEDICAL CARE FOUNDATION')"
    )

    rows = _search_dac(connection, ["LAUREN", "DESTEFANO"], "Los Angeles", "CA", 15)

    assert [row["npi"] for row in rows] == ["1710390513"]
    assert rows[0]["city"] == "TARZANA"


def test_dac_city_match_ranks_first_among_equal_names() -> None:
    connection = _connection()
    connection.execute(
        "insert into raw_dac_national values "
        "('1000000001', 'JANE', 'SMITH', 'MD', 'CARDIOLOGY', 'TARZANA', 'CA', null), "
        "('1000000002', 'JANE', 'SMITH', 'MD', 'CARDIOLOGY', 'LOS ANGELES', 'CA', null)"
    )

    rows = _search_dac(connection, ["JANE", "SMITH"], "Los Angeles", "CA", 15)

    assert [row["npi"] for row in rows] == ["1000000002", "1000000001"]


def test_dac_state_remains_a_hard_filter() -> None:
    connection = _connection()
    connection.execute(
        "insert into raw_dac_national values "
        "('1000000003', 'JANE', 'SMITH', 'MD', 'CARDIOLOGY', 'PHOENIX', 'AZ', null)"
    )

    assert _search_dac(connection, ["JANE", "SMITH"], None, "CA", 15) == []


def test_registry_city_boosts_but_exact_name_still_wins_elsewhere() -> None:
    connection = _connection()
    connection.execute(
        "insert into raw_nppes values "
        "('1710390513', 1, 'LAUREN', 'DESTEFANO', 'MD', 'TARZANA', 'CA', null), "
        "('1154889061', 1, 'MARTINIANA', 'LAURETA', null, 'LOS ANGELES', 'CA', null)"
    )

    rows = _search_registry(connection, ["LAUREN", "DESTEFANO"], "Los Angeles", "CA", 15)

    assert rows, "exact name match outside the queried city must not be filtered out"
    assert rows[0]["npi"] == "1710390513"


def test_registry_city_match_breaks_score_ties() -> None:
    connection = _connection()
    connection.execute(
        "insert into raw_nppes values "
        "('1000000004', 1, 'JANE', 'SMITH', 'MD', 'TARZANA', 'CA', null), "
        "('1000000005', 1, 'JANE', 'SMITH', 'MD', 'LOS ANGELES', 'CA', null)"
    )

    rows = _search_registry(connection, ["JANE", "SMITH"], "Los Angeles", "CA", 15)

    assert [row["npi"] for row in rows] == ["1000000005", "1000000004"]
