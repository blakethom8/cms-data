"""Access-lens practice doors: DAC enrollment merged with NPPES practice address.

Each location row carries ``sources`` (``dac`` / ``nppes`` / ``dac + nppes``)
so the dossier Access tab can label provenance the same way group affiliations
already do.
"""
import duckdb

from profiles import _profile_locations


def _connection() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    conn.execute(
        """
        create table raw_dac_national (
          "NPI" varchar,
          org_pac_id varchar,
          adr_ln_1 varchar,
          adr_ln_2 varchar,
          "City/Town" varchar,
          "State" varchar,
          "ZIP Code" varchar,
          "Telephone Number" varchar
        )
        """
    )
    conn.execute(
        """
        create table raw_nppes (
          npi varchar,
          practice_address_1 varchar,
          practice_address_2 varchar,
          practice_city varchar,
          practice_state varchar,
          practice_zip varchar,
          practice_phone varchar
        )
        """
    )
    conn.execute(
        """
        create table address_geocode (
          addr_key varchar,
          lat double,
          lng double
        )
        """
    )
    return conn


def test_locations_matching_dac_and_nppes_get_combined_sources() -> None:
    conn = _connection()
    conn.execute(
        "insert into raw_dac_national values "
        "('1396877080', '0941106645', '625 S FAIR OAKS AVE', 'SUITE 100', "
        "'PASADENA', 'CA', '91105', '4243147695')"
    )
    # Same street|zip5 as DAC — also a second clinician at the door for roster.
    conn.execute(
        "insert into raw_dac_national values "
        "('1000000001', '0941106645', '625 S FAIR OAKS AVE', null, "
        "'PASADENA', 'CA', '91105', null)"
    )
    conn.execute(
        "insert into raw_nppes values "
        "('1396877080', '625 S FAIR OAKS AVE', 'SUITE 100', "
        "'PASADENA', 'CA', '91105', '4243147695')"
    )
    conn.execute(
        "insert into address_geocode values "
        "('625 S FAIR OAKS AVE|91105', 34.1, -118.1)"
    )

    locations = _profile_locations(conn, "1396877080")

    assert len(locations) == 1
    row = locations[0]
    assert row["street"] == "625 S FAIR OAKS AVE"
    assert row["zip5"] == "91105"
    assert row["sources"] == "dac + nppes"
    assert row["roster_size"] == 2
    assert row["lat"] == 34.1


def test_locations_dac_only_and_nppes_only_doors_are_both_returned() -> None:
    conn = _connection()
    conn.execute(
        "insert into raw_dac_national values "
        "('1881985521', '1355248584', '100 MAIN ST', null, "
        "'LOS ANGELES', 'CA', '90012', '2135550100')"
    )
    conn.execute(
        "insert into raw_nppes values "
        "('1881985521', '200 OTHER AVE', null, "
        "'SANTA MONICA', 'CA', '90401', '3105550199')"
    )

    locations = _profile_locations(conn, "1881985521")
    by_street = {row["street"]: row for row in locations}

    assert set(by_street) == {"100 MAIN ST", "200 OTHER AVE"}
    assert by_street["100 MAIN ST"]["sources"] == "dac"
    assert by_street["100 MAIN ST"]["roster_size"] == 1
    assert by_street["200 OTHER AVE"]["sources"] == "nppes"
    assert by_street["200 OTHER AVE"]["roster_size"] is None
    assert by_street["200 OTHER AVE"]["likely_flagship"] is None


def test_locations_without_any_address_return_empty() -> None:
    conn = _connection()
    conn.execute(
        "insert into raw_dac_national values "
        "('1111111111', '123', ' ', '', 'LOS ANGELES', 'CA', '90012', null)"
    )
    conn.execute(
        "insert into raw_nppes values "
        "('1111111111', '', ' ', 'LOS ANGELES', 'CA', '90012', null)"
    )
    assert _profile_locations(conn, "1111111111") == []


def test_locations_choose_deterministic_values_and_suite_order() -> None:
    conn = _connection()
    conn.executemany(
        "insert into raw_dac_national values (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("1234567890", "200", "10 MAIN ST", "SUITE B", "B CITY", "CA", "90001", "222"),
            ("1234567890", "100", "10 MAIN ST", "SUITE A", "A CITY", "CA", "90001", "111"),
        ],
    )

    row = _profile_locations(conn, "1234567890")[0]

    assert row["suites"] == ["SUITE A", "SUITE B"]
    assert row["city"] == "A CITY"
    assert row["phone"] == "111"
