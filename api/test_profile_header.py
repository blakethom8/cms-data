"""NPPES is profile identity; Medicare DAC supplies optional enrichment."""

import duckdb

from profiles import CRED, TELE, _profile_header


def _connection() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    conn.execute(
        """
        create table raw_nppes (
          npi varchar,
          first_name varchar,
          last_name varchar,
          credentials varchar,
          practice_city varchar,
          practice_state varchar,
          taxonomy_1 varchar
        )
        """
    )
    conn.execute(
        f"""
        create table raw_dac_national (
          "NPI" varchar,
          "Provider First Name" varchar,
          "Provider Last Name" varchar,
          {CRED} varchar,
          pri_spec varchar,
          sec_spec_all varchar,
          "City/Town" varchar,
          "State" varchar,
          Med_sch varchar,
          Grd_yr integer,
          {TELE} varchar
        )
        """
    )
    conn.execute(
        "create table nucc_taxonomy "
        "(taxonomy_code varchar, classification varchar, specialization varchar)"
    )
    return conn


def test_nppes_only_provider_has_profile_header_with_taxonomy() -> None:
    conn = _connection()
    conn.execute(
        "insert into raw_nppes values "
        "('1000000001', 'JANE', 'SMITH', 'MD', 'OAKLAND', 'CA', '207Q00000X')"
    )
    conn.execute(
        "insert into nucc_taxonomy values "
        "('207Q00000X', 'Family Medicine', '')"
    )

    header = _profile_header(conn, "1000000001")

    assert header == {
        "npi": "1000000001",
        "name": "JANE SMITH",
        "credentials": "MD",
        "specialty": "Family Medicine",
        "secondary_specialties": None,
        "city": "OAKLAND",
        "state": "CA",
        "med_school": None,
        "grad_year": None,
        "years_in_practice": None,
        "telehealth": None,
    }


def test_nppes_identity_is_enriched_by_medicare() -> None:
    conn = _connection()
    conn.execute(
        "insert into raw_nppes values "
        "('1396877080', 'ALICIA', 'TERANDO', 'M.D.', 'PASADENA', 'CA', '2086X0206X')"
    )
    conn.execute(
        "insert into raw_dac_national values "
        "('1396877080', 'ALICIA', 'TERANDO', 'MD', 'SURGICAL ONCOLOGY', "
        "'GENERAL SURGERY', 'LOS ANGELES', 'CA', 'DUKE UNIVERSITY', 2005, 'Y')"
    )

    header = _profile_header(conn, "1396877080")

    assert header is not None
    assert header["name"] == "ALICIA TERANDO"
    assert header["credentials"] == "M.D."
    assert header["city"] == "PASADENA"
    assert header["specialty"] == "SURGICAL ONCOLOGY"
    assert header["secondary_specialties"] == "GENERAL SURGERY"
    assert header["med_school"] == "DUKE UNIVERSITY"
    assert header["grad_year"] == 2005
    assert header["telehealth"] is True


def test_dac_enrichment_selects_one_deterministic_coherent_row() -> None:
    conn = _connection()
    conn.execute(
        "insert into raw_nppes values "
        "('1811967433', 'MATTHEW', 'BUDOFF', 'MD', 'LOS ANGELES', 'CA', NULL)"
    )
    conn.executemany(
        "insert into raw_dac_national values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "1811967433", "MATTHEW", "BUDOFF", "MD",
                "INTERNAL MEDICINE", "CARDIOVASCULAR DISEASE (CARDIOLOGY)",
                "MISSION VIEJO", "CA", "SECOND SCHOOL", 1991, "N",
            ),
            (
                "1811967433", "MATTHEW", "BUDOFF", "MD",
                "CARDIOVASCULAR DISEASE (CARDIOLOGY)", None,
                "LOVELAND", "CO", "FIRST SCHOOL", 1990, "Y",
            ),
        ],
    )

    header = _profile_header(conn, "1811967433")

    assert header is not None
    assert header["specialty"] == "CARDIOVASCULAR DISEASE (CARDIOLOGY)"
    assert header["secondary_specialties"] is None
    assert header["med_school"] == "FIRST SCHOOL"
    assert header["grad_year"] == 1990
    assert header["telehealth"] is True


def test_missing_nppes_and_medicare_rows_has_no_profile_header() -> None:
    conn = _connection()
    assert _profile_header(conn, "1000000002") is None
