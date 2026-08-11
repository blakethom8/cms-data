"""Access-lens affiliation breadth: merged groups and hospital affiliations.

Validated against the live warehouse for NPI 1154580017 (Trevan Fischer):
DAC publishes one door (Cedars-Sinai), while reassignment carries two more
groups (Providence Saint Johns, SCPMG) and DAC facility affiliations carry
two hospitals. The profile must surface all of it, with provenance.
"""
import duckdb

from profiles import _affiliation_groups, _hospital_affiliations


def _connection() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    conn.execute(
        """
        create table raw_dac_national (
          "NPI" varchar,
          org_pac_id varchar,
          "Facility Name" varchar,
          num_org_mem integer,
          adr_ln_1 varchar
        )
        """
    )
    conn.execute(
        """
        create table raw_reassignment (
          "Individual NPI" bigint,
          "Group PAC ID" varchar,
          "Group Legal Business Name" varchar,
          "Group Reassignments and Physician Assistants" bigint
        )
        """
    )
    conn.execute(
        """
        create table raw_dac_facility_affiliations (
          "NPI" bigint,
          facility_type varchar,
          "Facility Affiliations Certification Number" varchar
        )
        """
    )
    conn.execute(
        """
        create table raw_hospital_general_info (
          "Facility ID" varchar,
          "Facility Name" varchar,
          "City/Town" varchar,
          "State" varchar
        )
        """
    )
    return conn


def test_groups_merge_dac_and_reassignment_with_provenance() -> None:
    conn = _connection()
    conn.executemany(
        "insert into raw_dac_national values (?, ?, ?, ?, ?)",
        [
            ("1154580017", "0941106645", "CEDARS-SINAI MEDICAL CARE FOUNDATION", 1570, "11800 WILSHIRE BLVD"),
        ],
    )
    conn.executemany(
        "insert into raw_reassignment values (?, ?, ?, ?)",
        [
            (1154580017, "0941106645", "Cedars-Sinai Medical Care Foundation", 2066),
            (1154580017, "0840548624", "Providence Saint Johns Medical Foundation", 456),
            (1154580017, "6002729175", "Southern California Permanente Medical Group", 13886),
        ],
    )

    groups = _affiliation_groups(conn, "1154580017")

    assert [g["group_id"] for g in groups] == ["0941106645", "6002729175", "0840548624"]
    dac_backed = groups[0]
    assert dac_backed["group_name"] == "CEDARS-SINAI MEDICAL CARE FOUNDATION"  # DAC name wins
    assert dac_backed["group_size"] == 1570
    assert dac_backed["n_addresses"] == 1
    assert dac_backed["reassignment_size"] == 2066
    assert dac_backed["sources"] == "dac + reassignment"

    reassignment_only = groups[1]
    assert reassignment_only["group_name"] == "Southern California Permanente Medical Group"
    assert reassignment_only["group_size"] is None
    assert reassignment_only["n_addresses"] == 0
    assert reassignment_only["reassignment_size"] == 13886
    assert reassignment_only["sources"] == "reassignment"


def test_groups_door_bearing_rows_sort_before_larger_reassignment_only_rows() -> None:
    conn = _connection()
    conn.executemany(
        "insert into raw_dac_national values (?, ?, ?, ?, ?)",
        [
            ("1881985521", "1355248584", "THE REGENTS OF THE UNIVERSITY OF CALIFORNIA", 1375, "100 MAIN ST"),
        ],
    )
    conn.executemany(
        "insert into raw_reassignment values (?, ?, ?, ?)",
        [
            (1881985521, "1850296534", "County Of Los Angeles", 1942),
        ],
    )

    groups = _affiliation_groups(conn, "1881985521")

    # The smaller DAC-backed group leads: it is the one with a published door.
    assert [g["sources"] for g in groups] == ["dac", "reassignment"]


def test_groups_without_any_affiliation_return_empty() -> None:
    conn = _connection()
    assert _affiliation_groups(conn, "1111111111") == []


def test_hospital_affiliations_resolve_names_and_keep_unresolved_ccns() -> None:
    conn = _connection()
    conn.executemany(
        "insert into raw_dac_facility_affiliations values (?, ?, ?)",
        [
            (1154580017, "Hospital", "050290"),
            (1154580017, "Hospital", "050069"),
            (1154580017, "Dialysis facility", "552001"),
        ],
    )
    conn.executemany(
        "insert into raw_hospital_general_info values (?, ?, ?, ?)",
        [
            ("050290", "SAINT JOHN'S HEALTH CENTER", "SANTA MONICA", "CA"),
            ("050069", "PROVIDENCE ST. JOSEPH HOSPITAL", "ORANGE", "CA"),
        ],
    )

    hospitals = _hospital_affiliations(conn, "1154580017")

    assert len(hospitals) == 3
    by_ccn = {h["ccn"]: h for h in hospitals}
    assert by_ccn["050290"]["facility_name"] == "SAINT JOHN'S HEALTH CENTER"
    assert by_ccn["050290"]["city"] == "SANTA MONICA"
    assert by_ccn["050069"]["facility_name"] == "PROVIDENCE ST. JOSEPH HOSPITAL"
    # Unresolved CCN keeps its row: the affiliation is real even without a name.
    assert by_ccn["552001"]["facility_name"] is None
    assert by_ccn["552001"]["facility_type"] == "Dialysis facility"


def test_hospital_affiliations_dedupe_repeated_certification_rows() -> None:
    conn = _connection()
    conn.executemany(
        "insert into raw_dac_facility_affiliations values (?, ?, ?)",
        [
            (1154580017, "Hospital", "050290"),
            (1154580017, "Hospital", "050290"),
        ],
    )
    conn.execute(
        "insert into raw_hospital_general_info values ('050290', 'SAINT JOHN''S HEALTH CENTER', 'SANTA MONICA', 'CA')"
    )

    hospitals = _hospital_affiliations(conn, "1154580017")

    assert len(hospitals) == 1
