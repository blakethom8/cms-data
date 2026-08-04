"""Consumer contract invariants for practice-shaped responses.

The downstream consumer validates every practice-shaped response before it will
trust it, and fails closed with a 502 when a check does not hold. Those checks
are *semantic*, not structural: counts must agree with each other, `site_id`
must match its own derivation from the site's address, and scope fields must
echo what the request asked for. A response can be perfectly well-typed and
still violate every one of them.

This repository already produces those properties and already tests them by
example — one fixture asserting `returned_count == 1`. What was missing is any
assertion that they hold *universally*. A change that breaks an invariant in an
untested case therefore ships green here and surfaces as a consumer 502 in
production.

This module closes that gap: it re-implements the consumer's checks as
assertions and runs them over a matrix of request shapes, so the break lands in
this suite instead. The operating model's comparison gate already requires the
consumer contract suite to pass before promotion; these are those contracts,
expressed where the responses are produced.

**Keep in sync with the consumer's validators.** When a rule changes there, it
changes here. When a rule is added here, the consumer is the reason.
"""

from __future__ import annotations

import math

import pytest

from practices import CONTRACT_VERSION, METRIC_SCOPE

# The rich warehouse fixture is shared deliberately: these invariants should be
# checked against the same data the behavioural tests use, not a parallel one
# that could drift into agreeing with a bug.
from test_primary_locations import _client, _database

POPULATION_SCOPE = "selected_specialties"
SITE_CLASSIFICATIONS = {"solo", "shared_unaffiliated", "organization_context"}
ORGANIZATION_SCOPE_FOR_BASIS = {
    "nppes_primary": "nppes_primary_address",
    "cms_enrollment": "cms_address_pac",
}
# The consumer allows this slack when re-deriving distance, so we must stay
# inside it rather than merely inside the requested radius.
RADIUS_TOLERANCE_MILES = 0.5


def site_id_for(
    location_basis: str, street: str, zip_code: str, org_pac_id: str | None
) -> str:
    """Re-derive a site's public identity the way the consumer does."""

    normalized_street = " ".join(street.upper().split())
    zip5 = zip_code[:5]
    if location_basis == "nppes_primary":
        return f"nppes_primary:{normalized_street}|{zip5}"
    pac = (org_pac_id or "").strip()
    pac_or_solo = pac if pac and pac.casefold() != "solo" else "SOLO"
    return f"cms_enrollment:{normalized_street}|{zip5}|{pac_or_solo}"


def haversine_miles(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lng = math.radians(lng2 - lng1)
    a = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lng / 2) ** 2
    )
    return 3958.7613 * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _is_int(value: object) -> bool:
    """Ints only — a bool passes isinstance(x, int) and must not count."""

    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def assert_scope_envelope(
    payload: dict,
    *,
    requested_specialties: list[str],
    location_basis: str,
    where: str,
) -> None:
    """Every practice-shaped payload carries the same scope envelope."""

    assert payload.get("contract_version") == CONTRACT_VERSION, (
        f"{where}: contract_version must stay {CONTRACT_VERSION}; the consumer "
        "rejects anything else with a 502"
    )
    assert payload.get("population_scope") == POPULATION_SCOPE, f"{where}: population_scope"
    assert payload.get("metric_scope") == METRIC_SCOPE, f"{where}: metric_scope"
    assert payload.get("location_basis") == location_basis, f"{where}: location_basis"

    echoed = payload.get("requested_specialties")
    assert isinstance(echoed, list), f"{where}: requested_specialties must be a list"
    assert [str(value).casefold() for value in echoed] == [
        value.casefold() for value in requested_specialties
    ], f"{where}: requested_specialties must echo the request"


def assert_site_identity(row: dict, *, location_basis: str, where: str) -> None:
    """Address fields are well-formed and site_id matches its own derivation."""

    address = row.get("address")
    city = row.get("city")
    state = row.get("state")
    zip5 = row.get("zip5")

    assert isinstance(address, str) and address.strip(), f"{where}: address"
    assert isinstance(city, str) and city.strip(), f"{where}: city"
    assert (
        isinstance(state, str) and len(state.strip()) == 2 and state.strip().isalpha()
    ), f"{where}: state must be two letters"
    assert (
        isinstance(zip5, str) and len(zip5) == 5 and zip5.isdigit()
    ), f"{where}: zip5 must be five digits"

    org_pac_id = row.get("org_pac_id")
    expected = site_id_for(
        location_basis,
        address,
        zip5,
        org_pac_id if isinstance(org_pac_id, str) else None,
    )
    assert row.get("site_id") == expected, (
        f"{where}: site_id must equal its derivation from address/zip/org_pac_id. "
        "The consumer recomputes this independently; a mismatch is a 502."
    )


def assert_roster_composition(row: dict, *, location_basis: str, where: str) -> None:
    """Roster counts, organization contexts, and classification agree."""

    providers_here = row.get("providers_here")
    roster_npi_count = row.get("roster_npi_count")
    unaffiliated = row.get("unaffiliated_provider_count")
    contexts = row.get("organization_contexts")
    classification = row.get("site_classification")

    assert _is_int(providers_here) and providers_here > 0, f"{where}: providers_here"
    assert _is_int(roster_npi_count), f"{where}: roster_npi_count"
    assert roster_npi_count == providers_here, (
        f"{where}: roster_npi_count must equal providers_here"
    )
    assert _is_int(unaffiliated), f"{where}: unaffiliated_provider_count"
    assert 0 <= unaffiliated <= roster_npi_count, (
        f"{where}: unaffiliated_provider_count must fall within the roster"
    )
    assert isinstance(contexts, list), f"{where}: organization_contexts must be a list"
    assert contexts or unaffiliated != 0, (
        f"{where}: a site with no contexts must have unaffiliated providers"
    )
    assert row.get("organization_scope") == ORGANIZATION_SCOPE_FOR_BASIS[location_basis], (
        f"{where}: organization_scope must match the location basis"
    )
    assert classification in SITE_CLASSIFICATIONS, f"{where}: site_classification"

    context_ids = [
        context.get("org_pac_id") if isinstance(context, dict) else None
        for context in contexts
    ]
    assert all(
        isinstance(value, str) and value.strip() for value in context_ids
    ), f"{where}: every organization context needs an org_pac_id"
    assert len(set(context_ids)) == len(context_ids), (
        f"{where}: organization contexts must be unique by org_pac_id"
    )

    for context in contexts:
        affiliated = context.get("affiliated_provider_count")
        matched = context.get("primary_address_match_count")
        assert _is_int(affiliated) and 0 < affiliated <= roster_npi_count, (
            f"{where}: affiliated_provider_count must fall within the roster"
        )
        assert _is_int(matched) and 0 <= matched <= affiliated, (
            f"{where}: primary_address_match_count must fall within its context"
        )

    if classification == "solo":
        assert not contexts and unaffiliated == 1 and roster_npi_count == 1, (
            f"{where}: a solo site is exactly one unaffiliated provider"
        )
    if classification == "shared_unaffiliated":
        assert not contexts and unaffiliated == roster_npi_count and roster_npi_count > 1, (
            f"{where}: a shared_unaffiliated site is many unaffiliated providers"
        )
    if classification == "organization_context":
        assert contexts, f"{where}: an organization_context site needs contexts"
    if location_basis == "cms_enrollment":
        assert len(contexts) <= 1, (
            f"{where}: a cms_enrollment site resolves to at most one context"
        )


def assert_search_response(
    payload: dict,
    *,
    requested_specialties: list[str],
    location_basis: str,
    boundary_zips: frozenset[str] | None = None,
    origin: tuple[float, float] | None = None,
    radius_miles: float | None = None,
    where: str,
) -> None:
    """Assert every invariant the consumer checks on a practice search."""

    assert_scope_envelope(
        payload,
        requested_specialties=requested_specialties,
        location_basis=location_basis,
        where=where,
    )

    results = payload.get("results")
    total = payload.get("total")
    returned_count = payload.get("returned_count")
    truncated = payload.get("truncated")

    assert isinstance(results, list), f"{where}: results must be a list"
    assert _is_int(total) and _is_int(returned_count), f"{where}: counts must be ints"
    assert returned_count == len(results), (
        f"{where}: returned_count must equal the number of results"
    )
    assert total >= returned_count, f"{where}: total cannot be below returned_count"
    assert isinstance(truncated, bool), f"{where}: truncated must be a bool"
    assert truncated == (returned_count < total), (
        f"{where}: truncated must be exactly (returned_count < total)"
    )

    seen_site_ids: set[str] = set()
    seen_primary_locations: set[tuple[str, str, str, str]] = set()

    for index, row in enumerate(results):
        row_where = f"{where} row {index}"
        assert isinstance(row, dict), f"{row_where}: must be an object"

        assert_scope_envelope(
            row,
            requested_specialties=requested_specialties,
            location_basis=location_basis,
            where=row_where,
        )
        assert_site_identity(row, location_basis=location_basis, where=row_where)
        assert_roster_composition(row, location_basis=location_basis, where=row_where)

        site_id = row["site_id"]
        assert site_id not in seen_site_ids, f"{row_where}: duplicate site_id"
        seen_site_ids.add(site_id)

        if boundary_zips:
            assert row["zip5"] in boundary_zips, (
                f"{row_where}: result escaped the requested ZIP boundary"
            )

        if origin is not None and radius_miles is not None:
            lat = row.get("lat")
            lng = row.get("lng")
            distance = row.get("distance_miles")
            assert _is_number(lat) and -90 <= lat <= 90, f"{row_where}: lat"
            assert _is_number(lng) and -180 <= lng <= 180, f"{row_where}: lng"
            assert _is_number(distance) and distance >= 0, f"{row_where}: distance_miles"
            limit = radius_miles + RADIUS_TOLERANCE_MILES
            assert distance <= limit, f"{row_where}: reported distance escaped the radius"
            assert haversine_miles(origin[0], origin[1], lat, lng) <= limit, (
                f"{row_where}: re-derived distance escaped the radius"
            )

        if location_basis == "nppes_primary":
            key = (
                " ".join(row["address"].casefold().split()),
                " ".join(row["city"].casefold().split()),
                row["state"].strip().casefold(),
                row["zip5"],
            )
            assert key not in seen_primary_locations, (
                f"{row_where}: duplicate primary location across rows"
            )
            seen_primary_locations.add(key)


# --- The request matrix -------------------------------------------------------
#
# Example-based tests pin one response. These run the same invariants across
# boundary kinds, both location bases, several specialties, and limits chosen to
# force both the truncated and untruncated branches.

SEARCH_CASES = [
    ("city", {"city": "Denver", "state": "CO"}),
    ("state", {"state": "CO"}),
    ("single-zip", {"zip": "80202"}),
    ("multi-zip", {"zips": "80202,80203"}),
    ("radius", {"lat": 39.74, "lng": -104.99, "radius_miles": 5.0}),
    ("wide-radius", {"lat": 39.74, "lng": -104.99, "radius_miles": 50.0}),
]


@pytest.mark.parametrize("case_name,location_params", SEARCH_CASES)
@pytest.mark.parametrize("location_basis", ["cms_enrollment", "nppes_primary"])
@pytest.mark.parametrize("specialty", ["Cardiology", "Dermatology"])
@pytest.mark.parametrize("limit", [1, 50])
def test_search_holds_consumer_invariants(
    case_name: str,
    location_params: dict,
    location_basis: str,
    specialty: str,
    limit: int,
) -> None:
    client = _client(_database())
    params = {
        **location_params,
        "specialty": specialty,
        "location_basis": location_basis,
        "limit": limit,
    }
    response = client.get("/practices/search", params=params)
    assert response.status_code == 200, response.text

    boundary_zips = None
    if "zip" in location_params:
        boundary_zips = frozenset({location_params["zip"]})
    elif "zips" in location_params:
        boundary_zips = frozenset(location_params["zips"].split(","))

    origin = None
    radius = None
    if "lat" in location_params:
        origin = (location_params["lat"], location_params["lng"])
        radius = location_params["radius_miles"]

    assert_search_response(
        response.json(),
        requested_specialties=[specialty],
        location_basis=location_basis,
        boundary_zips=boundary_zips,
        origin=origin,
        radius_miles=radius,
        where=f"search[{case_name}/{location_basis}/{specialty}/limit={limit}]",
    )


@pytest.mark.parametrize("location_basis", ["cms_enrollment", "nppes_primary"])
def test_multi_specialty_search_echoes_every_requested_specialty(
    location_basis: str,
) -> None:
    client = _client(_database())
    requested = ["Cardiology", "Dermatology"]
    response = client.get(
        "/practices/search",
        params={
            "specialties": ",".join(requested),
            "state": "CO",
            "location_basis": location_basis,
        },
    )
    assert response.status_code == 200, response.text
    assert_search_response(
        response.json(),
        requested_specialties=requested,
        location_basis=location_basis,
        where=f"multi-specialty[{location_basis}]",
    )


@pytest.mark.parametrize("location_basis", ["cms_enrollment", "nppes_primary"])
def test_roster_and_profile_agree_with_the_search_row_that_produced_them(
    location_basis: str,
) -> None:
    """Cross-endpoint identity: a site keeps one identity across all three routes."""

    client = _client(_database())
    specialty = "Cardiology"
    search = client.get(
        "/practices/search",
        params={
            "specialty": specialty,
            "state": "CO",
            "location_basis": location_basis,
        },
    )
    assert search.status_code == 200, search.text
    rows = search.json()["results"]
    assert rows, "fixture must produce at least one site to follow through"

    for row in rows:
        params = {
            "street": row["address"],
            "zip": row["zip5"],
            "specialty": specialty,
            "location_basis": location_basis,
        }
        if row.get("org_pac_id"):
            params["org_pac_id"] = row["org_pac_id"]

        for route in ("/practices/providers", "/practices/site-profile"):
            response = client.get(route, params=params)
            assert response.status_code == 200, response.text
            payload = response.json()
            where = f"{route}[{location_basis}] for {row['site_id']}"

            assert_scope_envelope(
                payload,
                requested_specialties=[specialty],
                location_basis=location_basis,
                where=where,
            )
            assert payload.get("site_id") == row["site_id"], (
                f"{where}: site_id must match the search row it came from"
            )


@pytest.mark.parametrize("location_basis", ["cms_enrollment", "nppes_primary"])
def test_roster_never_reports_more_people_than_the_site_holds(
    location_basis: str,
) -> None:
    client = _client(_database())
    specialty = "Cardiology"
    search = client.get(
        "/practices/search",
        params={
            "specialty": specialty,
            "state": "CO",
            "location_basis": location_basis,
        },
    )
    rows = search.json()["results"]

    for row in rows:
        params = {
            "street": row["address"],
            "zip": row["zip5"],
            "specialty": specialty,
            "location_basis": location_basis,
        }
        if row.get("org_pac_id"):
            params["org_pac_id"] = row["org_pac_id"]
        payload = client.get("/practices/providers", params=params).json()
        where = f"roster[{location_basis}] for {row['site_id']}"

        people = payload.get("providers")
        returned_count = payload.get("returned_count")
        truncated = payload.get("truncated")
        roster_npi_count = payload.get("roster_npi_count")

        assert isinstance(people, list), f"{where}: providers must be a list"
        assert _is_int(returned_count) and returned_count == len(people), (
            f"{where}: returned_count must equal the number of providers"
        )
        assert _is_int(roster_npi_count) and roster_npi_count >= returned_count, (
            f"{where}: roster_npi_count cannot be below what was returned"
        )
        assert isinstance(truncated, bool), f"{where}: truncated must be a bool"
        assert truncated == (returned_count < roster_npi_count), (
            f"{where}: truncated must be exactly (returned_count < roster_npi_count)"
        )
        assert roster_npi_count == row["roster_npi_count"], (
            f"{where}: roster size must agree with the search row"
        )


# --- Guard against a vacuous suite -------------------------------------------


def test_invariant_checks_actually_reject_violations() -> None:
    """A suite that cannot fail proves nothing; prove each check has teeth."""

    client = _client(_database())
    response = client.get(
        "/practices/search",
        params={"specialty": "Cardiology", "state": "CO", "location_basis": "cms_enrollment"},
    )
    good = response.json()
    assert good["results"], "fixture must return rows for this guard to be meaningful"

    def check(payload: dict) -> None:
        assert_search_response(
            payload,
            requested_specialties=["Cardiology"],
            location_basis="cms_enrollment",
            where="mutation guard",
        )

    check(good)  # the unmutated payload must pass

    mutations = {
        "count disagreement": lambda p: p.update(returned_count=p["returned_count"] + 1),
        "impossible total": lambda p: p.update(total=-1),
        "wrong truncation flag": lambda p: p.update(truncated=not p["truncated"]),
        "downgraded contract": lambda p: p.update(contract_version=1),
        "scope drift": lambda p: p.update(metric_scope="site_level_totals"),
        "unechoed specialty": lambda p: p.update(requested_specialties=["Oncology"]),
        "forged site_id": lambda p: p["results"][0].update(site_id="cms_enrollment:FAKE|00000|SOLO"),
        "roster mismatch": lambda p: p["results"][0].update(
            roster_npi_count=p["results"][0]["roster_npi_count"] + 1
        ),
        "malformed state": lambda p: p["results"][0].update(state="Colorado"),
        "malformed zip": lambda p: p["results"][0].update(zip5="802"),
        "over-count unaffiliated": lambda p: p["results"][0].update(
            unaffiliated_provider_count=p["results"][0]["roster_npi_count"] + 1
        ),
    }

    for name, mutate in mutations.items():
        payload = response.json()  # a fresh copy per mutation
        mutate(payload)
        try:
            check(payload)
        except AssertionError:
            continue
        pytest.fail(f"invariant check accepted a broken payload: {name}")
