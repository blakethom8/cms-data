"""Frozen request goldens plus explicitly synthetic names for ranking edge cases."""

import json
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from market_snapshot import OrganizationSearchResponse, get_market_snapshot_router
from test_market_snapshot import _database


@pytest.fixture
def org_client():
    connection = _database()
    connection.execute('DELETE FROM raw_dac_national')
    rows = json.loads(
        (Path(__file__).parent / 'fixtures/organization_search.json').read_text()
    )['organizations']
    # All other PACs, counts, locations and provider rows are synthetic.
    rows += [
        ['9000000001', 'CEDARS SINAI MEDICAL CENTER', 900, 'CA', 'LOS ANGELES', '90048'],
        ['9000000002', 'HEALTHONE CLINIC SERVICES - HEART CARE LLC', 200, 'CO', 'DENVER', '80201'],
        ['9000000003', 'CEDARS-SINAI LLC', 1, 'CA', 'LOS ANGELES', '90048'],
        ['9000000004', 'STRASSE ΣΟΣ CLINIC', 1, 'CO', 'DENVER', '80201'],
        ['9000000005', 'Straße Σος Clinic', 1, 'CO', 'DENVER', '80201'],
        ['9000000006', 'Null name lookup', 1, 'CA', 'LOS ANGELES', '90048'],
    ]
    rows += [[f'80000000{i:02}', f'CEDARS SINAI SPECIALTY UNIT {i}', 10000,
              'CA', 'LOS ANGELES', '90048'] for i in range(25)]
    rows += [
        ['7000000001', 'ALPHA BETA EXTENDED', 1, 'CO', 'DENVER', '80201'],
        ['7000000002', 'XALPHA BETA', 10000, 'CO', 'DENVER', '80201'],
        ['7000000003', 'BETA ALPHA', 10000, 'CO', 'DENVER', '80201'],
        ['7000000004', 'ALPHA BETA LLC', 2, 'CO', 'DENVER', '80201'],
        ['7000000005', 'ALPHA BETA INC', 2, 'CO', 'DENVER', '80201'],
        ['7000000006', 'ALPHA BETA PC', 2, 'CO', 'DENVER', '80201'],
    ]
    for i, (pac, name, size, state, city, zip_code) in enumerate(rows):
        connection.execute(
            '''INSERT INTO raw_dac_national
               ("NPI", "Facility Name", org_pac_id, num_org_mem,
                adr_ln_1, "ZIP Code", "City/Town", "State")
               VALUES (?, ?, ?, ?, '1 MAIN ST', ?, ?, ?)''',
            [str(i), name, pac, size, zip_code, city, state],
        )
    connection.execute('''UPDATE raw_dac_national SET "Facility Name" = NULL
                          WHERE org_pac_id = '9000000006' ''')
    # Provider count resolves tied national sizes before the final PAC key.
    connection.execute('''INSERT INTO raw_dac_national
        SELECT * REPLACE ('extra' AS "NPI") FROM raw_dac_national
        WHERE org_pac_id = '7000000006' ''')
    app = FastAPI()
    app.include_router(get_market_snapshot_router(lambda: connection))
    with TestClient(app) as client:
        yield client
    connection.close()


def search(client, q, **params):
    response = client.get('/practices/organizations', params={'q': q, **params})
    assert response.status_code == 200, response.text
    body = response.json()
    # This API model is the existing wire shape consumed by the proxy's
    # PracticeOrganizationsResponse (which lives in the separate UI repo).
    assert OrganizationSearchResponse.model_validate(body).model_dump() == body
    assert set(body) == {'query', 'results'}
    for row in body['results']:
        assert set(row) == {'org_pac_id', 'name', 'provider_count', 'site_count',
                            'group_size_national'}
    return [row['org_pac_id'] for row in body['results']]


@pytest.mark.parametrize('query', ['Cedars-Sinai', 'cedars sinai', 'cedars',
                                  "  CEDARS/SiNaI.,'  ", 'Cedars—Sinai'])
def test_cedars_first_page_and_rank(org_client, query):
    hits = search(org_client, query, limit=20)
    assert len(hits) == 20
    assert hits[:3] == ['0941106645', '9000000001', '9000000003']
    assert search(org_client, query, limit=1) == ['0941106645']


@pytest.mark.parametrize(('query', 'pac'), [
    ('USC Care', '0446157747'),
    ('Intermountain Medical Group Denver', '0840513552'),
    ('0941106645', '0941106645'),
    ('9000000006', '9000000006'),
    ('HEALTHONE Clinic Services - Orthopedic', '5799972725'),
    ('healthone orthopedic', '5799972725'),
])
def test_goldens(org_client, query, pac):
    assert search(org_client, query) == [pac]


def test_healthone_family(org_client):
    assert search(org_client, 'HEALTHONE') == ['9000000002', '5799972725']


def test_rank_whole_word_prefix_leftovers_size_count_and_pac(org_client):
    assert search(org_client, 'alpha beta') == [
        '7000000006', '7000000004', '7000000005',
        '7000000001', '7000000003', '7000000002',
    ]


@pytest.mark.parametrize('query', ['STRASSE ΣΟΣ', 'Straße Σος', 'strasse σος'])
def test_unicode_casefold_both_sides(org_client, query):
    assert search(org_client, query) == ['9000000004', '9000000005']


@pytest.mark.parametrize('query', ['Western Orthopaedics', 'Cedars Siani',
                                  'Cedars Sinai Medcial', 'Cedars Synai',
                                  '---', '9999999999', '0944106645'])
def test_aliases_and_fuzzy_matching_are_non_goals(org_client, query):
    assert search(org_client, query) == []


def test_legal_suffixes_are_required_for_matching(org_client):
    assert search(org_client, 'cedars foundation') == ['0941106645']
    assert search(org_client, 'cedars medical center') == ['9000000001']


@pytest.mark.parametrize('geo', [{'state': 'CO'}, {'zip': '80201'},
                                 {'zips': '80201,80202'}, {'city': 'DENVER'}])
def test_geography_applies_to_name_and_pac(org_client, geo):
    assert search(org_client, 'cedars', **geo) == []
    assert search(org_client, '0941106645', **geo) == []
    assert search(org_client, 'healthone', **geo) == ['9000000002', '5799972725']


@pytest.mark.parametrize('query', ['', 'x', '  x ', 'a' * 81, 'ce%', 'ce_'])
def test_validation_unchanged(org_client, query):
    assert org_client.get('/practices/organizations', params={'q': query}).status_code == 422


def test_limit_clamping_unchanged(org_client):
    assert search(org_client, 'cedars', limit=0) == ['0941106645']
    assert len(search(org_client, 'cedars', limit=100)) == 28
