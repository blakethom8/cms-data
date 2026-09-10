# Organization typeahead token matching

`GET /practices/organizations` now normalizes punctuation and Unicode case before
requiring every query token to occur in a CMS legal name. For example,
`cedars sinai` finds `CEDARS-SINAI MEDICAL CARE FOUNDATION`. Each token may
match within a word; tokens need not be adjacent or in query order.

The route, parameters, validation, limit clamping, and response fields are
unchanged. Provider Search can consume this through its existing proxy after
the CMS API deploy; no Provider Search frontend deploy is required.

Matching, aggregation, ranking, and limiting run in DuckDB. Names are normalized
once per distinct legal name in the geography-filtered candidate set. ASCII
names use a fast lower-case path; other names use a constant Unicode case-fold
lookup generated once from Python's Unicode table. Python does not scan
warehouse rows. No serving-time warehouse writes are introduced.

For each PAC, its best matching legal-name variant determines the first three
ranking keys:

1. Every query token occurs as a whole word.
2. The normalized name starts with the first query token.
3. Fewer remaining name tokens after removing matched tokens and trailing
   suffixes: `llc`, `inc`, `pc`, `pa`, `ltd`, `foundation`, `medical group`,
   `medical center`, `medical care`. Consecutive trailing suffixes are removed.
4. Larger national group size, nulls last, then larger provider count.
5. PAC ascending.

Suffix removal affects ranking only. Counts and the displayed name retain the
existing aggregation over matching enrollment rows within the requested
geography. Exactly ten ASCII digits after normalization trigger exact PAC
lookup, including records with no name, within the same geography filters.
Punctuation-only queries return an empty successful response. Existing wildcard
rejection remains in place.

Aliases and fuzzy matching are outside this change. `Western Orthopaedics`,
`Cedars Siani`, `Cedars Sinai Medcial`, and `Cedars Synai` return no hits in the
fixture. Ordinary substring matches such as `Cedar` remain intentional token
matching, without spelling correction.

## PR validation evidence

The frozen fixture uses the four name/PAC examples supplied in the request.
Additional PACs, counts, geography, and competing names are explicitly synthetic;
these are not claimed to be a current CMS warehouse extract. There is no local
warehouse database. The actual Provider Search `PracticeOrganizationsResponse`
type is in a separate repository and could not be imported here. Tests validate
against the unchanged API response model and assert the exact wire keys; the
repository's response-shape tests also cover this endpoint.

Before/after below was measured on that fixture with `limit=20`. PACs beginning
with `8` or `9` here are synthetic, including the Medical Center name variant.

`q=Cedars-Sinai`, before:

```text
9000000003, 0944106645
```

`q=cedars sinai`, before:

```text
9000000001, 8000000000, 8000000001, 8000000010, 8000000011,
8000000012, 8000000013, 8000000014, 8000000015, 8000000016,
8000000017, 8000000018, 8000000019, 8000000002, 8000000020,
8000000021, 8000000022, 8000000023, 8000000024, 8000000003
```

Both queries, after:

```text
0944106645, 9000000001, 9000000003, 8000000000, 8000000001,
8000000002, 8000000003, 8000000004, 8000000005, 8000000006,
8000000007, 8000000008, 8000000009, 8000000010, 8000000011,
8000000012, 8000000013, 8000000014, 8000000015, 8000000016
```

Focused validation: `cd api && ../.venv/bin/python -m pytest test_organization_search.py test_market_snapshot.py -q` — 46 passed.

Full validation: `cd api && ../.venv/bin/python -m pytest -q` — 604 passed,
1 skipped; three duplicate OpenAPI operation-ID warnings in unrelated industry
routes. The current response-shape snapshot passes unchanged.
Tests cover goldens, ranking before LIMIT, punctuation, Unicode case folding,
PAC lookup, suffix matching, HEALTHONE family retrieval, geographic exclusion,
non-goals, validation, counts, and response shape.

A local in-memory smoke benchmark added one million enrollment rows with 30,000
ASCII names to the fixture. Three requests each for `cedars sinai`, `healthone`,
and `0944106645` took 74–93 ms per request through TestClient. This is synthetic
performance evidence, not a production latency guarantee.
