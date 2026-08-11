# Provider evidence outputs

> **Last reviewed: 2026-08-11** · **Status: warehouse and reporting contract**

The provider evidence outputs make conflicting address and organization assertions reviewable
without collapsing them into a guessed primary relationship. They are curated evidence tables, not
identity-resolution tables, and they do not change NPI's role as the provider key.

## Tables and grain

`provider_address_evidence` contains one NPI/source-native address assertion. Its
`relationship_type` distinguishes registered practice, clinician practice, Medicare rendering,
payment-recipient business, and receiving-organization locations. Address precision remains
explicit in `address_granularity`; a city/state/ZIP row is not promoted to a street address.

`provider_organization_evidence` contains one NPI/source-native or explicitly derived organization
assertion. Identifiers retain their namespace, such as organization PAC ID, group PAC ID, PECOS
receiving enrollment ID, facility certification number, or hospital NPI. No row means "primary
organization."

`evidence_kind` separates publisher assertions, normalized publisher relationships, and derived or
inferred relationships. Consumers must not present those classes as equivalent proof.

## Provenance

Every row retains `source_tables`. `source_data_period` and `source_run_id` name the primary source
record for the assertion. For a direct publisher row they are that publisher artifact. For a PECOS
location join, the practice-location artifact is primary because it supplies the address; for a
PECOS organization join, the reassignment artifact is primary because it binds the clinician to
the receiving enrollment.

Multi-source assertions also retain the complete, sorted, de-duplicated contributors in
`source_data_periods` and `source_run_ids`. These arrays are authoritative when reconstructing a
join. Empty arrays are permitted only where an older derived table did not retain source-run
identity, such as facility-affiliation or normalized hospital evidence. Reporting exports preserve
the arrays as text values rather than discarding contributors.

`data_year` comes from the primary source period when its leading four characters are a year. The
release-supplied fallback is used only for sources that do not retain a parseable period.

## Refresh and validation

The outputs are rebuilt inside the staging-candidate transaction after their dependencies. Rebuilds
delete and deterministically recreate only these two curated tables; API request handlers never
write them. Evidence keys include the source relationship, NPI, source-native identifying values,
and source period so reruns are idempotent.

Full CMS candidates must contain both outputs. Production smoke counts include them, and reporting
publishes California-scoped `bridge_provider_address_evidence` and
`bridge_provider_organization_evidence` models with declared evidence-key grain and provider-orphan
checks. Promotion remains subject to the normal candidate comparison and approval-gated cutover.

## Consumer rules

- Keep rows separate unless a product explicitly explains a derived grouping.
- Display source, relationship type, evidence kind, and confidence where applicable.
- Do not infer practice ownership, employment, referral direction, or a primary organization from
  presence alone.
- Use the complete provenance arrays for audit and the scalar primary-source fields for concise
  display.
- Treat missing evidence as "not present in these sources," not proof that a relationship does not
  exist.
