"""Declared warehouse lineage used by read-only operating surfaces.

This is deliberately explicit rather than inferred from table names or SQL text.
It records the pipeline's intended dependency graph while the operations API adds
live inventory and manifest evidence to distinguish declared from observed state.
"""

from __future__ import annotations

from dataclasses import dataclass

from .candidate_sources import CMS_RAW_TABLES


@dataclass(frozen=True, slots=True)
class TransformSpec:
    """A materializing pipeline step and its table dependencies."""

    transform_id: str
    label: str
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]
    description: str


TRANSFORMS: tuple[TransformSpec, ...] = (
    TransformSpec(
        "build_core_providers",
        "Build core providers",
        ("raw_physician_by_provider", "raw_pecos_enrollment"),
        ("core_providers",),
        "Selects individual providers and enriches them with PECOS enrollment fields.",
    ),
    TransformSpec(
        "enrich_core_providers_from_nppes",
        "Enrich providers from NPPES",
        ("raw_nppes", "core_providers"),
        ("core_providers",),
        "Enriches Medicare providers and adds qualifying non-Medicare NPPIs.",
    ),
    TransformSpec(
        "build_utilization_metrics",
        "Assemble utilization metrics",
        (
            "raw_physician_by_provider",
            "raw_part_d_by_provider",
            "raw_dme_by_referring_provider",
        ),
        ("utilization_metrics",),
        "Combines Part B, Part D, and DME measures at provider and year grain.",
    ),
    TransformSpec(
        "build_practice_locations",
        "Build practice locations",
        ("raw_reassignment", "core_providers"),
        ("practice_locations",),
        "Filters reassignment records to known providers and chooses a primary location.",
    ),
    TransformSpec(
        "build_pecos_provider_organizations",
        "Build provider-organization bridge",
        ("raw_pecos_reassignment", "raw_pecos_enrollment"),
        ("pecos_provider_organizations",),
        "Builds a provider-to-receiving-organization bridge from PECOS enrollment and reassignment records.",
    ),
    TransformSpec(
        "build_pecos_enrollment_practice_locations",
        "Build enrollment-location bridge",
        ("raw_pecos_enrollment", "raw_pecos_practice_location"),
        ("pecos_enrollment_practice_locations",),
        "Resolves each receiving enrollment to its PECOS practice locations without provider fanout.",
    ),
    TransformSpec(
        "build_hospital_affiliations",
        "Build hospital affiliations",
        ("raw_reassignment", "raw_hospital_enrollments", "core_providers"),
        ("hospital_affiliations",),
        "Matches group reassignment records to hospital enrollment records.",
    ),
    TransformSpec(
        "build_provider_hospital_evidence",
        "Build provider-hospital evidence",
        (
            "hospital_affiliations",
            "pecos_provider_organizations",
            "raw_hospital_enrollments",
            "raw_dac_national",
            "core_providers",
        ),
        ("provider_hospital_evidence",),
        "Preserves PECOS, reassignment, and DAC name/address evidence as separate provider-to-hospital records.",
    ),
    TransformSpec(
        "build_provider_quality_scores",
        "Build quality scores",
        ("raw_qpp_experience", "core_providers"),
        ("provider_quality_scores",),
        "Selects one QPP quality record per known provider and program year.",
    ),
    TransformSpec(
        "build_provider_service_detail",
        "Build service detail",
        ("raw_physician_by_provider_and_service", "core_providers"),
        ("provider_service_detail",),
        "Curates provider, procedure, and place-of-service detail.",
    ),
    TransformSpec(
        "build_provider_drug_detail",
        "Build drug detail",
        ("raw_part_d_by_provider_and_drug", "core_providers"),
        ("provider_drug_detail",),
        "Aggregates drug-level prescribing detail for known providers.",
    ),
    TransformSpec(
        "build_order_referring_eligibility",
        "Build order and referring eligibility",
        ("raw_order_and_referring", "core_providers"),
        ("order_referring_eligibility",),
        "Curates a provider-level eligibility record for known providers.",
    ),
    TransformSpec(
        "build_industry_relationships",
        "Aggregate industry relationships",
        ("raw_open_payments_general",),
        ("industry_relationships",),
        "Aggregates Open Payments general transfers by provider, year, and company.",
    ),
    TransformSpec(
        "build_kol_summary",
        "Build KOL summary",
        ("industry_relationships", "core_providers"),
        ("kol_summary",),
        "Summarizes multi-year industry relationships for high-payment providers.",
    ),
    TransformSpec(
        "process_nppes_radar",
        "Process NPPES change radar",
        ("raw_nppes",),
        (
            "nppes_radar_provider_state",
            "nppes_radar_events",
            "nppes_radar_releases",
        ),
        "Compares NPPES releases and records provider-state changes and events.",
    ),
)


BRIDGE_TABLES = frozenset(
    {"pecos_provider_organizations", "pecos_enrollment_practice_locations"}
)
SUMMARY_TABLES = frozenset({"kol_summary", "nppes_radar_events", "nppes_radar_releases"})


def raw_table_for_source(source_id: str) -> str | None:
    """Return the declared raw landing table for a source when one is modeled."""

    return CMS_RAW_TABLES.get(source_id)


def table_kind(table: str) -> str:
    """Classify a materialized table for the lineage graph."""

    if table.startswith("raw_"):
        return "raw"
    if table in BRIDGE_TABLES:
        return "bridge"
    if table in SUMMARY_TABLES:
        return "summary"
    return "mart"
