"""Typed registry for publisher-owned public healthcare data sources."""

from dataclasses import dataclass
from enum import Enum


class Publisher(str, Enum):
    CMS = "Centers for Medicare & Medicaid Services"
    NPPES = "CMS National Plan and Provider Enumeration System"
    OPEN_PAYMENTS = "CMS Open Payments"
    AACT = "Clinical Trials Transformation Initiative AACT"


class DiscoveryMechanism(str, Enum):
    CMS_DATA_JSON = "cms_data_json"
    CMS_DATASET_RESOURCES = "cms_dataset_resources"
    NPPES_DOWNLOAD_INDEX = "nppes_download_index"
    OPEN_PAYMENTS_DOWNLOAD_INDEX = "open_payments_download_index"
    AACT_DOWNLOADS_PAGE = "aact_downloads_page"


class Cadence(str, Enum):
    ANNUAL = "annual"
    QUARTERLY = "quarterly"
    MONTHLY = "monthly"
    TWICE_WEEKLY = "about_twice_weekly"
    MONTHLY_FULL = "monthly_full"
    WEEKLY_INCREMENTAL = "weekly_incremental"
    ANNUAL_WITH_CORRECTION = "annual_with_january_correction"
    DAILY = "daily"


@dataclass(frozen=True, slots=True)
class SourceSpec:
    source_id: str
    title: str
    publisher: Publisher
    discovery: DiscoveryMechanism
    discovery_key: str
    cadence: Cadence
    source_period_semantics: str
    downstream_tables: tuple[str, ...]
    licensing_notes: str


CMS_ATTRIBUTION = (
    "U.S. Centers for Medicare & Medicaid Services public-use data; retain source "
    "attribution and do not imply government endorsement."
)
NPPES_ATTRIBUTION = (
    "FOIA-disclosable NPPES data; retain CMS/NPPES attribution. An NPI does not "
    "validate licensure or credentials."
)
OPEN_PAYMENTS_ATTRIBUTION = (
    "CMS Open Payments reported transfers of value; retain attribution and the "
    "accuracy disclaimer, and do not imply endorsement, causation, or misconduct."
)
AACT_ATTRIBUTION = (
    "Attribute ClinicalTrials.gov and AACT, display the processing date, disclose "
    "modifications, and do not use registry email addresses for marketing."
)


SOURCE_REGISTRY: dict[str, SourceSpec] = {
    spec.source_id: spec
    for spec in (
        SourceSpec(
            "cms_physician_by_provider",
            "Medicare Physician & Other Practitioners - by Provider",
            Publisher.CMS,
            DiscoveryMechanism.CMS_DATA_JSON,
            "8889d81e-2ee7-448f-8713-f071038289b5",
            Cadence.ANNUAL,
            "Calendar-year Medicare fee-for-service utilization and beneficiary summary.",
            ("core_providers", "utilization_metrics"),
            CMS_ATTRIBUTION,
        ),
        SourceSpec(
            "cms_physician_by_provider_and_service",
            "Medicare Physician & Other Practitioners - by Provider and Service",
            Publisher.CMS,
            DiscoveryMechanism.CMS_DATA_JSON,
            "92396110-2aed-4d63-a6a2-5d6207d46a29",
            Cadence.ANNUAL,
            "Calendar-year service utilization by NPI, HCPCS code, and place of service.",
            ("provider_service_detail",),
            CMS_ATTRIBUTION
            + " HCPCS Level I descriptions are an explicit AMA-license release gate.",
        ),
        SourceSpec(
            "cms_part_d_by_provider",
            "Medicare Part D Prescribers - by Provider",
            Publisher.CMS,
            DiscoveryMechanism.CMS_DATA_JSON,
            "14d8e8a9-7e9b-4370-a044-bf97c46b4b44",
            Cadence.ANNUAL,
            "Calendar-year Part D claims, costs, and beneficiary measures by prescriber.",
            ("utilization_metrics",),
            CMS_ATTRIBUTION,
        ),
        SourceSpec(
            "cms_part_d_by_provider_and_drug",
            "Medicare Part D Prescribers - by Provider and Drug",
            Publisher.CMS,
            DiscoveryMechanism.CMS_DATA_JSON,
            "9552739e-3d05-4c1b-8eff-ecabf391e2e5",
            Cadence.ANNUAL,
            "Calendar-year Part D utilization by prescriber and drug.",
            ("provider_drug_detail",),
            CMS_ATTRIBUTION,
        ),
        SourceSpec(
            "cms_dme_by_referring_provider",
            "Medicare Durable Medical Equipment, Devices & Supplies - by Referring Provider",
            Publisher.CMS,
            DiscoveryMechanism.CMS_DATA_JSON,
            "f8603e5b-9c47-4c52-9b47-a4ef92dfada4",
            Cadence.ANNUAL,
            "Calendar-year DMEPOS referral utilization by referring NPI.",
            ("utilization_metrics",),
            CMS_ATTRIBUTION,
        ),
        SourceSpec(
            "cms_qpp_experience",
            "Quality Payment Program Experience",
            Publisher.CMS,
            DiscoveryMechanism.CMS_DATA_JSON,
            "7adb8b1b-b85c-4ed3-b314-064776e50180",
            Cadence.ANNUAL,
            "QPP performance year represented by the distribution temporal interval.",
            ("provider_quality_scores",),
            CMS_ATTRIBUTION,
        ),
        SourceSpec(
            "cms_pecos_public_provider_enrollment",
            "Medicare Fee-For-Service Public Provider Enrollment",
            Publisher.CMS,
            DiscoveryMechanism.CMS_DATA_JSON,
            "2457ea29-fc82-48b0-86ec-3b0755de7515",
            Cadence.QUARTERLY,
            "Quarter-end PECOS enrollment snapshot, not an ingestion date.",
            ("core_providers",),
            CMS_ATTRIBUTION,
        ),
        SourceSpec(
            "cms_pecos_reassignment",
            "Medicare Fee-For-Service Public Provider Enrollment - Reassignment",
            Publisher.CMS,
            DiscoveryMechanism.CMS_DATASET_RESOURCES,
            "PPEF_Reassignment_Extract_",
            Cadence.QUARTERLY,
            "Quarter-end PECOS enrollment-pair reassignment snapshot, not an ingestion date.",
            ("raw_pecos_reassignment",),
            CMS_ATTRIBUTION,
        ),
        SourceSpec(
            "cms_pecos_practice_location",
            "Medicare Fee-For-Service Public Provider Enrollment - Practice Location",
            Publisher.CMS,
            DiscoveryMechanism.CMS_DATASET_RESOURCES,
            "PPEF_Practice_Location_Extract_",
            Cadence.QUARTERLY,
            "Quarter-end PECOS enrollment practice-location snapshot, not an ingestion date.",
            ("raw_pecos_practice_location",),
            CMS_ATTRIBUTION,
        ),
        SourceSpec(
            "cms_order_and_referring",
            "Order and Referring",
            Publisher.CMS,
            DiscoveryMechanism.CMS_DATA_JSON,
            "c99b5865-1119-4436-bb80-c5af2773ea1f",
            Cadence.TWICE_WEEKLY,
            "Eligibility snapshot interval published by CMS.",
            ("order_referring_eligibility",),
            CMS_ATTRIBUTION,
        ),
        SourceSpec(
            "cms_hospital_enrollments",
            "Hospital Enrollments",
            Publisher.CMS,
            DiscoveryMechanism.CMS_DATA_JSON,
            "f6f6505c-e8b0-4d57-b258-e2b94133aaf2",
            Cadence.MONTHLY,
            "Month-end hospital enrollment snapshot.",
            ("hospital_affiliations",),
            CMS_ATTRIBUTION,
        ),
        SourceSpec(
            "cms_revalidation_group_reassignment",
            "Revalidation Clinic Group Practice Reassignment",
            Publisher.CMS,
            DiscoveryMechanism.CMS_DATA_JSON,
            "e1f1fa9a-d6b4-417e-948a-c72dead8a41c",
            Cadence.MONTHLY,
            "Month-end reassignment snapshot.",
            ("practice_locations", "hospital_affiliations"),
            CMS_ATTRIBUTION,
        ),
        SourceSpec(
            "nppes_monthly_v2",
            "NPPES Monthly Downloadable File Version 2",
            Publisher.NPPES,
            DiscoveryMechanism.NPPES_DOWNLOAD_INDEX,
            "monthly_v2",
            Cadence.MONTHLY_FULL,
            "Full NPPES V2 snapshot identified by its publisher release label.",
            (
                "raw_nppes",
                "core_providers",
                "nppes_radar_provider_state",
                "nppes_radar_events",
            ),
            NPPES_ATTRIBUTION,
        ),
        SourceSpec(
            "nppes_weekly_incremental_v2",
            "NPPES Weekly Incremental File Version 2",
            Publisher.NPPES,
            DiscoveryMechanism.NPPES_DOWNLOAD_INDEX,
            "weekly_v2",
            Cadence.WEEKLY_INCREMENTAL,
            "Inclusive start and end dates encoded in the official weekly V2 filename.",
            (
                "raw_nppes",
                "core_providers",
                "nppes_radar_provider_state",
                "nppes_radar_events",
            ),
            NPPES_ATTRIBUTION,
        ),
        SourceSpec(
            "open_payments_general",
            "Open Payments General Payments",
            Publisher.OPEN_PAYMENTS,
            DiscoveryMechanism.OPEN_PAYMENTS_DOWNLOAD_INDEX,
            "general",
            Cadence.ANNUAL_WITH_CORRECTION,
            "Program-year transactions; later publications can correct active years.",
            ("raw_open_payments_general", "industry_relationships", "kol_summary"),
            OPEN_PAYMENTS_ATTRIBUTION,
        ),
        SourceSpec(
            "open_payments_research",
            "Open Payments Research Payments",
            Publisher.OPEN_PAYMENTS,
            DiscoveryMechanism.OPEN_PAYMENTS_DOWNLOAD_INDEX,
            "research",
            Cadence.ANNUAL_WITH_CORRECTION,
            "Program-year research payments; later publications can correct active years.",
            ("raw_open_payments_research",),
            OPEN_PAYMENTS_ATTRIBUTION,
        ),
        SourceSpec(
            "open_payments_ownership",
            "Open Payments Ownership and Investment Interests",
            Publisher.OPEN_PAYMENTS,
            DiscoveryMechanism.OPEN_PAYMENTS_DOWNLOAD_INDEX,
            "ownership",
            Cadence.ANNUAL_WITH_CORRECTION,
            "Program-year ownership records; later publications can correct active years.",
            ("raw_open_payments_ownership",),
            OPEN_PAYMENTS_ATTRIBUTION,
        ),
        SourceSpec(
            "aact_clinical_trials_snapshot",
            "AACT ClinicalTrials.gov PostgreSQL Snapshot",
            Publisher.AACT,
            DiscoveryMechanism.AACT_DOWNLOADS_PAGE,
            "pgdump",
            Cadence.DAILY,
            "Daily AACT export date; distinct from individual study update dates.",
            ("aact.studies", "aact.facilities"),
            AACT_ATTRIBUTION,
        ),
    )
}


def sources_for(mechanism: DiscoveryMechanism) -> tuple[SourceSpec, ...]:
    """Return registry entries for a discovery adapter in stable ID order."""
    return tuple(
        spec
        for _, spec in sorted(SOURCE_REGISTRY.items())
        if spec.discovery == mechanism
    )
