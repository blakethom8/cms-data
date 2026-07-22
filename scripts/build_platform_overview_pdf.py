#!/usr/bin/env python3
"""Build the styled Provider Intelligence Data Platform overview PDF (reviewed 2026-07-22)."""

from __future__ import annotations

import argparse
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.platypus import (
    Flowable,
    KeepTogether,
    LongTable,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)


NAVY = colors.HexColor("#0B1F33")
NAVY_2 = colors.HexColor("#163A59")
TEAL = colors.HexColor("#0A7C78")
TEAL_LIGHT = colors.HexColor("#DDF4F1")
BLUE_LIGHT = colors.HexColor("#EAF1F8")
GOLD = colors.HexColor("#E5A93D")
INK = colors.HexColor("#172A3A")
MUTED = colors.HexColor("#5B6B79")
LINE = colors.HexColor("#CBD6DF")
WHITE = colors.white
PAPER = colors.HexColor("#F7F9FB")
GREEN = colors.HexColor("#257A55")
RED = colors.HexColor("#B4473A")

PAGE_WIDTH, PAGE_HEIGHT = letter
LEFT = 0.68 * inch
RIGHT = 0.68 * inch
TOP = 0.72 * inch
BOTTOM = 0.62 * inch
CONTENT_WIDTH = PAGE_WIDTH - LEFT - RIGHT


MARTS = [
    (
        "Provider identity and directory",
        "NPPES monthly/weekly; Provider; PECOS",
        "core_providers, raw_nppes",
        "Identity, specialty, taxonomy, public enrollment, and location around NPI.",
    ),
    (
        "Practices and rosters",
        "Reassignment; NPPES monthly/weekly",
        "practice_locations",
        "Group membership, provider rosters, primary sites, and geographic footprint.",
    ),
    (
        "Medicare utilization",
        "Physician Provider; Part D Provider; DMEPOS",
        "utilization_metrics",
        "Service volume, payments, beneficiary mix, Part D prescribing, and DME referrals.",
    ),
    (
        "Services and drugs",
        "Physician Service; Part D Drug",
        "provider_service_detail, provider_drug_detail",
        "Provider-level service mix and medication activity.",
    ),
    (
        "Hospital network intelligence",
        "Reassignment; Hospital Enrollments",
        "hospital_affiliations, raw_hospital_enrollments",
        "Conservatively inferred relationships with explicit source and confidence.",
    ),
    (
        "Enrollment and eligibility",
        "PECOS; Order and Referring",
        "order_referring_eligibility, PECOS fields",
        "Public Medicare enrollment presence and order/referring eligibility.",
    ),
    (
        "Quality and participation",
        "Quality Payment Program Experience",
        "provider_quality_scores",
        "Public QPP participation, practice, and performance measures.",
    ),
    (
        "Industry and research",
        "Open Payments general, research, ownership",
        "industry_relationships, kol_summary, Open Payments raw",
        "Reported general, research, and ownership relationships.",
    ),
    (
        "New Provider Radar",
        "NPPES monthly/weekly",
        "nppes_radar_provider_state/events/releases",
        "New providers, moves, taxonomy changes, reactivations, and deactivations.",
    ),
    (
        "Clinical research",
        "AACT ClinicalTrials.gov snapshot",
        "AACT PostgreSQL ctgov schema",
        "Studies, investigators, sponsors, conditions, interventions, and facilities.",
    ),
]


SOURCES = [
    ("Physician & Other Practitioners - Provider", "Annual", "Daily check / 48h", "2024"),
    ("Physician & Other Practitioners - Service", "Annual", "Daily check / 48h", "2024"),
    ("Part D Prescribers - Provider", "Annual", "Daily check / 48h", "2024"),
    ("Part D Prescribers - Drug", "Annual", "Daily check / 48h", "2024"),
    ("DMEPOS - Referring Provider", "Annual", "Daily check / 48h", "2023"),
    ("Quality Payment Program Experience", "Annual", "Daily check / 48h", "2024"),
    ("PECOS Public Provider Enrollment", "Quarterly", "Weekly check / 72h", "Q1 2026"),
    ("Order and Referring", "About twice weekly", "Daily check / 48h", "Jul 12-18, 2026"),
    ("Hospital Enrollments", "Monthly", "Daily check / 48h", "May 2026"),
    ("Revalidation Group Reassignment", "Monthly", "Daily check / 48h", "Jul 2026"),
    ("NPPES Monthly V2", "Monthly full", "Reconcile each release", "Jul 13, 2026"),
    ("NPPES Weekly Incremental V2", "Weekly", "Apply in period order", "Jul 13-19, 2026"),
    ("Open Payments General", "Annual + correction", "Weekly; daily in windows", "2025"),
    ("Open Payments Research", "Annual + correction", "Weekly; daily in windows", "2025"),
    ("Open Payments Ownership", "Annual + correction", "Weekly; daily in windows", "2025"),
    ("AACT ClinicalTrials.gov Snapshot", "Daily", "Stage when available", "Jul 21, 2026"),
]


class ArchitectureFlow(Flowable):
    """Four-layer architecture graphic with aligned arrows."""

    def __init__(self, width: float = CONTENT_WIDTH, height: float = 3.25 * inch):
        super().__init__()
        self.width = width
        self.height = height

    def draw(self) -> None:
        c = self.canv
        box_h = 0.54 * inch
        gap = 0.25 * inch
        y = self.height - box_h
        layers = [
            (NAVY_2, "PRIMARY PUBLISHERS", "CMS data.json  |  NPPES index  |  Open Payments index  |  AACT downloads"),
            (TEAL, "CONTROL PLANE", "Typed registry  |  Read-only discovery  |  Freshness and provenance status"),
            (colors.HexColor("#2D657E"), "ISOLATED STAGING", "Immutable acquisition  |  Manifests  |  Validation  |  Complete candidates"),
            (GREEN, "PRODUCTION SERVING", "Atomic release bundle  |  Authenticated read-only API  |  Provider Search"),
        ]
        for index, (fill, title, detail) in enumerate(layers):
            c.setFillColor(fill)
            c.roundRect(0, y, self.width, box_h, 8, fill=1, stroke=0)
            c.setFillColor(WHITE)
            c.setFont("Helvetica-Bold", 9)
            c.drawString(0.18 * inch, y + 0.34 * inch, title)
            c.setFont("Helvetica", 8)
            c.drawRightString(self.width - 0.18 * inch, y + 0.20 * inch, detail)
            if index < len(layers) - 1:
                x = self.width / 2
                c.setStrokeColor(GOLD)
                c.setLineWidth(2)
                c.line(x, y, x, y - gap + 0.07 * inch)
                c.setFillColor(GOLD)
                p = c.beginPath()
                p.moveTo(x, y - gap)
                p.lineTo(x - 4, y - gap + 6)
                p.lineTo(x + 4, y - gap + 6)
                p.close()
                c.drawPath(p, fill=1, stroke=0)
            y -= box_h + gap


class ReleaseGateFlow(Flowable):
    """Horizontal release gates with failure paths."""

    def __init__(self, width: float = CONTENT_WIDTH, height: float = 1.65 * inch):
        super().__init__()
        self.width = width
        self.height = height

    def draw(self) -> None:
        c = self.canv
        names = ["VERSION", "ACQUIRE", "VALIDATE", "COMPARE", "PROMOTE"]
        box_w = (self.width - 4 * 0.12 * inch) / 5
        box_h = 0.58 * inch
        y = 0.75 * inch
        for i, name in enumerate(names):
            x = i * (box_w + 0.12 * inch)
            fill = TEAL if i < 4 else GREEN
            c.setFillColor(fill)
            c.roundRect(x, y, box_w, box_h, 6, fill=1, stroke=0)
            c.setFillColor(WHITE)
            c.setFont("Helvetica-Bold", 8.5)
            c.drawCentredString(x + box_w / 2, y + 0.34 * inch, f"{i + 1}. {name}")
            if i < 4:
                c.setStrokeColor(GOLD)
                c.setLineWidth(1.6)
                start = x + box_w
                end = x + box_w + 0.12 * inch
                c.line(start, y + box_h / 2, end, y + box_h / 2)
        c.setFillColor(RED)
        c.setFont("Helvetica-Bold", 8)
        c.drawString(0, 0.36 * inch, "ANY FAILED GATE")
        c.setFillColor(MUTED)
        c.setFont("Helvetica", 8)
        c.drawString(1.15 * inch, 0.36 * inch, "retains evidence, rejects the candidate, and leaves production unchanged")
        c.setStrokeColor(LINE)
        c.line(0, 0.60 * inch, self.width, 0.60 * inch)


class AtomicBundleGraphic(Flowable):
    """One pointer selecting a complete immutable bundle."""

    def __init__(self, width: float = CONTENT_WIDTH, height: float = 2.45 * inch):
        super().__init__()
        self.width = width
        self.height = height

    def draw(self) -> None:
        c = self.canv
        pointer_w = 1.55 * inch
        bundle_x = 2.25 * inch
        bundle_w = self.width - bundle_x
        c.setFillColor(NAVY)
        c.roundRect(0, 0.85 * inch, pointer_w, 0.72 * inch, 8, fill=1, stroke=0)
        c.setFillColor(WHITE)
        c.setFont("Helvetica-Bold", 9)
        c.drawCentredString(pointer_w / 2, 1.28 * inch, "release-current")
        c.setFont("Helvetica", 7.5)
        c.drawCentredString(pointer_w / 2, 1.06 * inch, "one atomic selector")
        c.setStrokeColor(GOLD)
        c.setLineWidth(2.2)
        c.line(pointer_w, 1.21 * inch, bundle_x - 0.08 * inch, 1.21 * inch)
        c.setFillColor(PAPER)
        c.setStrokeColor(TEAL)
        c.setLineWidth(1.4)
        c.roundRect(bundle_x, 0.25 * inch, bundle_w, 1.92 * inch, 10, fill=1, stroke=1)
        c.setFillColor(TEAL)
        c.setFont("Helvetica-Bold", 9)
        c.drawString(bundle_x + 0.18 * inch, 1.87 * inch, "VERSIONED DEPLOYMENT BUNDLE")
        labels = ["CODE", "RUNTIME", "DUCKDB", "EVIDENCE"]
        inner_gap = 0.10 * inch
        inner_w = (bundle_w - 0.36 * inch - 3 * inner_gap) / 4
        for i, label in enumerate(labels):
            x = bundle_x + 0.18 * inch + i * (inner_w + inner_gap)
            c.setFillColor(WHITE)
            c.setStrokeColor(LINE)
            c.roundRect(x, 0.60 * inch, inner_w, 0.86 * inch, 5, fill=1, stroke=1)
            c.setFillColor(NAVY_2)
            c.setFont("Helvetica-Bold", 8)
            c.drawCentredString(x + inner_w / 2, 1.10 * inch, label)
            c.setFillColor(MUTED)
            c.setFont("Helvetica", 6.8)
            detail = {
                "CODE": "full commit",
                "RUNTIME": "fingerprinted",
                "DUCKDB": "checksummed",
                "EVIDENCE": "manifests + smoke",
            }[label]
            c.drawCentredString(x + inner_w / 2, 0.84 * inch, detail)
        c.setFillColor(MUTED)
        c.setFont("Helvetica-Oblique", 7.5)
        c.drawString(bundle_x + 0.18 * inch, 0.38 * inch, "Prior verified bundle remains intact for complete rollback.")


def styles() -> dict[str, ParagraphStyle]:
    base = getSampleStyleSheet()
    return {
        "cover_kicker": ParagraphStyle(
            "CoverKicker",
            parent=base["Normal"],
            fontName="Helvetica-Bold",
            fontSize=10,
            leading=12,
            textColor=GOLD,
            spaceAfter=14,
            tracking=1.2,
        ),
        "cover_title": ParagraphStyle(
            "CoverTitle",
            parent=base["Title"],
            fontName="Helvetica-Bold",
            fontSize=30,
            leading=34,
            textColor=WHITE,
            alignment=TA_LEFT,
            spaceAfter=18,
        ),
        "cover_subtitle": ParagraphStyle(
            "CoverSubtitle",
            parent=base["Normal"],
            fontName="Helvetica",
            fontSize=13,
            leading=19,
            textColor=colors.HexColor("#D9E6EF"),
            spaceAfter=22,
        ),
        "cover_note": ParagraphStyle(
            "CoverNote",
            parent=base["Normal"],
            fontName="Helvetica",
            fontSize=8.5,
            leading=12,
            textColor=colors.HexColor("#AEC2D1"),
        ),
        "h1": ParagraphStyle(
            "H1",
            parent=base["Heading1"],
            fontName="Helvetica-Bold",
            fontSize=20,
            leading=24,
            textColor=NAVY,
            spaceBefore=6,
            spaceAfter=10,
        ),
        "h2": ParagraphStyle(
            "H2",
            parent=base["Heading2"],
            fontName="Helvetica-Bold",
            fontSize=13,
            leading=16,
            textColor=TEAL,
            spaceBefore=12,
            spaceAfter=7,
        ),
        "body": ParagraphStyle(
            "Body",
            parent=base["BodyText"],
            fontName="Helvetica",
            fontSize=9.3,
            leading=13.2,
            textColor=INK,
            spaceAfter=7,
        ),
        "callout": ParagraphStyle(
            "Callout",
            parent=base["BodyText"],
            fontName="Helvetica-Bold",
            fontSize=10.5,
            leading=15,
            textColor=NAVY,
            leftIndent=12,
            rightIndent=12,
            spaceBefore=7,
            spaceAfter=9,
            borderColor=TEAL,
            borderWidth=0,
            borderPadding=8,
            backColor=TEAL_LIGHT,
        ),
        "bullet": ParagraphStyle(
            "Bullet",
            parent=base["BodyText"],
            fontName="Helvetica",
            fontSize=9,
            leading=12.5,
            textColor=INK,
            leftIndent=14,
            firstLineIndent=-7,
            bulletIndent=0,
            spaceAfter=4,
        ),
        "small": ParagraphStyle(
            "Small",
            parent=base["BodyText"],
            fontName="Helvetica",
            fontSize=7.5,
            leading=10,
            textColor=MUTED,
        ),
        "table": ParagraphStyle(
            "TableCell",
            parent=base["BodyText"],
            fontName="Helvetica",
            fontSize=7.2,
            leading=9.2,
            textColor=INK,
        ),
        "table_head": ParagraphStyle(
            "TableHead",
            parent=base["BodyText"],
            fontName="Helvetica-Bold",
            fontSize=7.2,
            leading=9,
            textColor=WHITE,
        ),
        "metric_value": ParagraphStyle(
            "MetricValue",
            parent=base["Normal"],
            fontName="Helvetica-Bold",
            fontSize=18,
            leading=20,
            textColor=WHITE,
            alignment=TA_CENTER,
        ),
        "metric_label": ParagraphStyle(
            "MetricLabel",
            parent=base["Normal"],
            fontName="Helvetica",
            fontSize=7.5,
            leading=9,
            textColor=colors.HexColor("#D9E6EF"),
            alignment=TA_CENTER,
        ),
    }


def p(text: str, style: ParagraphStyle) -> Paragraph:
    return Paragraph(text, style)


def bullets(items: list[str], style: ParagraphStyle) -> list[Paragraph]:
    return [Paragraph(f"<bullet>&bull;</bullet>{item}", style) for item in items]


def section_title(number: str, title: str, s: dict[str, ParagraphStyle]) -> list[Flowable]:
    return [p(f"{number}  {title}", s["h1"]), Spacer(1, 0.03 * inch)]


def table(
    headers: list[str],
    rows: list[tuple[str, ...]],
    widths: list[float],
    s: dict[str, ParagraphStyle],
    font_size: float = 7.2,
) -> LongTable:
    head_style = s["table_head"]
    cell_style = ParagraphStyle(
        "DynamicTable",
        parent=s["table"],
        fontSize=font_size,
        leading=font_size + 2,
    )
    data = [[p(value, head_style) for value in headers]]
    data.extend([[p(value, cell_style) for value in row] for row in rows])
    result = LongTable(data, colWidths=widths, repeatRows=1, hAlign="LEFT")
    result.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), NAVY_2),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("GRID", (0, 0), (-1, -1), 0.35, LINE),
                ("LEFTPADDING", (0, 0), (-1, -1), 5),
                ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, PAPER]),
            ]
        )
    )
    return result


def draw_cover(canvas, doc) -> None:
    canvas.saveState()
    canvas.setFillColor(NAVY)
    canvas.rect(0, 0, PAGE_WIDTH, PAGE_HEIGHT, fill=1, stroke=0)
    canvas.setFillColor(TEAL)
    canvas.rect(0, 0, 0.18 * inch, PAGE_HEIGHT, fill=1, stroke=0)
    canvas.setFillColor(GOLD)
    canvas.rect(0.18 * inch, 0, 0.045 * inch, PAGE_HEIGHT, fill=1, stroke=0)
    canvas.setFillColor(colors.HexColor("#AEC2D1"))
    canvas.setFont("Helvetica", 7.5)
    canvas.drawString(LEFT, 0.34 * inch, "CMS-DATA  |  PLATFORM OVERVIEW  |  JULY 2026")
    canvas.restoreState()


def draw_standard_page(canvas, doc) -> None:
    canvas.saveState()
    canvas.setFillColor(WHITE)
    canvas.rect(0, 0, PAGE_WIDTH, PAGE_HEIGHT, fill=1, stroke=0)
    canvas.setFillColor(NAVY)
    canvas.rect(0, PAGE_HEIGHT - 0.32 * inch, PAGE_WIDTH, 0.32 * inch, fill=1, stroke=0)
    canvas.setFillColor(WHITE)
    canvas.setFont("Helvetica-Bold", 7.5)
    canvas.drawString(LEFT, PAGE_HEIGHT - 0.21 * inch, "PROVIDER INTELLIGENCE DATA PLATFORM")
    canvas.setStrokeColor(LINE)
    canvas.line(LEFT, 0.40 * inch, PAGE_WIDTH - RIGHT, 0.40 * inch)
    canvas.setFillColor(MUTED)
    canvas.setFont("Helvetica", 7.2)
    canvas.drawString(LEFT, 0.22 * inch, "Canonical public-data plane for Provider Search")
    canvas.drawRightString(PAGE_WIDTH - RIGHT, 0.22 * inch, f"{doc.page}")
    canvas.restoreState()


def cover_story(s: dict[str, ParagraphStyle]) -> list[Flowable]:
    metric_data = [
        [
            p("16", s["metric_value"]),
            p("7.37M", s["metric_value"]),
            p("22.44M", s["metric_value"]),
            p("594,772", s["metric_value"]),
        ],
        [
            p("registered sources", s["metric_label"]),
            p("core providers", s["metric_label"]),
            p("provider-drug records", s["metric_label"]),
            p("clinical studies", s["metric_label"]),
        ],
    ]
    metrics = Table(metric_data, colWidths=[CONTENT_WIDTH / 4] * 4, rowHeights=[0.36 * inch, 0.30 * inch])
    metrics.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), NAVY_2),
                ("BOX", (0, 0), (-1, -1), 0.6, colors.HexColor("#3F6079")),
                ("INNERGRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#3F6079")),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("TOPPADDING", (0, 0), (-1, -1), 2),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
            ]
        )
    )
    return [
        Spacer(1, 0.52 * inch),
        p("PUBLIC HEALTHCARE DATA, BUILT FOR DECISIONS", s["cover_kicker"]),
        p("Provider Intelligence<br/>Data Platform", s["cover_title"]),
        p(
            "A production-grade foundation that transforms fragmented federal datasets into "
            "trustworthy provider, practice, market, industry, and clinical-research intelligence.",
            s["cover_subtitle"],
        ),
        Spacer(1, 0.22 * inch),
        metrics,
        Spacer(1, 0.45 * inch),
        p(
            "Validated production release snapshot - July 22, 2026. Every change requires explicit "
            "approval, bounded smoke validation, and retention of a complete rollback release.",
            s["cover_note"],
        ),
        PageBreak(),
    ]


def build_story(s: dict[str, ParagraphStyle]) -> list[Flowable]:
    story: list[Flowable] = cover_story(s)

    story += section_title("01", "Platform at a glance", s)
    story.append(
        p(
            "The Provider Intelligence Data Platform is the canonical public-data plane for "
            "Provider Search. It discovers official publisher releases, preserves immutable source "
            "provenance, builds complete analytical releases, and serves stable read-only API "
            "contracts to downstream products.",
            s["body"],
        )
    )
    story.append(
        p(
            "One platform combines 16 registered sources from CMS, NPPES, Open Payments, and AACT / "
            "ClinicalTrials.gov. Every release is checksum-verified, compared with the prior "
            "warehouse, rehearsed in isolation, and promoted as one reversible serving unit.",
            s["callout"],
        )
    )
    story.append(p("What it enables", s["h2"]))
    story += bullets(
        [
            "<b>Provider discovery:</b> normalized identity, specialty, taxonomy, enrollment, and location around NPI.",
            "<b>Practice intelligence:</b> group practices, provider rosters, primary sites, and market footprints.",
            "<b>Utilization intelligence:</b> Medicare volume, payments, beneficiary mix, prescribing, DME referrals, and services.",
            "<b>Network intelligence:</b> conservatively inferred hospital relationships with explicit confidence labels.",
            "<b>Industry transparency:</b> reported general, research, and ownership relationships without causal claims.",
            "<b>Research intelligence:</b> investigator evidence and daily AACT / ClinicalTrials.gov snapshots.",
            "<b>Market change detection:</b> new providers, moves, taxonomy changes, reactivations, and deactivations.",
        ],
        s["bullet"],
    )
    story.append(Spacer(1, 0.08 * inch))
    overview_rows = [
        ("Registered publisher sources", "16"),
        ("Core provider records", "7,373,208"),
        ("Practice locations", "2,341,984"),
        ("Provider-service records", "9,306,818"),
        ("Provider-drug records", "22,444,680"),
        ("Open Payments general-payment rows", "16,131,856"),
        ("Inferred hospital affiliations", "139,775"),
        ("AACT clinical studies", "594,772"),
    ]
    story.append(table(["Validated production signal", "Scale"], overview_rows, [4.8 * inch, 1.7 * inch], s, 8.2))
    story.append(p("Snapshot metrics describe the active validated July 22, 2026 release.", s["small"]))

    story.append(PageBreak())
    story += section_title("02", "Curated data marts", s)
    story.append(
        p(
            "Publisher-shaped raw tables provide auditability. Curated marts provide stable, "
            "product-ready semantics while preserving source period and release provenance. NPI is "
            "the shared provider identity key.",
            s["body"],
        )
    )
    story.append(table(
        ["Data mart", "Primary sources", "Primary tables", "Decision surface"],
        MARTS,
        [1.25 * inch, 1.55 * inch, 1.55 * inch, 2.15 * inch],
        s,
        5.9,
    ))

    story.append(PageBreak())
    story += section_title("03", "Source portfolio and cadence", s)
    story.append(
        p(
            "A schedule is a discovery opportunity, not permission to ingest. A source advances "
            "only when primary publisher metadata exposes a different, parseable release and every "
            "validation gate passes.",
            s["body"],
        )
    )
    story.append(table(["Source", "Publisher cadence", "Platform policy", "Validated period"], SOURCES, [2.50 * inch, 1.20 * inch, 1.55 * inch, 1.25 * inch], s, 6.7))

    story.append(PageBreak())
    story += section_title("04", "NPPES: three operating loops", s)
    loop_rows = [
        (
            "Weekly change detection",
            "Apply each incremental in publisher-period order; emit idempotent Radar events for new providers, moves, taxonomy changes, deactivation, and reactivation.",
        ),
        (
            "Monthly authoritative reconciliation",
            "Replace the baseline from every full V2 snapshot; reconcile all NPIs and validate prior weekly state. Weekly files never substitute for the full refresh.",
        ),
        (
            "Daily targeted verification",
            "Use the Registry API only for selected NPIs and confidence labeling. It is not a bulk source and cannot advance the installed release.",
        ),
    ]
    story.append(table(["Loop", "Operating rule"], loop_rows, [2.05 * inch, 4.45 * inch], s, 8.2))
    story.append(
        p(
            "An unexplained gap or overlap blocks the incremental chain until resolved or until the "
            "next full monthly snapshot establishes a new baseline.",
            s["callout"],
        )
    )
    story.append(p("Primary publisher metadata", s["h2"]))
    story += bullets(
        [
            "CMS machine-readable catalog: data.cms.gov/data.json",
            "NPPES downloadable-file index: download.cms.gov/nppes/NPI_Files.html",
            "Open Payments dataset index: openpaymentsdata.cms.gov/datasets",
            "AACT download index: aact.ctti-clinicaltrials.org/downloads",
        ],
        s["bullet"],
    )

    story.append(PageBreak())
    story += section_title("05", "System architecture", s)
    story.append(
        p(
            "The platform separates read-only discovery, isolated build work, and immutable serving. "
            "The API never performs a refresh and the product repository never becomes a bulk-data "
            "ingestion engine.",
            s["body"],
        )
    )
    story.append(ArchitectureFlow())
    boundary_rows = [
        ("cms-data", "Discovery, acquisition, validation, releases, rollback, and read-only API", "Product UI, workflow state, PHI"),
        ("provider-search", "Product behavior, access plans, UI, workflows, and contract checks", "Bulk ingestion or canonical warehouse builds"),
        ("cms-public-data-catalog", "Metadata reference", "Runtime ingestion or production data"),
        ("healthcare-ai", "Separate experimental/private-data work", "Canonical public-data warehouse"),
    ]
    story.append(p("Clear ownership boundaries", s["h2"]))
    story.append(table(["Component", "Owns", "Does not own"], boundary_rows, [1.25 * inch, 3.10 * inch, 2.15 * inch], s, 7.0))

    story.append(PageBreak())
    story += section_title("06", "End-to-end release process", s)
    story.append(
        p(
            "Every source and every serving release moves through explicit proof gates. Unknown, "
            "unavailable, or malformed publisher metadata can never be interpreted as current.",
            s["body"],
        )
    )
    story.append(ReleaseGateFlow())
    steps = [
        ("1. Discover", "Parse primary publisher metadata without downloading bulk files."),
        ("2. Acquire", "Write a new immutable staging run and enforce transfer, archive, and schema limits."),
        ("3. Prove", "Record URL, version, source period, timestamps, bytes, SHA-256, schema fingerprint, rows, and code commit."),
        ("4. Validate", "Check required fields, identifiers, uniqueness, row bounds, period semantics, and source-specific invariants."),
        ("5. Build", "Construct a complete new DuckDB candidate from a checksum-verified baseline; restore AACT to a versioned PostgreSQL candidate."),
        ("6. Compare", "Open baseline and candidate read-only; permit only intended table changes and require exact evidence agreement."),
        ("7. Rehearse", "Run the candidate API on a temporary loopback port and prove API plus process identity."),
        ("8. Approve", "Require an explicit promotion decision after all artifacts and rollback state are sealed."),
        ("9. Select", "Atomically replace one release-current bundle pointer; never overwrite a database file."),
        ("10. Verify", "Restart once, run the full bounded smoke suite, and automatically restore the predecessor on failure."),
    ]
    story.append(table(["Stage", "Control"], steps, [1.18 * inch, 5.32 * inch], s, 7.7))

    story.append(PageBreak())
    story += section_title("07", "Production release and rollback", s)
    story.append(
        p(
            "Production selects one immutable bundle. That bundle fixes code, Python runtime, "
            "DuckDB, and deployment evidence as a coherent unit. The active database is never "
            "modified or overwritten in place.",
            s["body"],
        )
    )
    story.append(AtomicBundleGraphic())
    story.append(p("Combined DuckDB and AACT cutover", s["h2"]))
    story.append(
        p(
            "AACT is PostgreSQL-backed and cannot share a filesystem-atomic transaction with the "
            "DuckDB bundle pointer. The safe boundary is API-stopped: a transition sentinel blocks "
            "systemd startup, the prior AACT database remains under a versioned rollback name, both "
            "selectors are verified before startup, and failure restores both data systems before "
            "the API returns.",
            s["body"],
        )
    )
    story.append(p("Failure posture", s["h2"]))
    story += bullets(
        [
            "Incomplete pointer transitions block startup instead of serving a mixed release.",
            "Smoke evidence is deployment-specific and time-bounded; stale evidence cannot verify a new release.",
            "Rollback restores the complete predecessor: code, runtime, DuckDB, AACT snapshot, marker, and source evidence.",
            "The untouched predecessor remains available after a successful promotion.",
        ],
        s["bullet"],
    )

    story.append(PageBreak())
    story += section_title("08", "Read-only serving contracts", s)
    api_rows = [
        ("Provider profiles and search", "Identity, specialty, taxonomy, public enrollment, utilization, and enriched profiles"),
        ("Practice intelligence", "Specialty capabilities, practice search, rosters, site profiles, and market snapshots"),
        ("Industry relationships", "Open Payments search, normalized options, company summaries, and provider detail"),
        ("Research evidence", "Investigator matching and research-payment evidence"),
        ("Clinical trials", "Exact AACT version identity and study search"),
        ("New Provider Radar", "Market-change events with source release and effective date"),
        ("Explorer and matching", "Curated catalog, bounded samples, unified search, and entity matching"),
    ]
    story.append(table(["Contract family", "Representative capabilities"], api_rows, [2.15 * inch, 4.35 * inch], s, 8.0))
    story.append(p("Production smoke contract", s["h2"]))
    story.append(
        p(
            "Every promotion proves process code, runtime, and database identity; health and "
            "authentication; Provider Search practice and profile contracts; Open Payments; "
            "research and clinical trials; required tables; and exact expected row counts.",
            s["callout"],
        )
    )
    story.append(p("Public data, private boundaries", s["h2"]))
    story.append(
        p(
            "The warehouse is a public-data plane. Private customer claims, PHI, uploaded client "
            "datasets, and product workflow state require separate storage, access controls, "
            "retention policies, and contractual review.",
            s["body"],
        )
    )

    story.append(PageBreak())
    story += section_title("09", "Trust, interpretation, and attribution", s)
    story += bullets(
        [
            "<b>Primary publishers only.</b> Releases come from official CMS, NPPES, Open Payments, and AACT metadata. Dated archive URLs are never guessed.",
            "<b>Unknown stays unknown.</b> File modification time is not provenance, and missing evidence cannot be promoted to current by inference.",
            "<b>Immutable lineage.</b> Source identity, version, period, timestamps, URL, bytes, hash, schema fingerprint, rows, code commit, validation, and promotion state are recorded.",
            "<b>Conservative affiliations.</b> Hospital relationships are inferred only when normalized hospital name and state identify one hospital NPI; ambiguous keys are excluded.",
            "<b>No production writes from the API.</b> Refreshes happen in staging and serving remains read-only.",
        ],
        s["bullet"],
    )
    story.append(p("Data-use guardrails", s["h2"]))
    guardrail_rows = [
        ("CMS and NPPES", "Retain attribution; do not imply government endorsement. An NPI does not validate licensure or credentials."),
        ("Open Payments", "Describe reported transfers of value; do not imply endorsement, causation, or misconduct."),
        ("ClinicalTrials.gov / AACT", "Attribute sources, show processing date, disclose modifications, and do not market to registry email addresses."),
        ("HCPCS Level I", "Commercial exposure remains blocked until an appropriate AMA license or approved filter is confirmed."),
    ]
    story.append(table(["Source", "Required interpretation"], guardrail_rows, [1.70 * inch, 4.80 * inch], s, 7.8))
    story.append(p("Official metadata endpoints", s["h2"]))
    story += bullets(
        [
            "https://data.cms.gov/data.json",
            "https://download.cms.gov/nppes/NPI_Files.html",
            "https://openpaymentsdata.cms.gov/datasets",
            "https://aact.ctti-clinicaltrials.org/downloads",
        ],
        s["bullet"],
    )
    story.append(Spacer(1, 0.12 * inch))
    story.append(
        p(
            "Architectural and operating overview. Snapshot metrics and source periods reflect the "
            "active validated July 22, 2026 production release. Last reviewed July 22, 2026.",
            s["small"],
        )
    )
    return story


def build_pdf(output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    doc = SimpleDocTemplate(
        str(output),
        pagesize=letter,
        rightMargin=RIGHT,
        leftMargin=LEFT,
        topMargin=TOP,
        bottomMargin=BOTTOM,
        title="Provider Intelligence Data Platform",
        author="CMS Data Platform",
        subject="Sources, data marts, update cadence, process flows, and production architecture",
        creator="cms-data",
    )
    s = styles()
    doc.build(build_story(s), onFirstPage=draw_cover, onLaterPages=draw_standard_page)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("output/pdf/cms-data-platform-overview.pdf"),
        help="Destination PDF path",
    )
    args = parser.parse_args()
    build_pdf(args.output)
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
