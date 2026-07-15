"""Open Payments Research evidence for clinician investigator discovery."""

from collections import defaultdict
from typing import Callable

from fastapi import APIRouter
from pydantic import BaseModel, Field


router = APIRouter(prefix="/research", tags=["Research Evidence"])

RESEARCH_PAYMENT_CAVEAT = (
    "Research payments are generally paid to an institution under a research agreement "
    "and do not represent personal physician compensation."
)


class InvestigatorRequest(BaseModel):
    npis: list[str] = Field(min_length=1, max_length=250)
    active_nct_ids: list[str] = Field(default_factory=list, max_length=250)


def _text(value) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _nct_values(value) -> list[str]:
    if not value:
        return []
    import re

    return sorted(set(re.findall(r"NCT\d{8}", str(value).upper())))


def aggregate_investigator_rows(
    rows: list[dict], requested_npis: list[str], active_nct_ids: list[str]
) -> list[dict]:
    """Aggregate exact NPI evidence without counting a payment twice per clinician."""
    active = {nct.upper() for nct in active_nct_ids}
    requested = set(requested_npis)
    evidence: dict[str, dict] = {}
    seen: dict[str, set[str]] = defaultdict(set)

    for index, row in enumerate(rows):
        recipient = _text(row.get("recipient_npi"))
        pi_npis = {_text(row.get(f"pi_{slot}_npi")) for slot in range(1, 6)}
        pi_npis.discard(None)
        matched = ({recipient} | pi_npis) & requested
        if not matched:
            continue
        record_key = _text(row.get("record_id")) or f"row-{index}"
        ncts = _nct_values(row.get("nct_id"))
        amount = float(row.get("amount") or 0)
        for npi in matched:
            if record_key in seen[npi]:
                continue
            seen[npi].add(record_key)
            item = evidence.setdefault(
                npi,
                {
                    "npi": npi,
                    "research_payment_count": 0,
                    "research_dollars": 0.0,
                    "study_names": set(),
                    "sponsors": set(),
                    "nct_ids": set(),
                    "active_site_nct_matches": set(),
                    "program_years": set(),
                    "source_links": set(),
                    "pi_payment_count": 0,
                    "recipient_only_payment_count": 0,
                    "studies": {},
                },
            )
            is_pi = npi in pi_npis
            item["research_payment_count"] += 1
            item["research_dollars"] += amount
            item["pi_payment_count" if is_pi else "recipient_only_payment_count"] += 1
            study = _text(row.get("study_name"))
            sponsor = _text(row.get("sponsor"))
            year = _text(row.get("program_year"))
            source_link = _text(row.get("source_link"))
            if study:
                item["study_names"].add(study)
            if sponsor:
                item["sponsors"].add(sponsor)
            if year:
                item["program_years"].add(year)
            if source_link:
                item["source_links"].add(source_link)
            item["nct_ids"].update(ncts)
            if is_pi:
                item["active_site_nct_matches"].update(active & set(ncts))
            study_key = study or (ncts[0] if ncts else record_key)
            detail = item["studies"].setdefault(
                study_key,
                {"name": study, "nct_ids": set(), "sponsor": sponsor, "source_link": source_link},
            )
            detail["nct_ids"].update(ncts)

    output = []
    for item in evidence.values():
        item["research_dollars"] = round(item["research_dollars"], 2)
        for key in (
            "study_names",
            "sponsors",
            "nct_ids",
            "active_site_nct_matches",
            "program_years",
            "source_links",
        ):
            item[key] = sorted(item[key], reverse=key == "program_years")
        item["study_count"] = len(item["studies"])
        item["studies"] = [
            {**study, "nct_ids": sorted(study["nct_ids"])} for study in item["studies"].values()
        ]
        item["evidence_level"] = (
            "current_trial_match"
            if item["active_site_nct_matches"] and item["pi_payment_count"]
            else "research_investigator"
            if item["pi_payment_count"]
            else "research_payment_evidence"
        )
        output.append(item)
    rank = {"current_trial_match": 0, "research_investigator": 1, "research_payment_evidence": 2}
    return sorted(
        output,
        key=lambda row: (
            rank[row["evidence_level"]],
            -row["research_dollars"],
            row["npi"],
        ),
    )


def get_research_router(get_conn: Callable) -> APIRouter:
    @router.post("/investigators")
    async def investigators(request: InvestigatorRequest):
        conn = get_conn()
        columns = {
            row[1]
            for row in conn.execute(
                "pragma table_info('raw_open_payments_research')"
            ).fetchall()
        }

        def choose(*names: str) -> str | None:
            return next((name for name in names if name in columns), None)

        mapping = {
            "record_id": choose("Record_ID", "record_id"),
            "recipient_npi": choose("Covered_Recipient_NPI"),
            "amount": choose("Total_Amount_of_Payment_USDollars"),
            "study_name": choose("Name_of_Study"),
            "sponsor": choose(
                "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name",
                "Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name",
            ),
            "nct_id": choose(
                "ClinicalTrials_Gov_Identifier",
                "Clinical_Trials_Gov_Identifier",
                "ClinicalTrials.gov_Identifier",
            ),
            "program_year": choose("Program_Year"),
            "source_link": choose("Research_Information_Link"),
        }
        for slot in range(1, 6):
            mapping[f"pi_{slot}_npi"] = choose(f"Principal_Investigator_{slot}_NPI")
        npi_columns = [
            column
            for key, column in mapping.items()
            if (key == "recipient_npi" or key.startswith("pi_")) and column
        ]
        if not npi_columns:
            return {
                "investigators": [],
                "source": "CMS Open Payments Research",
                "caveat": RESEARCH_PAYMENT_CAVEAT,
            }
        select = [
            f'"{column}" as "{alias}"' if column else f'null as "{alias}"'
            for alias, column in mapping.items()
        ]
        placeholders = ",".join("?" for _ in request.npis)
        where = " or ".join(
            f'cast("{column}" as varchar) in ({placeholders})'
            for column in npi_columns
        )
        params = request.npis * len(npi_columns)
        result = conn.execute(f"select {', '.join(select)} from raw_open_payments_research where {where}", params)
        names = [description[0] for description in result.description]
        rows = [dict(zip(names, row)) for row in result.fetchall()]
        return {
            "investigators": aggregate_investigator_rows(rows, request.npis, request.active_nct_ids),
            "source": "CMS Open Payments Research",
            "caveat": RESEARCH_PAYMENT_CAVEAT,
        }

    return router
