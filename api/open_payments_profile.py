"""Open Payments profile aggregates and official CMS source-link mapping."""

import logging
import re

logger = logging.getLogger(__name__)

OPEN_PAYMENTS_PROFILE_BASE_URL = "https://openpaymentsdata.cms.gov/physician"


def _row(connection, sql: str, params: list) -> dict:
    cursor = connection.execute(sql, params)
    columns = [description[0] for description in cursor.description]
    result = cursor.fetchone()
    return dict(zip(columns, result)) if result else {}


def industry_summary(connection, npi: str) -> dict:
    """Return Open Payments totals plus the verified official CMS profile mapping."""
    summary = _row(connection, """
        select count(*) payment_count,
               round(sum(Total_Amount_of_Payment_USDollars)) total_usd,
               round(sum(case when Nature_of_Payment_or_Transfer_of_Value <> 'Food and Beverage'
                         then Total_Amount_of_Payment_USDollars else 0 end)) nonfood_usd,
               round(sum(case when Nature_of_Payment_or_Transfer_of_Value in
                                   ('Consulting Fee','Honoraria')
                           or Nature_of_Payment_or_Transfer_of_Value like 'Compensation for serv%'
                         then Total_Amount_of_Payment_USDollars else 0 end))
                   consulting_speaking_usd,
               count(distinct Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name)
                   n_manufacturers,
               max(try_cast(Program_Year as integer)) program_year,
               min(nullif(trim(CAST(Covered_Recipient_Profile_ID as varchar)), ''))
                   open_payments_profile_id,
               count(distinct nullif(trim(CAST(Covered_Recipient_Profile_ID as varchar)), ''))
                   open_payments_profile_id_count
        from raw_open_payments_general
        where CAST(Covered_Recipient_NPI as varchar) = ?
    """, [npi])

    profile_id_count = int(summary.pop("open_payments_profile_id_count", 0) or 0)
    profile_id = summary.get("open_payments_profile_id")
    if profile_id_count != 1 or not profile_id or not re.fullmatch(r"\d+", str(profile_id)):
        if profile_id_count > 1:
            logger.warning(
                "Open Payments NPI %s maps to %d profile IDs; suppressing official link",
                npi,
                profile_id_count,
            )
        summary["open_payments_profile_id"] = None
        summary["open_payments_url"] = None
    else:
        profile_id = str(profile_id)
        summary["open_payments_profile_id"] = profile_id
        summary["open_payments_url"] = f"{OPEN_PAYMENTS_PROFILE_BASE_URL}/{profile_id}"

    consulting_speaking = summary.get("consulting_speaking_usd") or 0
    nonfood = summary.get("nonfood_usd") or 0
    if not summary.get("payment_count"):
        tier, tier_label = 0, "No industry contact"
    elif nonfood == 0:
        tier, tier_label = 1, "Lunch-only"
    elif consulting_speaking < 5000:
        tier, tier_label = 2, "Engaged"
    elif consulting_speaking < 25000:
        tier, tier_label = 3, "Paid speaker/advisor"
    else:
        tier, tier_label = 4, "KOL"
    return {**summary, "tier": tier, "tier_label": tier_label}
