"""
Doctor Clinical Profiles — per-NPI, multi-lens rep-facing profiles.

Five lenses assembled from validated queries (designed via warehouse review):
  panel      — patient population (scale, demographics, risk, chronic mix)
  clinical   — what they actually do (payment-weighted procedures, F/O split)
  prescribing— Part D persona (brand share, top drugs, specialty-tier flags)
  industry   — Open Payments (engagement tier, manufacturers, products,
               research PI role, ownership stakes)
  access     — where to find them (best-door ranked locations, groups, MIPS)

All SQL is whitelisted; the only inputs are an NPI (validated digits) and
search strings, always bound as parameters.
"""
import re
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from open_payments_profile import industry_summary

router = APIRouter(prefix="/profiles", tags=["Doctor Profiles"])

CRED = '"Cred\t\t\t\t"'
TELE = '"Telehlth\t\t\t\t"'

# Curated demo exemplars (validated LA cardiologists with contrasting stories).
EXEMPLARS = [
    {"npi": "1811967433", "name": "Matthew Budoff, MD",
     "story": "KOL + trial PI: $407K industry (25 mfrs), $976K research, healthy prevention-focused panel"},
    {"npi": "1194759803", "name": "Stephen Corday, MD",
     "story": "Solo independent, office buy-and-bill (Leqvio), brand-heavy Rx — easiest door in LA"},
    {"npi": "1326205873", "name": "Christopher Chu, MD",
     "story": "Hospital rounder with a safety-net panel: 96% dual-eligible, HCC 3.07, 66% diabetic"},
    {"npi": "1043244296", "name": "Gary Reznik, MD",
     "story": "Mega-prescriber: 46K claims / $11.5M, 92% LIS, brand-loyal even where generics exist"},
    {"npi": "1780065508", "name": "Vasimahmed Lala, DO",
     "story": "Pure endovascular operator — 62% of $3.7M from leg atherectomy/stents; device target"},
    {"npi": "1881985521", "name": "Duc Do, MD",
     "story": "UCLA multi-site traveler — skip the flagship, catch him at the 33-clinician Torrance satellite"},
    {"npi": "1326164633", "name": "Sameer Amin, MD",
     "story": "System-locked: Kaiser SCPMG (9,573 clinicians), switchboard phone, no MIPS visibility"},
    {"npi": "1831159714", "name": "Jeffrey Goodman, MD",
     "story": "Lunch-only: 56 meals from 19 manufacturers, zero paid engagements — accessible, not an influencer"},
]

_mips_stats: dict = {}


def _npi(v: str) -> str:
    if not re.fullmatch(r"\d{10}", v):
        raise HTTPException(status_code=400, detail="NPI must be 10 digits")
    return v


def _rows(conn, sql: str, params: list) -> list[dict]:
    cur = conn.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _row(conn, sql: str, params: list) -> Optional[dict]:
    out = _rows(conn, sql, params)
    return out[0] if out else None


class SearchHit(BaseModel):
    npi: str
    name: str
    credentials: str | None = None
    specialty: str | None = None
    city: str | None = None
    state: str | None = None
    group_name: str | None = None
    source: str = "medicare"          # "medicare" (DAC) | "registry" (NPPES-only)
    match_score: float | None = None  # fuzzy similarity for registry-tier hits


# Registry-tier fuzzy thresholds (jaro-winkler); validated on misspelled
# real-world queries — genuine targets score >=0.92, noise tops out ~0.83.
_FUZZY_THRESHOLD_FULL = 0.85   # first + last name provided
_FUZZY_THRESHOLD_LAST = 0.88   # last name only


def _search_dac(conn, parts: list[str], city: Optional[str], state: Optional[str],
                limit: int) -> list[dict]:
    """Tier 1: strict prefix match against Medicare Doctors & Clinicians.

    City ranks but never filters: the CMS mailing city is often a suburb or
    billing address (e.g. Tarzana for an "LA" clinician), so an exact name
    match one town over must still surface — city matches just sort first.
    State remains a hard filter (clean two-letter values, rarely ambiguous).
    """
    preds = []
    params: list = []
    if len(parts) >= 2:
        preds.append('(upper("Provider First Name") like ? and upper("Provider Last Name") like ?)')
        params += [parts[0] + "%", parts[-1] + "%"]
    else:
        preds.append('upper("Provider Last Name") like ?')
        params.append(parts[0] + "%" if parts else "%")
    if state:
        preds.append('"State" = ?')
        params.append(state.upper())
    order_sql = ""
    if city:
        order_sql = 'order by (any_value(upper("City/Town")) = ?) desc'
        # bound after the where-clause params (positional binding follows SQL order)
    sql = f"""
        select CAST("NPI" as varchar) npi,
               any_value("Provider First Name") || ' ' || any_value("Provider Last Name") as "name",
               any_value({CRED}) credentials, any_value(pri_spec) specialty,
               any_value("City/Town") city, any_value("State") state,
               any_value("Facility Name") group_name
        from raw_dac_national
        where {' and '.join(preds)}
        group by "NPI" {order_sql} limit {limit}"""
    if city:
        params.append(city.upper())
    return _rows(conn, sql, params)


def _search_registry(conn, parts: list[str], city: Optional[str], state: Optional[str],
                     limit: int) -> list[dict]:
    """Tier 2: fuzzy match against the full NPPES registry (everyone with an NPI).

    Catches misspellings and providers who don't bill Medicare. Last name is
    weighted 0.7 vs first 0.3; stored last names are also compared with
    spaces/hyphens stripped so "EL ATTRACHE" / "EL-ATTRACHE" / "ELATTRACHE"
    all behave the same. Hits also present in DAC keep source="medicare".

    City ranks but never filters (same doctrine as the DAC tier): practice
    city is a mailing-address value, so a better name match in a neighboring
    town must not be hidden by a metro-name query. City acts as a tiebreaker
    below the name score; state stays a hard scope.
    """
    first = parts[0] if len(parts) >= 2 else None
    last = "".join(parts[1:]) if len(parts) >= 2 else parts[0]

    scope_preds = ["n.entity_type = 1", "n.last_name is not null"]
    scope_params: list = []
    if state:
        scope_preds.append("n.practice_state = ?")
        scope_params.append(state.upper())

    stripped_last = 'replace(replace(upper(n.last_name), \' \', \'\'), \'-\', \'\')'
    if first:
        # greatest() also scores the whole query as a compound surname, so
        # "el attrache" (no first name) still finds EL ATTRACHE / ELATTRACHE.
        score_expr = f"""
            greatest(
                0.7 * jaro_winkler_similarity({stripped_last}, ?)
              + 0.3 * jaro_winkler_similarity(upper(coalesce(n.first_name, '')), ?),
                jaro_winkler_similarity({stripped_last}, ?)
            )"""
        score_params = [last, first, "".join(parts)]
        threshold = _FUZZY_THRESHOLD_FULL
    else:
        score_expr = f"jaro_winkler_similarity({stripped_last}, ?)"
        score_params = [last]
        threshold = _FUZZY_THRESHOLD_LAST

    if city:
        city_match_expr = "(upper(coalesce(n.practice_city, '')) = ?)"
        city_params: list = [city.upper()]
    else:
        city_match_expr = "false"
        city_params = []

    sql = f"""
        with scored as (
            select CAST(n.npi as varchar) npi,
                   coalesce(n.first_name || ' ', '') || n.last_name as "name",
                   n.credentials, n.practice_city city, n.practice_state state,
                   n.taxonomy_1, ({score_expr}) score,
                   {city_match_expr} city_match
            from raw_nppes n
            where {' and '.join(scope_preds)}
            order by score desc, city_match desc
            limit {limit}
        )
        select s.npi, s."name", s.credentials, s.city, s.state,
               round(s.score, 3) match_score,
               coalesce(
                   any_value(d.pri_spec),
                   any_value(t.classification
                             || coalesce(' (' || nullif(t.specialization, '') || ')', ''))
               ) specialty,
               any_value(d."Facility Name") group_name,
               count(d."NPI") > 0 in_dac
        from scored s
        left join nucc_taxonomy t on s.taxonomy_1 = t.taxonomy_code
        left join raw_dac_national d on CAST(d."NPI" as varchar) = s.npi
        where s.score >= {threshold}
        group by s.npi, s."name", s.credentials, s.city, s.state, s.score, s.city_match
        order by s.score desc, s.city_match desc"""
    rows = _rows(conn, sql, score_params + city_params + scope_params)
    for row in rows:
        row["source"] = "medicare" if row.pop("in_dac", False) else "registry"
    return rows


def get_profiles_router(get_conn):
    @router.get("/exemplars")
    async def exemplars():
        return EXEMPLARS

    @router.get("/search", response_model=list[SearchHit])
    async def search(q: str, city: Optional[str] = None, state: Optional[str] = None,
                     limit: int = 15):
        """Find doctors by name (last or 'first last'), optional city/state.

        Tiered: exact-prefix Medicare (DAC) match first; when it comes up
        empty, fuzzy NPPES-registry fallback (typo-tolerant, includes
        providers who never bill Medicare).

        State is a hard scope; city only boosts ranking. CMS/NPPES city is a
        mailing-address value, so metro queries ("Los Angeles") must not hide
        exact name matches recorded in a neighboring suburb ("Tarzana").
        """
        limit = max(1, min(limit, 30))
        parts = q.strip().upper().split()
        if not parts:
            return []
        conn = get_conn()
        rows = _search_dac(conn, parts, city, state, limit)
        if not rows:
            rows = _search_registry(conn, parts, city, state, limit)
        return [SearchHit(**{**r, "credentials": (r.get("credentials") or "").strip() or None})
                for r in rows]

    @router.get("/{npi}")
    async def profile(npi: str):
        npi = _npi(npi)
        conn = get_conn()
        out: dict = {"npi": npi}

        # ------ header / background (DAC + NPPES) ------
        out["header"] = _row(conn, f"""
            select any_value(d."Provider First Name") || ' ' || any_value(d."Provider Last Name") as "name",
                   trim(coalesce(any_value(d.{CRED}), '')) credentials,
                   any_value(d.pri_spec) specialty, any_value(d.sec_spec_all) secondary_specialties,
                   any_value(d."City/Town") city, any_value(d."State") state,
                   any_value(d.Med_sch) med_school, any_value(d.Grd_yr) grad_year,
                   year(current_date) - any_value(d.Grd_yr) years_in_practice,
                   max(case when d.{TELE} = 'Y' then 1 else 0 end) = 1 telehealth
            from raw_dac_national d where CAST(d."NPI" as varchar) = ?
            group by d."NPI" """, [npi])
        if not out["header"]:
            raise HTTPException(status_code=404, detail="NPI not found in Doctors & Clinicians")

        # ------ 1. patient panel ------
        out["panel"] = _row(conn, """
            select Tot_Benes medicare_patients, Tot_Srvcs total_services,
                   round(Tot_Srvcs / nullif(Tot_Benes,0), 1) services_per_patient,
                   round(Tot_Mdcr_Alowd_Amt) medicare_allowed_amt,
                   round(Drug_Mdcr_Pymt_Amt) part_b_drug_payments,
                   Bene_Avg_Age avg_patient_age,
                   round(100.0*(coalesce(Bene_Age_75_84_Cnt,0)+coalesce(Bene_Age_GT_84_Cnt,0))
                         / nullif(Tot_Benes,0)) pct_age_75_plus,
                   round(100.0*Bene_Feml_Cnt/nullif(Tot_Benes,0)) pct_female,
                   round(100.0*Bene_Dual_Cnt/nullif(Tot_Benes,0)) pct_dual_eligible,
                   Bene_Avg_Risk_Scre avg_hcc_risk_score,
                   Bene_CC_PH_Hypertension_V2_Pct pct_hypertension,
                   Bene_CC_PH_Hyperlipidemia_V2_Pct pct_hyperlipidemia,
                   Bene_CC_PH_Diabetes_V2_Pct pct_diabetes,
                   Bene_CC_PH_IschemicHeart_V2_Pct pct_ischemic_heart,
                   Bene_CC_PH_HF_NonIHD_V2_Pct pct_heart_failure,
                   Bene_CC_PH_Afib_V2_Pct pct_afib,
                   Bene_CC_PH_CKD_V2_Pct pct_ckd,
                   Bene_CC_PH_COPD_V2_Pct pct_copd,
                   Bene_CC_BH_Depress_V1_Pct pct_depression
            from raw_physician_by_provider
            where CAST(Rndrng_NPI as varchar) = ? and Rndrng_Prvdr_Ent_Cd = 'I'
        """, [npi])

        # ------ 2. clinical focus ------
        out["clinical"] = _row(conn, """
            select any_value(Rndrng_Prvdr_Type) cms_specialty,
                   count(distinct HCPCS_Cd) distinct_codes,
                   sum(Tot_Srvcs) total_services,
                   round(sum(Tot_Srvcs*Avg_Mdcr_Pymt_Amt)) est_total_paid,
                   round(sum(Tot_Srvcs*Avg_Mdcr_Pymt_Amt) filter (where Place_Of_Srvc='F')
                         / nullif(sum(Tot_Srvcs*Avg_Mdcr_Pymt_Amt),0), 2) facility_paid_share,
                   round(sum(Tot_Srvcs*Avg_Mdcr_Pymt_Amt) filter (where HCPCS_Drug_Ind='Y')
                         / nullif(sum(Tot_Srvcs*Avg_Mdcr_Pymt_Amt),0), 2) drug_admin_paid_share,
                   round(sum(Tot_Srvcs*Avg_Mdcr_Pymt_Amt) filter (where HCPCS_Cd between '99091' and '99499')
                         / nullif(sum(Tot_Srvcs*Avg_Mdcr_Pymt_Amt),0), 2) em_paid_share
            from raw_physician_by_provider_and_service
            where CAST(Rndrng_NPI as varchar) = ?
        """, [npi])
        out["top_procedures"] = _rows(conn, """
            with svc as (
              select HCPCS_Cd, any_value(HCPCS_Desc) descr,
                     case when max(HCPCS_Drug_Ind)='Y' then 'drug_admin'
                          when HCPCS_Cd between '99091' and '99499' then 'evaluation_mgmt'
                          when HCPCS_Cd between '70000' and '79999' then 'imaging'
                          when HCPCS_Cd between '80000' and '89999' then 'lab_path'
                          when HCPCS_Cd between '90000' and '98999' then 'diagnostic_proc'
                          when HCPCS_Cd between '00100' and '69999' then 'surgical_proc'
                          else 'other' end category,
                     sum(Tot_Srvcs) services, max(Tot_Benes) patients,
                     round(sum(Tot_Srvcs*Avg_Mdcr_Pymt_Amt)) est_paid,
                     round(coalesce(sum(Tot_Srvcs) filter (where Place_Of_Srvc='F'),0)
                           / nullif(sum(Tot_Srvcs),0), 2) facility_share
              from raw_physician_by_provider_and_service
              where CAST(Rndrng_NPI as varchar) = ? group by HCPCS_Cd),
            tot as (select sum(est_paid) all_paid from svc)
            select s.HCPCS_Cd hcpcs, s.category, left(s.descr, 70) description,
                   s.services, s.patients, s.est_paid,
                   round(s.est_paid/nullif(t.all_paid,0), 2) pct_of_paid, s.facility_share
            from svc s cross join tot t order by s.est_paid desc limit 10
        """, [npi])

        # ------ 3. prescribing ------
        out["prescribing"] = _row(conn, """
            select Tot_Clms total_claims, Tot_Benes patients, round(Tot_Drug_Cst) total_cost,
                   round(Tot_Drug_Cst/nullif(Tot_Clms,0), 2) cost_per_claim,
                   round(Brnd_Tot_Clms*1.0/nullif(Brnd_Tot_Clms+Gnrc_Tot_Clms,0), 2) brand_claim_share,
                   round(Brnd_Tot_Drug_Cst/nullif(Tot_Drug_Cst,0), 2) brand_cost_share,
                   Opioid_Prscrbr_Rate opioid_rate_pct,
                   round(LIS_Tot_Clms*1.0/nullif(Tot_Clms,0), 2) lis_claim_share,
                   Bene_Avg_Age rx_panel_avg_age, Bene_Avg_Risk_Scre rx_panel_risk
            from raw_part_d_by_provider where CAST(PRSCRBR_NPI as varchar) = ?
        """, [npi])
        out["top_drugs"] = _rows(conn, """
            with rx as (
              select Brnd_Name brand, Gnrc_Name generic, Tot_Clms claims, Tot_Benes patients,
                     round(Tot_Drug_Cst) drug_cost,
                     round(Tot_Drug_Cst/nullif(Tot_Clms,0), 2) cost_per_claim,
                     round(Tot_Day_Suply*1.0/nullif(Tot_Clms,0)) days_per_claim,
                     (Tot_Drug_Cst/nullif(Tot_Clms,0)) >= 950 specialty_tier
              from raw_part_d_by_provider_and_drug where CAST(Prscrbr_NPI as varchar) = ?),
            tot as (select sum(drug_cost) all_cost from rx)
            select r.*, round(r.drug_cost/nullif(t.all_cost,0), 2) pct_of_cost
            from rx r cross join tot t order by r.drug_cost desc limit 10
        """, [npi])

        # ------ 4. industry (Open Payments) ------
        out["industry"] = industry_summary(conn, npi)
        out["industry_by_nature"] = _rows(conn, """
            select case
                when Nature_of_Payment_or_Transfer_of_Value = 'Food and Beverage' then 'Meals'
                when Nature_of_Payment_or_Transfer_of_Value = 'Travel and Lodging' then 'Travel'
                when Nature_of_Payment_or_Transfer_of_Value in ('Consulting Fee','Honoraria') then 'Consulting/Honoraria'
                when Nature_of_Payment_or_Transfer_of_Value like 'Compensation for serv%' then 'Speaking/Faculty'
                when Nature_of_Payment_or_Transfer_of_Value in ('Education','Gift','Entertainment','Charitable Contribution') then 'Education/Gifts'
                when Nature_of_Payment_or_Transfer_of_Value in ('Royalty or License','Acquisitions') then 'Royalties/IP'
                else 'Other' end nature_group,
                count(*) n_payments, round(sum(Total_Amount_of_Payment_USDollars)) usd
            from raw_open_payments_general
            where CAST(Covered_Recipient_NPI as varchar) = ?
            group by 1 order by usd desc
        """, [npi])
        out["industry_manufacturers"] = _rows(conn, """
            select Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name manufacturer,
                   round(sum(Total_Amount_of_Payment_USDollars)) usd, count(*) n_payments,
                   string_agg(distinct Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1, ', ') products
            from raw_open_payments_general
            where CAST(Covered_Recipient_NPI as varchar) = ?
            group by 1 order by usd desc limit 5
        """, [npi])
        out["research"] = _row(conn, """
            select count(*) research_rows,
                   round(sum(Total_Amount_of_Payment_USDollars)) research_usd,
                   count(distinct Name_of_Study) n_studies,
                   string_agg(distinct Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name, '; ') sponsors
            from raw_open_payments_research
            where CAST(Covered_Recipient_NPI as varchar) = ?
               or CAST(Principal_Investigator_1_NPI as varchar) = ?
               or CAST(Principal_Investigator_2_NPI as varchar) = ?
               or CAST(Principal_Investigator_3_NPI as varchar) = ?
               or CAST(Principal_Investigator_4_NPI as varchar) = ?
               or CAST(Principal_Investigator_5_NPI as varchar) = ?
        """, [npi] * 6)
        out["ownership"] = _row(conn, """
            select count(*) stakes, round(sum(Total_Amount_Invested_USDollars)) invested,
                   string_agg(distinct Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name, '; ') companies
            from raw_open_payments_ownership where CAST(Physician_NPI as varchar) = ?
        """, [npi])

        # ------ 5. access & affiliation ------
        out["locations"] = _rows(conn, """
            with doc as (
              select upper(trim(adr_ln_1)) || '|' || left(CAST("ZIP Code" as varchar),5) addr_key,
                     any_value(trim(adr_ln_1)) street,
                     list(distinct trim(adr_ln_2)) filter (where adr_ln_2 is not null) suites,
                     any_value("City/Town") city, any_value("State") state,
                     left(any_value(CAST("ZIP Code" as varchar)),5) zip5,
                     any_value(CAST("Telephone Number" as varchar)) phone,
                     any_value(org_pac_id) org_pac_id
              from raw_dac_national where CAST("NPI" as varchar) = ? group by 1),
            roster as (
              select org_pac_id, upper(trim(adr_ln_1)) || '|' || left(CAST("ZIP Code" as varchar),5) addr_key,
                     count(distinct "NPI") roster_size
              from raw_dac_national
              where org_pac_id in (select distinct org_pac_id from doc where org_pac_id is not null)
              group by 1, 2)
            select d.street, d.suites, d.city, d.state, d.zip5, d.phone,
                   r.roster_size, g.lat, g.lng,
                   (r.roster_size = max(r.roster_size) over () and r.roster_size > 50) likely_flagship
            from doc d
            left join roster r on r.org_pac_id = d.org_pac_id and r.addr_key = d.addr_key
            left join address_geocode g on g.addr_key = d.addr_key
            order by coalesce(r.roster_size, 0) asc
        """, [npi])
        home_state = (out["header"] or {}).get("state")
        out["locations"].sort(key=lambda l: (
            0 if l.get("state") == home_state else 1,
            l.get("roster_size") if l.get("roster_size") is not None else 10**9,
        ))

        out["groups"] = _rows(conn, """
            select org_pac_id group_id, any_value("Facility Name") group_name,
                   any_value(num_org_mem) group_size,
                   count(distinct upper(trim(adr_ln_1))) n_addresses
            from raw_dac_national
            where CAST("NPI" as varchar) = ? and org_pac_id is not null
            group by org_pac_id
        """, [npi])
        if not _mips_stats:
            s = _row(conn, """select median(final_MIPS_score) med,
                              quantile_cont(final_MIPS_score, 0.25) q25,
                              quantile_cont(final_MIPS_score, 0.75) q75
                              from raw_mips_performance""", [])
            _mips_stats.update(s or {})
        mips = _rows(conn, """
            select source, final_MIPS_score final_score, Quality_category_score quality_score,
                   Cost_category_score cost_score
            from raw_mips_performance where CAST("NPI" as varchar) = ?
            order by case source when 'individual' then 1 when 'group' then 2 else 3 end
        """, [npi])
        for m in mips:
            fs = m.get("final_score")
            if fs is not None and _mips_stats:
                m["interpretation"] = (
                    "TOP QUARTILE" if fs >= _mips_stats["q75"]
                    else "ABOVE MEDIAN" if fs >= _mips_stats["med"]
                    else "BELOW MEDIAN" if fs >= _mips_stats["q25"]
                    else "BOTTOM QUARTILE")
        out["mips"] = mips
        out["mips_national"] = _mips_stats

        return out

    return router
