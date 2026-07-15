"""Open Payments discovery endpoints for industry-engagement search."""
from typing import Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel

router = APIRouter(prefix="/industry", tags=["Industry Relationships"])


class IndustrySearchResult(BaseModel):
    npi: str
    name: str
    credentials: str | None = None
    specialty: str | None = None
    practice_name: str | None = None
    city: str | None = None
    state: str | None = None
    lat: float | None = None
    lng: float | None = None
    payment_count: int
    total_usd: float
    nonfood_usd: float
    consulting_speaking_usd: float
    matched_payment_count: int
    matched_total_usd: float
    matched_nonfood_usd: float
    matched_consulting_speaking_usd: float
    n_manufacturers: int
    top_manufacturer: str | None = None
    top_product: str | None = None
    tier: int
    tier_label: str


class IndustrySearchResponse(BaseModel):
    total: int
    offset: int
    limit: int
    results: list[IndustrySearchResult]


class IndustryOption(BaseModel):
    value: str
    physician_count: int
    payment_count: int
    total_usd: float


class IndustryOptionsResponse(BaseModel):
    total_values: int
    options: list[IndustryOption]


class IndustryBreakdownRow(BaseModel):
    label: str
    payment_count: int
    total_usd: float


class IndustryManufacturerDetail(BaseModel):
    manufacturer: str
    payment_count: int
    total_usd: float
    products: list[str]


class IndustryRelationshipDetailResponse(BaseModel):
    npi: str
    payment_count: int
    total_usd: float
    nonfood_usd: float
    consulting_speaking_usd: float
    by_nature: list[IndustryBreakdownRow]
    manufacturers: list[IndustryManufacturerDetail]
    products: list[IndustryBreakdownRow]


def _tier(consulting_speaking: float, nonfood: float, payment_count: int) -> tuple[int, str]:
    if payment_count == 0:
        return 0, "No industry contact"
    if nonfood == 0:
        return 1, "Lunch-only"
    if consulting_speaking < 5000:
        return 2, "Engaged"
    if consulting_speaking < 25000:
        return 3, "Paid speaker/advisor"
    return 4, "KOL"


def get_industry_router(get_conn):
    @router.get("/search", response_model=IndustrySearchResponse)
    async def search_industry(
        specialty: list[str] | None = Query(None),
        city: Optional[str] = None,
        state: Optional[str] = None,
        manufacturer: list[str] | None = Query(None),
        product: list[str] | None = Query(None),
        min_total_usd: float = Query(0, ge=0),
        min_tier: int = Query(1, ge=1, le=4),
        threshold_scope: str = Query("matched", pattern="^(matched|all)$"),
        sort: str = Query(
            "total", pattern="^(total|matched|consulting|manufacturers|payments)$"
        ),
        offset: int = Query(0, ge=0),
        limit: int = Query(100, ge=1, le=250),
    ):
        """Find physicians by disclosed manufacturer relationships.

        Full physician totals always cover all general payments. The ``matched_*``
        metrics cover only payment rows matching selected manufacturer/product
        filters, so competitor relationship dollars are not confused with the
        physician's complete industry history.
        """
        where = ["op.Covered_Recipient_NPI is not null"]
        params: list = []
        if manufacturer:
            where.append(
                "upper(trim(op.Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name)) "
                "in (" + ",".join(["?"] * len(manufacturer)) + ")"
            )
            params.extend(value.strip().upper() for value in manufacturer)
        if product:
            where.append(
                "upper(trim(coalesce("
                "op.Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1, ''))) "
                "in (" + ",".join(["?"] * len(product)) + ")"
            )
            params.extend(value.strip().upper() for value in product)
        if specialty:
            where.append(
                "upper(trim(coalesce(d.specialty, ''))) in ("
                + ",".join(["?"] * len(specialty)) + ")"
            )
            params.extend(value.strip().upper() for value in specialty)
        if city:
            where.append("upper(coalesce(d.city, '')) = ?")
            params.append(city.strip().upper())
        if state:
            where.append("upper(coalesce(d.state, '')) = ?")
            params.append(state.strip().upper())

        order_by = {
            "total": "total_usd",
            "matched": "matched_total_usd",
            "consulting": "consulting_speaking_usd",
            "manufacturers": "n_manufacturers",
            "payments": "payment_count",
        }[sort]

        sql = f"""
            with doctor as (
              select CAST("NPI" as varchar) npi,
                     any_value("Provider First Name") || ' ' || any_value("Provider Last Name") as "name",
                     trim(coalesce(any_value("Cred\t\t\t\t"), '')) as credentials,
                     any_value(pri_spec) as specialty,
                     any_value("Facility Name") as practice_name,
                     any_value("City/Town") as city, any_value("State") as state,
                     upper(trim(any_value(adr_ln_1))) || '|' ||
                       left(any_value(CAST("ZIP Code" as varchar)), 5) as addr_key
              from raw_dac_national group by "NPI"
            ), matched_rows as (
              select op.*, d.name, d.credentials, d.specialty, d.practice_name,
                     d.city, d.state, d.addr_key
              from raw_open_payments_general op
              join doctor d on d.npi = CAST(op.Covered_Recipient_NPI as varchar)
              where {' and '.join(where)}
            ), matched as (
              select CAST(Covered_Recipient_NPI as varchar) npi,
                     count(*) matched_payment_count,
                     round(sum(Total_Amount_of_Payment_USDollars), 2) matched_total_usd,
                     round(sum(case when Nature_of_Payment_or_Transfer_of_Value <> 'Food and Beverage'
                               then Total_Amount_of_Payment_USDollars else 0 end), 2)
                       matched_nonfood_usd,
                     round(sum(case when Nature_of_Payment_or_Transfer_of_Value in
                                        ('Consulting Fee', 'Honoraria')
                                     or Nature_of_Payment_or_Transfer_of_Value like 'Compensation for serv%'
                               then Total_Amount_of_Payment_USDollars else 0 end), 2)
                       matched_consulting_speaking_usd
              from matched_rows group by 1
            ), full_rows as (
              select op.*, d.name, d.credentials, d.specialty, d.practice_name,
                     d.city, d.state, g.lat, g.lng
              from raw_open_payments_general op
              join matched mt on mt.npi = CAST(op.Covered_Recipient_NPI as varchar)
              join doctor d on d.npi = mt.npi
              left join address_geocode g on g.addr_key = d.addr_key
            ), mfr as (
              select CAST(Covered_Recipient_NPI as varchar) npi,
                     Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name manufacturer,
                     sum(Total_Amount_of_Payment_USDollars) usd,
                     row_number() over (
                       partition by CAST(Covered_Recipient_NPI as varchar) order by usd desc
                     ) rank
              from full_rows group by 1, 2
            ), product_rank as (
              select CAST(Covered_Recipient_NPI as varchar) npi,
                     Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1 product,
                     sum(Total_Amount_of_Payment_USDollars) usd,
                     row_number() over (
                       partition by CAST(Covered_Recipient_NPI as varchar) order by usd desc
                     ) rank
              from full_rows
              where Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1 is not null
              group by 1, 2
            ), aggregated as (
              select CAST(f.Covered_Recipient_NPI as varchar) as npi,
                   any_value(f.name) as "name", any_value(f.credentials) as credentials,
                   any_value(f.specialty) as specialty, any_value(f.practice_name) as practice_name,
                   any_value(f.city) as city, any_value(f.state) as state,
                   any_value(f.lat) as lat, any_value(f.lng) as lng,
                   count(*) as payment_count,
                   round(sum(f.Total_Amount_of_Payment_USDollars), 2) total_usd,
                   round(sum(case when f.Nature_of_Payment_or_Transfer_of_Value <> 'Food and Beverage'
                             then f.Total_Amount_of_Payment_USDollars else 0 end), 2) nonfood_usd,
                   round(sum(case when f.Nature_of_Payment_or_Transfer_of_Value in
                                      ('Consulting Fee', 'Honoraria')
                                   or f.Nature_of_Payment_or_Transfer_of_Value like 'Compensation for serv%'
                             then f.Total_Amount_of_Payment_USDollars else 0 end), 2)
                     consulting_speaking_usd,
                   count(distinct f.Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name)
                     as n_manufacturers,
                   any_value(m.manufacturer) as top_manufacturer,
                   any_value(p.product) as top_product,
                   any_value(mt.matched_payment_count) matched_payment_count,
                   any_value(mt.matched_total_usd) matched_total_usd,
                   any_value(mt.matched_nonfood_usd) matched_nonfood_usd,
                   any_value(mt.matched_consulting_speaking_usd)
                     matched_consulting_speaking_usd,
                   case when consulting_speaking_usd >= 25000 then 4
                        when consulting_speaking_usd >= 5000 then 3
                        when nonfood_usd > 0 then 2 else 1 end tier_score
              from full_rows f
              join matched mt on mt.npi = CAST(f.Covered_Recipient_NPI as varchar)
              left join mfr m on m.npi = CAST(f.Covered_Recipient_NPI as varchar) and m.rank = 1
              left join product_rank p on p.npi = CAST(f.Covered_Recipient_NPI as varchar) and p.rank = 1
              group by f.Covered_Recipient_NPI
            ), scoped as (
              select *,
                     case when ? = 'matched' then matched_total_usd else total_usd end
                       qualification_total_usd,
                     case when ? = 'matched' then
                       case when matched_consulting_speaking_usd >= 25000 then 4
                            when matched_consulting_speaking_usd >= 5000 then 3
                            when matched_nonfood_usd > 0 then 2 else 1 end
                       else tier_score end qualification_tier_score
              from aggregated
            ), qualified as (
              select *, count(*) over () total_matches from scoped
              where qualification_total_usd >= ? and qualification_tier_score >= ?
            )
            select * from qualified order by {order_by} desc limit ? offset ?
        """
        cursor = get_conn().execute(
            sql,
            params
            + [threshold_scope, threshold_scope, min_total_usd, min_tier, limit, offset],
        )
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        total = rows[0][columns.index("total_matches")] if rows else 0
        results = []
        for raw in rows:
            row = dict(zip(columns, raw))
            row.pop("total_matches", None)
            row.pop("tier_score", None)
            row.pop("qualification_total_usd", None)
            row.pop("qualification_tier_score", None)
            row["tier"], row["tier_label"] = _tier(
                row["consulting_speaking_usd"], row["nonfood_usd"], row["payment_count"]
            )
            results.append(IndustrySearchResult(**row))
        return IndustrySearchResponse(
            total=total, offset=offset, limit=limit, results=results
        )

    @router.get("/{npi}/detail", response_model=IndustryRelationshipDetailResponse)
    async def industry_relationship_detail(
        npi: str,
        manufacturer: list[str] | None = Query(None),
        product: list[str] | None = Query(None),
    ):
        """Return payment reasons and counterparties for the selected relationship slice."""
        where = ["CAST(Covered_Recipient_NPI as varchar) = ?"]
        params: list = [npi]
        if manufacturer:
            where.append(
                "upper(trim(Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name)) "
                "in (" + ",".join(["?"] * len(manufacturer)) + ")"
            )
            params.extend(value.strip().upper() for value in manufacturer)
        if product:
            where.append(
                "upper(trim(coalesce("
                "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1, ''))) "
                "in (" + ",".join(["?"] * len(product)) + ")"
            )
            params.extend(value.strip().upper() for value in product)
        predicate = " and ".join(where)

        totals = get_conn().execute(
            f"""
              select count(*) payment_count,
                     round(coalesce(sum(Total_Amount_of_Payment_USDollars), 0), 2) total_usd,
                     round(coalesce(sum(case when Nature_of_Payment_or_Transfer_of_Value <>
                       'Food and Beverage' then Total_Amount_of_Payment_USDollars else 0 end), 0), 2)
                       nonfood_usd,
                     round(coalesce(sum(case when Nature_of_Payment_or_Transfer_of_Value in
                       ('Consulting Fee', 'Honoraria')
                       or Nature_of_Payment_or_Transfer_of_Value like 'Compensation for serv%'
                       then Total_Amount_of_Payment_USDollars else 0 end), 0), 2)
                       consulting_speaking_usd
              from raw_open_payments_general where {predicate}
            """,
            params,
        ).fetchone()

        nature_rows = get_conn().execute(
            f"""
              select Nature_of_Payment_or_Transfer_of_Value as "label",
                     count(*) payment_count,
                     round(sum(Total_Amount_of_Payment_USDollars), 2) total_usd
              from raw_open_payments_general where {predicate}
              group by 1 order by total_usd desc
            """,
            params,
        ).fetchall()
        manufacturer_rows = get_conn().execute(
            f"""
              select Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name manufacturer,
                     count(*) payment_count,
                     round(sum(Total_Amount_of_Payment_USDollars), 2) total_usd
              from raw_open_payments_general where {predicate}
              group by 1 order by total_usd desc
            """,
            params,
        ).fetchall()
        product_rows = get_conn().execute(
            f"""
              select Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1 as "label",
                     count(*) payment_count,
                     round(sum(Total_Amount_of_Payment_USDollars), 2) total_usd
              from raw_open_payments_general where {predicate}
                and Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1 is not null
                and trim(Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1) <> ''
              group by 1 order by total_usd desc
            """,
            params,
        ).fetchall()

        products_by_manufacturer: dict[str, list[str]] = {}
        for manufacturer_name, product_name in get_conn().execute(
            f"""
              select distinct Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name,
                              Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1
              from raw_open_payments_general where {predicate}
                and Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1 is not null
                and trim(Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1) <> ''
              order by 1, 2
            """,
            params,
        ).fetchall():
            products_by_manufacturer.setdefault(manufacturer_name, []).append(product_name)

        return IndustryRelationshipDetailResponse(
            npi=npi,
            payment_count=totals[0],
            total_usd=totals[1],
            nonfood_usd=totals[2],
            consulting_speaking_usd=totals[3],
            by_nature=[
                IndustryBreakdownRow(label=row[0], payment_count=row[1], total_usd=row[2])
                for row in nature_rows
            ],
            manufacturers=[
                IndustryManufacturerDetail(
                    manufacturer=row[0],
                    payment_count=row[1],
                    total_usd=row[2],
                    products=products_by_manufacturer.get(row[0], []),
                )
                for row in manufacturer_rows
            ],
            products=[
                IndustryBreakdownRow(label=row[0], payment_count=row[1], total_usd=row[2])
                for row in product_rows
            ],
        )

    @router.get("/options", response_model=IndustryOptionsResponse)
    async def industry_options(
        field: str = Query(..., pattern="^(specialty|manufacturer|product)$"),
        q: str = "",
        starts_with: str = "",
        sort: str = Query("relevance", pattern="^(relevance|alpha)$"),
        offset: int = Query(0, ge=0),
        specialty: list[str] | None = Query(None),
        manufacturer: list[str] | None = Query(None),
        product: list[str] | None = Query(None),
        min_total_usd: float = Query(0, ge=0),
        min_tier: int = Query(1, ge=1, le=4),
        threshold_scope: str = Query("matched", pattern="^(matched|all)$"),
        city: Optional[str] = None,
        state: Optional[str] = None,
        limit: int = Query(30, ge=1, le=100),
    ):
        """Search live Open Payments facet values with physician and dollar counts."""
        expressions = {
            "specialty": "d.specialty",
            "manufacturer": "op.Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name",
            "product": "op.Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1",
        }
        value_expr = expressions[field]
        where = [f"{value_expr} is not null", f"trim({value_expr}) <> ''"]
        params: list = []
        if q.strip():
            where.append(f"upper({value_expr}) like ?")
            params.append(f"%{q.strip().upper()}%")
        if starts_with.strip():
            where.append(f"upper({value_expr}) like ?")
            params.append(f"{starts_with.strip().upper()}%")
        if city:
            where.append("upper(d.city) = ?")
            params.append(city.strip().upper())
        if state:
            where.append("upper(d.state) = ?")
            params.append(state.strip().upper())
        if field != "specialty" and specialty:
            where.append("upper(d.specialty) in (" + ",".join(["?"] * len(specialty)) + ")")
            params.extend(value.strip().upper() for value in specialty)
        if field != "manufacturer" and manufacturer:
            where.append(
                "upper(op.Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name) "
                "in (" + ",".join(["?"] * len(manufacturer)) + ")"
            )
            params.extend(value.strip().upper() for value in manufacturer)
        if field != "product" and product:
            where.append(
                "upper(op.Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1) "
                "in (" + ",".join(["?"] * len(product)) + ")"
            )
            params.extend(value.strip().upper() for value in product)
        order_by = '"value" asc' if sort == "alpha" else "physician_count desc, total_usd desc"
        sql = f"""
            with doctor as (
              select CAST("NPI" as varchar) npi, any_value(pri_spec) specialty,
                     any_value("City/Town") city, any_value("State") state
              from raw_dac_national group by "NPI"
            ), full_stats as (
              select CAST(Covered_Recipient_NPI as varchar) npi,
                     sum(Total_Amount_of_Payment_USDollars) total_usd,
                     sum(case when Nature_of_Payment_or_Transfer_of_Value <> 'Food and Beverage'
                              then Total_Amount_of_Payment_USDollars else 0 end) nonfood_usd,
                     sum(case when Nature_of_Payment_or_Transfer_of_Value in
                                  ('Consulting Fee', 'Honoraria')
                               or Nature_of_Payment_or_Transfer_of_Value like 'Compensation for serv%'
                              then Total_Amount_of_Payment_USDollars else 0 end)
                       consulting_speaking_usd
              from raw_open_payments_general
              where Covered_Recipient_NPI is not null
              group by 1
            ), candidate_stats as (
              select CAST(op.Covered_Recipient_NPI as varchar) npi,
                     trim({value_expr}) as "value",
                     count(*) payment_count,
                     sum(op.Total_Amount_of_Payment_USDollars) total_usd,
                     sum(case when op.Nature_of_Payment_or_Transfer_of_Value <>
                                  'Food and Beverage'
                              then op.Total_Amount_of_Payment_USDollars else 0 end) nonfood_usd,
                     sum(case when op.Nature_of_Payment_or_Transfer_of_Value in
                                  ('Consulting Fee', 'Honoraria')
                               or op.Nature_of_Payment_or_Transfer_of_Value like
                                  'Compensation for serv%'
                              then op.Total_Amount_of_Payment_USDollars else 0 end)
                       consulting_speaking_usd
              from raw_open_payments_general op
              join doctor d on d.npi = CAST(op.Covered_Recipient_NPI as varchar)
              where {' and '.join(where)}
              group by 1, 2
            ), qualified as (
              select c.*
              from candidate_stats c join full_stats f on f.npi = c.npi
              where case when ? = 'matched' then c.total_usd else f.total_usd end >= ?
                and case when ? = 'matched' then
                  case when c.consulting_speaking_usd >= 25000 then 4
                       when c.consulting_speaking_usd >= 5000 then 3
                       when c.nonfood_usd > 0 then 2 else 1 end
                  else case when f.consulting_speaking_usd >= 25000 then 4
                            when f.consulting_speaking_usd >= 5000 then 3
                            when f.nonfood_usd > 0 then 2 else 1 end
                  end >= ?
            ), grouped as (
              select "value", count(distinct npi) physician_count,
                     sum(payment_count) payment_count,
                     round(sum(total_usd), 2) total_usd
              from qualified group by 1
            )
            select *, count(*) over () as total_values
            from grouped order by {order_by} limit ? offset ?
        """
        cursor = get_conn().execute(
            sql,
            params
            + [threshold_scope, min_total_usd, threshold_scope, min_tier, limit, offset],
        )
        columns = [column[0] for column in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        total_values = rows[0]["total_values"] if rows else 0
        return IndustryOptionsResponse(
            total_values=total_values,
            options=[IndustryOption(**row) for row in rows],
        )

    return router
