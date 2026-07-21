"""Read-only New Provider Radar event queries."""

from datetime import date, timedelta
from typing import Callable, Literal

import duckdb
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

RadarEventType = Literal[
    "newly_enumerated",
    "practice_location_changed",
    "primary_taxonomy_changed",
    "reactivated",
    "deactivated",
]

DEFAULT_EVENT_TYPES: tuple[RadarEventType, ...] = (
    "newly_enumerated",
    "practice_location_changed",
)


class RadarProviderEvent(BaseModel):
    event_id: str
    event_type: RadarEventType
    effective_date: date
    detected_at: str
    source_release_id: str
    source_data_period: str
    npi: str
    first_name: str | None = None
    last_name: str | None = None
    credentials: str | None = None
    enumeration_date: date | None = None
    source_last_updated_date: date | None = None
    primary_taxonomy_code: str | None = None
    taxonomy_codes: list[str] = Field(default_factory=list)
    practice_address_1: str | None = None
    practice_address_2: str | None = None
    practice_city: str | None = None
    practice_state: str | None = None
    practice_zip5: str | None = None
    practice_phone: str | None = None
    old_zip5: str | None = None
    new_zip5: str | None = None
    old_primary_taxonomy_code: str | None = None
    new_primary_taxonomy_code: str | None = None
    deactivation_date: date | None = None
    reactivation_date: date | None = None
    reason: str


class RadarProviderEventResponse(BaseModel):
    total: int
    offset: int
    limit: int
    source_fresh_through: date | None = None
    events: list[RadarProviderEvent]


def _reason(row: dict) -> str:
    event_type = row["event_type"]
    effective_date = row["effective_date"]
    if event_type == "newly_enumerated":
        return f"NPI issued on {effective_date}"
    if event_type == "practice_location_changed":
        previous = row.get("old_zip5") or "no prior ZIP"
        current = row.get("new_zip5") or "no current ZIP"
        return f"Practice ZIP changed from {previous} to {current}"
    if event_type == "primary_taxonomy_changed":
        previous = row.get("old_primary_taxonomy_code") or "no prior taxonomy"
        current = row.get("new_primary_taxonomy_code") or "no current taxonomy"
        return f"Primary taxonomy changed from {previous} to {current}"
    if event_type == "reactivated":
        return f"NPI reactivated on {effective_date}"
    return f"NPI deactivated on {effective_date}"


def _rows(cursor: duckdb.DuckDBPyConnection) -> list[dict]:
    columns = [description[0] for description in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def get_radar_router(get_conn: Callable) -> APIRouter:
    router = APIRouter(prefix="/radar", tags=["New Provider Radar"])

    @router.get("/providers", response_model=RadarProviderEventResponse)
    async def provider_events(
        zip5: list[str] = Query(...),
        event_type: list[RadarEventType] | None = Query(None),
        taxonomy_code: list[str] | None = Query(None),
        since: date | None = None,
        until: date | None = None,
        include_deactivated: bool = False,
        offset: int = Query(0, ge=0),
        limit: int = Query(100, ge=1, le=250),
    ) -> RadarProviderEventResponse:
        """Return provider changes whose resulting primary ZIP is in a saved market."""
        normalized_zips = list(dict.fromkeys(value.strip() for value in zip5))
        if not normalized_zips or len(normalized_zips) > 100:
            raise HTTPException(status_code=422, detail="Provide 1 to 100 ZIP codes")
        if any(len(value) != 5 or not value.isdigit() for value in normalized_zips):
            raise HTTPException(status_code=422, detail="ZIP codes must contain five digits")

        selected_events = list(dict.fromkeys(event_type or DEFAULT_EVENT_TYPES))
        start_date = since or date.today() - timedelta(days=30)
        end_date = until or date.today()
        if start_date > end_date:
            raise HTTPException(status_code=422, detail="since cannot be after until")

        where = [
            "e.new_zip5 IN (" + ",".join(["?"] * len(normalized_zips)) + ")",
            "e.event_type IN (" + ",".join(["?"] * len(selected_events)) + ")",
            "e.effective_date BETWEEN ? AND ?",
        ]
        params: list = [*normalized_zips, *selected_events, start_date, end_date]
        if not include_deactivated:
            where.append("p.deactivation_date IS NULL")
        normalized_taxonomies = list(
            dict.fromkeys(value.strip().upper() for value in taxonomy_code or [] if value.strip())
        )
        if len(normalized_taxonomies) > 100 or any(
            len(value) != 10 or not value.isalnum() for value in normalized_taxonomies
        ):
            raise HTTPException(
                status_code=422,
                detail="Provide at most 100 ten-character taxonomy codes",
            )
        if normalized_taxonomies:
            where.append(
                "EXISTS ("
                "SELECT 1 FROM UNNEST(p.taxonomy_codes) AS taxonomy(code) "
                "WHERE taxonomy.code IN ("
                + ",".join(["?"] * len(normalized_taxonomies))
                + "))"
            )
            params.extend(normalized_taxonomies)

        where_sql = " AND ".join(where)
        connection = get_conn()
        try:
            total = connection.execute(
                f"""
                SELECT COUNT(*)
                FROM nppes_radar_events e
                JOIN nppes_radar_provider_state p ON p.npi = e.npi
                WHERE {where_sql}
                """,
                params,
            ).fetchone()[0]
            cursor = connection.execute(
                f"""
                SELECT
                    e.event_id,
                    e.event_type,
                    e.effective_date,
                    CAST(e.detected_at AS VARCHAR) AS detected_at,
                    e.source_release_id,
                    e.source_data_period,
                    e.npi,
                    p.first_name,
                    p.last_name,
                    p.credentials,
                    p.enumeration_date,
                    p.source_last_updated_date,
                    p.primary_taxonomy_code,
                    p.taxonomy_codes,
                    p.practice_address_1,
                    p.practice_address_2,
                    p.practice_city,
                    p.practice_state,
                    p.practice_zip5,
                    p.practice_phone,
                    e.old_zip5,
                    e.new_zip5,
                    e.old_primary_taxonomy_code,
                    e.new_primary_taxonomy_code,
                    e.deactivation_date,
                    e.reactivation_date
                FROM nppes_radar_events e
                JOIN nppes_radar_provider_state p ON p.npi = e.npi
                WHERE {where_sql}
                ORDER BY e.effective_date DESC, e.detected_at DESC, e.npi
                LIMIT ? OFFSET ?
                """,
                [*params, limit, offset],
            )
            rows = _rows(cursor)
            source_fresh_through = connection.execute(
                "SELECT MAX(period_end) FROM nppes_radar_releases"
            ).fetchone()[0]
        except duckdb.CatalogException as error:
            raise HTTPException(
                status_code=503,
                detail="New Provider Radar data has not been installed",
            ) from error

        for row in rows:
            row["reason"] = _reason(row)
        return RadarProviderEventResponse(
            total=total,
            offset=offset,
            limit=limit,
            source_fresh_through=source_fresh_through,
            events=[RadarProviderEvent(**row) for row in rows],
        )

    return router
