"""Read-only New Provider Radar event and reconciliation queries."""

from datetime import date, datetime, timedelta
from typing import Callable, Literal

import duckdb
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

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

WatchRadarEventType = Literal[
    "newly_enumerated",
    "practice_location_changed",
    "primary_taxonomy_changed",
    "reactivated",
]

MAX_MATCH_SCOPES = 100
MAX_SCOPE_ZIPS = 100
MAX_SCOPE_TAXONOMIES = 100
MAX_MATCHES_PER_SCOPE = 5000
MAX_MATCHES_PER_REQUEST = 5000
MAX_HYDRATE_REFERENCES = 100


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


class RadarReleaseReceipt(BaseModel):
    """Immutable source release selected for one reconciliation pass."""

    model_config = ConfigDict(extra="forbid")

    contract_version: Literal[1] = 1
    source_release_id: str
    source_data_period: str
    source_fresh_through: date


class RadarScopeMatch(BaseModel):
    """Opaque event attribution returned to Provider Search."""

    model_config = ConfigDict(extra="forbid")

    event_id: str
    location_change: Literal["entered_market", "within_market"] | None = None


class RadarScopeResult(BaseModel):
    """Every bounded event reference matching one opaque request scope."""

    model_config = ConfigDict(extra="forbid")

    scope_key: str
    matches: list[RadarScopeMatch]


class RadarMatchScope(BaseModel):
    """One tenant-free exact geography and targeting predicate."""

    model_config = ConfigDict(extra="forbid")

    scope_key: str = Field(min_length=1, max_length=100)
    zip_codes: list[str] = Field(default_factory=list, max_length=MAX_SCOPE_ZIPS)
    city: str | None = Field(default=None, max_length=100)
    state: str | None = Field(default=None, max_length=2)
    taxonomy_codes: list[str] = Field(
        default_factory=list, max_length=MAX_SCOPE_TAXONOMIES
    )
    event_types: list[WatchRadarEventType] = Field(min_length=1, max_length=4)
    baseline_as_of: datetime | None = None

    @model_validator(mode="after")
    def normalize_and_validate(self) -> "RadarMatchScope":
        self.scope_key = self.scope_key.strip()
        if not self.scope_key:
            raise ValueError("scope_key must not be blank")
        self.zip_codes = sorted(set(value.strip() for value in self.zip_codes))
        if any(len(value) != 5 or not value.isdigit() for value in self.zip_codes):
            raise ValueError("ZIP codes must contain five digits")
        normalized_city = self.city.strip().upper() if self.city else None
        normalized_state = self.state.strip().upper() if self.state else None
        has_zips = bool(self.zip_codes)
        has_city = bool(normalized_city or normalized_state)
        if has_zips == has_city or (has_city and not (normalized_city and normalized_state)):
            raise ValueError("scope must contain exact ZIPs or a complete city/state")
        if normalized_state and (len(normalized_state) != 2 or not normalized_state.isalpha()):
            raise ValueError("state must be a two-letter code")
        self.city = normalized_city
        self.state = normalized_state
        self.taxonomy_codes = sorted(
            set(value.strip().upper() for value in self.taxonomy_codes if value.strip())
        )
        if any(
            len(value) != 10 or not value.isalnum() for value in self.taxonomy_codes
        ):
            raise ValueError("taxonomy codes must be ten alphanumeric characters")
        self.event_types = list(dict.fromkeys(self.event_types))
        if self.baseline_as_of is not None and self.baseline_as_of.tzinfo is None:
            raise ValueError("baseline_as_of must include a timezone")
        return self


class RadarMatchScopesRequest(BaseModel):
    """Bounded scopes matched against one current immutable Radar release."""

    model_config = ConfigDict(extra="forbid")

    source_release_id: str | None = Field(default=None, min_length=1, max_length=200)
    scopes: list[RadarMatchScope] = Field(min_length=1, max_length=MAX_MATCH_SCOPES)

    @field_validator("source_release_id")
    @classmethod
    def normalize_release(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("source_release_id must not be blank")
        return normalized

    @model_validator(mode="after")
    def unique_scope_keys(self) -> "RadarMatchScopesRequest":
        keys = [scope.scope_key for scope in self.scopes]
        if len(keys) != len(set(keys)):
            raise ValueError("scope keys must be unique")
        return self


class RadarMatchScopesResponse(BaseModel):
    """Release receipt and bounded opaque references for every request scope."""

    model_config = ConfigDict(extra="forbid")

    source_release_id: str
    source_data_period: str
    source_fresh_through: date
    scopes: list[RadarScopeResult]


class RadarEventReference(BaseModel):
    """An event observed during one installed Radar release."""

    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(min_length=1, max_length=200)
    source_release_id: str = Field(min_length=1, max_length=200)

    @model_validator(mode="after")
    def normalized(self) -> "RadarEventReference":
        self.event_id = self.event_id.strip()
        self.source_release_id = self.source_release_id.strip()
        if not self.event_id or not self.source_release_id:
            raise ValueError("event and release identifiers must not be blank")
        return self


class RadarHydrateRequest(BaseModel):
    """Bounded event references whose request order must be preserved."""

    model_config = ConfigDict(extra="forbid")

    references: list[RadarEventReference] = Field(
        min_length=1, max_length=MAX_HYDRATE_REFERENCES
    )

    @model_validator(mode="after")
    def unique_event_ids(self) -> "RadarHydrateRequest":
        event_ids = [reference.event_id for reference in self.references]
        if len(event_ids) != len(set(event_ids)):
            raise ValueError("event references must be unique")
        return self


class RadarHydrateResponse(BaseModel):
    """Current provider facts for immutable event references."""

    model_config = ConfigDict(extra="forbid")

    source_fresh_through: date
    references: list[RadarEventReference]
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


def _current_release(connection: duckdb.DuckDBPyConnection) -> dict:
    """Return the latest installed source release without guessing from file time."""
    try:
        cursor = connection.execute(
            """
            SELECT source_release_id, source_data_period, period_end
            FROM nppes_radar_releases
            ORDER BY period_end DESC, processed_at DESC, source_release_id DESC
            LIMIT 1
            """
        )
        rows = _rows(cursor)
    except duckdb.CatalogException as error:
        raise HTTPException(
            status_code=503,
            detail="New Provider Radar data has not been installed",
        ) from error
    if not rows:
        raise HTTPException(
            status_code=503,
            detail="New Provider Radar has no installed source release",
        )
    return rows[0]


def _release_receipt(row: dict) -> RadarReleaseReceipt:
    return RadarReleaseReceipt(
        source_release_id=row["source_release_id"],
        source_data_period=row["source_data_period"],
        source_fresh_through=row["period_end"],
    )


def _require_current_release(requested: str | None, current: dict) -> None:
    if requested is None or requested == current["source_release_id"]:
        return
    raise HTTPException(
        status_code=409,
        detail={
            "code": "radar_release_changed",
            "requested_source_release_id": requested,
            "current_source_release_id": current["source_release_id"],
        },
    )


def _scope_matches(
    connection: duckdb.DuckDBPyConnection,
    scope: RadarMatchScope,
) -> list[RadarScopeMatch]:
    where = [
        "e.event_type IN (" + ",".join(["?"] * len(scope.event_types)) + ")",
        "p.deactivation_date IS NULL",
    ]
    params: list = list(scope.event_types)
    if scope.zip_codes:
        where.append("e.new_zip5 IN (" + ",".join(["?"] * len(scope.zip_codes)) + ")")
        params.extend(scope.zip_codes)
    else:
        where.extend(
            [
                "UPPER(TRIM(p.practice_city)) = ?",
                "UPPER(TRIM(p.practice_state)) = ?",
            ]
        )
        params.extend([scope.city, scope.state])
    if scope.taxonomy_codes:
        where.append(
            "EXISTS (SELECT 1 FROM UNNEST(p.taxonomy_codes) AS taxonomy(code) "
            "WHERE taxonomy.code IN ("
            + ",".join(["?"] * len(scope.taxonomy_codes))
            + "))"
        )
        params.extend(scope.taxonomy_codes)
    if scope.baseline_as_of is not None:
        # A delayed first reconciliation must not classify events first seen
        # after watch creation as creation-release history.
        where.append("e.detected_at <= ?")
        params.append(scope.baseline_as_of)
    cursor = connection.execute(
        f"""
        SELECT e.event_id, e.event_type, e.old_zip5
        FROM nppes_radar_events e
        JOIN nppes_radar_provider_state p ON p.npi = e.npi
        WHERE {' AND '.join(where)}
        ORDER BY e.effective_date DESC, e.detected_at DESC, e.event_id
        LIMIT {MAX_MATCHES_PER_SCOPE + 1}
        """,
        params,
    )
    rows = _rows(cursor)
    if len(rows) > MAX_MATCHES_PER_SCOPE:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "radar_scope_too_broad",
                "scope_key": scope.scope_key,
                "maximum_matches": MAX_MATCHES_PER_SCOPE,
            },
        )
    zip_boundary = set(scope.zip_codes)
    return [
        RadarScopeMatch(
            event_id=row["event_id"],
            location_change=(
                "within_market" if row.get("old_zip5") in zip_boundary else "entered_market"
            )
            if zip_boundary and row["event_type"] == "practice_location_changed"
            else None,
        )
        for row in rows
    ]


def _hydrated_event_rows(
    connection: duckdb.DuckDBPyConnection,
    references: list[RadarEventReference],
) -> tuple[list[dict], date]:
    current = _current_release(connection)
    release_ids = list(dict.fromkeys(row.source_release_id for row in references))
    releases = {
        row[0]: (row[1], row[2], row[0])
        for row in connection.execute(
            "SELECT source_release_id, period_end, CAST(processed_at AS VARCHAR) "
            "FROM nppes_radar_releases WHERE "
            "source_release_id IN (" + ",".join(["?"] * len(release_ids)) + ")",
            release_ids,
        ).fetchall()
    }
    if set(releases) != set(release_ids):
        raise HTTPException(
            status_code=409,
            detail={"code": "radar_reference_release_unavailable"},
        )
    event_ids = [row.event_id for row in references]
    cursor = connection.execute(
        """
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
            e.reactivation_date,
            event_release.period_end AS event_release_period_end,
            CAST(event_release.processed_at AS VARCHAR) AS event_release_processed_at
        FROM nppes_radar_events e
        JOIN nppes_radar_provider_state p ON p.npi = e.npi
        JOIN nppes_radar_releases event_release
          ON event_release.source_release_id = e.source_release_id
        WHERE e.event_id IN ("""
        + ",".join(["?"] * len(event_ids))
        + ")",
        event_ids,
    )
    by_id = {row["event_id"]: row for row in _rows(cursor)}
    if set(by_id) != set(event_ids) or any(
        (
            by_id[reference.event_id]["event_release_period_end"],
            by_id[reference.event_id]["event_release_processed_at"],
            by_id[reference.event_id]["source_release_id"],
        )
        > releases[reference.source_release_id]
        for reference in references
    ):
        raise HTTPException(
            status_code=409,
            detail={"code": "radar_event_reference_unavailable"},
        )
    ordered = []
    for reference in references:
        row = by_id[reference.event_id]
        row.pop("event_release_period_end", None)
        row.pop("event_release_processed_at", None)
        row["reason"] = _reason(row)
        ordered.append(row)
    return ordered, current["period_end"]


def get_radar_router(get_conn: Callable) -> APIRouter:
    router = APIRouter(prefix="/radar", tags=["New Provider Radar"])

    @router.get("/providers/release", response_model=RadarReleaseReceipt)
    def provider_release() -> RadarReleaseReceipt:
        """Return the exact current Radar release used for job admission."""
        return _release_receipt(_current_release(get_conn()))

    @router.post("/providers/match-scopes", response_model=RadarMatchScopesResponse)
    def match_provider_scopes(
        request: RadarMatchScopesRequest,
    ) -> RadarMatchScopesResponse:
        """Match bounded tenant-free scopes against one current release."""
        connection = get_conn()
        current = _current_release(connection)
        _require_current_release(request.source_release_id, current)
        try:
            scopes: list[RadarScopeResult] = []
            matched_count = 0
            for scope in request.scopes:
                result = RadarScopeResult(
                    scope_key=scope.scope_key,
                    matches=_scope_matches(connection, scope),
                )
                matched_count += len(result.matches)
                if matched_count > MAX_MATCHES_PER_REQUEST:
                    raise HTTPException(
                        status_code=422,
                        detail={
                            "code": "radar_request_too_broad",
                            "maximum_matches": MAX_MATCHES_PER_REQUEST,
                        },
                    )
                scopes.append(result)
        except duckdb.CatalogException as error:
            raise HTTPException(
                status_code=503,
                detail="New Provider Radar data has not been installed",
            ) from error
        return RadarMatchScopesResponse(
            source_release_id=current["source_release_id"],
            source_data_period=current["source_data_period"],
            source_fresh_through=current["period_end"],
            scopes=scopes,
        )

    @router.post("/providers/hydrate", response_model=RadarHydrateResponse)
    def hydrate_provider_events(request: RadarHydrateRequest) -> RadarHydrateResponse:
        """Hydrate immutable references while preserving the request order."""
        try:
            rows, fresh_through = _hydrated_event_rows(get_conn(), request.references)
        except duckdb.CatalogException as error:
            raise HTTPException(
                status_code=503,
                detail="New Provider Radar data has not been installed",
            ) from error
        return RadarHydrateResponse(
            source_fresh_through=fresh_through,
            references=request.references,
            events=[RadarProviderEvent(**row) for row in rows],
        )

    @router.get("/providers", response_model=RadarProviderEventResponse)
    def provider_events(
        zip5: list[str] | None = Query(None),
        city: str | None = None,
        state: str | None = None,
        event_type: list[RadarEventType] | None = Query(None),
        taxonomy_code: list[str] | None = Query(None),
        since: date | None = None,
        until: date | None = None,
        include_deactivated: bool = False,
        offset: int = Query(0, ge=0),
        limit: int = Query(100, ge=1, le=250),
    ) -> RadarProviderEventResponse:
        """Return provider changes for one ZIP-set or exact city/state scope.

        NPPES city values are noisy and city scope can miss abbreviations,
        neighborhoods, and suburbs. A ZCTA/metro crosswalk is the upgrade path.
        """
        zip_scope = zip5 is not None
        city_scope = city is not None or state is not None
        if zip_scope == city_scope:
            raise HTTPException(
                status_code=422,
                detail="Provide exactly one scope: zip5 or city with state",
            )

        where: list[str] = []
        params: list = []
        if zip_scope:
            normalized_zips = list(dict.fromkeys(value.strip() for value in zip5 or []))
            if not normalized_zips or len(normalized_zips) > 100:
                raise HTTPException(status_code=422, detail="Provide 1 to 100 ZIP codes")
            if any(len(value) != 5 or not value.isdigit() for value in normalized_zips):
                raise HTTPException(status_code=422, detail="ZIP codes must contain five digits")
            where.append(
                "e.new_zip5 IN (" + ",".join(["?"] * len(normalized_zips)) + ")"
            )
            params.extend(normalized_zips)
        else:
            if city is None or state is None:
                raise HTTPException(
                    status_code=422,
                    detail="City scope requires both city and state",
                )
            normalized_city = city.strip().upper()
            normalized_state = state.strip().upper()
            if not normalized_city:
                raise HTTPException(status_code=422, detail="City must not be blank")
            if len(normalized_state) != 2 or not normalized_state.isalpha():
                raise HTTPException(
                    status_code=422,
                    detail="State must be a two-letter code",
                )
            where.extend(
                [
                    "UPPER(TRIM(p.practice_city)) = ?",
                    "UPPER(TRIM(p.practice_state)) = ?",
                ]
            )
            params.extend([normalized_city, normalized_state])

        selected_events = list(dict.fromkeys(event_type or DEFAULT_EVENT_TYPES))
        start_date = since or date.today() - timedelta(days=30)
        end_date = until or date.today()
        if start_date > end_date:
            raise HTTPException(status_code=422, detail="since cannot be after until")

        where.extend(
            [
                "e.event_type IN (" + ",".join(["?"] * len(selected_events)) + ")",
                "e.effective_date BETWEEN ? AND ?",
            ]
        )
        params.extend([*selected_events, start_date, end_date])
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
