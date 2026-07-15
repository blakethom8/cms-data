"""AACT-backed subset of the ClinicalTrials.gov v2 API used by Provider Search."""

from __future__ import annotations

import math
import os
import re
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any

import psycopg
from fastapi import APIRouter, HTTPException, Query
from psycopg.rows import dict_row


AACT_DATABASE_URL = os.getenv(
    "AACT_DATABASE_URL",
    "postgresql://aact_reader@127.0.0.1:5433/aact",
)
AACT_SNAPSHOT_FILE = Path(os.getenv("AACT_SNAPSHOT_FILE", "/srv/aact/CURRENT_SNAPSHOT"))
ALLOWED_STATUSES = {
    "ACTIVE_NOT_RECRUITING",
    "APPROVED_FOR_MARKETING",
    "AVAILABLE",
    "COMPLETED",
    "ENROLLING_BY_INVITATION",
    "NO_LONGER_AVAILABLE",
    "NOT_YET_RECRUITING",
    "RECRUITING",
    "SUSPENDED",
    "TEMPORARILY_NOT_AVAILABLE",
    "TERMINATED",
    "WITHDRAWN",
}
FACILITY_STOP_WORDS = {
    "center",
    "clinical",
    "health",
    "healthcare",
    "hospital",
    "medical",
    "research",
    "site",
    "system",
}
GEO_FILTER_RE = re.compile(
    r"^distance\(\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)\s*,"
    r"\s*(\d+(?:\.\d+)?)mi\s*\)$",
    re.IGNORECASE,
)


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def _parse_geo_filter(value: str | None) -> tuple[float, float, float] | None:
    if not value:
        return None
    match = GEO_FILTER_RE.fullmatch(value.strip())
    if not match:
        raise HTTPException(status_code=422, detail="Invalid filter.geo distance expression")
    lat, lng, radius = (float(part) for part in match.groups())
    if not -90 <= lat <= 90 or not -180 <= lng <= 180 or not 0 < radius <= 500:
        raise HTTPException(status_code=422, detail="Invalid geographic search bounds")
    return lat, lng, radius


def _parse_statuses(value: str | None) -> list[str]:
    statuses = [part.strip().upper() for part in (value or "RECRUITING").split("|")]
    invalid = sorted(set(statuses) - ALLOWED_STATUSES)
    if invalid:
        raise HTTPException(
            status_code=422,
            detail=f"Unsupported trial status: {', '.join(invalid)}",
        )
    return list(dict.fromkeys(statuses))


def _facility_tokens(value: str) -> list[str]:
    tokens = [
        token
        for token in re.findall(r"[a-z0-9]+", value.lower())
        if token not in FACILITY_STOP_WORDS
    ]
    distinctive = list(dict.fromkeys(tokens))
    return distinctive or [" ".join(value.lower().split())]


def _search_predicate(search_type: str, query: str) -> tuple[str, list[str]]:
    needle = query.strip().lower()
    if search_type == "condition":
        return (
            "exists (select 1 from ctgov.conditions q where q.nct_id = s.nct_id "
            "and position(%s in q.downcase_name) > 0)",
            [needle],
        )
    if search_type == "intervention":
        return (
            "exists (select 1 from ctgov.interventions q where q.nct_id = s.nct_id "
            "and position(%s in lower(coalesce(q.name, ''))) > 0)",
            [needle],
        )
    return (
        "(position(%s in lower(coalesce(s.brief_title, ''))) > 0 "
        "or position(%s in lower(coalesce(s.official_title, ''))) > 0 "
        "or exists (select 1 from ctgov.conditions q where q.nct_id = s.nct_id "
        "and position(%s in q.downcase_name) > 0) "
        "or exists (select 1 from ctgov.interventions q where q.nct_id = s.nct_id "
        "and position(%s in lower(coalesce(q.name, ''))) > 0) "
        "or exists (select 1 from ctgov.keywords q where q.nct_id = s.nct_id "
        "and position(%s in q.downcase_name) > 0))",
        [needle] * 5,
    )


def _distance_sql(alias: str = "f") -> str:
    return (
        "3958.8 * acos(least(1.0, greatest(-1.0, "
        f"cos(radians(%s)) * cos(radians({alias}.latitude::double precision)) * "
        f"cos(radians({alias}.longitude::double precision) - radians(%s)) + "
        f"sin(radians(%s)) * sin(radians({alias}.latitude::double precision))"
        ")))"
    )


class AACTStore:
    """Read-only query adapter over the locally hosted AACT PostgreSQL database."""

    def __init__(self, database_url: str = AACT_DATABASE_URL):
        self.database_url = database_url

    async def _connect(self) -> psycopg.AsyncConnection:
        return await psycopg.AsyncConnection.connect(
            self.database_url,
            connect_timeout=5,
            row_factory=dict_row,
        )

    async def version(self) -> dict[str, Any]:
        async with await self._connect() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    "select max(last_update_posted_date) as data_timestamp, "
                    "count(*)::bigint as study_count from ctgov.studies"
                )
                row = await cursor.fetchone()
        snapshot = AACT_SNAPSHOT_FILE.read_text().strip() if AACT_SNAPSHOT_FILE.exists() else None
        if snapshot and re.fullmatch(r"\d{8}", snapshot):
            snapshot = f"{snapshot[:4]}-{snapshot[4:6]}-{snapshot[6:]}"
        return {
            "apiVersion": "AACT PostgreSQL mirror v1",
            "dataTimestamp": _iso(row["data_timestamp"]),
            "snapshotDate": snapshot,
            "studyCount": int(row["study_count"]),
            "source": "ClinicalTrials.gov via AACT",
        }

    async def search(
        self,
        *,
        query: str,
        search_type: str,
        statuses: list[str],
        page_size: int,
        geo: tuple[float, float, float] | None,
        facility: str | None,
        city: str | None,
        states: list[str],
    ) -> dict[str, Any]:
        predicate, predicate_params = _search_predicate(search_type, query)
        where = ["s.overall_status = any(%s)", predicate]
        params: list[Any] = [statuses, *predicate_params]

        if geo is not None:
            lat, lng, radius = geo
            lat_delta = radius / 69.0
            longitude_scale = max(0.15, abs(math.cos(math.radians(lat))))
            lng_delta = radius / (69.0 * longitude_scale)
            where.extend(
                [
                    "f.latitude between %s and %s",
                    "f.longitude between %s and %s",
                    f"{_distance_sql()} <= %s",
                ]
            )
            params.extend(
                [
                    lat - lat_delta,
                    lat + lat_delta,
                    lng - lng_delta,
                    lng + lng_delta,
                    lat,
                    lng,
                    lat,
                    radius,
                ]
            )
        elif facility:
            if city:
                where.append("lower(coalesce(f.city, '')) = lower(%s)")
                params.append(city.strip())
            if states:
                where.append("lower(coalesce(f.state, '')) = any(%s)")
                params.append([state.lower() for state in states])
            for token in _facility_tokens(facility):
                where.append("position(%s in lower(coalesce(f.name, ''))) > 0")
                params.append(token)
        else:
            raise HTTPException(
                status_code=422,
                detail="A geographic filter or facility is required",
            )

        candidate_sql = f"""
            with matched as (
                select distinct s.nct_id, s.last_update_posted_date
                from ctgov.studies s
                join ctgov.facilities f on f.nct_id = s.nct_id
                where {' and '.join(where)}
            )
            select nct_id, count(*) over()::bigint as total_count
            from matched
            order by last_update_posted_date desc nulls last, nct_id
            limit %s
        """
        params.append(page_size)

        async with await self._connect() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(candidate_sql, params)
                candidates = await cursor.fetchall()
                nct_ids = [row["nct_id"] for row in candidates]
                total_count = int(candidates[0]["total_count"]) if candidates else 0
                studies = await self._load_studies(cursor, nct_ids, geo=geo)

        return {
            "studies": studies,
            "totalCount": total_count,
            "nextPageToken": None,
            "source": "ClinicalTrials.gov via AACT",
        }

    async def _load_studies(
        self,
        cursor: psycopg.AsyncCursor,
        nct_ids: list[str],
        *,
        geo: tuple[float, float, float] | None,
    ) -> list[dict[str, Any]]:
        if not nct_ids:
            return []

        async def fetch(sql: str, params: list[Any]) -> list[dict[str, Any]]:
            await cursor.execute(sql, params)
            return list(await cursor.fetchall())

        core = await fetch(
            """
            select nct_id, brief_title, official_title, overall_status, phase, study_type,
                   enrollment, start_date, completion_date, primary_completion_date,
                   last_update_posted_date
            from ctgov.studies where nct_id = any(%s)
            """,
            [nct_ids],
        )
        conditions = await fetch(
            "select nct_id, name from ctgov.conditions where nct_id = any(%s) order by id",
            [nct_ids],
        )
        interventions = await fetch(
            "select nct_id, name, intervention_type from ctgov.interventions "
            "where nct_id = any(%s) order by id",
            [nct_ids],
        )
        sponsors = await fetch(
            "select nct_id, name, agency_class from ctgov.sponsors "
            "where nct_id = any(%s) and lead_or_collaborator = 'lead' order by id",
            [nct_ids],
        )

        facility_where = ["nct_id = any(%s)"]
        facility_params: list[Any] = [nct_ids]
        if geo is not None:
            lat, lng, radius = geo
            lat_delta = radius / 69.0
            longitude_scale = max(0.15, abs(math.cos(math.radians(lat))))
            lng_delta = radius / (69.0 * longitude_scale)
            facility_where.extend(
                [
                    "latitude between %s and %s",
                    "longitude between %s and %s",
                    f"{_distance_sql('ctgov.facilities')} <= %s",
                ]
            )
            facility_params.extend(
                [
                    lat - lat_delta,
                    lat + lat_delta,
                    lng - lng_delta,
                    lng + lng_delta,
                    lat,
                    lng,
                    lat,
                    radius,
                ]
            )
        facilities = await fetch(
            "select id, nct_id, status, name, city, state, zip, country, latitude, longitude "
            f"from ctgov.facilities where {' and '.join(facility_where)} order by id",
            facility_params,
        )
        facility_ids = [row["id"] for row in facilities]
        if facility_ids:
            facility_people = await fetch(
                """
                select facility_id, nct_id, name, 'CONTACT'::varchar as role
                from ctgov.facility_contacts
                where facility_id = any(%s) and name is not null
                union all
                select facility_id, nct_id, name, role
                from ctgov.facility_investigators
                where facility_id = any(%s) and name is not null
                """,
                [facility_ids, facility_ids],
            )
        else:
            facility_people = []
        officials = await fetch(
            "select nct_id, name, role, affiliation from ctgov.overall_officials "
            "where nct_id = any(%s) order by id",
            [nct_ids],
        )
        references = await fetch(
            "select nct_id, pmid, reference_type, citation from ctgov.study_references "
            "where nct_id = any(%s) order by id",
            [nct_ids],
        )
        return _build_study_payloads(
            nct_ids=nct_ids,
            core=core,
            conditions=conditions,
            interventions=interventions,
            sponsors=sponsors,
            facilities=facilities,
            facility_people=facility_people,
            officials=officials,
            references=references,
        )


def _build_study_payloads(
    *,
    nct_ids: list[str],
    core: list[dict[str, Any]],
    conditions: list[dict[str, Any]],
    interventions: list[dict[str, Any]],
    sponsors: list[dict[str, Any]],
    facilities: list[dict[str, Any]],
    facility_people: list[dict[str, Any]],
    officials: list[dict[str, Any]],
    references: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Assemble the ClinicalTrials.gov-compatible response slice used by the app."""
    by_id = {row["nct_id"]: row for row in core}
    conditions_by_nct: dict[str, list[str]] = defaultdict(list)
    interventions_by_nct: dict[str, list[dict[str, Any]]] = defaultdict(list)
    sponsors_by_nct: dict[str, dict[str, Any]] = {}
    locations_by_nct: dict[str, list[dict[str, Any]]] = defaultdict(list)
    location_by_id: dict[int, dict[str, Any]] = {}
    officials_by_nct: dict[str, list[dict[str, Any]]] = defaultdict(list)
    references_by_nct: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for row in conditions:
        if row.get("name"):
            conditions_by_nct[row["nct_id"]].append(row["name"])
    for row in interventions:
        interventions_by_nct[row["nct_id"]].append(
            {"name": row.get("name"), "type": row.get("intervention_type")}
        )
    for row in sponsors:
        sponsors_by_nct.setdefault(
            row["nct_id"],
            {"name": row.get("name"), "class": row.get("agency_class")},
        )
    for row in facilities:
        location = {
            "facility": row.get("name"),
            "status": row.get("status"),
            "city": row.get("city"),
            "state": row.get("state"),
            "zip": row.get("zip"),
            "country": row.get("country"),
            "geoPoint": {
                "lat": float(row["latitude"]) if row.get("latitude") is not None else None,
                "lon": float(row["longitude"]) if row.get("longitude") is not None else None,
            },
            "contacts": [],
        }
        locations_by_nct[row["nct_id"]].append(location)
        location_by_id[row["id"]] = location
    seen_people: dict[int, set[tuple[str, str]]] = defaultdict(set)
    for row in facility_people:
        location = location_by_id.get(row["facility_id"])
        if location is None:
            continue
        key = ((row.get("name") or "").lower(), (row.get("role") or "CONTACT").upper())
        if key in seen_people[row["facility_id"]]:
            continue
        seen_people[row["facility_id"]].add(key)
        location["contacts"].append(
            {"name": row.get("name"), "role": row.get("role") or "CONTACT"}
        )
    for row in officials:
        officials_by_nct[row["nct_id"]].append(
            {
                "name": row.get("name"),
                "role": row.get("role"),
                "affiliation": row.get("affiliation"),
            }
        )
    for row in references:
        references_by_nct[row["nct_id"]].append(
            {
                "pmid": row.get("pmid"),
                "type": row.get("reference_type"),
                "citation": row.get("citation"),
            }
        )

    output = []
    for nct_id in nct_ids:
        row = by_id.get(nct_id)
        if row is None:
            continue
        phase = row.get("phase")
        output.append(
            {
                "protocolSection": {
                    "identificationModule": {
                        "nctId": nct_id,
                        "briefTitle": row.get("brief_title"),
                        "officialTitle": row.get("official_title"),
                    },
                    "statusModule": {
                        "overallStatus": row.get("overall_status"),
                        "startDateStruct": {"date": _iso(row.get("start_date"))},
                        "completionDateStruct": {"date": _iso(row.get("completion_date"))},
                        "primaryCompletionDateStruct": {
                            "date": _iso(row.get("primary_completion_date"))
                        },
                        "lastUpdatePostDateStruct": {
                            "date": _iso(row.get("last_update_posted_date"))
                        },
                    },
                    "designModule": {
                        "phases": [phase] if phase else [],
                        "studyType": row.get("study_type"),
                        "enrollmentInfo": {"count": row.get("enrollment")},
                    },
                    "conditionsModule": {"conditions": conditions_by_nct[nct_id]},
                    "armsInterventionsModule": {
                        "interventions": interventions_by_nct[nct_id]
                    },
                    "sponsorCollaboratorsModule": {
                        "leadSponsor": sponsors_by_nct.get(nct_id, {})
                    },
                    "contactsLocationsModule": {
                        "overallOfficials": officials_by_nct[nct_id],
                        "locations": locations_by_nct[nct_id],
                    },
                    "referencesModule": {"references": references_by_nct[nct_id]},
                }
            }
        )
    return output


def get_clinical_trials_router(store: AACTStore | None = None) -> APIRouter:
    """Create the secured AACT router; the caller supplies authentication dependencies."""
    router = APIRouter(prefix="/clinical-trials", tags=["Clinical Trials"])
    aact = store or AACTStore()

    @router.get("/version")
    async def version() -> dict[str, Any]:
        try:
            return await aact.version()
        except (psycopg.Error, OSError) as exc:
            raise HTTPException(status_code=503, detail="AACT database is unavailable") from exc

    @router.get("/studies")
    async def studies(
        query_condition: str | None = Query(None, alias="query.cond", min_length=1),
        query_intervention: str | None = Query(None, alias="query.intr", min_length=1),
        query_term: str | None = Query(None, alias="query.term", min_length=1),
        query_location: str | None = Query(None, alias="query.locn"),
        query_city: str | None = Query(None, alias="query.city"),
        query_state: str | None = Query(None, alias="query.state"),
        filter_geo: str | None = Query(None, alias="filter.geo"),
        filter_status: str | None = Query(None, alias="filter.overallStatus"),
        page_size: int = Query(100, alias="pageSize", ge=1, le=1000),
        count_total: bool = Query(True, alias="countTotal"),
        response_format: str = Query("json", alias="format", pattern="^json$"),
    ) -> dict[str, Any]:
        del count_total, response_format
        searches = [
            ("condition", query_condition),
            ("intervention", query_intervention),
            ("term", query_term),
        ]
        selected = [(kind, value.strip()) for kind, value in searches if value and value.strip()]
        if len(selected) != 1:
            raise HTTPException(status_code=422, detail="Provide exactly one search query")
        search_type, query = selected[0]
        states = [value.strip() for value in (query_state or "").split("|") if value.strip()]
        try:
            return await aact.search(
                query=query,
                search_type=search_type,
                statuses=_parse_statuses(filter_status),
                page_size=page_size,
                geo=_parse_geo_filter(filter_geo),
                facility=query_location,
                city=query_city,
                states=states,
            )
        except psycopg.Error as exc:
            raise HTTPException(status_code=503, detail="AACT search is unavailable") from exc

    return router
