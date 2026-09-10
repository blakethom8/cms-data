"""Correlation echo and access logging, including the 304 short-circuit."""

import logging
import sys
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT))

from auth import make_key_resolver
from release_info import REPRESENTATION_VERSION, ReleaseCacheMiddleware, ReleaseMetadata
from request_context import (
    REQUEST_ID_HEADER,
    RequestContextMiddleware,
    validate_request_id,
)

RELEASE = ReleaseMetadata(release_id="deployment-20260804T163418Z-2ad954a774")
ETAG = f'"deployment-20260804T163418Z-2ad954a774:{REPRESENTATION_VERSION}"'
SHARED = "shared-secret"
SCOPED = "ps-prod:prod-value"


def _app(*, with_cache: bool = False) -> TestClient:
    """Mirror main.py's wiring, including middleware ordering."""

    resolve = make_key_resolver(SHARED, SCOPED)
    app = FastAPI()

    @app.get("/practices/capabilities")
    async def capabilities():
        return {"contract_version": 2}

    @app.get("/boom")
    async def boom():
        raise RuntimeError("deliberate failure")

    if with_cache:
        app.add_middleware(
            ReleaseCacheMiddleware,
            resolve_metadata=lambda: RELEASE,
            is_authorized=lambda request: resolve(request.headers.get("X-API-Key")) is not None,
        )
    # Added last => outermost, so it wraps the cache middleware.
    app.add_middleware(
        RequestContextMiddleware,
        resolve_key_name=resolve,
        resolve_release=lambda: RELEASE,
    )
    return TestClient(app, raise_server_exceptions=False)


def test_supplied_request_id_is_echoed_unchanged() -> None:
    client = _app()
    response = client.get(
        "/practices/capabilities",
        headers={"X-API-Key": "prod-value", REQUEST_ID_HEADER: "abc-123"},
    )

    assert response.status_code == 200
    assert response.headers[REQUEST_ID_HEADER] == "abc-123"


def test_missing_request_id_is_generated_so_logs_are_always_joinable() -> None:
    client = _app()
    response = client.get("/practices/capabilities", headers={"X-API-Key": "prod-value"})

    generated = response.headers[REQUEST_ID_HEADER]
    assert generated and generated != "-"
    assert validate_request_id(generated) == generated


@pytest.mark.parametrize(
    "bad", ["with space", "new\nline", "", "   ", "x" * 200, "-leading-dash-ok?"]
)
def test_unsafe_caller_ids_are_not_echoed_back(bad: str) -> None:
    """A caller must not be able to inject arbitrary text into our logs."""

    client = _app()
    response = client.get(
        "/practices/capabilities",
        headers={"X-API-Key": "prod-value", REQUEST_ID_HEADER: bad},
    )
    echoed = response.headers[REQUEST_ID_HEADER]
    assert echoed != bad
    assert validate_request_id(echoed) == echoed


def test_the_304_short_circuit_still_echoes_and_logs(caplog) -> None:
    """The gotcha: 304 returns before the route, and must not lose correlation."""

    client = _app(with_cache=True)
    with caplog.at_level(logging.INFO, logger="api.access"):
        response = client.get(
            "/practices/capabilities",
            headers={
                "X-API-Key": "prod-value",
                "If-None-Match": ETAG,
                REQUEST_ID_HEADER: "trace-me",
            },
        )

    assert response.status_code == 304
    assert not response.content
    assert response.headers[REQUEST_ID_HEADER] == "trace-me"

    line = caplog.text
    assert "-> 304" in line
    assert "request_id=trace-me" in line
    assert "key=ps-prod" in line


def test_log_line_carries_key_name_release_and_never_the_key_value(caplog) -> None:
    client = _app()
    with caplog.at_level(logging.INFO, logger="api.access"):
        client.get(
            "/practices/capabilities",
            headers={"X-API-Key": "prod-value", REQUEST_ID_HEADER: "abc-123"},
        )

    line = caplog.text
    assert "GET /practices/capabilities -> 200" in line
    assert "key=ps-prod" in line
    assert "request_id=abc-123" in line
    assert "release=deployment-20260804T163418Z-2ad954a774" in line
    assert "duration_ms=" in line
    assert "prod-value" not in line, "the key value must never reach a log line"


def test_shared_key_is_logged_under_its_own_name(caplog) -> None:
    """Migration visibility: you can see who has not adopted a scoped key yet."""

    client = _app()
    with caplog.at_level(logging.INFO, logger="api.access"):
        client.get("/practices/capabilities", headers={"X-API-Key": SHARED})

    assert "key=shared" in caplog.text
    assert SHARED not in caplog.text


def test_rejected_requests_are_logged_without_a_consumer_name(caplog) -> None:
    client = _app()
    with caplog.at_level(logging.INFO, logger="api.access"):
        client.get("/practices/capabilities", headers={"X-API-Key": "wrong-value"})

    assert "key=-" in caplog.text
    assert "wrong-value" not in caplog.text, "a rejected key must not be logged either"


def test_a_failing_route_is_still_logged(caplog) -> None:
    client = _app()
    with caplog.at_level(logging.INFO, logger="api.access"):
        response = client.get("/boom", headers={"X-API-Key": "prod-value"})

    assert response.status_code == 500
    assert "GET /boom -> 500" in caplog.text
