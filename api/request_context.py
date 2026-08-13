"""Request correlation and per-request access logging.

Consumers mint an `X-Request-ID` per inbound request and log it beside the user
who caused it. When that ID is forwarded to this API and echoed back, a single
identifier joins the two systems' logs: their line carries the actor, ours
carries which consumer key called, which release answered, and how long it
took. The user's identity never crosses the boundary — only an opaque token —
which is what keeps this compatible with the serving-box invariant that this
box holds zero client data.

The header name and validation pattern deliberately match the consumer's, so
one ID flows unchanged rather than being translated at the seam.

**Placement matters.** This middleware must sit *outside* `ReleaseCacheMiddleware`.
That one answers a matching `If-None-Match` with a 304 before the route runs;
wrapping it from outside means conditional requests — the ones most worth
tracing — still get their ID echoed and still produce a log line, with no
special case in the cache path.
"""

from __future__ import annotations

import logging
import re
import time
import uuid
from typing import Callable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from auth import API_KEY_HEADER

logger = logging.getLogger("api.access")

REQUEST_ID_HEADER = "X-Request-ID"
# Matches the consumer's validation so a forwarded ID survives the hop intact.
REQUEST_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
# Logged when a request presents no valid key (its own rejection is logged too).
UNKNOWN_KEY_NAME = "-"


def validate_request_id(value: str | None) -> str | None:
    """Return a caller-supplied ID only if it is conservative enough to log."""

    candidate = (value or "").strip()
    return candidate if REQUEST_ID_PATTERN.fullmatch(candidate) else None


class RequestContextMiddleware(BaseHTTPMiddleware):
    """Echo the correlation ID and log one line per request.

    Key *names* are logged; key values never are.
    """

    def __init__(
        self,
        app,
        resolve_key_name: Callable[[str | None], str | None],
        resolve_release: Callable[[], object] | None = None,
    ) -> None:
        super().__init__(app)
        self._resolve_key_name = resolve_key_name
        self._resolve_release = resolve_release

    def _release_id(self) -> str:
        if self._resolve_release is None:
            return "-"
        try:
            metadata = self._resolve_release()
        except Exception:  # noqa: BLE001 - observability must never break serving
            return "-"
        return getattr(metadata, "release_id", None) or "-"

    async def dispatch(self, request: Request, call_next) -> Response:
        request_id = validate_request_id(request.headers.get(REQUEST_ID_HEADER))
        echoed = request_id is not None
        if request_id is None:
            # Give every request an ID so our own logs are always joinable,
            # even when the caller did not supply one.
            request_id = uuid.uuid4().hex
        key_name = self._resolve_key_name(request.headers.get(API_KEY_HEADER))

        request.state.request_id = request_id
        request.state.api_key_name = key_name

        started = time.perf_counter()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
        finally:
            logger.info(
                "%s %s -> %s key=%s request_id=%s caller_supplied_id=%s "
                "release=%s duration_ms=%.1f pool_wait_ms=%s pool_result=%s",
                request.method,
                request.url.path,
                status,
                key_name or UNKNOWN_KEY_NAME,
                request_id,
                "yes" if echoed else "no",
                self._release_id(),
                (time.perf_counter() - started) * 1000,
                (
                    f"{request.state.pool_wait_ms:.2f}"
                    if hasattr(request.state, "pool_wait_ms")
                    else "-"
                ),
                getattr(request.state, "pool_result", "not_applicable"),
            )

        response.headers[REQUEST_ID_HEADER] = request_id
        return response
