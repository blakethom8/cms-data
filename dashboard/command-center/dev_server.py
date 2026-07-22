"""Serve the Command Center locally and proxy read-only CMS API requests.

The browser calls same-origin ``/api/*`` URLs. This development server forwards
GET and HEAD requests to the configured CMS API and injects ``CMS_API_KEY`` on
the server side, so credentials never enter browser code.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, unquote, urlparse
from urllib.request import Request, urlopen


STATIC_DIRECTORY = Path(__file__).resolve().parent
REPOSITORY_ROOT = STATIC_DIRECTORY.parent.parent
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from api.explorer import CATALOG, SAMPLES, STATE_ONLY_SAMPLES


CATALOG_BY_KEY = {entry["key"]: entry for entry in CATALOG}
TIMEZONE_SAMPLE_KEYS = {
    "nppes",
    "physician_by_provider",
    "physician_by_service",
    "part_d_by_drug",
    "open_payments_general",
    "open_payments_research",
    "open_payments_ownership",
    "reassignment",
    "dme_referring",
}
HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
}


class CommandCenterHandler(SimpleHTTPRequestHandler):
    api_base_url: str
    api_key: str

    def do_GET(self) -> None:  # noqa: N802 - stdlib handler contract
        if self.path == "/api" or self.path.startswith("/api/"):
            self._proxy_read_request(include_body=True)
            return
        super().do_GET()

    def do_HEAD(self) -> None:  # noqa: N802 - stdlib handler contract
        if self.path == "/api" or self.path.startswith("/api/"):
            self._proxy_read_request(include_body=False)
            return
        super().do_HEAD()

    def _proxy_read_request(self, *, include_body: bool) -> None:
        compatibility_query = self._sample_compatibility_query()
        if compatibility_query is not None:
            sql, limit = compatibility_query
            self._proxy_sql_query(sql, limit, include_body=include_body)
            return

        upstream_path = self.path.removeprefix("/api") or "/"
        target = f"{self.api_base_url.rstrip('/')}{upstream_path}"
        headers = {"Accept": self.headers.get("Accept", "application/json")}
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        request = Request(target, headers=headers, method="GET" if include_body else "HEAD")

        try:
            with urlopen(request, timeout=60) as response:
                body = response.read() if include_body else b""
                self._send_upstream_response(response.status, response.headers.items(), body)
        except HTTPError as error:
            body = error.read() if include_body else b""
            self._send_upstream_response(error.code, error.headers.items(), body)
        except URLError as error:
            payload = json.dumps(
                {
                    "detail": "CMS API is unavailable through the configured local tunnel.",
                    "reason": str(error.reason),
                }
            ).encode("utf-8")
            self.send_response(502)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            if include_body:
                self.wfile.write(payload)

    def _sample_compatibility_query(self) -> tuple[str, int] | None:
        """Serve new sample routes through the deployed read-only query endpoint.

        This lets the local Command Center preview a newer catalog UI before the
        matching explorer routes are deployed to the remote API.
        """
        parsed = urlparse(self.path)
        all_prefix = "/api/explorer/sample-all/"
        curated_prefix = "/api/explorer/sample/"
        if parsed.path.startswith(all_prefix):
            key = unquote(parsed.path.removeprefix(all_prefix))
            entry = CATALOG_BY_KEY.get(key)
            if not entry:
                return None
            limit = self._sample_limit(parsed.query)
            projection = "*"
            if key in TIMEZONE_SAMPLE_KEYS:
                # The currently deployed query service does not have pytz, so
                # DuckDB cannot materialize TIMESTAMP WITH TIME ZONE values.
                # Preserve the physical column and its position as ISO text in
                # the local preview bridge until the explorer route is deployed.
                projection = "* REPLACE(CAST(ingested_at AS VARCHAR) AS ingested_at)"
            return (
                f'SELECT {projection} FROM "{entry["table"]}" LIMIT {limit}',
                limit,
            )

        if not parsed.path.startswith(curated_prefix):
            return None
        key = unquote(parsed.path.removeprefix(curated_prefix))
        sql = SAMPLES.get(key)
        if not sql:
            return None
        query = parse_qs(parsed.query)
        city = query.get("city", ["Los Angeles"])[0]
        state = query.get("state", ["CA"])[0].upper()
        if not re.fullmatch(r"[A-Za-z .'-]{1,80}", city) or not re.fullmatch(
            r"[A-Z]{2}", state
        ):
            return "SELECT NULL WHERE FALSE", 1
        params = [state, state] if key in STATE_ONLY_SAMPLES else [city, state]
        for value in params:
            sql = sql.replace("?", self._sql_literal(value), 1)
        limit = self._sample_limit(parsed.query)
        return f"{sql.strip().rstrip(';')} LIMIT {limit}", limit

    @staticmethod
    def _sample_limit(query_string: str) -> int:
        raw = parse_qs(query_string).get("limit", ["50"])[0]
        try:
            return max(1, min(int(raw), 200))
        except ValueError:
            return 50

    @staticmethod
    def _sql_literal(value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    def _proxy_sql_query(self, sql: str, limit: int, *, include_body: bool) -> None:
        target = f"{self.api_base_url.rstrip('/')}/query"
        payload = json.dumps({"sql": sql, "limit": limit}).encode("utf-8")
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        request = Request(target, data=payload, headers=headers, method="POST")
        try:
            with urlopen(request, timeout=60) as response:
                body = response.read() if include_body else b""
                self._send_upstream_response(response.status, response.headers.items(), body)
        except HTTPError as error:
            body = error.read() if include_body else b""
            self._send_upstream_response(error.code, error.headers.items(), body)
        except URLError as error:
            payload = json.dumps(
                {"detail": "CMS API query endpoint is unavailable.", "reason": str(error.reason)}
            ).encode("utf-8")
            self.send_response(502)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            if include_body:
                self.wfile.write(payload)

    def _send_upstream_response(self, status: int, headers, body: bytes) -> None:
        self.send_response(status)
        for name, value in headers:
            if name.lower() not in HOP_BY_HOP_HEADERS and name.lower() != "content-length":
                self.send_header(name, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)


def main() -> None:
    parser = argparse.ArgumentParser(description="Serve the CMS Data Command Center locally")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=4199)
    parser.add_argument(
        "--api-base-url",
        default=os.getenv("CMS_API_BASE_URL", "http://127.0.0.1:9080"),
    )
    args = parser.parse_args()

    handler = partial(CommandCenterHandler, directory=str(STATIC_DIRECTORY))
    CommandCenterHandler.api_base_url = args.api_base_url
    CommandCenterHandler.api_key = os.getenv("CMS_API_KEY", "")
    server = ThreadingHTTPServer((args.host, args.port), handler)
    print(
        f"CMS Data Command Center: http://{args.host}:{args.port} "
        f"(API: {args.api_base_url})"
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
