import importlib.util
from pathlib import Path


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "dashboard"
    / "command-center"
    / "dev_server.py"
)
SPEC = importlib.util.spec_from_file_location("command_center_dev_server", MODULE_PATH)
assert SPEC and SPEC.loader
DEV_SERVER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(DEV_SERVER)


def _handler(path: str):
    handler = DEV_SERVER.CommandCenterHandler.__new__(DEV_SERVER.CommandCenterHandler)
    handler.path = path
    return handler


def test_provider_evidence_compat_groups_rows_and_reports_missing_tables() -> None:
    handler = _handler(
        "/api/explorer/provider-evidence?npis=1710390513,1962509216&limit=5"
    )
    queries: list[str] = []

    def post_query(sql: str, limit: int) -> dict:
        queries.append(sql)
        if "information_schema.tables" in sql:
            return {"rows": [["raw_nppes"]]}
        if "information_schema.columns" in sql:
            return {"rows": [["raw_nppes", "ingested_at"]]}
        assert "* REPLACE(CAST(\"ingested_at\" AS VARCHAR) AS \"ingested_at\")" in sql
        assert limit == 10
        return {
            "columns": ["__requested_npi", "npi", "ingested_at"],
            "rows": [
                ["1710390513", "1710390513", "2026-07-22 00:00:00+00"],
                ["1962509216", "1962509216", "2026-07-22 00:00:00+00"],
            ],
        }

    response: dict = {}
    handler._post_query_json = post_query
    handler._send_json = lambda status, value, include_body: response.update(
        status=status, value=value, include_body=include_body
    )

    handler._serve_provider_evidence_compat(include_body=True)

    assert response["status"] == 200
    assert response["include_body"] is True
    nppes = next(
        source for source in response["value"]["sources"] if source["key"] == "nppes"
    )
    assert nppes["providers"]["1710390513"] == {
        "columns": ["npi", "ingested_at"],
        "rows": [["1710390513", "2026-07-22 00:00:00+00"]],
    }
    ppef = next(
        source
        for source in response["value"]["sources"]
        if source["key"] == "ppef_reassignment"
    )
    assert ppef["availability"] == "unavailable"
    assert "raw_pecos_reassignment" in ppef["missing_tables"]
    assert len(queries) == 3


def test_provider_evidence_compat_rejects_invalid_npi_without_querying() -> None:
    handler = _handler("/api/explorer/provider-evidence?npis=not-an-npi")
    response: dict = {}
    handler._post_query_json = lambda *_: (_ for _ in ()).throw(
        AssertionError("query should not run")
    )
    handler._send_json = lambda status, value, include_body: response.update(
        status=status, value=value
    )

    handler._serve_provider_evidence_compat(include_body=True)

    assert response["status"] == 422
