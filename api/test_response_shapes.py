"""Make `representation_version` mechanically enforceable.

Consumers cache data responses keyed on `release_id` + `representation_version`.
A response-shape change shipped without a version bump therefore serves wrong
data out of a correct cache — the same class of obligation as the
`contract_version` pin, but until now enforced only by remembering.

These tests compare the live app's response schemas against the snapshot
committed for the current `representation_version`. See `response_shapes.py`
for the rule and the regeneration command.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import main
from release_info import REPRESENTATION_VERSION
from response_shapes import (
    build_response_shapes,
    compare,
    load_snapshot,
    serialize,
    snapshot_path,
)

REGENERATE = "cd api && ../.venv/bin/python response_shapes.py --write"


@pytest.fixture(scope="module")
def current_shapes() -> dict:
    return build_response_shapes(main.app)


def test_a_snapshot_exists_for_the_current_representation_version() -> None:
    """Bumping the version obliges you to record what the new version means."""

    recorded = load_snapshot(REPRESENTATION_VERSION)
    assert recorded is not None, (
        f"representation_version is {REPRESENTATION_VERSION} but "
        f"{snapshot_path(REPRESENTATION_VERSION).name} does not exist.\n"
        f"If you just bumped the version, record the new shapes:\n    {REGENERATE}"
    )


def test_response_shapes_match_the_current_version_snapshot(current_shapes: dict) -> None:
    """A changed response shape must come with a bumped representation_version."""

    recorded = load_snapshot(REPRESENTATION_VERSION)
    if recorded is None:
        pytest.skip("covered by the snapshot-exists test")

    changed, removed, added = compare(recorded, current_shapes)

    assert not changed and not removed, (
        "Response shapes changed without a representation_version bump.\n"
        f"  changed: {changed or 'none'}\n"
        f"  removed: {removed or 'none'}\n\n"
        "Consumers cache on (release_id, representation_version), so a shape "
        "change under the same version serves stale data from a valid cache.\n"
        "Either revert the shape change, or bump REPRESENTATION_VERSION in "
        f"api/release_info.py and then run:\n    {REGENERATE}"
    )

    # Additions are deliberately allowed: no consumer can hold a cached
    # response for an operation that did not exist. Still record them so the
    # snapshot stays a complete description of the version.
    if added:
        pytest.fail(
            "New operations are not yet recorded in the snapshot: "
            f"{added}\nNo version bump is needed for additions — just run:\n"
            f"    {REGENERATE}"
        )


def test_snapshot_is_byte_identical_to_a_fresh_render(current_shapes: dict) -> None:
    """Guard against a hand-edited or stale-formatted snapshot file."""

    recorded_text = snapshot_path(REPRESENTATION_VERSION).read_text(encoding="utf-8")
    assert recorded_text == serialize(current_shapes), (
        "The committed snapshot is not byte-identical to a fresh render "
        "(hand-edited, or written by a different serializer).\n"
        f"Regenerate it:\n    {REGENERATE}"
    )


def test_practice_and_release_shapes_are_actually_covered(current_shapes: dict) -> None:
    """The snapshot is worthless if it misses the routes consumers depend on."""

    for path in ("/practices/search", "/practices/providers", "/practices/site-profile"):
        assert path in current_shapes, f"{path} must be covered by the shape snapshot"

    search = current_shapes["/practices/search"]["get"]["200"]["properties"]
    assert "contract_version" in search, "the practice contract pin must be in the snapshot"
    # Nested models must be inlined, or a change inside PracticeResult would be
    # invisible at the operation level.
    row = search["results"]["items"]["properties"]
    assert "site_id" in row and "organization_contexts" in row, (
        "nested response models must be resolved inline, not left as $ref"
    )

    release = current_shapes["/release"]["get"]["200"]
    assert release, "/release must be covered by the shape snapshot"


def test_detects_a_changed_field_type(current_shapes: dict) -> None:
    """Prove the comparison has teeth rather than passing vacuously."""

    recorded = load_snapshot(REPRESENTATION_VERSION)
    assert recorded is not None

    # Retype an existing field the way a careless edit would.
    mutated = {
        path: {method: dict(statuses) for method, statuses in methods.items()}
        for path, methods in recorded.items()
    }
    mutated["/practices/search"]["get"]["200"] = {
        **mutated["/practices/search"]["get"]["200"],
        "properties": {
            **mutated["/practices/search"]["get"]["200"]["properties"],
            "total": {"type": "string"},
        },
    }
    changed, removed, added = compare(mutated, current_shapes)
    assert "GET /practices/search" in changed, "a retyped field must be reported as changed"

    # A removed operation must also be caught.
    without_release = {
        path: methods for path, methods in recorded.items() if path != "/release"
    }
    changed, removed, added = compare(recorded, without_release)
    assert "GET /release" in removed, "a removed operation must be reported"

    # A brand-new operation must NOT be treated as a breaking change.
    with_new = {**current_shapes, "/brand-new": {"get": {"200": {"type": "object"}}}}
    changed, removed, added = compare(recorded, with_new)
    assert not changed and not removed, "adding an operation must not require a bump"
    assert "GET /brand-new" in added


def test_utilization_database_path_prefers_bundle_sidecar_and_preserves_rollback(
    tmp_path, monkeypatch
) -> None:
    warehouse = tmp_path / "warehouse"
    warehouse.write_bytes(b"warehouse")
    monkeypatch.delenv("UTILIZATION_DUCKDB_PATH", raising=False)

    assert main._resolve_utilization_db_path(str(warehouse)) == str(warehouse)

    utilization = tmp_path / "utilization"
    utilization.write_bytes(b"sidecar")
    assert main._resolve_utilization_db_path(str(warehouse)) == str(utilization)

    monkeypatch.setenv("UTILIZATION_DUCKDB_PATH", "/explicit/utilization.duckdb")
    assert main._resolve_utilization_db_path(str(warehouse)) == (
        "/explicit/utilization.duckdb"
    )
