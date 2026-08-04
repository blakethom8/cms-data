"""Response-shape snapshots that make `representation_version` enforceable.

`representation_version` names the shape of data responses. Consumers cache
keyed on `release_id` + `representation_version`, so a response-shape change
that ships *without* a bump serves wrong data from a correct cache. That rule
was documented but nothing enforced it — remembering to bump was discipline.

This module turns the rule into a test. It renders the live FastAPI app's
response schemas into a normalized snapshot, committed as
`response_shapes/v<representation_version>.json`. Because the filename carries
the version, the enforcement falls out of a single comparison:

* change a response shape without bumping → the snapshot for the current
  version no longer matches, and the test says so;
* bump the version → no snapshot exists for the new version yet, and the test
  says to write one;
* write it → green, and the new shape is recorded against the version that
  names it.

**What counts as a change.** Only responses of *already-published* operations.
Adding a new endpoint cannot invalidate a cache entry that nobody holds, so new
paths are allowed without a bump. Editing an existing response — adding,
removing, renaming, or retyping a field, at any nesting depth — is a change,
because a consumer holding a cached copy under the old version would keep
serving it. `$ref`s are resolved inline so a change to a nested model is
visible at every operation that returns it.

Regenerate after an intentional change:

    cd api && ../.venv/bin/python response_shapes.py --write
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

SNAPSHOT_DIR = Path(__file__).resolve().parent / "response_shapes"
JSON_CONTENT_TYPE = "application/json"
# Emitted in place of a schema that refers back to itself. Recursive models
# would otherwise expand forever; the marker keeps the snapshot finite while
# still changing if the cycle's entry point changes.
CYCLE_MARKER = "<recursive>"


def snapshot_path(representation_version: int) -> Path:
    return SNAPSHOT_DIR / f"v{representation_version}.json"


def _resolve(schema: Any, components: dict[str, Any], seen: tuple[str, ...]) -> Any:
    """Inline `$ref`s so nested model changes surface at the operation level."""

    if isinstance(schema, list):
        return [_resolve(item, components, seen) for item in schema]
    if not isinstance(schema, dict):
        return schema

    reference = schema.get("$ref")
    if isinstance(reference, str):
        name = reference.rsplit("/", 1)[-1]
        if name in seen:
            return CYCLE_MARKER
        target = components.get(name)
        if target is None:
            # An unresolvable ref is itself worth noticing rather than hiding.
            return {"$ref": reference}
        resolved = _resolve(target, components, (*seen, name))
        # Preserve any sibling keys (e.g. a description alongside the ref).
        siblings = {key: value for key, value in schema.items() if key != "$ref"}
        if siblings and isinstance(resolved, dict):
            return {**resolved, **_resolve(siblings, components, seen)}
        return resolved

    return {key: _resolve(value, components, seen) for key, value in schema.items()}


def build_response_shapes(app) -> dict[str, Any]:
    """Return {path: {method: {status: resolved JSON response schema}}}."""

    document = app.openapi()
    components = document.get("components", {}).get("schemas", {})
    shapes: dict[str, Any] = {}

    for path, operations in document.get("paths", {}).items():
        if not isinstance(operations, dict):
            continue
        for method, operation in operations.items():
            if not isinstance(operation, dict):
                continue
            by_status: dict[str, Any] = {}
            for status, response in (operation.get("responses") or {}).items():
                schema = (
                    (response or {}).get("content", {}).get(JSON_CONTENT_TYPE, {}).get("schema")
                )
                if schema is None:
                    continue
                by_status[str(status)] = _resolve(schema, components, ())
            if by_status:
                shapes.setdefault(path, {})[method.lower()] = by_status

    return shapes


def serialize(shapes: dict[str, Any]) -> str:
    return json.dumps(shapes, indent=2, sort_keys=True) + "\n"


def load_snapshot(representation_version: int) -> dict[str, Any] | None:
    path = snapshot_path(representation_version)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def compare(
    recorded: dict[str, Any], current: dict[str, Any]
) -> tuple[list[str], list[str], list[str]]:
    """Return (changed, removed, added) operation keys.

    `changed` and `removed` require a `representation_version` bump; `added`
    does not, because no consumer can hold a cached response for an operation
    that did not exist.
    """

    changed: list[str] = []
    removed: list[str] = []
    added: list[str] = []

    for path, methods in recorded.items():
        for method, statuses in methods.items():
            key = f"{method.upper()} {path}"
            current_statuses = current.get(path, {}).get(method)
            if current_statuses is None:
                removed.append(key)
            elif current_statuses != statuses:
                changed.append(key)

    for path, methods in current.items():
        for method in methods:
            if recorded.get(path, {}).get(method) is None:
                added.append(f"{method.upper()} {path}")

    return sorted(changed), sorted(removed), sorted(added)


def _load_app():
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import main
    from release_info import REPRESENTATION_VERSION

    return main.app, REPRESENTATION_VERSION


def main_cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--write",
        action="store_true",
        help="Write the snapshot for the current representation_version",
    )
    args = parser.parse_args(argv)

    app, version = _load_app()
    shapes = build_response_shapes(app)
    path = snapshot_path(version)

    if args.write:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(serialize(shapes), encoding="utf-8")
        print(f"Wrote {path.relative_to(Path.cwd())} for representation_version {version}")
        return 0

    recorded = load_snapshot(version)
    if recorded is None:
        print(f"No snapshot for representation_version {version}; run with --write")
        return 1
    changed, removed, added = compare(recorded, shapes)
    if changed or removed:
        print(f"Response shapes differ from v{version}: {changed + removed}")
        return 1
    print(f"Response shapes match v{version} ({len(added)} new operation(s))")
    return 0


if __name__ == "__main__":
    raise SystemExit(main_cli())
