from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parent.parent


def test_api_unit_refuses_incomplete_aact_or_bundle_transition() -> None:
    unit = (REPOSITORY_ROOT / "deploy/systemd/cms-api.service").read_text(
        encoding="utf-8"
    )
    aact_guard = (
        "ExecStartPre=+/usr/bin/test ! -e "
        "/srv/cms-data-platform/production/aact-transition-pending"
    )
    bundle_guard = (
        "ExecStartPre=+/usr/bin/python3 "
        "/srv/cms-data-platform/production-ops/current/pipeline/production_manager.py "
        "startup-check --production-root /srv/cms-data-platform/production"
    )

    assert aact_guard in unit
    assert bundle_guard in unit
    assert unit.index(aact_guard) < unit.index(bundle_guard)
    assert "WorkingDirectory=/srv/cms-data-platform/production/release-current/code" in unit
    assert "ExecStart=/srv/cms-data-platform/production/release-current/runtime/bin/python" in unit
