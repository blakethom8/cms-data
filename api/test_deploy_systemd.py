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


def test_api_unit_requires_private_network_boundary() -> None:
    unit = (REPOSITORY_ROOT / "deploy/systemd/cms-api.service").read_text(
        encoding="utf-8"
    )

    assert "Requires=cms-private-firewall.service wg-quick@wg-cms.service" in unit
    assert "BindsTo=wg-quick@wg-cms.service" in unit
    assert "--host 10.77.0.1 --port 8080" in unit
    assert "--host 0.0.0.0" not in unit
    assert "LogRateLimitIntervalSec=30s" in unit


def test_loopback_smoke_proxy_does_not_reopen_the_public_api() -> None:
    socket = (REPOSITORY_ROOT / "deploy/systemd/cms-api-loopback.socket").read_text(
        encoding="utf-8"
    )
    service = (
        REPOSITORY_ROOT / "deploy/systemd/cms-api-loopback.service"
    ).read_text(encoding="utf-8")

    assert "ListenStream=127.0.0.1:8080" in socket
    assert "ListenStream=0.0.0.0" not in socket
    assert "systemd-socket-proxyd 10.77.0.1:8080" in service
    assert "Requires=cms-api.service" in service
