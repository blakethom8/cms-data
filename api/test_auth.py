import sys
from pathlib import Path

import pytest
from fastapi import HTTPException

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT))

from auth import (
    OPEN_ACCESS_NAME,
    SHARED_KEY_NAME,
    configured_consumer_names,
    make_key_resolver,
    parse_consumer_names,
    parse_scoped_keys,
)


# --- Shared key (must keep working until the owner retires it) ---------------


def test_no_configuration_means_open_access() -> None:
    resolve = make_key_resolver("")

    assert resolve(None) == OPEN_ACCESS_NAME
    assert resolve("anything") == OPEN_ACCESS_NAME


def test_shared_key_requires_exact_match_and_reports_its_name() -> None:
    resolve = make_key_resolver("secret")

    assert resolve("secret") == SHARED_KEY_NAME
    assert resolve(None) is None
    assert resolve("") is None
    assert resolve("Secret") is None
    assert resolve("secret ") is None


# --- Scoped keys -------------------------------------------------------------


def test_scoped_keys_resolve_to_their_consumer_name() -> None:
    resolve = make_key_resolver(
        "shared-secret", "ps-prod:prod-value,ps-dev:dev-value,command-center:cc-value"
    )

    assert resolve("prod-value") == "ps-prod"
    assert resolve("dev-value") == "ps-dev"
    assert resolve("cc-value") == "command-center"
    assert resolve("nope") is None


def test_shared_key_stays_valid_alongside_scoped_keys() -> None:
    """The compatibility rule: no simultaneous-break window during migration."""

    resolve = make_key_resolver("shared-secret", "ps-prod:prod-value")

    assert resolve("shared-secret") == SHARED_KEY_NAME
    assert resolve("prod-value") == "ps-prod"


def test_a_consumer_may_hold_two_keys_during_rotation() -> None:
    """Overlap rotation: issue new, migrate, retire old — never a broken window."""

    resolve = make_key_resolver("", "ps-prod:current-value,ps-prod:next-value")

    assert resolve("current-value") == "ps-prod"
    assert resolve("next-value") == "ps-prod"

    # Retiring the old key is removal from the mapping.
    after_retirement = make_key_resolver("", "ps-prod:next-value")
    assert after_retirement("next-value") == "ps-prod"
    assert after_retirement("current-value") is None


def test_scoped_keys_alone_are_enough_to_close_open_access() -> None:
    resolve = make_key_resolver("", "ps-prod:prod-value")

    assert resolve("prod-value") == "ps-prod"
    assert resolve(None) is None
    assert resolve("anything-else") is None


# --- Configuration is rejected rather than silently misread ------------------


@pytest.mark.parametrize(
    "raw",
    [
        "ps-prod",  # no separator
        "ps-prod:",  # empty value would match a missing header
        ":value",  # no name
        "  :  ",  # blank both
        "PS-PROD:value",  # names are lowercase by pattern
        "ps prod:value",  # whitespace could forge a log line
        "ps\nprod:value",  # newline could forge a log record
    ],
)
def test_malformed_mappings_raise(raw: str) -> None:
    with pytest.raises(ValueError):
        parse_scoped_keys(raw)


def test_one_value_cannot_serve_two_consumers() -> None:
    """Otherwise the logged consumer name would be a guess."""

    with pytest.raises(ValueError, match="cannot belong to both"):
        parse_scoped_keys("ps-prod:same-value,ps-dev:same-value")


def test_shared_key_cannot_double_as_a_scoped_key() -> None:
    with pytest.raises(ValueError, match="ambiguous"):
        make_key_resolver("shared-secret", "ps-prod:shared-secret")


def test_blank_entries_and_whitespace_are_tolerated() -> None:
    resolve = make_key_resolver("", " ps-prod : prod-value , , ps-dev:dev-value ")

    assert resolve("prod-value") == "ps-prod"
    assert resolve("dev-value") == "ps-dev"


def test_configured_names_are_safe_to_log_and_exclude_values() -> None:
    names = configured_consumer_names("shared-secret", "ps-prod:prod-value,ps-dev:dev-value")

    assert names == ["ps-dev", "ps-prod", SHARED_KEY_NAME]
    assert "prod-value" not in names and "shared-secret" not in names


def test_consumer_allowlist_is_normalized_and_contains_no_key_values() -> None:
    assert parse_consumer_names(" command-center,ops,command-center ") == {
        "command-center",
        "ops",
    }
    assert parse_consumer_names("") == frozenset()


@pytest.mark.parametrize("raw", ["shared key", "UPPER", "bad:name", "line\nbreak"])
def test_consumer_allowlist_rejects_unsafe_names(raw: str) -> None:
    with pytest.raises(ValueError, match="Invalid consumer name"):
        parse_consumer_names(raw)


# --- Both enforcement points must follow the same resolver -------------------


@pytest.mark.anyio
async def test_main_wires_dependency_and_middleware_to_one_resolver(monkeypatch) -> None:
    """Scoped keys must never be honored in one place and rejected in another."""

    import main

    # The context middleware binds the resolver object when it is added, while
    # the dependency looks the module attribute up per call. Both must be the
    # one resolver built at startup, so capture it before patching.
    startup_resolver = main.resolve_api_key_name

    seen: list[str | None] = []

    def strict(presented: str | None) -> str | None:
        seen.append(presented)
        return "ps-prod" if presented == "scoped-key" else None

    monkeypatch.setattr(main, "resolve_api_key_name", strict)

    with pytest.raises(HTTPException) as denied:
        await main.check_api_key("wrong")
    assert denied.value.status_code == 401
    assert await main.check_api_key("scoped-key") is None

    class FakeRequest:
        def __init__(self, key: str | None) -> None:
            self.headers = {"X-API-Key": key} if key is not None else {}

    cache = next(
        m for m in main.app.user_middleware if m.cls.__name__ == "ReleaseCacheMiddleware"
    )
    is_authorized = cache.kwargs["is_authorized"]
    assert is_authorized(FakeRequest("scoped-key")) is True
    assert is_authorized(FakeRequest("wrong")) is False

    context = next(
        m for m in main.app.user_middleware if m.cls.__name__ == "RequestContextMiddleware"
    )
    assert context.kwargs["resolve_key_name"] is startup_resolver

    assert seen == ["wrong", "scoped-key", "scoped-key", "wrong"]


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"
