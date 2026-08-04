import sys
from pathlib import Path

import pytest
from fastapi import HTTPException

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT))

from auth import make_key_validator


def test_empty_configured_key_means_open_access() -> None:
    is_valid = make_key_validator("")

    assert is_valid(None) is True
    assert is_valid("") is True
    assert is_valid("anything") is True


def test_configured_key_requires_exact_match() -> None:
    is_valid = make_key_validator("secret")

    assert is_valid("secret") is True
    assert is_valid(None) is False
    assert is_valid("") is False
    assert is_valid("Secret") is False
    assert is_valid("secret ") is False


@pytest.mark.anyio
async def test_main_wires_dependency_and_middleware_to_one_predicate(
    monkeypatch,
) -> None:
    """Both enforcement points must follow the shared predicate in lockstep."""

    import main

    seen: list[str | None] = []

    def strict(presented: str | None) -> bool:
        seen.append(presented)
        return presented == "scoped-key"

    monkeypatch.setattr(main, "is_valid_api_key", strict)

    # Route dependency follows the patched predicate.
    with pytest.raises(HTTPException) as denied:
        await main.check_api_key("wrong")
    assert denied.value.status_code == 401
    assert await main.check_api_key("scoped-key") is None

    # The cache middleware's is_authorized follows the same predicate.
    middleware = next(
        m for m in main.app.user_middleware if m.cls.__name__ == "ReleaseCacheMiddleware"
    )
    is_authorized = middleware.kwargs["is_authorized"]

    class FakeRequest:
        def __init__(self, key: str | None) -> None:
            self.headers = {"X-API-Key": key} if key is not None else {}

    assert is_authorized(FakeRequest("scoped-key")) is True
    assert is_authorized(FakeRequest("wrong")) is False
    assert is_authorized(FakeRequest(None)) is False
    assert seen == ["wrong", "scoped-key", "scoped-key", "wrong", None]


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"
