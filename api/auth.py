"""Shared API-key authorization.

Single source of truth for whether a presented `X-API-Key` value is valid.
Both enforcement points — the `check_api_key` route dependency and the
`ReleaseCacheMiddleware` conditional-request short-circuit — must consult the
same predicate so they cannot drift. S3's scoped per-consumer keys change the
predicate built here and nothing at the call sites.
"""

from __future__ import annotations

from typing import Callable

KeyValidator = Callable[[str | None], bool]


def make_key_validator(configured_key: str) -> KeyValidator:
    """Build the key-validity predicate for one configured shared secret.

    An empty configured key means open access (dev mode), matching the
    behavior the serving API has always had.
    """

    def is_valid(presented: str | None) -> bool:
        return not configured_key or presented == configured_key

    return is_valid
