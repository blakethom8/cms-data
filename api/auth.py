"""Shared API-key authorization and per-consumer key identity.

Single source of truth for whether a presented `X-API-Key` is valid and, when
it is, which consumer presented it. Both enforcement points — the
`check_api_key` route dependency and the cache middleware's conditional-request
short-circuit — resolve through the same function so they cannot drift.

**Scoped keys.** `CMS_API_KEYS` holds `name:value` pairs, comma separated:

    CMS_API_KEYS=ps-prod:<value>,ps-dev:<value>,command-center:<value>

**Rotation with overlap.** A consumer may hold two valid keys at once — list
the name twice with different values. A rotation is therefore issue-new →
migrate the consumer → retire-old, with no window where the consumer is broken.
Revocation is removal from the mapping.

**The shared key keeps working.** `CMS_API_KEY` stays valid alongside any
scoped keys and resolves to the name `shared`, so scoped keys can be introduced
and adopted one consumer at a time. Retiring the shared key is a separate,
owner-coordinated last step — not something this module decides.

Malformed configuration raises at construction rather than degrading quietly: a
mapping that silently dropped a consumer would surface as a confusing 401, and
the serving unit's startup check is designed to fail closed.

Key *values* are secrets and are never returned, logged, or included in an
error. Only names are.
"""

from __future__ import annotations

import re
import secrets
from typing import Callable

API_KEY_HEADER = "X-API-Key"

# Log name for a caller presenting the legacy shared secret.
SHARED_KEY_NAME = "shared"
# Log name when no key is configured at all — the long-standing dev-mode
# behaviour where the API is open.
OPEN_ACCESS_NAME = "open"

# Consumer names reach log lines, so keep them boring: no whitespace, no
# control characters, nothing that could forge a log record.
NAME_PATTERN = re.compile(r"[a-z0-9][a-z0-9._-]{0,63}")

# Returns the consumer name for a valid key, or None when the key is rejected.
KeyResolver = Callable[[str | None], str | None]


def parse_consumer_names(raw: str) -> frozenset[str]:
    """Parse a comma-separated consumer allowlist without accepting secrets."""

    names: set[str] = set()
    for entry in raw.split(","):
        name = entry.strip()
        if not name:
            continue
        if not NAME_PATTERN.fullmatch(name):
            raise ValueError(f"Invalid consumer name in allowlist: {name!r}")
        names.add(name)
    return frozenset(names)


def parse_scoped_keys(raw: str) -> dict[str, str]:
    """Parse `name:value,…` into {key value: consumer name}.

    Indexed by value because that is the lookup direction at request time. Two
    entries sharing a name is how overlap rotation is expressed.
    """

    by_value: dict[str, str] = {}
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        name, separator, value = entry.partition(":")
        name = name.strip()
        value = value.strip()
        if not separator or not name or not value:
            raise ValueError(
                "CMS_API_KEYS entries must be name:value (a blank name or value "
                "would authorize the wrong caller)"
            )
        if not NAME_PATTERN.fullmatch(name):
            raise ValueError(f"Invalid consumer key name: {name!r}")
        claimed_by = by_value.get(value)
        if claimed_by is not None and claimed_by != name:
            raise ValueError(
                f"One key value cannot belong to both {claimed_by!r} and {name!r}; "
                "the logged consumer name would be a guess"
            )
        by_value[value] = name
    return by_value


def make_key_resolver(shared_key: str, scoped_keys: str = "") -> KeyResolver:
    """Build the key → consumer-name resolver for one configuration."""

    by_value = parse_scoped_keys(scoped_keys)
    shared = shared_key.strip()
    if shared and shared in by_value:
        raise ValueError(
            "The shared key is also configured as a scoped key; its consumer "
            "name would be ambiguous"
        )
    open_access = not shared and not by_value

    def resolve(presented: str | None) -> str | None:
        if open_access:
            return OPEN_ACCESS_NAME
        if not presented:
            return None
        scoped_name = by_value.get(presented)
        if scoped_name is not None:
            return scoped_name
        if shared and secrets.compare_digest(presented, shared):
            return SHARED_KEY_NAME
        return None

    return resolve


def configured_consumer_names(shared_key: str, scoped_keys: str = "") -> list[str]:
    """Names this configuration accepts — safe to log at startup, values are not."""

    names = sorted(set(parse_scoped_keys(scoped_keys).values()))
    if shared_key.strip():
        names.append(SHARED_KEY_NAME)
    return names
