from __future__ import annotations

import json
import logging
import os
from typing import Any, Callable, Iterable
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import redis.asyncio as aioredis

from indexer.core.settings import Settings

logger = logging.getLogger(__name__)

KEY_PREFIX = "ton-index:v1"

settings = Settings()
_client: aioredis.Redis | None = None
_client_pid: int | None = None


def configure_from_settings(source: Settings):
    settings.kvrocks = source.kvrocks
    settings.kvrocks_sentinels = source.kvrocks_sentinels
    settings.kvrocks_sentinel_master = source.kvrocks_sentinel_master
    settings.kvrocks_user = source.kvrocks_user
    settings.kvrocks_password = source.kvrocks_password
    settings.kvrocks_sentinel_user = source.kvrocks_sentinel_user
    settings.kvrocks_sentinel_password = source.kvrocks_sentinel_password
    settings.kvrocks_db = source.kvrocks_db
    settings.kvrocks_max_connections = source.kvrocks_max_connections
    settings.kvrocks_pool_timeout = source.kvrocks_pool_timeout


def is_enabled() -> bool:
    return bool(settings.kvrocks or settings.kvrocks_sentinels)


def normalize_address_id(value: str | None) -> str | None:
    if value is None:
        return None
    value = str(value).strip()
    if value == "":
        return None
    return value.upper()


def payload_key(table: str, item_id: str) -> str:
    return f"{KEY_PREFIX}:{table}:{item_id.strip()}:payload"


def _parse_host_port(value: str) -> tuple[str, int]:
    host, _, port = value.strip().partition(":")
    return host, int(port or 6379)


def _parse_sentinels(value: str) -> list[tuple[str, int]]:
    return [_parse_host_port(item) for item in value.split(",") if item.strip()]


def _auth_kwargs(username: str = "", password: str = "") -> dict[str, Any]:
    kwargs: dict[str, Any] = {}
    if username:
        kwargs["username"] = username
    if password:
        kwargs["password"] = password
    return kwargs


def _connection_kwargs(username: str = "", password: str = "") -> dict[str, Any]:
    kwargs = _auth_kwargs(username, password)
    kwargs.update({
        "db": settings.kvrocks_db,
    })
    return kwargs


def _max_connections() -> int:
    return max(1, int(settings.kvrocks_max_connections or 1))


def _blocking_pool_kwargs(username: str = "", password: str = "") -> dict[str, Any]:
    kwargs = _connection_kwargs(username, password)
    kwargs.update({
        "max_connections": _max_connections(),
        "timeout": max(1, int(settings.kvrocks_pool_timeout or 1)),
    })
    return kwargs


def _redact_url(value: str) -> str:
    try:
        parts = urlsplit(value)
    except ValueError:
        return "<invalid-url>"

    redacted_query = urlencode(
        [
            (key, "***" if key.lower() in {"password", "pass", "username", "user"} else query_value)
            for key, query_value in parse_qsl(parts.query, keep_blank_values=True)
        ],
        doseq=True,
        safe="*",
    )

    has_userinfo = "@" in parts.netloc
    if not has_userinfo:
        return urlunsplit((parts.scheme, parts.netloc, parts.path, redacted_query, parts.fragment))

    try:
        port = parts.port
    except ValueError:
        port = None

    host = parts.hostname or ""
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    if port is not None:
        host = f"{host}:{port}"
    return urlunsplit((parts.scheme, f"***@{host}", parts.path, redacted_query, parts.fragment))


def _create_client() -> aioredis.Redis:
    common_kwargs = _connection_kwargs(settings.kvrocks_user, settings.kvrocks_password)

    if settings.kvrocks_sentinels:
        if not settings.kvrocks_sentinel_master:
            raise RuntimeError("Kvrocks sentinel master name must be configured")
        sentinel_kwargs = _auth_kwargs(
            settings.kvrocks_sentinel_user,
            settings.kvrocks_sentinel_password,
        )
        sentinel = aioredis.Sentinel(
            _parse_sentinels(settings.kvrocks_sentinels),
            sentinel_kwargs=sentinel_kwargs,
        )
        logger.info("kvrocks: sentinel master %s via %s, max_connections=%s",
                    settings.kvrocks_sentinel_master, settings.kvrocks_sentinels, _max_connections())
        return sentinel.master_for(
            settings.kvrocks_sentinel_master,
            max_connections=_max_connections(),
            **common_kwargs,
        )

    if not settings.kvrocks:
        raise RuntimeError("Kvrocks address or sentinels must be configured")
    if "://" in settings.kvrocks:
        logger.info("kvrocks: %s, max_connections=%s", _redact_url(settings.kvrocks), _max_connections())
        pool = aioredis.BlockingConnectionPool.from_url(
            settings.kvrocks,
            **_blocking_pool_kwargs(settings.kvrocks_user, settings.kvrocks_password),
        )
        return aioredis.Redis(connection_pool=pool)

    host, port = _parse_host_port(settings.kvrocks)
    logger.info("kvrocks: %s:%s, max_connections=%s", host, port, _max_connections())
    pool = aioredis.BlockingConnectionPool(
        host=host,
        port=port,
        **_blocking_pool_kwargs(settings.kvrocks_user, settings.kvrocks_password),
    )
    return aioredis.Redis(connection_pool=pool)


def get_client() -> aioredis.Redis:
    global _client, _client_pid
    if not is_enabled():
        raise RuntimeError("Kvrocks is not configured")
    pid = os.getpid()
    if _client is None or _client_pid != pid:
        _client = _create_client()
        _client_pid = pid
    return _client


async def get_payloads(
        table: str,
        ids: Iterable[str | None],
        normalize: Callable[[str | None], str | None] | None = None,
) -> dict[str, dict[str, Any]]:
    if not is_enabled():
        return {}

    seen = set()
    normalized_ids = []
    keys = []
    for item_id in ids:
        if normalize is not None:
            item_id = normalize(item_id)
        elif item_id is not None:
            item_id = str(item_id).strip()
        if not item_id or item_id in seen:
            continue
        seen.add(item_id)
        normalized_ids.append(item_id)
        keys.append(payload_key(table, item_id))

    if not keys:
        return {}

    values = await get_client().mget(keys)
    result: dict[str, dict[str, Any]] = {}
    for item_id, raw_value in zip(normalized_ids, values):
        if raw_value is None:
            continue
        if isinstance(raw_value, bytes):
            raw_value = raw_value.decode("utf-8")
        result[item_id] = json.loads(raw_value)
    return result
