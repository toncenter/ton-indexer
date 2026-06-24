from __future__ import annotations

from collections.abc import Iterable

import asyncpg
import psycopg2
import redis.exceptions
import sqlalchemy.exc


class RetryableDataAccessError(Exception):
    """Transient storage error; retry the whole classifier task later."""


_RETRYABLE_MESSAGE_PARTS = (
    "connection was closed",
    "connection is closed",
    "connection does not exist",
    "could not receive data from server",
    "ssl syscall error",
    "too many connections",
    "no connection available",
    "connection timed out",
)


def _iter_exception_chain(exc: BaseException) -> Iterable[BaseException]:
    seen: set[int] = set()
    cur: BaseException | None = exc
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        yield cur
        if isinstance(cur, sqlalchemy.exc.StatementError) and cur.orig is not None:
            cur = cur.orig
        else:
            cur = cur.__cause__ or cur.__context__


def is_retryable_data_access_error(exc: BaseException) -> bool:
    for item in _iter_exception_chain(exc):
        if isinstance(item, RetryableDataAccessError):
            return True

        if isinstance(item, (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError)):
            return True

        if isinstance(item, (
            asyncpg.exceptions.PostgresConnectionError,
            asyncpg.exceptions.CannotConnectNowError,
            asyncpg.exceptions.TooManyConnectionsError,
        )):
            return True

        if isinstance(item, psycopg2.OperationalError):
            return True

        if isinstance(item, (
            sqlalchemy.exc.OperationalError,
            sqlalchemy.exc.TimeoutError,
            sqlalchemy.exc.DisconnectionError,
        )):
            return True

        if isinstance(item, sqlalchemy.exc.DBAPIError) and item.connection_invalidated:
            return True

        if isinstance(item, (ConnectionError, TimeoutError)):
            return True

        message = str(item).lower()
        if any(part in message for part in _RETRYABLE_MESSAGE_PARTS):
            return True

    return False


def raise_if_retryable_data_access_error(exc: BaseException) -> None:
    if is_retryable_data_access_error(exc):
        raise RetryableDataAccessError(str(exc)) from exc
