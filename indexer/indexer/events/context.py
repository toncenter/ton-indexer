from contextvars import ContextVar

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

# noinspection PyTypeChecker
session: ContextVar[AsyncSession] = ContextVar('db_session', default=None)
