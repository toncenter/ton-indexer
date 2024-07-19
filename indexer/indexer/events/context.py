from contextvars import ContextVar

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from indexer.events.extra_data_repository import ExtraDataRepository

# noinspection PyTypeChecker
session: ContextVar[AsyncSession] = ContextVar('db_session', default=None)
# noinspection PyTypeChecker
extra_data_repository: ContextVar[ExtraDataRepository] = ContextVar('extra_data_repository', default=None)