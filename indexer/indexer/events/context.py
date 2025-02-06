from contextvars import ContextVar

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from indexer.events.interface_repository import InterfaceRepository

# noinspection PyTypeChecker
session: ContextVar[AsyncSession] = ContextVar('db_session', default=None)
dedust_pools: ContextVar[dict] = ContextVar('dedust_pools', default={})
# noinspection PyTypeChecker
interface_repository: ContextVar[InterfaceRepository] = ContextVar('interface_repository', default=None)