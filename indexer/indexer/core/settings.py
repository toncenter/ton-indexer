from typing import Optional
from pydantic import (
    BaseSettings,
    PostgresDsn
)


class Settings(BaseSettings):
    pg_dsn: PostgresDsn = 'postgresql+asyncpg://localhost:5432/ton_index_a'
    api_root_path: str = ''
    api_title: str = ''
    ton_http_api_endpoint: str = ''

    class Config:
        env_prefix = 'ton_indexer_'
