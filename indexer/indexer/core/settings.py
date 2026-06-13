from pydantic_settings import BaseSettings
from typing_extensions import Optional


class Settings(BaseSettings):
    pg_dsn: str = 'postgresql+asyncpg://localhost:5432/ton_index_a'
    redis_dsn: str = ''
    api_root_path: str = ''
    api_title: str = ''
    ton_http_api_endpoint: str = ''
    emulated_traces: bool = False
    is_testnet: bool = False
    emulated_traces_redis_channel: str = 'new_trace'
    emulated_traces_redis_response_channel: Optional[str] = None

    interfaces_cache_size: int = 10000
    interfaces_cache_ttl: int = 300
    use_combined_repository: bool = False

    kvrocks: str = ''
    kvrocks_sentinels: str = ''
    kvrocks_sentinel_master: str = ''
    kvrocks_user: str = ''
    kvrocks_password: str = ''
    kvrocks_sentinel_user: str = ''
    kvrocks_sentinel_password: str = ''
    kvrocks_db: int = 0
    kvrocks_max_connections: int = 16
    kvrocks_pool_timeout: int = 30

    class Config:
        env_prefix = 'ton_indexer_'
