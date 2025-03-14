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

    class Config:
        env_prefix = 'ton_indexer_'
