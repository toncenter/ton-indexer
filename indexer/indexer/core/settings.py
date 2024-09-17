from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    pg_dsn: str = 'postgresql+asyncpg://localhost:5432/ton_index_a'
    redis_dsn: str = ''
    api_root_path: str = ''
    api_title: str = ''
    ton_http_api_endpoint: str = ''
    emulated_traces: bool = False
    is_testnet: bool = False

    class Config:
        env_prefix = 'ton_indexer_'
