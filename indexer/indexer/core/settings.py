from typing import Optional

from pydantic import (
    BaseSettings,
    RedisDsn,
    PostgresDsn,
    AmqpDsn
)


class Settings(BaseSettings):
    redis_dsn: RedisDsn = 'redis://localhost:6379'
    pg_dsn: PostgresDsn = 'postgres+asyncpg://localhost:5432/ton_index_a'
    amqp_dsn: AmqpDsn = 'amqp://localhost:5672'

    use_ext_method: bool = False
    cdll_path: Optional[str] = None
    liteserver_config: str
    ls_index: int = 0

    start_seqno: int 
    bottom_seqno: int

    blocks_per_task: int = 32
    max_tasks_per_child: int = 256  # recreate after
    task_time_limit: int = 1200

    max_scheduled_tasks: int = 32
    max_workers: int = 1
    min_workers: int = 1

    api_root_path: str = ''

    class Config:
        env_prefix = 'ton_indexer_'
