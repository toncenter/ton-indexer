from celery import Celery
from celery.backends.redis import RedisBackend
from indexer.core.settings import Settings


settings = Settings()

# To prevent celery issue https://github.com/celery/celery/issues/4983 which causes
# clients to disconnect with this log: "Client ... scheduled to be closed ASAP for overcoming of output buffer limits."
# this workaround disables pub-sub mechanism. 
# More details here: https://github.com/celery/celery/issues/4983#issuecomment-518302708

class CustomBackend(RedisBackend):
    def on_task_call(self, producer, task_id):
        pass

redis_dsn = f"{settings.redis_dsn}".replace('redis://', 'indexer.core.celery.CustomBackend://')

app = Celery('indexer',
             broker=settings.amqp_dsn,
             backend=redis_dsn,
             include=['indexer.core.tasks'])

# Optional configuration, see the application user guide.
app.conf.update(
    accept_content=['pickle', 'json'],
    task_serializer='pickle',
    result_serializer='pickle',
    result_expires=600,
    result_extended=True,
    worker_send_task_events=True,
    task_default_priority=5,
    task_queue_max_priority=10,
    worker_max_tasks_per_child=settings.max_tasks_per_child,  # recreate worker process after every max_tasks_per_child tasks
    task_time_limit=settings.task_time_limit,
    task_reject_on_worker_lost=True
)
