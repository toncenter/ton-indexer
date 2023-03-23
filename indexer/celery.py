import os
from celery import Celery
from celery.backends.redis import RedisBackend
from config import settings

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = os.getenv("RABBITMQ_PORT")

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")

CELERY_BROKER_URL = f"amqp://{RABBITMQ_HOST}:{RABBITMQ_PORT}"
CELERY_BACKEND_URL = f"indexer.celery.CustomBackend://{REDIS_HOST}:{REDIS_PORT}"

# To prevent celery issue https://github.com/celery/celery/issues/4983 which causes
# clients to disconnect with this log: "Client ... scheduled to be closed ASAP for overcoming of output buffer limits."
# this workaround disables pub-sub mechanism. 
# More details here: https://github.com/celery/celery/issues/4983#issuecomment-518302708
class CustomBackend(RedisBackend):
    def on_task_call(self, producer, task_id):
        pass

app = Celery('indexer',
             broker=CELERY_BROKER_URL,
             backend=CELERY_BACKEND_URL,
             include=['indexer.tasks'])

# Optional configuration, see the application user guide.
app.conf.update(
    accept_content=['pickle'],
    task_serializer='pickle',
    result_serializer='pickle',
    result_expires=600,
    result_extended=True,
    worker_max_tasks_per_child=settings.indexer.max_tasks_per_child, # recreate worker process after every max_tasks_per_child tasks
    task_time_limit=settings.indexer.task_time_limit,
    task_reject_on_worker_lost=True
)