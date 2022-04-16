import os
from celery import Celery
from config import settings

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = os.getenv("RABBITMQ_PORT")

CELERY_BROKER_URL = f"amqp://{RABBITMQ_HOST}:{RABBITMQ_PORT}"
CELERY_BACKEND_URL = f"rpc://{RABBITMQ_HOST}:{RABBITMQ_PORT}"

app = Celery('indexer',
             broker=CELERY_BROKER_URL,
             backend=CELERY_BACKEND_URL,
             include=['indexer.tasks'])

# Optional configuration, see the application user guide.
app.conf.update(
#     result_expires=3600, # what is it?
    accept_content=['pickle'],
    result_serializer='pickle',
    task_serializer='pickle',
    worker_max_tasks_per_child=settings.indexer.max_tasks_per_child, # recreate worker process after every max_tasks_per_child tasks
    task_time_limit=settings.indexer.task_time_limit
)