import os
from celery import Celery
from config import settings

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = os.getenv("RABBITMQ_PORT")

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")

CELERY_BROKER_URL = f"amqp://{RABBITMQ_HOST}:{RABBITMQ_PORT}"
CELERY_BACKEND_URL = f"redis://{REDIS_HOST}:{REDIS_PORT}"

app = Celery('parser',
             broker=CELERY_BROKER_URL,
             backend=CELERY_BACKEND_URL,
             include=['parser.tasks'])

# Optional configuration, see the application user guide.
app.conf.update(
    accept_content=['pickle'],
    task_serializer='pickle',
    result_serializer='pickle',
    result_expires=3600,
    result_extended=True,
    task_time_limit=settings.parser.task_time_limit,
    task_reject_on_worker_lost=True
)
app.autodiscover_tasks()

