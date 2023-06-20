from celery.signals import worker_process_init
from parser.celery import app
import asyncio
import traceback
from config import settings
from parser.eventbus import EventBus, KafkaEventBus, NullEventBus
from indexer.database import *
from indexer.crud import *
from parser.parsers_collection import ALL_PARSERS, GeneratedEvent

from loguru import logger

loop = None
worker = None

@worker_process_init.connect()
def configure_worker(signal=None, sender=None, **kwargs):
    global loop
    global worker
    loop = asyncio.get_event_loop()

@app.on_after_finalize.connect
def setup_periodic_tasks(sender, **kwargs):
    logger.info("Setting up periodic parser invocation")
    sender.add_periodic_task(settings.parser.poll_interval, parse_outbox_task.s("test"), name='Parser task')


async def process_item(session: SessionMaker, eventbus: EventBus, task: ParseOutbox):
    # logger.info(f"Got task {task}")
    successful = True
    delayedEvents = []
    def generated_events_iterator(g):
        if type(g) == GeneratedEvent:
            yield g
        elif type(g) == list:
            for event in g:
                yield event
    try:
        if task.entity_type == ParseOutbox.PARSE_TYPE_MESSAGE:
            ctx = await get_messages_context(session, task.entity_id)
        elif task.entity_type == ParseOutbox.PARSE_TYPE_ACCOUNT:
            ctx = await get_account_context(session, task.entity_id)
        else:
            raise Exception(f"entity_type not supported: {task.entity_type}")
        for parser in ALL_PARSERS:
            if parser.predicate.match(ctx):
                generated = await parser.parse(session, ctx)
                if generated:
                    for event in generated_events_iterator(generated):
                        if event.waitCommit:
                            delayedEvents.append(event.event)
                        else:
                            eventbus.push_event(event.event)
    except Exception as e:
        logger.error(f'Failed to perform parsing for outbox item {task.outbox_id}: {traceback.format_exc()}')
        await postpone_outbox_item(session, task, settings.parser.retry.timeout)
        await asyncio.sleep(1) # simple throttling
        successful = False
    if successful:
        await remove_outbox_item(session, task.outbox_id)
    return delayedEvents

async def parse_outbox():
    logger.info(f"Starting parse outbox loop, eventbus enabled: {settings.eventbus.enabled}")
    if settings.eventbus.enabled:
        eventbus = KafkaEventBus(settings.eventbus.kafka.broker, settings.eventbus.kafka.topic)
    else:
        eventbus = NullEventBus()

    while True:
        async with SessionMaker() as session:
            # batch mode is supported but not recommended due to batch processing occurs in one transaction
            tasks = await get_outbox_items(session, settings.parser.batch_size)
            if len(tasks) == 0:
                logger.info("Parser outbox is empty, exiting")
                break
            tasks = [process_item(session, eventbus, task[0]) for task in tasks]
            res = await asyncio.gather(*tasks)

            await session.commit()
            for delayedEvents in res:
                for event in delayedEvents:
                    logger.info(f"Sending delayed event: {event}")
                    eventbus.push_event(event)

@app.task
def parse_outbox_task(arg):
    logger.info(f"Running parse outbox iteration")
    loop.run_until_complete(parse_outbox())

    return



