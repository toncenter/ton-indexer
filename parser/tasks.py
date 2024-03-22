from celery.signals import worker_process_init
from parser.celery import app
import asyncio
import traceback
import json
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
            if ctx.message.destination == 'EQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAM9c' or ctx.message.destination == ctx.message.source:
                logger.info("Skipping inscription message")
                payload = ctx.message.comment
                prefix = "data:application/json,"
                def make_lower(x):
                    if x and type(x) == str:
                        return x.lower()
                    return x

                if payload and payload.startswith(prefix):
                    try:
                        obj = json.loads(payload[len(prefix):])
                        op = obj.get('op', None)
                        if op:
                            op = op.lower()
                        if op == 'deploy':
                            await upsert_entity(session, TonanoDeploy(
                                msg_id=ctx.message.msg_id,
                                created_lt=ctx.message.created_lt,
                                utime=ctx.source_tx.utime if ctx.source_tx else None,
                                owner=ctx.message.source,
                                tick=make_lower(obj.get('tick', None)),
                                max_supply=int(obj.get('max', '-1')),
                                mint_limit=int(obj.get('lim', '-1')),
                            ))
                        elif op == 'mint':
                            if int(obj.get('amt', '-1')) >= 0:
                                await upsert_entity(session, TonanoMint(
                                    msg_id=ctx.message.msg_id,
                                    created_lt=ctx.message.created_lt,
                                    utime=ctx.source_tx.utime if ctx.source_tx else None,
                                    owner=ctx.message.source,
                                    tick=make_lower(obj.get('tick', None)),
                                    amount=int(obj.get('amt', '-1')),
                                    target=1 if ctx.message.destination == 'EQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAM9c' else 0
                                ))
                        elif op == 'transfer':
                            await upsert_entity(session, TonanoTransfer(
                                msg_id=ctx.message.msg_id,
                                created_lt=ctx.message.created_lt,
                                utime=ctx.source_tx.utime if ctx.source_tx else None,
                                owner=ctx.message.source,
                                tick=make_lower(obj.get('tick', None)),
                                destination=obj.get('to', None),
                                amount=int(obj.get('amt', '-1'))
                            ))
                    except:
                        logger.error(f'Failed to parse ton-20 payload for {task.outbox_id} ({payload}): {traceback.format_exc()}')
                await remove_outbox_item(session, task.outbox_id)
                return []
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
            tasks = await get_outbox_items_by_min_seqno(session)
            if len(tasks) == 0:
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



