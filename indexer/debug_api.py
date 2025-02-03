# A debugger to toggle some actions locally

import asyncio
import base64
import binascii
import decimal
import inspect
import json
import logging
import re
from copyreg import pickle
from pprint import pformat, pprint
from typing import final

import fastapi
from fastapi import FastAPI
from pytoniq_core import Slice
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import contains_eager, sessionmaker

import indexer.events.blocks.messages as messages
from indexer.core import redis
from indexer.core.database import (
    Message,
    MessageContent,
    SyncSessionMaker,
    Trace,
    Transaction,
    engine,
    logger,
)
from indexer.events import context
from indexer.events.blocks.basic_blocks import Block, CallContractBlock

# from indexer.events.blocks.basic_matchers import debugger
# from indexer.events.blocks.core import EmptyBlock
from indexer.events.blocks.messages import (
    DedustSwapNotification,
    JettonNotify,
    JettonTransfer,
)
from indexer.events.blocks.utils import to_tree
from indexer.events.blocks.utils.address_selectors import extract_additional_addresses

# from indexer.events.blocks.utils.address_selectors import extract_additional_addresses
from indexer.events.blocks.utils.block_tree_serializer import block_to_action
from indexer.events.blocks.utils.event_deserializer import deserialize_event
from indexer.events.event_processing import init_block, matchers, trace_post_processors
from indexer.events.interface_repository import (
    EmulatedTransactionsInterfaceRepository,
    RedisInterfaceRepository,
    gather_interfaces,
)

app = FastAPI()
logging.basicConfig(level=logging.DEBUG, format="%(message)s")
async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
last_arg = None

classes = [
    (name, obj) for name, obj in inspect.getmembers(messages) if inspect.isclass(obj)
]
opcode_mapping = dict()
for c, o in classes:
    try:
        code = o.opcode
        if code:
            opcode_mapping[code] = (c, o)
    except:
        pass

logger.debug("Matchers: " + pformat(opcode_mapping, width=200))


def encoder(obj):
    if isinstance(obj, bytes):
        return str(base64.b64encode(obj), encoding="utf-8")
    if isinstance(obj, decimal.Decimal):
        return str(obj)
    try:
        return obj.to_json()
    except AttributeError:
        return obj.__dict__


def block_to_dict(
    block: Block, is_inner_block: bool = False, scope: list[Block] = []
) -> dict:
    if isinstance(block.data, dict):
        data = block.data
        if len(block.contract_deployments) > 0:
            data["deployments"] = [x.as_str() for x in block.contract_deployments]
    else:
        data = {"data": block.data}
    if len(block.event_nodes) == 1:
        tx = block.event_nodes[0].get_tx()
        data["exit_code"] = tx.compute_exit_code if tx else None
        data["tx_hash"] = block.event_nodes[0].message.tx_hash
    if block.initiating_event_node is not None:
        data["initiator_tx"] = block.initiating_event_node.get_tx_hash()
    if isinstance(block, CallContractBlock) and block.opcode is not None:
        if block.opcode in opcode_mapping:
            short_name, c = opcode_mapping[block.opcode]
            body = block.get_body()
            try:
                m = c(body)
                data["details"] = m.__dict__
            except:
                pass
        else:
            short_name = hex(block.opcode)
    else:
        short_name = block.btype
    result = {
        # "id": block.id,
        "failed": block.failed,
        "short_name": short_name,
        "data": data,  # or "data": block.data, depending on what you want to display
        "flow": {},
        "progress": block.calculate_progress(),
    }
    if not is_inner_block:
        result["children"] = [block_to_dict(b) for b in block.next_blocks]
        if len(block.children_blocks) == 0:
            result["inner"] = []
        else:
            inner_root = Block("root", [], {})
            # inner_root.id = ""
            root_blocks = set()
            inner_blocks = block.children_blocks
            for b in inner_blocks:
                if b.previous_block not in inner_blocks:
                    root_blocks.add(b)
                    inner_root.next_blocks.append(b)
            result["inner"] = block_to_dict(inner_root, True, inner_blocks)
    else:
        result["children"] = [
            block_to_dict(b, True, scope) for b in block.next_blocks if b in scope
        ]
    return result


async def handle_trace(trace):
    # with open('test.pck', 'wb') as f:
    #     for tx in trace.transactions:
    #         for msg in tx.messages:
    #             assert msg.transaction is not None
    #     pickle.dump(trace, f)
    # with open('test.pck', 'rb') as f:
    #     trace = pickle.load(f)
    debug_steps = []
    try:
        tree = to_tree(trace.transactions)
        block = init_block(tree)

        root = Block("root", [])
        root.connect(block)
        dicts = [block_to_dict(root)]
        idx_to_matcher = ["init"]

        for m in matchers:
            for b in root.bfs_iter():
                if b.parent is None:
                    # debugger.clear()
                    await m.try_build(b)
                    # debugger.end_try(b, m)
                    # debug_steps += debugger.steps

                result = block_to_dict(root)
                last_dict_json = json.dumps(dicts[-1], default=encoder)
                result_json = json.dumps(result, default=encoder)
                if last_dict_json != result_json:
                    # print(last_dict_json)
                    # print("----")
                    # print(result_json)
                    dicts.append(result)
                    idx_to_matcher.append(m.__class__.__name__)
                    debug_steps.append(("SPLIT", len(idx_to_matcher) - 1))
        # ext_in_block = None
        # for block in root.bfs_iter():
        #     if block.btype == 'call_contract' and block.event_nodes[0].message.source is None:
        #         ext_in_block = block
        #         break
        # for b in ext_in_block.next_blocks:
        #     b.event_nodes.append(block.event_nodes[0])
        #     root.connect(b)
        # root.next_blocks.remove(block)
        # result = block_to_dict(root)
        # last_dict_json = json.dumps(dicts[-1], default=encoder)
        # result_json = json.dumps(result, default=encoder)
        # if last_dict_json != result_json:
        #     print(last_dict_json)
        #     print("----")
        #     print(result_json)
        #     dicts.append(result)
        #     idx_to_matcher.append("COLLAPSE")
        #     debug_steps.append(('SPLIT', len(idx_to_matcher) - 1))
        blocks = [root] + list(root.bfs_iter())
        for pp in trace_post_processors:
            blocks = await pp(blocks)
        for block in blocks:
            logger.debug("Block: " + pformat(block, depth=8))
            if block.btype != "root":
                if (
                    block.btype == "call_contract"
                    and block.event_nodes[0].message.destination is None
                ):
                    continue
                if block.btype == "empty":
                    continue
                if (
                    block.btype == "call_contract"
                    and block.event_nodes[0].message.source is None
                ):
                    continue
                # if isinstance(block, EmptyBlock):
                #     continue
                if block.broken:
                    state = "broken"
                # print(block)
                action = block_to_action(block, trace.trace_id)
                logger.debug("Trace: " + pformat(trace, depth=8))
                logger.debug("Block to action: " + pformat(action, width=200))
        response = {
            "dicts": dicts,
            "matchers": idx_to_matcher,
            "flow": {},
            # 'steps': debug_steps
        }
        return response
    except Exception as e:
        logger.exception(e, exc_info=True)


async def try_find_trace_id(session, id: str):
    # check if id is hex and convert to base64 string
    try:
        hex_id = binascii.a2b_hex(id)
        id = base64.b64encode(hex_id).decode("utf-8")
    except:
        pass
    # Check if trace with trace id exists
    query = select(Trace).filter(Trace.trace_id.in_([id]))
    result = await session.execute(query)
    trace = result.scalars().unique().all()
    if len(trace) != 0:
        return id

    # Check if transaction if given id exists
    query = select(Transaction).filter(Transaction.hash.in_([id]))
    result = await session.execute(query)
    tx = result.scalars().unique().all()
    if len(tx) != 0:
        return tx[0].trace_id

    # Check if message with given id exists
    query = select(Message).filter(Message.msg_hash.in_([id]))
    result = await session.execute(query)
    msg = result.scalars().unique().all()
    if len(msg) != 0:
        return msg[0].transaction.trace_id
    return None


async def get_trace(session, trace_id: str):
    query = (
        select(
            Trace.trace_id,
            Trace.nodes_,
            Transaction.hash,
            Transaction.aborted,
            Transaction.account_state_hash_after,
            Transaction.account_state_hash_before,
            Transaction.orig_status,
            Transaction.end_status,
            Transaction.lt,
            Transaction.now,
            Message.opcode,
            Message.trace_id,
            Message.tx_hash,
            Message.msg_hash,
            Message.body_hash,
            Message.source,
            Message.destination,
            Message.direction,
            Message.bounce,
            Message.opcode,
            Message.value,
            Message.created_lt,
            Message.created_at,
            MessageContent.body,
        )
        .join(Trace.transactions)
        .join(Transaction.messages, isouter=True)
        .join(Message.message_content, isouter=True)
        .options(
            contains_eager(
                Trace.transactions, Transaction.messages, Message.message_content
            )
        )
        .filter(Trace.trace_id.in_([trace_id]))
    )
    result = await session.execute(query)
    return result.scalars().unique().all()[0]


async def get_default_redis_repo(accounts, session):
    interfaces = await gather_interfaces(accounts, session)
    repository = RedisInterfaceRepository(redis.sync_client)
    await repository.put_interfaces(interfaces)
    return repository


async def get_test_repo(accounts, session):
    return EmulatedTransactionsInterfaceRepository(dict())


# @app.get("/api/async/event")
# async def run_matchers_async(id: str, is_redis: bool = False):
#     trace = None
#     session = async_session()
#     try:
#         context.session.set(session)
#         event_id = await try_find_trace_id(session, id)
#         # context.extra_data_repository.set(SqlAlchemyExtraDataRepository(context.session))
#
#         query = select(Trace) \
#             .join(Trace.transactions) \
#             .join(Transaction.messages, isouter=True) \
#             .join(Message.message_content, isouter=True) \
#             .options(contains_eager(Trace.transactions, Transaction.messages, Message.message_content)) \
#             .filter(Trace.trace_id.in_([event_id]))
#         result = await session.execute(query)
#         trace = result.scalars().unique().all()[0]
#
#         # Gather interfaces for each account
#         accounts = set()
#         for tx in trace.transactions:
#             accounts.add(tx.account)
#             accounts.update(extract_additional_addresses(tx))
#         # accounts.update(extract_additional_addresses([trace]))
#         additional_accs = ['0:BBA4407317E8585370A37C6EDCB4E71400811ED5D2BA4D9EF7330337A92E2223',
#                 '0:3CCB55AA151C5D686612ECF880A2C6537593D8FE93BB5FCDF9B6B805E380F6A9',
#                 '0:178981FD78D67EFECA9351C94C1EEF7CA1C63AABF26EF43D06BE772F1CA11F39',
#                 '0:981ECDBB343B9BF76A343E8826729B8686AB94D3DC97FAD42BA12F48EA6EAD76',
#                 '0:9CFFE538A014BCF631A67DFD1462EA157E4280446C53810401FEA137410A1088']
#         # for acc in additional_accs:
#         #     accounts.add(acc)
#         # repository = await get_test_repo(accounts, session)
#         interfaces = await gather_interfaces(accounts, session)
#         repository = RedisInterfaceRepository(redis.sync_client)
#         await repository.put_interfaces(interfaces)
#         context.interface_repository.set(repository)
#         response = await handle_trace(trace)
#         return fastapi.Response(content=json.dumps(response, default=encoder), media_type="application/json")
#     except Exception as e:
#         logger.exception(e, exc_info=True)
#     finally:
#         session.close()


async def get_trace_and_setup_repositories_from_db(id: str, session):
    context.session.set(session)
    event_id = await try_find_trace_id(session, id)
    # context.extra_data_repository.set(SqlAlchemyExtraDataRepository(context.session))

    query = (
        select(Trace)
        .join(Trace.transactions)
        .join(Transaction.messages, isouter=True)
        .join(Message.message_content, isouter=True)
        .options(
            contains_eager(
                Trace.transactions, Transaction.messages, Message.message_content
            )
        )
        .filter(Trace.trace_id.in_([event_id]))
    )
    result = await session.execute(query)
    trace = result.scalars().unique().all()[0]

    # Gather interfaces for each account
    accounts = set()
    for tx in trace.transactions:
        accounts.add(tx.account)
        accounts.update(extract_additional_addresses(tx))
    # accounts.update(extract_additional_addresses([trace]))
    additional_accs = [
        "0:BBA4407317E8585370A37C6EDCB4E71400811ED5D2BA4D9EF7330337A92E2223",
        "0:3CCB55AA151C5D686612ECF880A2C6537593D8FE93BB5FCDF9B6B805E380F6A9",
        "0:178981FD78D67EFECA9351C94C1EEF7CA1C63AABF26EF43D06BE772F1CA11F39",
        "0:981ECDBB343B9BF76A343E8826729B8686AB94D3DC97FAD42BA12F48EA6EAD76",
        "0:9CFFE538A014BCF631A67DFD1462EA157E4280446C53810401FEA137410A1088",
    ]
    # for acc in additional_accs:
    #     accounts.add(acc)
    # repository = await get_test_repo(accounts, session)
    interfaces = await gather_interfaces(accounts, session)
    repository = RedisInterfaceRepository(redis.sync_client)
    await repository.put_interfaces(interfaces)
    context.interface_repository.set(repository)
    return trace


async def get_trace_and_setup_repositories_from_redis(id):
    trace_map = await redis.client.hgetall(id)
    trace_map = dict(
        (str(key, encoding="utf-8"), value) for key, value in trace_map.items()
    )
    trace = deserialize_event(id, trace_map)
    context.interface_repository.set(EmulatedTransactionsInterfaceRepository(trace_map))
    return trace


@app.get("/api/async/event")
async def run_matchers_async(id: str, redis: bool = False):
    logger.setLevel(10)
    trace = None
    session = async_session()
    try:
        if redis:
            trace = await get_trace_and_setup_repositories_from_redis(id)
        else:
            logger.debug("Getting trace from db")
            trace = await get_trace_and_setup_repositories_from_db(id, session)
        logger.debug("Handling trace")
        response = await handle_trace(trace)
        return fastapi.Response(
            content=json.dumps(response, default=encoder), media_type="application/json"
        )
    except Exception as e:
        logger.exception(e, exc_info=True)
    finally:
        await session.close()  # type: ignore


async def test():
    limit = 100
    offset = 0
    with SyncSessionMaker() as session:
        while True:
            query = (
                select(Message)
                .join(Message.message_content)
                .options(contains_eager(Message.message_content))
                .filter(Message.opcode == -1671361053)
                .order_by(Message.created_lt.desc())
                .limit(limit)
                .offset(offset)
            )
            result = session.execute(query)
            messages = result.scalars().all()
            for m in messages:
                try:
                    notification = DedustSwapNotification(
                        Slice.one_from_boc(m.message_content.body)
                    )
                    if notification.ref_address is not None:
                        print(m.tx_hash)
                except:
                    pass
            offset += limit
            print(offset, limit)


if __name__ == "__main__":
    import uvicorn

    # index_keys()
    # asyncio.run(test())
    logger.debug("Starting debug server for toncenter actions")
    uvicorn.run(app, host="0.0.0.0", port=8000)

