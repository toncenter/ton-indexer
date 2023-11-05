import random
import traceback

from datetime import datetime
from typing import Optional, List, Tuple, Dict
from copy import deepcopy
from tqdm.auto import tqdm

import indexer.core.database as D
from indexer.core.database import SessionMaker, SyncSessionMaker
from sqlalchemy import and_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session, Query, joinedload, contains_eager

from collections import defaultdict


display = print
DISABLE = False
SHOW_TXS = False

# queries
def query_transactions(session: Session) -> Query:
    query = session.query(D.Transaction) \
                   .options(joinedload(D.Transaction.messages)) \
                   .options(joinedload(D.Transaction.messages).joinedload(D.TransactionMessage.message))
    query = query.order_by(D.Transaction.lt.asc())
    return query


def count_new_transactions(session: Session):
    return session.query(D.Transaction).filter(D.Transaction.event_id.is_(None)).count()


# utils
SYSTEM_ACCOUNTS = {'-1:3333333333333333333333333333333333333333333333333333333333333333',
                   '-1:5555555555555555555555555555555555555555555555555555555555555555',
                   '-1:0000000000000000000000000000000000000000000000000000000000000000',}

def get_message_type(msg: D.TransactionMessage) -> str:
    if msg.message.source is None:
        return 'external'
    if msg.message.destination is None:
        return 'log'
    if msg.message.source in SYSTEM_ACCOUNTS and msg.message.destination in SYSTEM_ACCOUNTS:
        return 'system'
    return 'ord'


# event detector
class EventGraph:
    def __init__(self, edges: List[Tuple[D.Transaction, D.Transaction]]):
        self.txs = {}
        self.edges = []
        self.edges_left = 0

        for edge in edges:
            self.add_edge(edge)

    def add_tx(self, tx: D.Transaction):
        if tx.hash in self.txs:
            return self
        count = 0
        for msg in tx.messages:
            message_type = get_message_type(msg)
            if message_type == 'ord':
                count += 1
        self.txs[tx.hash] = [tx, count]
        self.edges_left += count
        return self

    def add_edge(self, edge: Tuple[D.Transaction, D.Transaction]):
        self.add_tx(edge[0]).add_tx(edge[1])
        self.txs[edge[0].hash][1] -= 1
        self.txs[edge[1].hash][1] -= 1
        self.edges.append((edge[0].hash, edge[1].hash))
        self.edges_left -= 2
        return self

    def check_finished(self):
        return self.edges_left == 0
    
    def _check_finished_debug(self):
        total = 0
        for _, count in self.txs.values():
            total += count
        if total != self.edges_left:
            print(f"WTF: {total} != {self.edges_left}")
        return total == 0

    def get_tx_hashes(self):
        return list(self.txs.keys())

    def get_edges(self):
        return [(self.txs[left], self.txs[right]) for left, right in self.edges]

    def __repr__(self):
        return f"Event(txs: {len(self.txs)}, edges: {len(self.edges)})"
    
    def __hash__(self) -> int:
        return hash((tuple(self.txs.keys()), tuple(self.edges)))

    def __eq__(self, right):
        return hash(self) == hash(right)


# event processor class
class EdgeDetector:
    def __init__(self):
        self.edges: Dict[Tuple[str, bool], D.Transaction] = {}
        self.events: Dict[str, EventGraph] = {}

        self._finished_events = 0
        self._found_edges = 0
        self._last_stats_timestamp = datetime.now().timestamp()

    def print_stats(self) -> "EdgeDetector":
        new_timestamp = datetime.now().timestamp()
        if new_timestamp - self._last_stats_timestamp > 5:
            print(f'events: {self._finished_events}, txs: {len(self.events)}, edges found: {self._found_edges} (active: {len(self.edges)})')
            if SHOW_TXS and self.edges:
                tx = list(self.edges.values())[random.choice(range(len(self.edges)))]
                print('=' * 90)
                print(tx.__dict__)
                print('-' * 90)
                for msg in tx.messages:
                    print(msg.message.__dict__)
                print('=' * 90)
            self._last_stats_timestamp = new_timestamp
        return self

    def process_tx(self, tx: D.Transaction) -> "EdgeDetector":
        if tx.description['type'] == 'tick_tock':
            return self
        
        # process transaction
        for msg in tx.messages:
            is_input = msg.direction == 'in'
            msg_hash = msg.message_hash
            
            message_type = get_message_type(msg)
            if (msg_hash, not is_input) in self.edges:
                # saving for the future
                tx2 = self.edges.pop((msg_hash, not is_input))
                edge = (tx2, tx) if is_input else (tx, tx2)
                self._found_edges += 1

                # new item
                if edge[0].hash not in self.events and edge[1].hash not in self.events:
                    # print(f'New event: {edge[0].hash} -> {edge[1].hash}')
                    event = EventGraph([edge])
                    self.events[edge[0].hash] = event
                    self.events[edge[1].hash] = event
                # left exists
                elif edge[0].hash in self.events and edge[1].hash not in self.events:
                    # print(f'New edge to the right: {edge[0].hash} -> {edge[1].hash}')
                    event = self.events[edge[0].hash]
                    event.add_edge(edge)
                    self.events[edge[1].hash] = event
                # right exists
                elif edge[0].hash not in self.events and edge[1].hash in self.events:
                    # print(f'New edge to the left: {edge[1].hash} <- {edge[0].hash}')
                    event = self.events[edge[1].hash]
                    event.add_edge(edge)
                    self.events[edge[0].hash] = event
                # merge events
                elif edge[0].hash in self.events and edge[1].hash in self.events:
                    event = self.events[edge[0].hash]
                    right = self.events[edge[1].hash]
                    print(f'Merge events: {event} <- {right}')
                    for edge in right.get_edges():
                        event.add_edge(edge)
                    for tx_hash in right.get_tx_hashes():
                        self.events[tx_hash] = event

                # # test
                # for tx_hash in event.get_tx_hashes():
                #     if tx_hash in self.events and not self.events[tx_hash] == event:
                #         print(f'WARNING! {self.events[tx_hash]}')
                    
                # test if finished
                if event.check_finished():
                    if len(event.edges) > 10:
                        print(f'Big event found: {event}')
                    for tx_hash in event.get_tx_hashes():
                        self.events.pop(tx_hash)
                    # TODO: insert event
                    self._finished_events += 1
                    del event
            elif message_type == 'ord':
                # edge found
                self.edges[(msg_hash, is_input)] = tx
        self.print_stats()
        return self


# instance
if __name__ == '__main__':
    edge_detector = EdgeDetector()

    try:
        with SyncSessionMaker() as session:
            query = query_transactions(session)
            # query = query.filter(D.Transaction.event_id.is_(None))
            # query = query.filter(and_(D.Transaction.lt >= 1229773000001, D.Transaction.lt <= 1231067000010))
            query = query.yield_per(16 * 1024).enable_eagerloads(False)
            
            total = query.count()
            pbar = tqdm(total=total, smoothing=0.001, disable=DISABLE)
            for tx in query:
                edge_detector.process_tx(tx)
                pbar.update(1)
    except KeyboardInterrupt:
        print('Gracefully stopped')
    except:
        print(traceback.format_exc())
    exit(0)
# end script

