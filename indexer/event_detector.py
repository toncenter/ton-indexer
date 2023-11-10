#!/usr/bin/env python3
import random
import sys
import argparse
import traceback
import time
import pickle
import multiprocessing as mp

from datetime import datetime
from typing import Optional, List, Tuple, Dict
from copy import deepcopy
from tqdm.auto import tqdm

import indexer.core.database as D
from indexer.core.database import SessionMaker, SyncSessionMaker
from sqlalchemy import and_, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session, Query, joinedload, sessionmaker
from sqlalchemy.dialects.postgresql import insert

from collections import defaultdict


DISABLE = False
SHOW_TXS = False

# queries
def query_new_transactions(session: Session) -> Query:
    # query = session.query(D.Transaction) \
    #                .options(joinedload(D.Transaction.messages)) \
    #                .options(joinedload(D.Transaction.messages).joinedload(D.TransactionMessage.message)) \
    #                .outerjoin(D.Transaction.event) \
    #                .filter(D.EventTransaction.event_id.is_(None))
    query = session.query(D.Transaction) \
                   .options(joinedload(D.Transaction.messages)) \
                   .options(joinedload(D.Transaction.messages).joinedload(D.TransactionMessage.message)) \
                   .filter(D.Transaction.event_id.is_(None))
    return query


# insert others
class OtherProcess(mp.Process):
    def __init__(self, queue: mp.Queue, session_maker: sessionmaker, batch_size=4096):
        super().__init__()
        self.queue = queue
        self.session_maker = session_maker
        self.batch_size = batch_size
        
        self._txs = []

    def insert_batch(self):
        if len(self._txs) > EdgeDetector.OTHER_BATCH_SIZE:
            with self.session_maker() as session:
                with session.begin():
                    for tx_hash, is_tick_tock in self._txs:
                        session.execute(update(D.Transaction)
                                        .where(D.Transaction.hash == tx_hash)
                                        .values(event_id=0 if is_tick_tock else hash(tx_hash)))
                self._txs.clear()

    def run(self):
        try:
            while True:
                if not self.queue.empty():
                    item = self.queue.get(False)
                    if item is not None:
                        self._txs.append(item)
                    if len(self._txs) > self.batch_size:
                        self.insert_batch()
                else:
                    time.sleep(0.5)
        except KeyboardInterrupt:
            print('Gracefully stopped in the Thread')
        except:
            print(traceback.format_exc())
        print('Thread finished')
        return


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

        # forkbombs and big events
        self._event_id = None

    def mark_partial_event(self, event_id):
        self._event_id = event_id

    @property
    def event_id(self):
        return self._event_id

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
        
        for tt in edge:
            self.txs[tt.hash][1] -= 1
            if self.txs[tt.hash][1] == 0:
                # compress finished nodes
                self.txs[tt.hash][0] = None
        self.edges.append((edge[0].hash, edge[1].hash))
        self.edges_left -= 2
        return self
    
    def _set_event_id(self) -> "EventGraph":
        self._event_id = hash(tuple(sorted(self.get_tx_hashes())))
        return self

    def check_finished(self):
        if self.edges_left == 0:
            self._set_event_id()
            return True
        return False
    
    def _check_finished_debug(self):
        total = 0
        for _, count in self.txs.values():
            total += count
        if total != self.edges_left:
            print(f"WTF: {total} != {self.edges_left}")
        return total == 0
    
    def merge_with(self, right: "EventGraph") -> "EventGraph":
        for k, v in right.txs.items():
            self.txs[k] = v
        self.edges.extend(right.edges)
        return self

    def get_tx_hashes(self):
        return list(self.txs.keys())

    def get_edges(self):
        return [(self.txs[left][0], self.txs[right][0]) for left, right in self.edges]

    def __repr__(self):
        return f"Event(txs: {len(self.txs)}, edges: {len(self.edges)})"
    
    def __hash__(self) -> int:
        return hash((tuple(self.txs.keys()), tuple(self.edges)))

    def __eq__(self, right):
        return hash(self) == hash(right)


# event processor class
class EdgeDetector:
    OTHER_BATCH_SIZE = 10000
    FORK_BOMB_SIZE = 10240

    def __init__(self, other_queue: mp.Queue, insert_tick_tocks=False):
        self.other_queue = other_queue
        self.insert_tick_tocks = insert_tick_tocks

        self.edges: Dict[Tuple[str, bool], D.Transaction] = {}
        self.events: Dict[str, EventGraph] = {}

        self._active_events = 0
        self._finished_events = 0
        self._merge_count = 0
        self._found_edges = 0
        self._ignored_txs = 0
        self._last_stats_timestamp = datetime.now().timestamp()

        self._other_txs = []
        self._pbar = None
        self._session_maker: Session = None

    def set_session_maker(self, session_maker: sessionmaker) -> "EdgeDetector":
        self._session_maker = session_maker
        return self

    def set_pbar(self, pbar: tqdm) -> "EdgeDetector":
        self._pbar = pbar
        return self

    def print_stats(self) -> "EdgeDetector":
        new_timestamp = datetime.now().timestamp()
        # if len(self.events) > 1000:
        #     with open('private/events.pickle', 'wb') as f:
        #         print('Debug pickle')
        #         pickle.dump(self.events, f)
        #         raise RuntimeError('STOP')
        if new_timestamp - self._last_stats_timestamp > 5:
            # active_events = len(set(self.events.values()))
            active_events = self._active_events
            msg = (f'events found: {self._finished_events} (active: {active_events}), '
                   f'txs: {len(self.events)} (ignored: {self._ignored_txs}), '
                   f'merges: {self._merge_count}, '
                   f'edges found: {self._found_edges} (active: {len(self.edges)})')
            if self._pbar is not None:
                self._pbar.set_description(msg)
            self._last_stats_timestamp = new_timestamp
        return self

    def insert_event(self, event: EventGraph) -> "EdgeDetector":
        with self._session_maker() as session:
            with session.begin():
                # event
                data = [{'id': event.event_id, 'meta': {}}]
                session.execute(insert(D.Event).on_conflict_do_nothing(), data)

                # # event_transaction
                # data = [{'event_id': event.event_id, 'tx_hash': tx_hash} for tx_hash in event.txs]
                # session.execute(insert(D.EventTransaction).on_conflict_do_nothing(), data)
                for tx_hash in event.txs:
                    session.execute(update(D.Transaction)
                                    .where(D.Transaction.hash == tx_hash)
                                    .values(event_id=event.event_id))

                # event_edges
                data = [{'event_id': event.event_id,
                        'left_tx_hash': edge[0],
                        'right_tx_hash': edge[1]} for edge in event.edges]
                session.execute(insert(D.EventEdge).on_conflict_do_nothing(), data)
        return self

    def insert_other(self, tx_hash, is_tick_tock=False) -> "EdgeDetector":
        self.other_queue.put((tx_hash, is_tick_tock))
        return self

    def process_tx(self, tx: D.Transaction) -> "EdgeDetector":
        if tx.description['type'] == 'tick_tock':
            if self.insert_tick_tocks:
                self.insert_other(tx.hash, is_tick_tock=True)
            return self

        # process transaction
        count = 0
        for msg in tx.messages:
            is_input = msg.direction == 'in'
            msg_hash = msg.message_hash
            
            message_type = get_message_type(msg)
            if (msg_hash, not is_input) in self.edges:
                count += 1
                # saving for the future
                tx2 = self.edges.pop((msg_hash, not is_input))
                edge = (tx2, tx) if is_input else (tx, tx2)
                self._found_edges += 1

                # new item
                if edge[0].hash not in self.events and edge[1].hash not in self.events:
                    self._active_events += 1
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
                    self._active_events -= 1
                    event = self.events[edge[0].hash]
                    right = self.events[edge[1].hash]
                    # print(f'Merge events: {event} <- {right}')
                    self._merge_count += 1
                    
                    event.merge_with(right)
                    event.add_edge(edge)

                    for tx_hash in right.get_tx_hashes():
                        self.events[tx_hash] = event

                # # test
                # for tx_hash in event.get_tx_hashes():
                #     if tx_hash in self.events and not self.events[tx_hash] == event:
                #         print(f'WARNING! {self.events[tx_hash]}')
                    
                # test big event
                if len(event.edges) > EdgeDetector.FORK_BOMB_SIZE:
                    pass

                # test if finished
                if event.check_finished():
                    if len(event.edges) > 100:
                        print(f'Big event found: {event}')
                    for tx_hash in event.get_tx_hashes():
                        self.events.pop(tx_hash)
                    # TODO: insert event
                    self._finished_events += 1
                    self._active_events -= 1
                    self.insert_event(event)
            elif message_type == 'ord':
                count += 1
                # edge found
                self.edges[(msg_hash, is_input)] = tx
        if count == 0:
            # mark system transaction
            self.insert_other(tx.hash)
            self._ignored_txs += 1
        self.print_stats()
        return self


# instance
if __name__ == '__main__':
    parser = argparse.ArgumentParser('Event Detector')
    parser.add_argument('--batch-size', type=int, default=8192, help='Select transactions batch size')
    parser.add_argument('--other-batch-size', type=int, default=4096, help='Insert other transactions batch size')
    parser.add_argument('--start-lt', type=int, default=None, help='Start from lt')
    parser.add_argument('--inverse', action='store_true', help='Scan in inverse order (DANGER: may cause undetected events)')
    parser.add_argument('--verbose', action='store_true', help='Show progress bar')
    parser.add_argument('--count-txs', action='store_true', help='Do not count transactions')
    args = parser.parse_args()

    other_queue = mp.Queue()
    other_thread = OtherProcess(other_queue, SyncSessionMaker, batch_size=args.other_batch_size)
    other_thread.start()

    edge_detector = EdgeDetector(other_queue, insert_tick_tocks=True)
    edge_detector.set_session_maker(SyncSessionMaker)

    try:
        if args.count_txs:
            with SyncSessionMaker() as session:
                query = query_new_transactions(session)
                query = query.order_by(D.Transaction.lt.asc() if not args.inverse else D.Transaction.lt.desc())
                if args.start_lt is not None:
                    query = query.filter(D.Transaction.lt >= args.start_lt)
                print('Counting new transactions')
                total = query.count()
        else: 
            total = 0
            
        print('Detecting events')
        pbar = tqdm(total=total, smoothing=0.001, disable=not args.verbose)
        edge_detector.set_pbar(pbar)
        while True:
            with SyncSessionMaker() as session:
                query = query_new_transactions(session)
                query = query.order_by(D.Transaction.lt.asc() if not args.inverse else D.Transaction.lt.desc())
                if args.start_lt is not None:
                    query = query.filter(D.Transaction.lt >= args.start_lt)
                query = query.yield_per(args.batch_size).enable_eagerloads(False)
                for tx in query:
                    edge_detector.process_tx(tx)
                    pbar.update(1)
    except KeyboardInterrupt:
        print('Gracefully stopped')
        print(traceback.format_exc())

        # print('Dumping events')
        # data = list(set(edge_detector.events.values()))
        # with open('private/events.pickle', 'wb') as f:
        #     print('Num events:', len(data))
        #     pickle.dump(data, f)
    except:
        print(traceback.format_exc())
    other_thread.terminate()
    other_thread.join()
    print('Whole program finished')
    sys.exit(0)
# end script
