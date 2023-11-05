#!/usr/bin/env python3
import random
import traceback

from typing import Optional
from tqdm.auto import tqdm

import indexer.core.database as D
from indexer.core.database import SessionMaker, SyncSessionMaker
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session, joinedload, contains_eager

from collections import defaultdict


display = print
DISABLE = False
SHOW_TXS = True

# queries
def query_transactions(session: Session):
    query = session.query(D.Transaction) \
                   .options(joinedload(D.Transaction.messages)) \
                   .options(joinedload(D.Transaction.messages).joinedload(D.TransactionMessage.message)) \
                   .order_by(D.Transaction.lt.asc())
    return query


def count_new_transactions(session: Session):
    return session.query(D.Transaction).filter(D.Transaction.event_id.is_(None)).count()



# # event detector
# class Event:
#     def __init__(self):
#         self.txs = {}
#         self.edges = []

#     def add_edge(self, 
#                  left: D.Transaction, 
#                  right: D.Transaction, 
#                  is_inverse: bool=False) -> "Event":
#         edge = (right.hash, left.hash) if is_inverse else (left.hash, right.hash)
#         if left.hash not in self.txs:
#             self.txs[left.hash] = left
#         if right.hash not in self.txs:
#             self.txs[right.hash] = right
#         return self


# event processor class
class EdgeDetector:
    system_accounts = {'-1:3333333333333333333333333333333333333333333333333333333333333333',
                       '-1:5555555555555555555555555555555555555555555555555555555555555555',
                       '-1:0000000000000000000000000000000000000000000000000000000000000000',}
    def __init__(self):
        self.txs = {}
        self.edges = {}

        self._found_edges = 0
        self._stats = 0

    def print_stats(self) -> "EdgeDetector":
        self._stats += 1
        if self._stats % 10000 == 0:
            print(f'txs: {len(self.txs)}, edges: {len(self.edges)}, found: {self._found_edges}')
            if SHOW_TXS and self.edges:
                tx_hash = list(self.edges.values())[random.choice(range(len(self.edges)))]
                tx, ctp = self.txs[tx_hash]
                
                print('=' * 90)
                print('=' * 90)
                print(tx.__dict__)
                print(ctp)
                print('-' * 90)
                for msg in tx.messages:
                    print(msg.message.__dict__)
                print('=' * 90)
                tx, ctp = self.txs[random.choice(list(self.txs.keys()))]
                print(tx.__dict__)
                print(ctp)
                print('-' * 90)
                for msg in tx.messages:
                    print(msg.message.__dict__)
                print('=' * 90)
                print('=' * 90)

        return self

    def _get_message_type(self, msg: D.TransactionMessage) -> str:
        if msg.message.source is None:
            return 'external'
        if msg.message.destination is None:
            return 'log'

        if msg.message.source in EdgeDetector.system_accounts and msg.message.destination in EdgeDetector.system_accounts:
            return 'system'
        return 'ord'

    def process_tx(self, tx: D.Transaction) -> "EdgeDetector":
        # if tx.account in EdgeDetector.account_blacklist:
        #     return self
        if tx.description['type'] == 'tick_tock':
            return self
        
        # process transaction
        tx_hash = tx.hash
        count_to_process = 0
        for msg in tx.messages:
            is_input = msg.direction == 'in'
            msg_hash = msg.message_hash
            
            message_type = self._get_message_type(msg)
            if (msg_hash, not is_input) in self.edges:
                # saving for the future
                tx2_hash = self.edges.pop((msg_hash, not is_input))
                edge = (tx2_hash, tx_hash, msg_hash) if is_input else (tx_hash, tx2_hash, msg_hash)

                # remove processed nodes
                tx2, ctp = self.txs[tx2_hash]
                ctp -= 1
                if ctp == 0:
                    self.txs.pop(tx2_hash)
                else:
                    self.txs[tx_hash] = (tx2, ctp)

                self._found_edges += 1
            elif message_type == 'ord':
                # edge found
                count_to_process += 1
                self.edges[(msg_hash, is_input)] = tx_hash
        
        if count_to_process > 0:
            self.txs[tx_hash] = (tx, count_to_process)
        self.print_stats()
        return self



# instance
if __name__ == '__main__':
    edge_detector = EdgeDetector()

    try:
        with SyncSessionMaker() as session:
            query = query_transactions(session)
            # query = query.filter(D.Transaction.event_id.is_(None))
            query = query.yield_per(4096).enable_eagerloads(False)
            
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

