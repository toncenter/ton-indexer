import abc
import base64
import json

import bson
import pymongo
from pymongo.database import Database
from pytoniq_core import Address

from indexer.events.blocks.core import Block, AccountValueFlow
from indexer.events.blocks.utils import AccountId, Asset, Amount
from indexer.core.database import Event


class MongoRepository:

    def __init__(self, db: Database):
        self.db = db

    def has_event(self, event_id: int) -> bool:
        collection = self.db["events"]
        _id = bson.ObjectId(event_id.to_bytes(12, byteorder="big", signed=True))
        return collection.count_documents({"_id": _id}) > 0

    def save_events(self, events: [tuple[Event, Block]]):
        collection = self.db["events"]
        documents = [self._get_document(e, b) for e, b in events]
        collection.insert_many(documents)

    def _get_document(self, event: Event, block: Block) -> dict:
        actions = []
        total_flow = AccountValueFlow()
        for b in block.bfs_iter():
            if b.btype != 'root':
                actions.append({
                    "type": b.btype,
                    "data": b.data,
                    "value": b.value_flow.to_dict()
                })
                total_flow.merge(b.value_flow)
        transaction_hashes = [base64.b64decode(m.hash) for m in event.transactions]
        lt = min([m.lt for m in event.transactions])
        collection = self.db["events"]
        return {
            "_id": bson.ObjectId(event.id.to_bytes(12, byteorder="big", signed=True)),
            "hashes": transaction_hashes,
            "lt": lt,
            "actions": [self.preprocess_action_dict(a) for a in actions],
            "accounts": [a.as_bytes() for a in total_flow.flow.keys()],
            "flow": total_flow.to_dict()
        }

    def save_actions(self, event: Event, block: Block):
        collection = self.db["events"]
        collection.insert_one(self._get_document(event, block))

    def preprocess_action_dict(self, data):
        for key, value in data.items():
            if isinstance(value, dict):
                # Recursive call for nested dictionaries
                self.preprocess_action_dict(value)
            elif isinstance(value, list):
                # Recursive call for nested lists
                for item in value:
                    if isinstance(item, dict):
                        self.preprocess_action_dict(item)
            elif isinstance(value, AccountId):
                # Modify the string to keep only the part after the last ':'
                data[key] = value.as_bytes()
            elif isinstance(value, Amount):
                data[key] = str(value)
            elif isinstance(value, Asset):
                data[key] = value.__dict__
                if value.is_jetton:
                    data[key]["jetton_address"] = value.jetton_address.as_bytes()
        return data
