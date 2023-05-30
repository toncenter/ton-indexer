#!/usr/bin/env python

from kafka import KafkaProducer
import json
import time
import uuid
from dataclasses import dataclass, asdict


@dataclass
class Event:
    event_scope: str
    event_target: str
    finding_type: str
    event_type: str
    severity: str
    data: dict
    # bot_id: str = None # will be generated at push_event
    # event_id: str = None # will be generated at push_event
    # timestamp: int = None # will be generated at push_event

class EventBus:
    def __init__(self, kafka_broker, topic):
        self.producer = KafkaProducer(bootstrap_servers=kafka_broker)
        self.topic = topic

    def push_event(self, event: Event):
        event = asdict(event)
        event['timestamp'] = int(time.time())
        event['event_id'] = str(uuid.uuid4())
        event['bot_id'] = "ton-indexer"
        self.producer.send(self.topic, json.dumps(event).encode("utf-8"))
