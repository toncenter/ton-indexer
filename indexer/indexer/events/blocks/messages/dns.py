from pytoniq_core import Slice

from indexer.events.blocks.utils import AccountId


class ChangeDnsRecordMessage:
    opcode = 0x4eb1f0f9

    def __init__(self, boc: Slice):
        op = boc.load_uint(32)  # opcode
        self.query_id = boc.load_uint(64)
        self.key = boc.load_bytes(32)
        self.has_value = boc.remaining_refs > 0
        if self.has_value:
            self._parse_value(boc.load_ref().to_slice())
        else:
            self.value = None

    def _parse_value(self, value: Slice):
        schema = value.load_uint(16)
        if schema == 0xba93:
            self.value = {
                'schema': 'DNSNextResolver',
                'address': AccountId(value.load_address())
            }
        elif schema == 0xad01:
            self.value = {
                'schema': 'DNSAdnlAddress',
                'address': value.load_bytes(32),
                'flags': value.load_uint(8)
            }
        elif schema == 0x9fd3:
            self.value = {
                'schema': 'DNSSmcAddress',
                'address': AccountId(value.load_address()),
                'flags': value.load_uint(8)
            }
        elif schema == 0x7473:
            self.value = {
                'schema': 'DNSStorageAddress',
                'address': value.load_bytes(32)
            }
        elif schema == 0x1eda:
            dns_text = ""
            chunks_count = value.load_uint(8)
            value_slice = value
            while chunks_count > 0:
                length = value.load_uint(8)
                dns_text += value.load_string(length)
                chunks_count -= 1
                if chunks_count > 0:
                    value_slice = value_slice.load_ref()
            self.value = {
                'schema': 'DNSText',
                'dns_text': dns_text
            }
        else:
            self.value = {
                'schema': 'Unknown'
            }
