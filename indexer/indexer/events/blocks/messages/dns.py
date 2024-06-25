from pytoniq_core import Slice

from events.blocks.utils import AccountId


class ChangeDnsRecordMessage:
    opcode = 0x4eb1f0f9

    def __init__(self, boc: Slice):
        op = boc.load_uint(32)  # opcode
        self.key = boc.load_bytes(32)
        self.has_value = boc.remaining_refs > 0
        if self.has_value:
            self._parse_value(boc.load_ref().to_slice())
        else:
            self.value = None

    def _parse_value(self, value: Slice):
        sum_type = value.load_uint(16)
        if sum_type == 0xba93:
            self.value = {
                'sum_type': 'DNSNextResolver',
                'dns_next_resolver': AccountId(value.load_address())
            }
        elif sum_type == 0xad01:
            self.value = {
                'sum_type': 'DNSAdnlAddress',
                'dns_adnl_address': value.load_bytes(32),
                'flags': value.load_uint(8)
            }
        elif sum_type == 0x9fd3:
            self.value = {
                'sum_type': 'DNSSmcAddress',
                'dns_smc_address': AccountId(value.load_address()),
                'flags': value.load_uint(8)
            }
        elif sum_type == 0x7473:
            self.value = {
                'sum_type': 'DNSStorageAddress',
                'dns_storage_address': value.load_bytes(32)
            }
        elif sum_type == 0x1eda:
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
                'sum_type': 'DNSText',
                'dns_text': dns_text
            }
        else:
            self.value = {
                'sum_type': 'Unknown'
            }
