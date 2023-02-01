#!/usr/bin/env python
"""
Yet another bitreader...
TODO get rid of it and switch to tonsdk
"""

from bitarray import bitarray
import codecs

class BitReader:

    def __init__(self, array: bitarray):
        self.array = array

    def read_bits(self, count):
        assert len(self.array) >= count, f"Array size: {len(self.array)}, requested {count}"
        bits = self.array[:count]
        self.array = self.array[count:]
        return bits

    def read_remaining(self):
        return self.array

    def slice_bits(self):
        return len(self.array)

    # based on pytonlib: https://github.com/toncenter/pytonlib/blob/main/pytonlib/utils/address.py
    @staticmethod
    def calc_crc(message):
        poly = 0x1021
        reg = 0
        message += b'\x00\x00'
        for byte in message:
            mask = 0x80
            while (mask > 0):
                reg <<= 1
                if byte & mask:
                    reg += 1
                mask >>= 1
                if reg > 0xffff:
                    reg &= 0xffff
                    reg ^= poly
        return reg.to_bytes(2, "big")

    """
    From whitepaper:
    
    addr_none$00 = MsgAddressExt;
    addr_extern$01 len:(## 8) external_address:(len * Bit)
    = MsgAddressExt;
    anycast_info$_ depth:(## 5) rewrite_pfx:(depth * Bit) = Anycast;
    addr_std$10 anycast:(Maybe Anycast)
    workchain_id:int8 address:uint256 = MsgAddressInt;
    addr_var$11 anycast:(Maybe Anycast) addr_len:(## 9)
    workchain_id:int32 address:(addr_len * Bit) = MsgAddressInt;
    _ MsgAddressInt = MsgAddress;
    _ MsgAddressExt = MsgAddress;
    
    """

    def read_address(self):
        addr_type = self.read_bits(2).to01()
        if addr_type == '10':  # addr_std
            assert self.read_bits(1).to01() == '0', f"Anycast not supported"
            wc = int(self.read_bits(8).to01(), 2)
            account_id = int(self.read_bits(256).to01(), 2).to_bytes(32, "big")
            tag = b'\xff' if wc == -1 else wc.to_bytes(1, "big")
            addr = b'\x11' + tag + account_id
            return codecs.decode(codecs.encode(addr + BitReader.calc_crc(addr), "base64"), "utf-8").strip() \
                .replace('/', '_').replace("+", '-')
        elif addr_type == '00':  # addr_none
            return None
        else:
            raise Exception(f"Unsupported addr type: {addr_type}")

    """
    var_uint$_ {n:#} len:(#< n) value:(uint (len * 8)) = VarUInteger n;
    nanograms$_ amount:(VarUInteger 16) = Grams;  
    """

    def read_coins(self):
        l = int(self.read_bits(4).to01(), 2)
        if l == 0:
            return 0
        return int(self.read_bits(l * 8).to01(), 2)

    def read_uint(self, num):
        return int(self.read_bits(num).to01(), 2)
