from pytonlib.utils.address import detect_address
from base64 import (
    b64decode,
    b64encode,
    urlsafe_b64decode,
    urlsafe_b64encode
)
from bitarray.util import (
    hex2ba, 
    ba2hex,
    int2ba,
    ba2int
)


def b64_to_bytes(value: str):
    return b64decode(value)


def b64url_to_bytes(value: str):
    return urlsafe_b64decode(value)


def hex_to_bytes(value: str):
    return bytes.fromhex(value)


def bytes_to_b64(value: bytes):
    return b64encode(value).decode('utf8')


def bytes_to_b64url(value: bytes):
    return urlsafe_b64encode(value).decode('utf8')


def bytes_to_hex(value: bytes):
    return value.hex()


# converters
def b64_to_hex(value: str):
    return bytes_to_hex(b64_to_bytes(value))


def b64url_to_hex(value: str):
    return bytes_to_b64url(hex_to_bytes(value))


def b64url_to_b64(value: str):
    return bytes_to_b64(b64url_to_bytes(value))


def hex_to_b64(value: str):
    return bytes_to_b64(hex_to_bytes(value))


def hash_to_b64(b64_or_hex_hash):
    """
    Detect encoding of transactions hash and if necessary convert it to Base64.
    """
    if len(b64_or_hex_hash) == 44:
        # Hash is base64 or base64url
        if '_' in b64_or_hex_hash or '-' in b64_or_hex_hash:
            return b64url_to_b64(b64_or_hex_hash)
        return b64_or_hex_hash
    if len(b64_or_hex_hash) == 64:
        # Hash is hex
        return hex_to_b64(b64_or_hex_hash)
    raise ValueError(f"Invalid hash: '{b64_or_hex_hash}'")


# address utils
def address_to_raw(address):
    try:
        raw_address = detect_address(address)["raw_form"]
    except Exception:
        raise ValueError(f"Invalid address: '{address}'")
    return raw_address


# int64 <-> hex
def hex_to_int(value):
    return ba2int(hex2ba(value), signed=True)


def int_to_hex(value, length=64, signed=True):
    return ba2hex(int2ba(value, length=length, signed=signed))
