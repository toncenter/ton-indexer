from pytonlib.utils.address import detect_address, account_forms
from functools import wraps
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


def optional_value(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if len(args) > 0 and args[0] is None:
            return None
        return func(*args, **kwargs)
    return wrapper


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


@optional_value
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
@optional_value
def address_to_raw(address):
    if address is None or address == 'addr_none':
        return None
    if address.lower() == 'null' or address.lower() == 'none':
        return 'null'
    try:
        raw_address = detect_address(address)["raw_form"].upper()
    except Exception:
        raise ValueError(f"Invalid address: '{address}'")
    return raw_address

def address_to_friendly(address: str, bounceable: bool, is_testnet: bool):
    try:
        if bounceable:
            return account_forms(address, is_testnet)["bounceable"]["b64url"]
        else:
            return account_forms(address, is_testnet)["non_bounceable"]["b64url"]
    except Exception:
        raise ValueError(f"Invalid address: '{address}'")    

hex_prefix = '0x'
# int64 <-> hex
@optional_value
def hex_to_int(value):
    value = value.lower()
    if value.startswith(hex_prefix):
        value = value[len(hex_prefix):]
    return ba2int(hex2ba(value), signed=True)

@optional_value
def int_to_hex(value, length=64, signed=True):
    return ba2hex(int2ba(value, length=length, signed=signed))
