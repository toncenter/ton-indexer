from __future__ import annotations

import json
import pathlib

import lz4.frame
import msgpack

from indexer.core.database import MessageContent, Transaction, Message, Trace, NFTItem, JettonWallet
from typing import Dict, Any, List, Tuple

from indexer.events.interface_repository import DedustPool


def deserialize_message_content(data: Dict[str, Any]) -> MessageContent:
    """Deserialize dictionary to MessageContent."""
    if not data:
        return None
    return MessageContent(
        hash=data['hash'],
        body=data['body'],
    )


def deserialize_message(data: Dict[str, Any], transaction: Transaction = None) -> Message:
    """Deserialize dictionary to Message."""
    if not data:
        return None

    message = Message(
        msg_hash=data['msg_hash'],
        tx_hash=data['tx_hash'],
        tx_lt=data['tx_lt'],
        direction=data['direction'],
        trace_id=data['trace_id'],
        source=data['source'],
        destination=data['destination'],
        value=data['value'],
        fwd_fee=data['fwd_fee'],
        ihr_fee=data['ihr_fee'],
        created_lt=data['created_lt'],
        created_at=data['created_at'],
        opcode=data['opcode'],
        ihr_disabled=data['ihr_disabled'],
        bounce=data['bounce'],
        bounced=data['bounced'],
        import_fee=data['import_fee'],
        body_hash=data['body_hash'],
        init_state_hash=data['init_state_hash'],
        value_extra_currencies=data['value_extra_currencies'],
    )

    # Set up the transaction relationship
    if transaction:
        message.transaction = transaction

    if 'message_content' in data:
        message.message_content = deserialize_message_content(data['message_content'])

    if 'init_state' in data:
        message.init_state = deserialize_message_content(data['init_state'])

    return message


def deserialize_transaction(data: Dict[str, Any]) -> Transaction:
    """Deserialize dictionary to Transaction."""
    if not data:
        return None

    tx = Transaction(
        block_seqno=data['block_seqno'],
        mc_block_seqno=data['mc_block_seqno'],
        trace_id=data['trace_id'],
        account=data['account'],
        hash=data['hash'],
        lt=data['lt'],
        prev_trans_hash=data['prev_trans_hash'],
        prev_trans_lt=data['prev_trans_lt'],
        now=data['now'],
        orig_status=data['orig_status'],
        end_status=data['end_status'],
        total_fees=data['total_fees'],
        account_state_hash_before=data['account_state_hash_before'],
        account_state_hash_after=data['account_state_hash_after'],
        descr=data['descr'],
        aborted=data['aborted'],
        destroyed=data['destroyed'],
        credit_first=data['credit_first'],
        is_tock=data['is_tock'],
        installed=data['installed'],
        compute_mode=data['compute_mode'],
        compute_exit_code=data['compute_exit_code'],
        compute_exit_arg=data['compute_exit_arg'],
        bounce=data['bounce'],
        emulated=False
    )

    tx.messages = [deserialize_message(msg_data, tx) for msg_data in data['messages']]

    return tx


def deserialize_trace(data: Dict[str, Any]) -> Trace:
    """Deserialize dictionary to Trace."""
    return Trace(
        trace_id=data['trace_id'],
        external_hash=data['external_hash'],
        mc_seqno_start=data['mc_seqno_start'],
        mc_seqno_end=data['mc_seqno_end'],
        start_lt=data['start_lt'],
        start_utime=data['start_utime'],
        end_lt=data['end_lt'],
        end_utime=data['end_utime'],
        state=data['state'],
        pending_edges_=data['pending_edges_'],
        edges_=data['edges_'],
        nodes_=data['nodes_'],
        classification_state=data['classification_state'],
    )


def deserialize_nft_item(data: Dict[str, Any]) -> NFTItem:
    """Deserialize dictionary to NFTItem."""
    return NFTItem(
        address=data['address'],
        init=data['init'],
        index=data['index'],
        collection_address=data['collection_address'],
        owner_address=data['owner_address'],
        content=data['content'],
    )


def deserialize_jetton_wallet(data: Dict[str, Any]) -> JettonWallet:
    """Deserialize dictionary to JettonWallet."""
    return JettonWallet(
        balance=data['balance'],
        address=data['address'],
        owner=data['owner'],
        jetton=data['jetton'],
    )

def deserialize_nft_item(data: Dict[str, Any]) -> NFTItem:
    """Deserialize dictionary to NFTItem."""
    return NFTItem(
        address=data['address'],
        init=data['init'],
        index=data['index'],
        collection_address=data['collection_address'],
        owner_address=data['owner_address'],
        content=data['content'],
    )

def deserialize_dedust_pool(account, data: Dict[str, Any]) -> DedustPool:
    """Deserialize dictionary to DedustPool"""
    return DedustPool(address=account, assets=data['assets'])


def load_trace(data: Dict) -> Tuple[Trace, Dict[str, Any]]:
    """
    Load a trace and all its related data from JSON.

    Args:
        data: Trace data

    Returns:
        Tuple[Trace, Dict[str, Any]]: Trace and interfaces
    """

    transactions = []
    for tx_data in data['transactions']:
        tx = deserialize_transaction(tx_data)
        transactions.append(tx)

    trace = deserialize_trace(data['trace'])
    trace.transactions = transactions

    interfaces = data.get('interfaces', {})
    return trace, interfaces


def load_trace_from_file(file_path: str | pathlib.Path) -> Tuple[Trace, Dict[str, Any]]:
    """
    Load a trace and all its related data from a file.

    Args:
        file_path: Path to the JSON/LZ4 file containing the trace data

    Returns:
        Tuple[Trace, Dict[str, Any]]: The reconstructed trace object and interfaces
    """
    with open(file_path, 'rb') as file:
        data = file.read()
    if file_path.suffix == '.json':
        json_str = data.decode('utf-8')
        loaded_data = json.loads(json_str)
    elif file_path.suffix == '.lz4':
        decompressed = lz4.frame.decompress(data)
        loaded_data = msgpack.unpackb(decompressed)
    else:
        raise ValueError(f"Unsupported file format: {file_path.suffix}")
    return load_trace(loaded_data)
