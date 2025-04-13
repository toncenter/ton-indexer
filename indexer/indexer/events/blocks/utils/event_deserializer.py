from __future__ import annotations

import base64

import msgpack

from indexer.core.database import MessageContent, Transaction, Message, Trace, TraceEdge

account_status_map = ['uninit', 'frozen', 'active', 'nonexist']


def _message_from_tuple(tx: Transaction, data, direction: str) -> Message:
    message_content = MessageContent(hash='', body=data['body_boc'])
    message = Message(
        msg_hash=base64.b64encode(data['hash']).decode(),
        tx_hash=tx.hash,
        tx_lt=tx.lt,
        source=data['source'],
        destination=data['destination'],
        direction=direction,
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
        message_content=message_content,
        transaction=tx,
    )
    if data['init_state_boc'] is not None:
        message.init_state = MessageContent(hash='', body=data['init_state_boc'])
    return message

def fill_tx_description(tx: Transaction, data):
    tx.descr = 'ord'
    tx.credit_first = data['credit_first']
    tx.aborted = data['aborted']
    tx.bounce = data['bounce']
    tx.destroyed = data['destroyed']
    if data.get('storage_ph') is not None:
        tx.storage_fees_collected = data['storage_ph']['storage_fees_collected']
        tx.storage_fees_due = data['storage_ph']['storage_fees_due']
    match data['storage_ph']['status_change']:
        case 0:
            tx.storage_fees_change = 'unchanged'
        case 1:
            tx.storage_fees_change = 'frozen'
        case 2:
            tx.storage_fees_change = 'deleted'
    if data.get('credit_ph') is not None:
        tx.due_fees_collected = data['credit_ph']['due_fees_collected']
        tx.credit = data['credit_ph']['credit']
    compute_ph_type, compute_ph = data['compute_ph']
    match compute_ph_type:
        case 0:
            tx.compute_skipped = True
            tx.skipped_reason = compute_ph['reason']
        case 1:
            tx.compute_mode = 'vm'
            tx.compute_success = compute_ph['success']
            tx.compute_msg_state_used = compute_ph['msg_state_used']
            tx.compute_account_activated = compute_ph['account_activated']
            tx.compute_gas_fees = compute_ph['gas_fees']
            tx.compute_gas_used = compute_ph['gas_used']
            tx.compute_gas_limit = compute_ph['gas_limit']
            tx.compute_gas_credit = compute_ph['gas_credit']
            tx.compute_mode = compute_ph['mode']
            tx.compute_exit_code = compute_ph['exit_code']
            tx.compute_exit_arg = compute_ph['exit_arg']
            tx.compute_vm_steps = compute_ph['vm_steps']
            tx.compute_vm_init_state_hash = compute_ph['vm_init_state_hash']
            tx.compute_vm_final_state_hash = compute_ph['vm_final_state_hash']
    action = data['action']
    if action is not None:
        tx.action_success = action['success']
        tx.action_valid = action['valid']
        tx.action_no_funds = action['no_funds']
        tx.action_status_change = action['status_change']
        tx.action_total_fwd_fees = action['total_fwd_fees']
        tx.action_total_action_fees = action['total_action_fees']
        tx.action_result_code = action['result_code']
        tx.action_result_arg = action['result_arg']
        tx.action_tot_actions = action['tot_actions']
        tx.action_spec_actions = action['spec_actions']
        tx.action_skipped_actions = action['skipped_actions']
        tx.action_msgs_created = action['msgs_created']
        tx.action_action_list_hash = action['action_list_hash']
        tx.action_tot_msg_size_cells = action['tot_msg_size']['cells']
        tx.action_tot_msg_size_bits = action['tot_msg_size']['bits']

def unpack_messagepack_tx(data: bytes) -> Transaction:
    decoded_data = msgpack.unpackb(data, raw=False)
    tx_data = decoded_data['transaction']
    tx = Transaction(
        lt=tx_data['lt'],
        hash=base64.b64encode(tx_data['hash']).decode(),
        prev_trans_hash=base64.b64encode(tx_data['prev_trans_hash']).decode(),
        prev_trans_lt=tx_data['prev_trans_lt'],
        account=tx_data['account'],
        now=tx_data['now'],
        mc_block_seqno=decoded_data['mc_block_seqno'],
        orig_status=account_status_map[tx_data['orig_status']],
        end_status=account_status_map[tx_data['end_status']],
        total_fees=tx_data['total_fees'],
        account_state_hash_before=base64.b64encode(tx_data['account_state_hash_before']).decode(),
        account_state_hash_after=base64.b64encode(tx_data['account_state_hash_after']).decode(),
        emulated=decoded_data['emulated']
    )
    fill_tx_description(tx, tx_data['description'])
    tx.messages = [_message_from_tuple(tx, msg, 'out') for msg in tx_data['out_msgs']] + [
        _message_from_tuple(tx, tx_data['in_msg'], 'in')]
    return tx


def deserialize_event(trace_id, packed_transactions_map: dict[str, bytes]) -> Trace:
    edges = []
    transactions = []
    try:
        root_id = packed_transactions_map['root_node'].decode()
    except KeyError:
        raise ValueError(f"root_node key not found for trace '{trace_id}'")
    try:
        root = packed_transactions_map[root_id]
    except KeyError:
        raise ValueError(f"Root tx not found for trace '{trace_id}'")
    
    def load_leaf(tx):
        for msg in tx.messages:
            if msg.direction != 'out' or msg.destination is None:
                continue
            child_tx = unpack_messagepack_tx(packed_transactions_map[msg.msg_hash])
            edges.append(TraceEdge(left_tx=tx.hash, right_tx=child_tx.hash, msg_hash=msg.msg_hash, trace_id=trace_id))
            transactions.append(child_tx)
            load_leaf(child_tx)

    root_tx = unpack_messagepack_tx(root)
    if root_tx and not root_tx.emulated:
        trace_id = root_tx.hash
    transactions.append(root_tx)
    load_leaf(root_tx)

    return Trace(transactions=transactions, trace_id=trace_id, classification_state='unclassified',
                 state='complete', start_lt=root_tx.lt, external_hash=root_id)
