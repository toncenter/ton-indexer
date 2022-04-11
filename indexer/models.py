import enum

class BlockShort:
    def __init__(self, block_short_raw):
        self._init_(block_short_raw['workchain'], block_short_raw['shard'], block_short_raw['seqno'])

    def _init_(self, workchain, shard, seqno):
        self.workchain = workchain
        self.shard = shard
        self.seqno = seqno

class BlockFull:
    def __init__(self, full_block_raw):
        self._init_(full_block_raw['id']['workchain'], full_block_raw['id']['shard'], full_block_raw['id']['seqno'], 
            full_block_raw['id']['root_hash'], full_block_raw['id']['file_hash'], full_block_raw['global_id'], full_block_raw['version'], 
            full_block_raw['after_merge'], full_block_raw['after_split'], full_block_raw['before_split'], 
            full_block_raw['want_merge'], full_block_raw['want_split'], full_block_raw['validator_list_hash_short'], 
            full_block_raw['catchain_seqno'], full_block_raw['min_ref_mc_seqno'], full_block_raw['is_key_block'], 
            full_block_raw['prev_key_block_seqno'], full_block_raw['start_lt'], full_block_raw['end_lt'], full_block_raw['vert_seqno'])

    def _init_(self, workchain, shard, seqno, root_hash, file_hash, global_id, version, after_merge,
        after_split, before_split, want_merge, want_split, validator_list_hash_short, catchain_seqno, 
        min_ref_mc_seqno, is_key_block, prev_key_block_seqno, start_lt, end_lt, vert_seqno):
        self.workchain = workchain, 
        self.shard = shard
        self.seqno = seqno
        self.root_hash = root_hash 
        self.file_hash = file_hash
        self.global_id = global_id
        self.version = version
        self.after_merge = after_merge
        self.after_split = after_split
        self.before_split = before_split
        self.want_merge = want_merge
        self.want_split = want_split
        self.validator_list_hash_short = validator_list_hash_short
        self.catchain_seqno = catchain_seqno 
        self.min_ref_mc_seqno = min_ref_mc_seqno
        self.is_key_block = is_key_block
        self.prev_key_block_seqno = prev_key_block_seqno
        self.start_lt = start_lt
        self.end_lt = end_lt
        self.vert_seqno = vert_seqno
        self.transactions = []

class Transaction:
    def __init__(self, addr, transaction_raw):
        in_msg_raw = transaction_raw.get('in_msg')
        if in_msg_raw is not None:
            in_msg = Message(Message.Direction.msg_in, in_msg_raw)
        else:
            in_msg = None
        out_msgs = []
        for out_msg_raw in transaction_raw['out_msgs']:
            out_msgs.append(Message(Message.Direction.msg_out, out_msg_raw))

        self._init_(addr, transaction_raw['transaction_id']['lt'], transaction_raw['transaction_id']['hash'], 
            transaction_raw['fee'], transaction_raw['storage_fee'], transaction_raw['other_fee'], transaction_raw['data'],
            transaction_raw['utime'], in_msg, out_msgs)

    def _init_(self, addr, lt, hash, fee, storage_fee, other_fee, data, utime, in_msg, out_msgs):
        self.addr = addr
        self.lt = lt
        self.hash = hash
        self.fee = fee
        self.storage_fee = storage_fee
        self.other_fee = other_fee
        self.data = data
        self.utime = utime
        self.in_msg = in_msg
        self.out_msgs = out_msgs

class Message:
    class Type(enum.Enum):
        external = 1
        internal = 2
        in_out = 3

    class Direction(enum.Enum):
        msg_in = 1
        msg_out = 2

    def __init__(self, direction, message_raw):
        if direction == Message.Direction.msg_in and message_raw['value'] == 0:
            msg_type = Message.Type.external
        elif message_raw['destination'] == '':
            msg_type = Message.Type.in_out
        else:
            msg_type = Message.Type.internal

        self._init_(msg_type, message_raw['source'], message_raw['destination'], message_raw['created_lt'], 
            message_raw['body_hash'], message_raw['value'], message_raw['fwd_fee'], message_raw['ihr_fee'], '') # TODO: body


    def _init_(self, msg_type, source, destination, created_lt, body_hash, value, fwd_fee, ihr_fee, body):
        self.msg_type = msg_type
        self.source = source
        self.destination = destination
        self.created_lt = created_lt
        self.body_hash = body_hash
        self.value = value
        self.fwd_fee = fwd_fee
        self.ihr_fee = ihr_fee
        self.body = body
