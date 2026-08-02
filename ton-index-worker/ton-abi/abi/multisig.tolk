tolk 1.0

@overflow1023_policy("suppress")
struct (0xf718510f) MultisigNewOrder {
    query_id: uint64;
    order_seqno: uint256;
    is_signer: bool;
    singer_index: uint8;
    expiration_date: uint48;
    order: cell;
}

@overflow1023_policy("suppress")
struct (0x9c73fba2) MultisigInitOrder {
    _query_id: bits64;
    _threshold: bits8;
    _signers: cell?;
    _expiration_date: bits48;
    order: cell;
    approve_on_init: bool;
}

@overflow1023_policy("suppress")
struct (0xa762230f) MultisigApprove {
    query_id: uint64;
    signer_index: uint8;
}

@overflow1023_policy("suppress")
struct (0xafaf283e) MultisigApproveRejected {
    _query_id: bits64;
    exit_code: uint32;
}

@overflow1023_policy("suppress")
struct (0x75097f5d) MultisigExecute {
    query_id: uint64;
    order_seqno: uint256;
    expiration_date: uint48;
    approvals_num: uint8;
    signers_hash: bits256;
    order: cell;
}

type AllowedMessage = MultisigNewOrder | MultisigInitOrder | MultisigApprove | MultisigApproveRejected | MultisigExecute

struct Storage {
    dummy: uint1
}

contract MultisigMessages {
    incomingMessages: AllowedMessage
    storage: Storage
}

fun onInternalMessage(in: InMessage) {
    val msg = lazy AllowedMessage.fromSlice(in.body);
    match (msg) {
        MultisigNewOrder => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        MultisigInitOrder => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        MultisigApprove => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        MultisigApproveRejected => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        MultisigExecute => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        else => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
    }
}
