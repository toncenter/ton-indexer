tolk 1.0

@overflow1023_policy("suppress")
struct (0x00000001) EvaaSupplyMaster {
    _query_id: bits64;
    _include_user_code: bits2;
    supply_amount: uint64;
    recipient_address: any_address;
}

@overflow1023_policy("suppress")
struct (0x0000011a) EvaaSupplySuccess {
    _query_id: bits64;
    _owner_address: any_address;
    _asset_id: uint256;
    amount_supplied: uint64;
}

@overflow1023_policy("suppress")
struct (0x00000002) EvaaWithdrawMaster {
    query_id: uint64;
    asset_id: uint256;
    amount: uint64;
    recipient_address: any_address;
}

@overflow1023_policy("suppress")
struct (0x00000211) EvaaWithdrawCollateralized {
    query_id: uint64;
    owner_address: any_address;
    asset_id: uint256;
    withdraw_amount_current: uint64;
}

type AllowedMessage = EvaaSupplyMaster | EvaaSupplySuccess | EvaaWithdrawMaster | EvaaWithdrawCollateralized

struct Storage {
    dummy: uint1
}

contract EvaaMessages {
    incomingMessages: AllowedMessage
    storage: Storage
}

fun onInternalMessage(in: InMessage) {
    val msg = lazy AllowedMessage.fromSlice(in.body);
    match (msg) {
        EvaaSupplyMaster => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        EvaaSupplySuccess => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        EvaaWithdrawMaster => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        EvaaWithdrawCollateralized => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        else => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
    }
}
