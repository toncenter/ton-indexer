tolk 1.0

struct (0b0000) AssetTon {}

struct (0b0001) AssetJetton {
    workchain: uint8;
    hash: bits256;
}

type Asset = AssetTon | AssetJetton

struct DedustSwapInfo {
    sender_address: any_address;
    ref_address: any_address;
    reserve_0: coins;
    reserve_1: coins;
}

@overflow1023_policy("suppress")
struct DedustDepositLiquidityAssets {
    asset0: Asset;
    asset0_amount: coins;
    asset1: Asset;
    asset1_amount: coins;
}

@overflow1023_policy("suppress")
struct (0xad4eb6f5) DedustPayoutFromPool {
    query_id: uint64;
    proof: cell;
    amount: coins;
}

@overflow1023_policy("suppress")
struct (0x9c610de3) DedustSwapNotification {
    asset_in: Asset;
    asset_out: Asset;
    amount_in: coins;
    amount_out: coins;
    info: Cell<DedustSwapInfo>;
}

@overflow1023_policy("suppress")
struct (0xb56b9598) DedustDepositLiquidityToPool {
    _query_id: bits64;
    _proof: cell;
    owner: any_address;
    _min_lp: coins;
    field4: Cell<DedustDepositLiquidityAssets>;
}

type AllowedMessage = DedustPayoutFromPool | DedustSwapNotification | DedustDepositLiquidityToPool

struct Storage {
    dummy: uint1
}

contract DedustMessages {
    incomingMessages: AllowedMessage
    storage: Storage
}

fun onInternalMessage(in: InMessage) {
    val msg = lazy AllowedMessage.fromSlice(in.body);
    match (msg) {
        DedustPayoutFromPool => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        DedustSwapNotification => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        DedustDepositLiquidityToPool => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        else => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
    }
}
