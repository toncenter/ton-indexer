tolk 1.0

@overflow1023_policy("suppress")
struct (0x05138d91) NftOwnershipAssignedPrevOwner {
    _query_id: bits64;
    prev_owner: any_address;
}

@overflow1023_policy("suppress")
struct (0x6c6c2080) SaleUpdateMessage {
    _query_id: bits64;
    new_full_price: coins;
    new_mp_fee: coins;
    new_royalty_amount: coins;
}

@overflow1023_policy("suppress")
struct (0x8b771735) NftReportStaticData {
    query_id: uint64;
    index: uint256;
    collection: any_address;
}

type AllowedMessage = NftOwnershipAssignedPrevOwner | SaleUpdateMessage | NftReportStaticData

struct Storage {
    dummy: uint1
}

contract NftSaleMessages {
    incomingMessages: AllowedMessage
    storage: Storage
}

fun onInternalMessage(in: InMessage) {
    val msg = lazy AllowedMessage.fromSlice(in.body);
    match (msg) {
        NftOwnershipAssignedPrevOwner => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        SaleUpdateMessage => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        NftReportStaticData => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        else => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
    }
}
