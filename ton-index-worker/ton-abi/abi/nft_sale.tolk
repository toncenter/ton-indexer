tolk 1.4

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

contract NftSaleMessages {
    incomingMessages: AllowedMessage
}
