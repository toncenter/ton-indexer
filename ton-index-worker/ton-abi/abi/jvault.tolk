tolk 1.4

type JVaultClaimEntry = RemainingBitsAndRefs

@overflow1023_policy("suppress")
struct (0x499a9262) JVaultUnstakeJettons {
    query_id: uint64;
    jettons_to_unstake: coins;
}

@overflow1023_policy("suppress")
struct (0x0168d4b7) JVaultUnstakeRequest {
    query_id: uint64;
    jettons_to_unstake: coins;
}

@overflow1023_policy("suppress")
struct (0x78d9f109) JVaultClaim {
    query_id: uint64;
    jettons_to_claim: map<address, JVaultClaimEntry>;
}

type AllowedMessage = JVaultUnstakeJettons | JVaultUnstakeRequest | JVaultClaim

contract JVaultMessages {
    incomingMessages: AllowedMessage
}
