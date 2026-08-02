tolk 1.0

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

struct Storage {
    dummy: uint1
}

contract JVaultMessages {
    incomingMessages: AllowedMessage
    storage: Storage
}

fun onInternalMessage(in: InMessage) {
    val msg = lazy AllowedMessage.fromSlice(in.body);
    match (msg) {
        JVaultUnstakeJettons => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        JVaultUnstakeRequest => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        JVaultClaim => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        else => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
    }
}
