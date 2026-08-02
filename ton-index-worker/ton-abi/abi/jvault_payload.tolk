tolk 1.0

@overflow1023_policy("suppress")
struct JVaultStakePeriodPayload {
    payload_opcode: uint32;
    stake_period: uint32;
}

type AllowedMessage = JVaultStakePeriodPayload

struct Storage {
    dummy: uint1
}

contract JVaultPayloadMessages {
    incomingMessages: AllowedMessage
    storage: Storage
}

fun onInternalMessage(in: InMessage) {
    val msg = lazy AllowedMessage.fromSlice(in.body);
    match (msg) {
        JVaultStakePeriodPayload => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        else => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
    }
}
