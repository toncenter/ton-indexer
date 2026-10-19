tolk 1.5.0

@overflow1023_policy("suppress")
struct JVaultStakePeriodPayload {
    payload_opcode: uint32;
    stake_period: uint32;
}

type AllowedMessage = JVaultStakePeriodPayload

contract JVaultPayloadMessages {
    incomingMessages: AllowedMessage
}
