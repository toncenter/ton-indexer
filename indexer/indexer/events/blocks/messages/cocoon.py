from pytoniq_core import Slice


# === Common Messages ===


class CocoonReturnExcessesBack:
    """
    struct (0x2565934c) ReturnExcessesBack {
        queryId: uint64
    }
    """
    opcode = 0x2565934C

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.query_id = body.load_uint(64)


class CocoonPayout:
    """
    struct (0xc59a7cd3) Payout {
        queryId: uint64
    }
    """
    opcode = 0xC59A7CD3

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.query_id = body.load_uint(64)


# === Worker <-> Proxy Messages ===


class CocoonWorkerProxyRequest:
    """
    struct (0x4d725d2c) WorkerProxyRequest {
        queryId: uint64
        ownerAddress: address
        state: uint2
        tokens: uint64
        payload: cell?
    }
    """
    opcode = 0x4D725D2C

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.query_id = body.load_uint(64)
        self.owner_address = body.load_address()
        self.state = body.load_uint(2)
        self.tokens = body.load_uint(64)
        self.payload = body.load_maybe_ref()


class CocoonWorkerProxyPayoutRequest:
    """
    struct (0x08e7d036) WorkerProxyPayoutRequest {
        workerPart: coins
        proxyPart: coins
        sendExcessesTo: address
    }
    """
    opcode = 0x08E7D036

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.worker_part = body.load_coins()
        self.proxy_part = body.load_coins()
        self.send_excesses_to = body.load_address()


# === Client <-> Proxy Messages ===


class CocoonClientProxyRequest:
    """
    struct (0x65448ff4) ClientProxyRequest {
        queryId: uint64
        ownerAddress: address
        stateData: Cell<ClientStateData>
        payload: cell?
    }
    """
    opcode = 0x65448FF4

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.query_id = body.load_uint(64)
        self.owner_address = body.load_address()
        self.state_data = body.load_ref()
        self.payload = body.load_maybe_ref()


class CocoonClientProxyTopUp:
    """
    struct (0x5cfc6b87) ClientProxyTopUp {
        topUpCoins: coins
        sendExcessesTo: address
    }
    """
    opcode = 0x5CFC6B87

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.top_up_coins = body.load_coins()
        self.send_excesses_to = body.load_address()


class ClientProxyRegister:
    """
    struct (0xa35cb580) ClientProxyRegister {
        // empty payload
    }
    """
    opcode = 0xA35CB580

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode


class CocoonClientProxyRefundGranted:
    """
    struct (0xc68ebc7b) ClientProxyRefundGranted {
        coins: coins
        sendExcessesTo: address
    }
    """
    opcode = 0xC68EBC7B

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.coins = body.load_coins()
        self.send_excesses_to = body.load_address()


class CocoonClientProxyRefundForce:
    """
    struct (0xf4c354c9) ClientProxyRefundForce {
        coins: coins
        sendExcessesTo: address
    }
    """
    opcode = 0xF4C354C9

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.coins = body.load_coins()
        self.send_excesses_to = body.load_address()


# === Signed Message Structures ===


class CocoonSignedMessage:
    """
    struct SignedMessage {
        op: uint32
        queryId: uint64
        sendExcessesTo: address
        signature: bits512
        signedDataCell: cell
    }
    """
    def __init__(self, body: Slice):
        self.op = body.load_uint(32)
        self.query_id = body.load_uint(64)
        self.send_excesses_to = body.load_address()
        self.signature = body.load_bits(512)
        self.signed_data_cell = body.load_ref()


class CocoonPayoutPayload:
    """
    struct (0xa040ad28) PayoutPayload {
        data: PayoutPayloadData {
            queryId: uint64
            newTokens: uint64
            expectedMyAddress: address
        }
    }
    """
    opcode = 0xA040AD28

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.query_id = body.load_uint(64)
        self.new_tokens = body.load_uint(64)
        self.expected_my_address = body.load_address()


class CocoonLastPayoutPayload:
    """
    struct (0xf5f26a36) LastPayoutPayload {
        data: PayoutPayloadData {
            queryId: uint64
            newTokens: uint64
            expectedMyAddress: address
        }
    }
    """
    opcode = 0xF5F26A36

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.query_id = body.load_uint(64)
        self.new_tokens = body.load_uint(64)
        self.expected_my_address = body.load_address()


# === Root Messages ===


class CocoonAddWorkerType:
    """
    struct (0xe34b1c60) AddWorkerType {
        queryId: uint64
        workerHash: uint256
    }
    """
    opcode = 0xE34B1C60

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.query_id = body.load_uint(64)
        self.worker_hash = body.load_uint(256)


class CocoonDelWorkerType:
    """
    struct (0x8d94a79a) DelWorkerType {
        queryId: uint64
        workerHash: uint256
    }
    """
    opcode = 0x8D94A79A

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.query_id = body.load_uint(64)
        self.worker_hash = body.load_uint(256)


class CocoonAddModelType:
    """
    struct (0xc146134d) AddModelType {
        queryId: uint64
        modelHash: uint256
    }
    """
    opcode = 0xC146134D

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.query_id = body.load_uint(64)
        self.model_hash = body.load_uint(256)


class CocoonDelModelType:
    """
    struct (0x92b11c18) DelModelType {
        queryId: uint64
        modelHash: uint256
    }
    """
    opcode = 0x92B11C18

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.query_id = body.load_uint(64)
        self.model_hash = body.load_uint(256)


class CocoonAddProxyType:
    """
    struct (0x71860e80) AddProxyType {
        queryId: uint64
        proxyHash: uint256
    }
    """
    opcode = 0x71860E80

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.query_id = body.load_uint(64)
        self.proxy_hash = body.load_uint(256)


class CocoonDelProxyType:
    """
    struct (0x3c41d0b2) DelProxyType {
        queryId: uint64
        proxyHash: uint256
    }
    """
    opcode = 0x3C41D0B2

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.query_id = body.load_uint(64)
        self.proxy_hash = body.load_uint(256)


class CocoonRegisterProxy:
    """
    struct (0x927c7cb5) RegisterProxy {
        queryId: uint64
        proxyInfo: RemainingBitsAndRefs
    }
    """
    opcode = 0x927C7CB5

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.query_id = body.load_uint(64)
        # remaining bits and refs - store as is
        self.proxy_info = body


class CocoonUnregisterProxy:
    """
    struct (0x6d49eaf2) UnregisterProxy {
        queryId: uint64
        seqno: uint32
    }
    """
    opcode = 0x6D49EAF2

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.query_id = body.load_uint(64)
        self.seqno = body.load_uint(32)


class CocoonUpdateProxy:
    """
    struct (0x9c7924ba) UpdateProxy {
        queryId: uint64
        seqno: uint32
        proxyAddr: RemainingBitsAndRefs
    }
    """
    opcode = 0x9C7924BA

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.query_id = body.load_uint(64)
        self.seqno = body.load_uint(32)
        # remaining bits and refs - store as is
        self.proxy_addr = body


class CocoonChangeFees:
    """
    struct (0xc52ed8d4) ChangeFees {
        queryId: uint64
        pricePerToken: coins
        workerFeePerToken: coins
    }
    """
    opcode = 0xC52ED8D4

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.query_id = body.load_uint(64)
        self.price_per_token = body.load_coins()
        self.worker_fee_per_token = body.load_coins()


class CocoonChangeParams:
    """
    struct (0x022fa189) ChangeParams {
        queryId: uint64
        pricePerToken: coins
        workerFeePerToken: coins
        proxyDelayBeforeClose: uint32
        clientDelayBeforeClose: uint32
        minProxyStake: coins
        minClientStake: coins
    }
    """
    opcode = 0x022FA189

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.query_id = body.load_uint(64)
        self.price_per_token = body.load_coins()
        self.worker_fee_per_token = body.load_coins()
        self.proxy_delay_before_close = body.load_uint(32)
        self.client_delay_before_close = body.load_uint(32)
        self.min_proxy_stake = body.load_coins()
        self.min_client_stake = body.load_coins()


class CocoonUpgradeContracts:
    """
    struct (0xa2370f61) UpgradeContracts {
        queryId: uint64
        proxyCode: cell
        workerCode: cell
        clientCode: cell
    }
    """
    opcode = 0xA2370F61

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.query_id = body.load_uint(64)
        self.proxy_code = body.load_ref()
        self.worker_code = body.load_ref()
        self.client_code = body.load_ref()


class CocoonUpgradeCode:
    """
    struct (0x11aefd51) UpgradeCode {
        queryId: uint64
        newCode: cell
    }
    """
    opcode = 0x11AEFD51

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.query_id = body.load_uint(64)
        self.new_code = body.load_ref()


class CocoonResetRoot:
    """
    struct (0x563c1d96) ResetRoot {
        queryId: uint64
    }
    """
    opcode = 0x563C1D96

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.query_id = body.load_uint(64)


class CocoonUpgradeFull:
    """
    struct (0x4f7c5789) UpgradeFull {
        queryId: uint64
        newData: cell
        newCode: cell
    }
    """
    opcode = 0x4F7C5789

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.query_id = body.load_uint(64)
        self.new_data = body.load_ref()
        self.new_code = body.load_ref()


class CocoonChangeOwner:
    """
    struct (0xc4a1ae54) ChangeOwner {
        queryId: uint64
        newOwnerAddress: address
    }
    """
    opcode = 0xC4A1AE54

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.query_id = body.load_uint(64)
        self.new_owner_address = body.load_address()


# === Worker Messages ===


class CocoonOwnerWorkerRegister:
    """
    struct (0x26ed7f65) OwnerWorkerRegister {
        queryId: uint64
        sendExcessesTo: address
    }
    """
    opcode = 0x26ED7F65

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.query_id = body.load_uint(64)
        self.send_excesses_to = body.load_address()


# === Proxy Messages ===


class CocoonExtProxyPayoutRequest:
    """
    struct (0x7610e6eb) ExtProxyPayoutRequest {
        queryId: uint64
        sendExcessesTo: address
    }
    """
    opcode = 0x7610E6EB

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.query_id = body.load_uint(64)
        self.send_excesses_to = body.load_address()


class CocoonExtProxyIncreaseStake:
    """
    struct (0x9713f187) ExtProxyIncreaseStake {
        queryId: uint64
        grams: coins
        sendExcessesTo: address
    }
    """
    opcode = 0x9713F187

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.query_id = body.load_uint(64)
        self.grams = body.load_coins()
        self.send_excesses_to = body.load_address()


class CocoonOwnerProxyClose:
    """
    struct (0xb51d5a01) OwnerProxyClose {
        queryId: uint64
        sendExcessesTo: address
    }
    """
    opcode = 0xB51D5A01

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.query_id = body.load_uint(64)
        self.send_excesses_to = body.load_address()


class CocoonCloseRequestPayload:
    """
    struct (0x636a4391) CloseRequestPayload {
        queryId: uint64
        expectedMyAddress: address
    }
    """
    opcode = 0x636A4391

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.query_id = body.load_uint(64)
        self.expected_my_address = body.load_address()


class CocoonCloseCompleteRequestPayload:
    """
    struct (0xe511abc7) CloseCompleteRequestPayload {
        queryId: uint64
        expectedMyAddress: address
    }
    """
    opcode = 0xE511ABC7

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.query_id = body.load_uint(64)
        self.expected_my_address = body.load_address()


# === Client Messages ===


class CocoonExtClientTopUp:
    """
    struct (0xf172e6c2) ExtClientTopUp {
        queryId: uint64
        topUpAmount: coins
        sendExcessesTo: address
    }
    """
    opcode = 0xF172E6C2

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.query_id = body.load_uint(64)
        self.top_up_amount = body.load_coins() or 0
        self.send_excesses_to = body.load_address()


class CocoonOwnerClientChangeSecretHashAndTopUp:
    """
    struct (0x8473b408) OwnerClientChangeSecretHashAndTopUp {
        queryId: uint64
        topUpAmount: coins
        newSecretHash: uint256
        sendExcessesTo: address
    }
    """
    opcode = 0x8473B408

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.query_id = body.load_uint(64)
        self.top_up_amount = body.load_coins()
        self.new_secret_hash = body.load_uint(256)
        self.send_excesses_to = body.load_address()


class CocoonOwnerClientRegister:
    """
    struct (0xc45f9f3b) OwnerClientRegister {
        queryId: uint64
        nonce: uint64
        sendExcessesTo: address
    }
    """
    opcode = 0xC45F9F3B

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.query_id = body.load_uint(64)
        self.nonce = body.load_uint(64)
        self.send_excesses_to = body.load_address()


class CocoonOwnerClientChangeSecretHash:
    """
    struct (0xa9357034) OwnerClientChangeSecretHash {
        queryId: uint64
        newSecretHash: uint256
        sendExcessesTo: address
    }
    """
    opcode = 0xA9357034

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.query_id = body.load_uint(64)
        self.new_secret_hash = body.load_uint(256)
        self.send_excesses_to = body.load_address()


class CocoonOwnerClientIncreaseStake:
    """
    struct (0x6a1f6a60) OwnerClientIncreaseStake {
        queryId: uint64
        newStake: coins
        sendExcessesTo: address
    }
    """
    opcode = 0x6A1F6A60

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.query_id = body.load_uint(64)
        self.new_stake = body.load_coins()
        self.send_excesses_to = body.load_address()


class CocoonOwnerClientWithdraw:
    """
    struct (0xda068e78) OwnerClientWithdraw {
        queryId: uint64
        sendExcessesTo: address
    }
    """
    opcode = 0xDA068E78

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.query_id = body.load_uint(64)
        self.send_excesses_to = body.load_address()


class CocoonOwnerClientRequestRefund:
    """
    struct (0xfafa6cc1) OwnerClientRequestRefund {
        queryId: uint64
        sendExcessesTo: address
    }
    """
    opcode = 0xFAFA6CC1

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.query_id = body.load_uint(64)
        self.send_excesses_to = body.load_address()


class CocoonChargePayload:
    """
    struct (0xbb63ff93) ChargePayload {
        queryId: uint64
        newTokensUsed: uint64
        expectedMyAddress: address
    }
    """
    opcode = 0xBB63FF93

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.query_id = body.load_uint(64)
        self.new_tokens_used = body.load_uint(64)
        self.expected_my_address = body.load_address()


class CocoonGrantRefundPayload:
    """
    struct (0xefd711e1) GrantRefundPayload {
        queryId: uint64
        newTokensUsed: uint64
        expectedMyAddress: address
    }
    """
    opcode = 0xEFD711E1

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.query_id = body.load_uint(64)
        self.new_tokens_used = body.load_uint(64)
        self.expected_my_address = body.load_address()


class CocoonOwnerWalletSendMessage:
    """
    struct (0x9c69f376) OwnerWalletSendMessage {
        queryId: uint64
        mode: uint8
        body: cell
    }
    """
    opcode = 0x9c69f376

    def __init__(self, body: Slice):
        assert body.load_uint(32) == self.opcode
        self.query_id = body.load_uint(64)
        self.mode = body.load_uint(8)
        self.body = body.load_ref()
