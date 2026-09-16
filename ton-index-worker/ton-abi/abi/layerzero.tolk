tolk 1.4

struct LzPath {
    header_info: uint152 = 8903714975572488637007080065659;
    header_filler: int198 = -1;
    src_eid: uint32;
    src_oapp: uint256;
    dst_eid: uint32;
    dst_oapp: uint256;
}

struct LzPacket {
    header_info: uint152 = 417359019239977417716476838698419835;
    header_filler: int198 = -1;
    path: Cell<LzPath>;
    message: cell;
    nonce: uint64;
    guid: uint256;
}

struct LzMdLzSendRef {
    send_msglib_connection: uint256;
    native_fee: uint128;
    zro_fee: uint128;
    enforced_options: cell;
    callback_data: cell;
}

struct LzMdLzSend {
    name: uint80 = 119272640966244;
    header_info: uint180 = 582890735024998957421269964955452773563747974476099581;
    header_filler: int90 = -1;
    send_request_id: uint64;
    send_msglib_manager: uint256;
    send_msglib: uint256;
    packet: Cell<LzPacket>;
    extra_options: cell;
    ref: Cell<LzMdLzSendRef>;
}

enum UlnVerificationStatusCode : uint32 {
    succeeded = 0x3bbc306b,
    nonce_out_of_range = 0x7fcbb4ac,
    dvn_not_configured = 0x29c53fab,
}

struct UlnVerificationStatus {
    header_info: uint116 = 38421788582694199859296615363593851;
    header_filler: int234 = -1;
    nonce: uint64;
    status_code: UlnVerificationStatusCode;
}

struct LzOappExecutePacketRef {
    packet: Cell<LzPacket>;
}

struct LayerZeroOappExecuteCallbackAbi {
    packet_ref: Cell<LzOappExecutePacketRef>;
}

struct LzChannelSendCallbackRef2 {
    lz_send: Cell<LzMdLzSend>;
}

struct LzChannelSendCallbackRef1 {
    next: Cell<LzChannelSendCallbackRef2>;
}

struct (0xa2b5fbae) LayerzeroChannelSendCallbackAbi {
    next: Cell<LzChannelSendCallbackRef1>;
}

struct LzExtendedMd {
    packet: Cell<LzPacket>;
}

struct ChannelCommitPacketAbi {
    extended_md: Cell<LzExtendedMd>;
}

struct UlnMdObj {
    verification_status: Cell<UlnVerificationStatus>;
}

struct UlnConnectionVerifyCallbackAbi {
    md_obj: Cell<UlnMdObj>;
}

type AllowedMessage = LayerzeroChannelSendCallbackAbi

// LayerZeroOappExecuteCallbackAbi, ChannelCommitPacketAbi, and
// UlnConnectionVerifyCallbackAbi can't join AllowedMessage (they carry no opcode
// prefix); forceAbiExport keeps them visible to the C++ generator.

contract LayerZeroMessages {
    incomingMessages: AllowedMessage
    forceAbiExport: LayerZeroOappExecuteCallbackAbi | ChannelCommitPacketAbi | UlnConnectionVerifyCallbackAbi
}
