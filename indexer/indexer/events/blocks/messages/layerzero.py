from __future__ import annotations

from pytoniq_core import Cell, Slice, begin_cell, Address

from Crypto.Hash import keccak

# ---- Here are correct parsers done by hand: ----

# Just ordinary layerzero msg body:
# (builder) beginTonMessage(int _opcode) asm "txnContext GETGLOB 4 INDEX SWAP NEWC 32 STU 64 STU";
# ;; ^ this just creates a builder with 32 bits for opcode and 64 bits for query_id from txnContext
# cell buildLayerzeroMessageBody(int donationNanos, int opcode, cell $md) impure inline {
#     cell ret = beginTonMessage(opcode)
#         .store_coins(donationNanos)
#         .store_slice(getOriginStd())
#         .store_ref($md)
#         .end_cell();
#     return ret;
# }
class LayerZeroMessageBody:
    def __init__(self, b: Cell):
        self.b = b
        self.s = b.to_slice()
        self.opcode = self.s.load_uint(32)
        self.query_id = self.s.load_uint(64)
        self.donationNanos = self.s.load_coins()
        self.origin = self.s.load_address()
        self.message_data = self.s.load_ref() # called `md` in func
        try:
            self.lz_send = parse_md_obj_in_lz_send(self.message_data)
        except:
            self.lz_send = None

    def __repr__(self):
        return f"""LayerZeroMessageBody(
            opcode={self.opcode},
            query_id={self.query_id},
            donationNanos={self.donationNanos},
            origin={self.origin},
            message_data={self.message_data},
            lz_send={self.lz_send})"""

# Just taking ref from md object to get another md object (silly, innit?)
# cell md::MdObj::build(cell $md, cell $obj) impure inline {
#     return begin_cell()
#         .store_uint(md::MdObj::_headerInfo, md::MdObj::_headerInfoBits) ;; header info
#         .store_ones(md::MdObj::_headerFillerBits)                       ;; header filler
#         .store_ref($md)                                                  ;; ref[0]
#         .store_ref($obj)                                                 ;; ref[1]
#         .end_cell();
# } 
def parse_md_obj_in_lz_send(b: Cell):
    # second ref is just a config, no specific user/action data
    return LayerZeroMDLzSend(b.refs[0])

# LzSend decoding:
# ;; this function is unused by the protocol but will be used by OApps
# cell md::LzSend::build(
#     int nativeFee,
#     int zroFee,
#     cell $extraOptions,
#     cell $enforcedOptions,
#     cell $packet,
#     cell callbackData
# ) impure inline {
#     return begin_cell()
#         .store_uint(md::LzSend::NAME, _NAME_WIDTH)
#         .store_uint(md::LzSend::_headerInfo, md::LzSend::_headerPostNameBits)
#         .store_ones(md::LzSend::_headerFillerBits)
#         .store_uint64(0)                       ;; sendRequestId
#         .store_uint256(NULLADDRESS)            ;; sendMsglibManager
#         .store_uint256(NULLADDRESS)            ;; sendMsglib
#         .store_ref($packet)
#         .store_ref($extraOptions)
#         .store_ref(
#             begin_cell()
#                 .store_uint256(NULLADDRESS)    ;; sendMsglibConnection
#                 .store_uint128(nativeFee)      ;; nativeFee
#                 .store_uint128(zroFee)         ;; zroFee
#                 .store_ref($enforcedOptions)   ;; enforcedOptions
#                 .store_ref(callbackData)       ;; callbackData
#                 .end_cell()
#         )
#         .end_cell();
# }
class LayerZeroMDLzSend:
    MD_NAME = int.from_bytes("lzSend".encode(), "big")
    NAME_WIDTH = 80
    HEADER_INFO = 582890735024998957421269964955452773563747974476099581 # md::LzSend::_headerInfo
    HEADER_POST_NAME_BITS = 180
    HEADER_FILLER_BITS = 90

    def __init__(self, b: Cell):
        self.b = b
        self.s = b.to_slice()
        self.name = self.s.load_uint(self.NAME_WIDTH)
        assert self.name == self.MD_NAME, f"wrong name: {self.name} != {self.MD_NAME} (expected)"
        self.header_info = self.s.load_uint(self.HEADER_POST_NAME_BITS)
        assert self.header_info == self.HEADER_INFO, "header info mismatch"
        self.header_filler = self.s.load_int(self.HEADER_FILLER_BITS) # load_int is used because `~` in python isn't for uint
        assert (~self.header_filler == 0), "header filler ones number mismatch"

        self.send_request_id = self.s.load_uint(64)
        self.send_msglib_manager = hex(self.s.load_uint(256))
        self.send_msglib = hex(self.s.load_uint(256))

        self.packet = LayerZeroPacket(self.s.load_ref())
        self.extra_options = self.s.load_ref()

        self.ref = self.s.load_ref()
        ref_s = self.ref.to_slice()
        self.send_msglib_connection = ref_s.load_uint(256)
        self.native_fee = ref_s.load_uint(128)
        self.zro_fee = ref_s.load_uint(128)
        self.enforced_options = ref_s.load_ref()
        self.callback_data = ref_s.load_ref()

    def __repr__(self):
        return f"""LayerZeroMDLzSend(
            send_request_id={self.send_request_id},
            send_msglib_manager={self.send_msglib_manager},
            send_msglib={self.send_msglib},
            packet={self.packet},
            extra_options={self.extra_options},
            enforced_options={self.enforced_options},
            callback_data={self.callback_data})"""
    
# Packet decoding:
# ;; in usdt-oft:
# lz::Packet::nonceless(
#     $SendPath,
#     lzMessage
# )
# ;; this function is unused by the protocol but will be used by OApps
# cell lz::Packet::nonceless(cell $path, cell message) impure inline method_id {
#     return lz::Packet::build($path, message, 0);
# }
# ;; this function is unused by the protocol but will be used by OApps
# cell lz::Packet::build(cell $path, cell message, int nonce) impure inline method_id {
#     return begin_cell()
#         .store_uint(lz::Packet::_headerInfo, lz::Packet::_headerInfoBits) ;; header info
#         .store_ones(lz::Packet::_headerFillerBits)                        ;; header filler
#         .store_ref($path)                                                  ;; path
#         .store_ref(message)                                                ;; message
#         .store_uint64(nonce)                                               ;; nonce
#         .store_uint256(0)                                                  ;; guid (default = 0)
#         .end_cell();
# }
class LayerZeroPacket:
    HEADER_INFO = 417359019239977417716476838698419835
    HEADER_INFO_BITS = 152
    HEADER_FILLER_BITS = 198

    def __init__(self, b: Cell):
        self.b = b
        self.s = b.to_slice()
        self.header_info = self.s.load_uint(self.HEADER_INFO_BITS)
        assert self.header_info == self.HEADER_INFO, f"wrong header info: {self.header_info}"
        self.header_filler = self.s.load_int(self.HEADER_FILLER_BITS) # load_int is used because `~` in python isn't for uint
        assert (~self.header_filler == 0), "header filler ones number mismatch"

        self.path = LayerZeroPath(self.s.load_ref())
        self.message = '0x' + self.s.load_ref().bits.tobytes().hex()
        self.nonce = self.s.load_uint(64)
        self.guid = hex(self.s.load_uint(256))

    def __repr__(self):
        return f"""LayerZeroPacket(
            path={self.path},
            message={self.message},
            nonce={self.nonce},
            guid={self.guid})"""

# Path decoding:
# ;; this function is unused by the protocol but will be used by OApps
# cell lz::Path::build(int srcEid, int srcOApp, int dstEid, int dstOApp) impure inline {
#     return begin_cell()
#         .store_uint(lz::Path::_headerInfo, lz::Path::_headerInfoBits)    ;; header info
#         .store_ones(lz::Path::_headerFillerBits)                         ;; header filler
#         .store_uint32(srcEid)
#         .store_uint256(srcOApp)
#         .store_uint32(dstEid)
#         .store_uint256(dstOApp)
#         .end_cell();
# }
class LayerZeroPath:
    HEADER_INFO = 8903714975572488637007080065659
    HEADER_INFO_BITS = 152
    HEADER_FILLER_BITS = 198

    def __init__(self, b: Cell):
        self.b = b
        self.s = b.to_slice()
        self.header_info = self.s.load_uint(self.HEADER_INFO_BITS)
        assert self.header_info == self.HEADER_INFO, "header info mismatch"
        self.header_filler = self.s.load_int(self.HEADER_FILLER_BITS) # load_int is used because `~` in python isn't for uint
        assert (~self.header_filler == 0), "header filler ones number mismatch"
        self.src_eid = self.s.load_uint(32)
        self.src_oapp = hex(self.s.load_uint(256))
        self.dst_eid = self.s.load_uint(32)
        self.dst_oapp = hex(self.s.load_uint(256))
    
    def __repr__(self):
        return f"""LayerZeroPath(
            src_eid={self.src_eid},
            src_oapp={self.src_oapp},
            dst_eid={self.dst_eid},
            dst_oapp={self.dst_oapp})"""


# Guid calculation:
# int lz::Packet::calculateGuid(cell $path, int nonce) inline method_id {
#     (int srcEid, int srcOApp, int dstEid, int dstOApp) = $path.lz::Path::deserialize();
#     return keccak256Builder(
#         begin_cell()
#             .store_uint64(nonce)
#             .store_uint32(srcEid)
#             .store_uint256(srcOApp)
#             .store_uint32(dstEid)
#             .store_uint256(dstOApp)
#     );
# }
def calculate_guid(path: LayerZeroPath, nonce: int):
    if isinstance(path.src_oapp, Address) and isinstance(path.dst_oapp, Address):
        src_oapp_hash_part = int.from_bytes(path.src_oapp.hash_part, "big")
        dst_oapp_hash_part = int.from_bytes(path.dst_oapp.hash_part, "big")
    else:
        src_oapp_hash_part = 0
        dst_oapp_hash_part = 0
    cell =( begin_cell() 
        .store_uint(nonce, 64)
        .store_uint(path.src_eid, 32) 
        .store_uint(src_oapp_hash_part, 256) 
        .store_uint(path.dst_eid, 32) 
        .store_uint(dst_oapp_hash_part, 256)
        .end_cell())
    keccak_hash = keccak.new(digest_bits=256)
    keccak_hash.update(cell.bits.tobytes())
    return keccak_hash.hexdigest()

# MdGuid decoding:
# const int MdGuid::_headerInfo = 5847552683615412884211067;

# cell MdGuid::Build(cell $md, int guid) inline method_id {
#     return begin_cell()
#         .store_uint(MdGuid::_headerInfo, MdGuid::_headerInfoBits)
#         .store_ones(MdGuid::_headerFillerBits)
#         .store_uint256(guid)
#         .store_ref($md)
#         .end_cell();
# }
class LayerZeroMdGuid:
    HEADER_INFO = 5847552683615412884211067
    HEADER_INFO_BITS = 116
    HEADER_FILLER_BITS = 234
    
    def __init__(self, b: Cell):
        self.b = b
        self.s = b.to_slice()
        self.header_info = self.s.load_uint(self.HEADER_INFO_BITS)
        assert self.header_info == self.HEADER_INFO, "header info mismatch"
        self.header_filler = self.s.load_int(self.HEADER_FILLER_BITS) # load_int is used because `~` in python isn't for uint
        assert (~self.header_filler == 0), "header filler ones number mismatch"
        self.guid = hex(self.s.load_uint(256))
        self.md = self.s.load_ref()
    
    def __repr__(self):
        return f"""LayerZeroMdGuid(
            guid={self.guid},
            md={self.md})"""
    

# Event msg parser:
# int executeEvent(tuple action) impure inline {
#     ;; send event to event sink
#     sendNonTerminalAction(
#         SEND_MSG_NON_BOUNCEABLE,
#         0,
#         _getEventSink(),
#         buildLayerzeroMessageBody(
#             0,
#             BaseInterface::OP::EVENT,
#             action.cell_at(action::event::bodyIndex)
#         ),
#         PAID_EXTERNALLY
#     );
#     return true;
# }
# ;; @param donationNanos: the amount of TON that the sender intended to be
# ;; withheld within our contract
# ;; @info baseHandler::refund_addr is the last known "origin" of a message
# ;; flow, and is used to refund the sender if the handler does not
# ;; use all remaining value from the in_message
# cell buildLayerzeroMessageBody(int donationNanos, int opcode, cell $md) impure inline {
#     cell ret = beginTonMessage(opcode)
#         .store_coins(donationNanos)
#         .store_slice(getOriginStd())
#         .store_ref($md)
#         .end_cell();
#     return ret;
# }
class LayerZeroEventMsgBody:
    opcode = 0xe33b9873

    def __init__(self, s: Slice):
        assert s.load_uint(32) == self.opcode, "opcode mismatch"
        self.query_id = s.load_uint(64)
        self.donation_nanos = s.load_coins()
        self.origin = s.load_address()
        self.action_data_cell = s.load_ref() # called `md` in func
        self.action_data = LayerZeroEventAction(self.action_data_cell)
    
    def __repr__(self):
        return f"""LayerZeroEventMsgBody(
            opcode={self.opcode},
            query_id={self.query_id},
            donation_nanos={self.donation_nanos},
            origin={self.origin},
            action_data_cell={self.action_data_cell},
            action_data={self.action_data})"""

# Event action data (first ref) parser:
# const int action::event::_headerInfo = 7850279558805522911016931325;

# cell action::event::build(int topic, cell $body, cell $initialStorage) impure inline method_id {
#     return begin_cell()
#         .store_uint(action::event::_headerInfo, action::event::_headerInfoBits)     ;; header info
#         .store_ones(action::event::_headerFillerBits)                               ;; header filler
#         .store_uint256(topic)
#         .store_ref($body)
#         .store_ref($initialStorage)
#         .end_cell();
# }
class LayerZeroEventAction:
    HEADER_INFO = 7850279558805522911016931325
    HEADER_INFO_BITS = 134
    HEADER_FILLER_BITS = 216
    
    def __init__(self, c: Cell):
        self.c = c
        self.s = c.to_slice()
        self.header_info = self.s.load_uint(self.HEADER_INFO_BITS)
        assert self.header_info == self.HEADER_INFO, "header info mismatch"
        self.header_filler = self.s.load_int(self.HEADER_FILLER_BITS) # load_int is used because `~` in python isn't for uint
        assert (~self.header_filler == 0), "header filler ones number mismatch"
        self.topic = self.s.load_uint(256)
        self.body = self.s.load_ref()
        self.initial_storage = self.s.load_ref()
        # try:
        self.body_oftsent = LayerZeroEventActionBodyOFTSentSucceed(self.body)
        # except:
        #     self.body_oftsent = None
            
    
    def __repr__(self):
        return f"""LayerZeroEventAction(
            topic={self.topic},
            body={self.body},
            initial_storage={self.initial_storage},
            body_oftsent={self.body_oftsent})"""

# OFTSentSucceed event specific body:
# actions~pushAction<event>(
#     EVENT::OFTSentSucceed,
#     MdGuid::Build(oftSendEncoded, guid)
# );
class LayerZeroEventActionBodyOFTSentSucceed:
    topic = 435778055796 # const int EVENT::OFTSentSucceed

    def __init__(self, c: Cell):
        self.c = c
        self.s = c.to_slice()
        self.topic = self.s.load_uint(256)
        assert self.topic == self.topic, "topic mismatch"
        self.mdguid = LayerZeroMdGuid(self.s.load_ref())
        # self.s.load_ref() # initial storage - not needed

    def __repr__(self):
        return f"""LayerZeroEventActionBodyOFTSentSucceed(
            topic={self.topic},
            mdguid={self.mdguid})"""


# --- Here are just opcodes without parsers (parsed with script from *.fc): ---

# Controller Operations

class ControllerAddMsglib:
    """
    Original: Controller::OP::ADD_MSGLIB
    String: "Controller::OP::ADD_MSGLIB"
    """
    opcode = 0xe6ea2ecf

class ControllerClaimOwnership:
    """
    Original: Controller::OP::CLAIM_OWNERSHIP
    String: "Controller::OP::CLAIM_OWNERSHIP"
    """
    opcode = 0xd3ab2154

class ControllerDeployChannel:
    """
    Original: Controller::OP::DEPLOY_CHANNEL
    String: "Controller::OP::DEPLOY_CHANNEL"
    """
    opcode = 0x06e445b2

class ControllerDeployEndpoint:
    """
    Original: Controller::OP::DEPLOY_ENDPOINT
    String: "Controller::OP::DEPLOY_ENDPOINT"
    """
    opcode = 0x325f34eb

class ControllerDepositZro:
    """
    Original: Controller::OP::DEPOSIT_ZRO
    String: "Controller::OP::DEPOSIT_ZRO"
    """
    opcode = 0x88bcbf9c

class ControllerSetEpConfigDefaults:
    """
    Original: Controller::OP::SET_EP_CONFIG_DEFAULTS
    String: "Controller::OP::SET_EP_CONFIG_DEFAULTS"
    """
    opcode = 0xf611349a

class ControllerSetEpConfigOapp:
    """
    Original: Controller::OP::SET_EP_CONFIG_OAPP
    String: "Controller::OP::SET_EP_CONFIG_OAPP"
    """
    opcode = 0x0a4ea4b3

class ControllerSetZroWallet:
    """
    Original: Controller::OP::SET_ZRO_WALLET
    String: "Controller::OP::SET_ZRO_WALLET"
    """
    opcode = 0xe6ff6db6

class ControllerTransferOwnership:
    """
    Original: Controller::OP::TRANSFER_OWNERSHIP
    String: "Controller::OP::TRANSFER_OWNERSHIP"
    """
    opcode = 0xc0a4791f


# Endpoint Operations

class EndpointAddMsglib:
    """
    Original: Endpoint::OP::ADD_MSGLIB
    String: "Endpoint::OP::ADD_MSGLIB"
    """
    opcode = 0xbabd9e46

class EndpointEndpointCommitPacket:
    """
    Original: Endpoint::OP::ENDPOINT_COMMIT_PACKET
    String: "Endpoint::OP::ENDPOINT_COMMIT_PACKET"
    """
    opcode = 0x5dab749a

class EndpointEndpointSend:
    """
    Original: Endpoint::OP::ENDPOINT_SEND
    String: "Endpoint::OP::ENDPOINT_SEND"
    """
    opcode = 0xdd4ea3b4

class EndpointGetMsglibInfoCallback:
    """
    Original: Endpoint::OP::GET_MSGLIB_INFO_CALLBACK
    String: "Endpoint::OP::GET_MSGLIB_INFO_CALLBACK"
    """
    opcode = 0xe70d70d6

class EndpointSetEpConfigDefaults:
    """
    Original: Endpoint::OP::SET_EP_CONFIG_DEFAULTS
    String: "Endpoint::OP::SET_EP_CONFIG_DEFAULTS"
    """
    opcode = 0x9b5a303b

class EndpointSetEpConfigOapp:
    """
    Original: Endpoint::OP::SET_EP_CONFIG_OAPP
    String: "Endpoint::OP::SET_EP_CONFIG_OAPP"
    """
    opcode = 0x8a276f67


# Channel Operations

class ChannelBurn:
    """
    Original: Channel::OP::BURN
    String: "Channel::OP::BURN"
    """
    opcode = 0x349d9aa5

class ChannelChannelCommitPacket:
    """
    Original: Channel::OP::CHANNEL_COMMIT_PACKET
    String: "Channel::OP::CHANNEL_COMMIT_PACKET"
    """
    opcode = 0x5388cd88

class ChannelChannelSend:
    """
    Original: Channel::OP::CHANNEL_SEND
    String: "Channel::OP::CHANNEL_SEND"
    """
    opcode = 0x536feb09

class ChannelDepositZro:
    """
    Original: Channel::OP::DEPOSIT_ZRO
    String: "Channel::OP::DEPOSIT_ZRO"
    """
    opcode = 0x24c8efb6

class ChannelEmitLzReceiveAlert:
    """
    Original: Channel::OP::EMIT_LZ_RECEIVE_ALERT
    String: "Channel::OP::EMIT_LZ_RECEIVE_ALERT"
    """
    opcode = 0x7508b830

class ChannelForceAbort:
    """
    Original: Channel::OP::FORCE_ABORT
    String: "Channel::OP::FORCE_ABORT"
    """
    opcode = 0x59a58cc2

class ChannelLzReceiveExecuteCallback:
    """
    Original: Channel::OP::LZ_RECEIVE_EXECUTE_CALLBACK
    String: "Channel::OP::LZ_RECEIVE_EXECUTE_CALLBACK"
    """
    opcode = 0xcaae25a1

class ChannelLzReceiveLock:
    """
    Original: Channel::OP::LZ_RECEIVE_LOCK
    String: "Channel::OP::LZ_RECEIVE_LOCK"
    """
    opcode = 0xb7680bc6

class ChannelLzReceivePrepare:
    """
    Original: Channel::OP::LZ_RECEIVE_PREPARE
    String: "Channel::OP::LZ_RECEIVE_PREPARE"
    """
    opcode = 0x22f3ac09

class ChannelMsglibSendCallback:
    """
    Original: Channel::OP::MSGLIB_SEND_CALLBACK
    String: "Channel::OP::MSGLIB_SEND_CALLBACK"
    """
    opcode = 0x421c1a25

class ChannelNilify:
    """
    Original: Channel::OP::NILIFY
    String: "Channel::OP::NILIFY"
    """
    opcode = 0x51bc9996

class ChannelNotifyPacketExecuted:
    """
    Original: Channel::OP::NOTIFY_PACKET_EXECUTED
    String: "Channel::OP::NOTIFY_PACKET_EXECUTED"
    """
    opcode = 0x91721efc

class ChannelSetEpConfigOapp:
    """
    Original: Channel::OP::SET_EP_CONFIG_OAPP
    String: "Channel::OP::SET_EP_CONFIG_OAPP"
    """
    opcode = 0x66e22a24

class ChannelSyncMsglibConnection:
    """
    Original: Channel::OP::SYNC_MSGLIB_CONNECTION
    String: "Channel::OP::SYNC_MSGLIB_CONNECTION"
    """
    opcode = 0xad13b0d1

class LayerzeroChannelSendCallback:
    """
    Original: Layerzero::OP::CHANNEL_SEND_CALLBACK
    String: "Layerzero::OP::CHANNEL_SEND_CALLBACK"
    """
    opcode = 0xa2b5fbae

    def __init__(self, s: Slice):
        self.cell = s.to_cell()
        self.s = s
        self.opcode = self.s.load_uint(32)
        assert self.opcode == self.opcode, "but it's not a callback"
        self.lz_send = self._get_lz_send_from_callback(self.cell)

    @staticmethod
    def _get_lz_send_from_callback(c: Cell):
        s = c.begin_parse()
        return LayerZeroMDLzSend(c.refs[0].refs[0].refs[0]) # got it experimentally


class MsglibconnectionMsglibConnectionSyncChannelState:
    """
    Original: MsglibConnection::OP::MSGLIB_CONNECTION_SYNC_CHANNEL_STATE
    String: "MsglibConnection::OP::MSGLIB_CONNECTION_SYNC_CHANNEL_STATE"
    """
    opcode = 0x8698f936

class Deploychannel:
    """
    Original: OP::DeployChannel
    String: "OP::DeployChannel"
    """
    opcode = 0x70ead753


# Msglib Operations

class MsglibReturnQuote:
    """
    Original: Msglib::OP::RETURN_QUOTE
    String: "Msglib::OP::RETURN_QUOTE"
    """
    opcode = 0x2fd8ee72

class MsglibconnectionMsglibConnectionCommitPacketCallback:
    """
    Original: MsglibConnection::OP::MSGLIB_CONNECTION_COMMIT_PACKET_CALLBACK
    String: "MsglibConnection::OP::MSGLIB_CONNECTION_COMMIT_PACKET_CALLBACK"
    """
    opcode = 0x5e178f33

class MsglibconnectionMsglibConnectionQuote:
    """
    Original: MsglibConnection::OP::MSGLIB_CONNECTION_QUOTE
    String: "MsglibConnection::OP::MSGLIB_CONNECTION_QUOTE"
    """
    opcode = 0x4522b7f8

class MsglibconnectionMsglibConnectionSend:
    """
    Original: MsglibConnection::OP::MSGLIB_CONNECTION_SEND
    String: "MsglibConnection::OP::MSGLIB_CONNECTION_SEND"
    """
    opcode = 0x4002b790

class MsglibmanagerDeployConnection:
    """
    Original: MsglibManager::OP::DEPLOY_CONNECTION
    String: "MsglibManager::OP::DEPLOY_CONNECTION"
    """
    opcode = 0x78d2912f

class MsglibmanagerGetMsglibInfo:
    """
    Original: MsglibManager::OP::GET_MSGLIB_INFO
    String: "MsglibManager::OP::GET_MSGLIB_INFO"
    """
    opcode = 0xec10d652

class MsglibmanagerSetOappMsglibReceiveConfig:
    """
    Original: MsglibManager::OP::SET_OAPP_MSGLIB_RECEIVE_CONFIG
    String: "MsglibManager::OP::SET_OAPP_MSGLIB_RECEIVE_CONFIG"
    """
    opcode = 0x43997bfc

class MsglibmanagerSetOappMsglibSendConfig:
    """
    Original: MsglibManager::OP::SET_OAPP_MSGLIB_SEND_CONFIG
    String: "MsglibManager::OP::SET_OAPP_MSGLIB_SEND_CONFIG"
    """
    opcode = 0xabeb58fa


# ULN Operations

class UlnCollectWorkerRent:
    """
    Original: Uln::OP::COLLECT_WORKER_RENT
    String: "Uln::OP::COLLECT_WORKER_RENT"
    """
    opcode = 0x5df40385

class UlnDeregisterWorkerFeelib:
    """
    Original: Uln::OP::DEREGISTER_WORKER_FEELIB
    String: "Uln::OP::DEREGISTER_WORKER_FEELIB"
    """
    opcode = 0xbeea5b26

class UlnGcAttestations:
    """
    Original: Uln::OP::GC_ATTESTATIONS
    String: "Uln::OP::GC_ATTESTATIONS"
    """
    opcode = 0xb960706f

class UlnRefillWorkerRent:
    """
    Original: Uln::OP::REFILL_WORKER_RENT
    String: "Uln::OP::REFILL_WORKER_RENT"
    """
    opcode = 0xac0727dd

class UlnSetDefaultUlnReceiveConfig:
    """
    Original: Uln::OP::SET_DEFAULT_ULN_RECEIVE_CONFIG
    String: "Uln::OP::SET_DEFAULT_ULN_RECEIVE_CONFIG"
    """
    opcode = 0x38c94d81

class UlnSetDefaultUlnSendConfig:
    """
    Original: Uln::OP::SET_DEFAULT_ULN_SEND_CONFIG
    String: "Uln::OP::SET_DEFAULT_ULN_SEND_CONFIG"
    """
    opcode = 0x8ca145e2

class UlnSetOappUlnReceiveConfig:
    """
    Original: Uln::OP::SET_OAPP_ULN_RECEIVE_CONFIG
    String: "Uln::OP::SET_OAPP_ULN_RECEIVE_CONFIG"
    """
    opcode = 0x2faf0808

class UlnSetOappUlnSendConfig:
    """
    Original: Uln::OP::SET_OAPP_ULN_SEND_CONFIG
    String: "Uln::OP::SET_OAPP_ULN_SEND_CONFIG"
    """
    opcode = 0xfec58708

class UlnSetTreasuryFeeBps:
    """
    Original: Uln::OP::SET_TREASURY_FEE_BPS
    String: "Uln::OP::SET_TREASURY_FEE_BPS"
    """
    opcode = 0xcc53c906

class UlnSetWorkerFeelibStorage:
    """
    Original: Uln::OP::SET_WORKER_FEELIB_STORAGE
    String: "Uln::OP::SET_WORKER_FEELIB_STORAGE"
    """
    opcode = 0xd14c39cc

class UlnSetWorkerFeelibStorageCallback:
    """
    Original: Uln::OP::SET_WORKER_FEELIB_STORAGE_CALLBACK
    String: "Uln::OP::SET_WORKER_FEELIB_STORAGE_CALLBACK"
    """
    opcode = 0x0fb2c587

class UlnUlnCommitPacket:
    """
    Original: Uln::OP::ULN_COMMIT_PACKET
    String: "Uln::OP::ULN_COMMIT_PACKET"
    """
    opcode = 0x28b97077

class UlnUlnQuote:
    """
    Original: Uln::OP::ULN_QUOTE
    String: "Uln::OP::ULN_QUOTE"
    """
    opcode = 0xdc360276

class UlnUlnSend:
    """
    Original: Uln::OP::ULN_SEND
    String: "Uln::OP::ULN_SEND"
    """
    opcode = 0x5de68393

class UlnUlnVerify:
    """
    Original: Uln::OP::ULN_VERIFY
    String: "Uln::OP::ULN_VERIFY"
    """
    opcode = 0x994aaf4e

class UlnUpdateWorkerFeelib:
    """
    Original: Uln::OP::UPDATE_WORKER_FEELIB
    String: "Uln::OP::UPDATE_WORKER_FEELIB"
    """
    opcode = 0xf851d3f5

class UlnconnectionGarbageCollectExecutedNonces:
    """
    Original: UlnConnection::OP::GARBAGE_COLLECT_EXECUTED_NONCES
    String: "UlnConnection::OP::GARBAGE_COLLECT_EXECUTED_NONCES"
    """
    opcode = 0xd751aada

class UlnconnectionGarbageCollectInvalidAttestations:
    """
    Original: UlnConnection::OP::GARBAGE_COLLECT_INVALID_ATTESTATIONS
    String: "UlnConnection::OP::GARBAGE_COLLECT_INVALID_ATTESTATIONS"
    """
    opcode = 0x4f40d2b2

class UlnconnectionSetOappUlnReceiveConfig:
    """
    Original: UlnConnection::OP::SET_OAPP_ULN_RECEIVE_CONFIG
    String: "UlnConnection::OP::SET_OAPP_ULN_RECEIVE_CONFIG"
    """
    opcode = 0x659bd16d

class UlnconnectionSetOappUlnSendConfig:
    """
    Original: UlnConnection::OP::SET_OAPP_ULN_SEND_CONFIG
    String: "UlnConnection::OP::SET_OAPP_ULN_SEND_CONFIG"
    """
    opcode = 0xebf9c7ce

class UlnconnectionUlnConnectionCommitPacket:
    """
    Original: UlnConnection::OP::ULN_CONNECTION_COMMIT_PACKET
    String: "UlnConnection::OP::ULN_CONNECTION_COMMIT_PACKET"
    """
    opcode = 0xf9d37b80

class UlnconnectionUlnConnectionVerify:
    """
    Original: UlnConnection::OP::ULN_CONNECTION_VERIFY
    String: "UlnConnection::OP::ULN_CONNECTION_VERIFY"
    """
    opcode = 0x4ec8f80a

class UlnmanagerAddUlnWorker:
    """
    Original: UlnManager::OP::ADD_ULN_WORKER
    String: "UlnManager::OP::ADD_ULN_WORKER"
    """
    opcode = 0xa91df8ba

class UlnmanagerClaimOwnership:
    """
    Original: UlnManager::OP::CLAIM_OWNERSHIP
    String: "UlnManager::OP::CLAIM_OWNERSHIP"
    """
    opcode = 0x8c0d60ea

class UlnmanagerClaimTreasuryFees:
    """
    Original: UlnManager::OP::CLAIM_TREASURY_FEES
    String: "UlnManager::OP::CLAIM_TREASURY_FEES"
    """
    opcode = 0x4428f5bb

class UlnmanagerDeployUln:
    """
    Original: UlnManager::OP::DEPLOY_ULN
    String: "UlnManager::OP::DEPLOY_ULN"
    """
    opcode = 0x675344a3

class UlnmanagerRegisterWorkerFeelibBytecode:
    """
    Original: UlnManager::OP::REGISTER_WORKER_FEELIB_BYTECODE
    String: "UlnManager::OP::REGISTER_WORKER_FEELIB_INFO"
    """
    opcode = 0x9b879237

class UlnmanagerSetAdminWorkers:
    """
    Original: UlnManager::OP::SET_ADMIN_WORKERS
    String: "UlnManager::OP::SET_ADMIN_WORKERS"
    """
    opcode = 0x267204ed

class UlnmanagerSetDefaultUlnReceiveConfig:
    """
    Original: UlnManager::OP::SET_DEFAULT_ULN_RECEIVE_CONFIG
    String: "UlnManager::OP::SET_DEFAULT_ULN_RECEIVE_CONFIG"
    """
    opcode = 0xbe2e1fb3

class UlnmanagerSetDefaultUlnSendConfig:
    """
    Original: UlnManager::OP::SET_DEFAULT_ULN_SEND_CONFIG
    String: "UlnManager::OP::SET_DEFAULT_ULN_SEND_CONFIG"
    """
    opcode = 0xbdec00e0

class UlnmanagerSetUlnTreasuryFeeBps:
    """
    Original: UlnManager::OP::SET_ULN_TREASURY_FEE_BPS
    String: "UlnManager::OP::SET_ULN_TREASURY_FEE_BPS"
    """
    opcode = 0x3b511097

class UlnmanagerTransferOwnership:
    """
    Original: UlnManager::OP::TRANSFER_OWNERSHIP
    String: "UlnManager::OP::TRANSFER_OWNERSHIP"
    """
    opcode = 0xd9b3d2c3

class UltralightnodeUlnConnectionVerifyCallback:
    """
    Original: UltraLightNode::OP::ULN_CONNECTION_VERIFY_CALLBACK
    String: "UltraLightNode::OP::ULN_CONNECTION_VERIFY_CALLBACK"
    """
    opcode = 0x3cb38090


# SML Operations

class SmlconnectionSmlConnectionCommitPacket:
    """
    Original: SmlConnection::OP::SML_CONNECTION_COMMIT_PACKET
    String: "SmlConnection::OP::SML_CONNECTION_COMMIT_PACKET"
    """
    opcode = 0xc85ad5b5

class SmlmanagerSetMsglibConfig:
    """
    Original: SmlManager::OP::SET_MSGLIB_CONFIG
    String: "SmlManager::OP::SET_MSGLIB_CONFIG"
    """
    opcode = 0x43025917

class SmlmanagerSmlManagerCommitPacket:
    """
    Original: SmlManager::OP::SML_MANAGER_COMMIT_PACKET
    String: "SmlManager::OP::SML_MANAGER_COMMIT_PACKET"
    """
    opcode = 0xb5a5ce43

class SmlmanagerSmlManagerCommitPacketCallback:
    """
    Original: SmlManager::OP::SML_MANAGER_COMMIT_PACKET_CALLBACK
    String: "SmlManager::OP::SML_MANAGER_COMMIT_PACKET_CALLBACK"
    """
    opcode = 0x46adb9c2

class SmlmanagerSmlManagerSend:
    """
    Original: SmlManager::OP::SML_MANAGER_SEND
    String: "SmlManager::OP::SML_MANAGER_SEND"
    """
    opcode = 0x6c29e6d1


# Other Operations

class BaseinterfaceEvent:
    """
    Original: BaseInterface::OP::EVENT
    String: "BaseInterface::OP::EVENT"
    """
    opcode = 0xe33b9873

class BaseinterfaceInitialize:
    """
    Original: BaseInterface::OP::INITIALIZE
    String: "BaseInterface::OP::INITIALIZE"
    """
    opcode = 0xf65ce988

class CounterFailNextLzReceive:
    """
    Original: Counter::OP::FAIL_NEXT_LZ_RECEIVE
    String: "Counter::OP::FAIL_NEXT_LZ_RECEIVE"
    """
    opcode = 0x344b96cc

class CounterIncrement:
    """
    Original: Counter::OP::INCREMENT
    String: "Counter::OP::INCREMENT"
    """
    opcode = 0x444fa047

class DvnSetAdminsByQuorum:
    """
    Original: Dvn::OP::SET_ADMINS_BY_QUORUM
    String: "Dvn::OP::SET_ADMINS_BY_QUORUM"
    """
    opcode = 0x091b705a

class DvnSetProxyAdmins:
    """
    Original: Dvn::OP::SET_PROXY_ADMINS
    String: "Dvn::OP::SET_PROXY_ADMINS"
    """
    opcode = 0x4ca1141b

class DvnSetQuorum:
    """
    Original: Dvn::OP::SET_QUORUM
    String: "Dvn::OP::SET_QUORUM"
    """
    opcode = 0x99da9772

class DvnSetVerifiers:
    """
    Original: Dvn::OP::SET_VERIFIERS
    String: "Dvn::OP::SET_VERIFIERS"
    """
    opcode = 0x236c1ba0

class DvnVerify:
    """
    Original: Dvn::OP::VERIFY
    String: "Dvn::OP::VERIFY"
    """
    opcode = 0xa0d51a8d

class ExecutorCommitPacket:
    """
    Original: Executor::OP::COMMIT_PACKET
    String: "Executor::OP::COMMIT_PACKET"
    """
    opcode = 0x41704585

class ExecutorLzReceiveAlert:
    """
    Original: Executor::OP::LZ_RECEIVE_ALERT
    String: "Executor::OP::LZ_RECEIVE_ALERT"
    """
    opcode = 0xe5260f73

class ExecutorLzReceivePrepare:
    """
    Original: Executor::OP::LZ_RECEIVE_PREPARE
    String: "Executor::OP::LZ_RECEIVE_PREPARE"
    """
    opcode = 0x13de1735

class ExecutorNativeDrop:
    """
    Original: Executor::OP::NATIVE_DROP
    String: "Executor::OP::NATIVE_DROP"
    """
    opcode = 0x0477e6d5

class ExecutorNativeDropAndLzReceivePrepare:
    """
    Original: Executor::OP::NATIVE_DROP_AND_LZ_RECEIVE_PREPARE
    String: "Executor::OP::NATIVE_DROP_AND_LZ_RECEIVE_PREPARE"
    """
    opcode = 0x6c05c076

class ExecutorSetNativeDropTotalCap:
    """
    Original: Executor::OP::SET_NATIVE_DROP_TOTAL_CAP
    String: "Executor::OP::SET_NATIVE_DROP_TOTAL_CAP"
    """
    opcode = 0x5c19a203

class LayerzeroBurnCallback:
    """
    Original: Layerzero::OP::BURN_CALLBACK
    String: "Layerzero::OP::BURN_CALLBACK"
    """
    opcode = 0x95c38382

class LayerzeroLzReceiveExecute:
    """
    Original: Layerzero::OP::LZ_RECEIVE_EXECUTE
    String: "Layerzero::OP::LZ_RECEIVE_EXECUTE"
    """
    opcode = 0x0c7b8418

class LayerzeroLzReceivePrepare:
    """
    Original: Layerzero::OP::LZ_RECEIVE_PREPARE
    String: "Layerzero::OP::LZ_RECEIVE_PREPARE"
    """
    opcode = 0x97df404c

class LayerzeroNilifyCallback:
    """
    Original: Layerzero::OP::NILIFY_CALLBACK
    String: "Layerzero::OP::NILIFY_CALLBACK"
    """
    opcode = 0x7b3f2a63

class Burn:
    """
    Original: OP::Burn
    String: "OP::Burn"
    """
    opcode = 0x2c1e0f55

class Claimownership:
    """
    Original: OP::ClaimOwnership
    String: "OP::ClaimOwnership"
    """
    opcode = 0xa67308c8

class Deployconnection:
    """
    Original: OP::DeployConnection
    String: "OP::DeployConnection"
    """
    opcode = 0xdd1fdfdb

class Forceabort:
    """
    Original: OP::ForceAbort
    String: "OP::ForceAbort"
    """
    opcode = 0x1afdcb9f

class Nilify:
    """
    Original: OP::Nilify
    String: "OP::Nilify"
    """
    opcode = 0x6b8f0309

class Setenforcedoptions:
    """
    Original: OP::SetEnforcedOptions
    String: "OP::SetEnforcedOptions"
    """
    opcode = 0x0075a62d

class Setlzconfig:
    """
    Original: OP::SetLzConfig
    String: "OP::SetLzConfig"
    """
    opcode = 0x82801010

class Setowner:
    """
    Original: OP::SetOwner
    String: "OP::SetOwner"
    """
    opcode = 0xdf458da2

class Setpeer:
    """
    Original: OP::SetPeer
    String: "OP::SetPeer"
    """
    opcode = 0x5df77d23

class Transferownership:
    """
    Original: OP::TransferOwnership
    String: "OP::TransferOwnership"
    """
    opcode = 0xb721bed0

class PricefeedcacheUpdateArbExtension:
    """
    Original: PriceFeedCache::OP::UPDATE_ARB_EXTENSION
    String: "PriceFeedCache::OP::UPDATE_ARB_EXTENSION"
    """
    opcode = 0xf9a69104

class PricefeedcacheUpdateNativePrice:
    """
    Original: PriceFeedCache::OP::UPDATE_NATIVE_PRICE
    String: "PriceFeedCache::OP::UPDATE_NATIVE_PRICE"
    """
    opcode = 0x877000f9

class PricefeedcacheUpdateOpNativePrices:
    """
    Original: PriceFeedCache::OP::UPDATE_OP_NATIVE_PRICES
    String: "PriceFeedCache::OP::UPDATE_OP_NATIVE_PRICES"
    """
    opcode = 0x677fa3db

class PricefeedcacheUpdateOpPrices:
    """
    Original: PriceFeedCache::OP::UPDATE_OP_PRICES
    String: "PriceFeedCache::OP::UPDATE_OP_PRICES"
    """
    opcode = 0x040f08e0

class PricefeedcacheUpdatePrice:
    """
    Original: PriceFeedCache::OP::UPDATE_PRICE
    String: "PriceFeedCache::OP::UPDATE_PRICE"
    """
    opcode = 0xc809fa10

class ProxyCallContract:
    """
    Original: Proxy::OP::CALL_CONTRACT
    String: "Proxy::OP::CALL_CONTRACT"
    """
    opcode = 0x09f047d9

class ProxyEmitEvent:
    """
    Original: Proxy::OP::EMIT_EVENT
    String: "Proxy::OP::EMIT_EVENT"
    """
    opcode = 0xcd5ce847

class ProxyHandleCallback:
    """
    Original: Proxy::OP::HANDLE_CALLBACK
    String: "Proxy::OP::HANDLE_CALLBACK"
    """
    opcode = 0x2051c71f

class ProxyToggleCallback:
    """
    Original: Proxy::OP::TOGGLE_CALLBACK
    String: "Proxy::OP::TOGGLE_CALLBACK"
    """
    opcode = 0x1a567c2c

class WorkerCallViaProxy:
    """
    Original: Worker::OP::CALL_VIA_PROXY
    String: "Worker::OP::CALL_VIA_PROXY"
    """
    opcode = 0xe1404fea

class WorkerClaimTon:
    """
    Original: Worker::OP::CLAIM_TON
    String: "Worker::OP::CLAIM_TON"
    """
    opcode = 0x217607d4

class WorkerClaimTonFromProxy:
    """
    Original: Worker::OP::CLAIM_TON_FROM_PROXY
    String: "Worker::OP::CLAIM_TON_FROM_PROXY"
    """
    opcode = 0xce435c07

class WorkerSetAdmins:
    """
    Original: Worker::OP::SET_ADMINS
    String: "Worker::OP::SET_ADMINS"
    """
    opcode = 0x53c92c4e

class WorkerSetProxy:
    """
    Original: Worker::OP::SET_PROXY
    String: "Worker::OP::SET_PROXY"
    """
    opcode = 0x8ea9ca4f

# Silly utility functions:

def get_opcode_by_name(name: str) -> int | None:
    import sys
    current_module = sys.modules[__name__]
    
    for attr_name in dir(current_module):
        attr = getattr(current_module, attr_name)
        if (isinstance(attr, type) and 
            hasattr(attr, 'opcode') and
            attr_name.lower() == name.lower()):
            return attr.opcode
    return None

def get_class_by_opcode(opcode: int) -> type | None:
    import sys
    current_module = sys.modules[__name__]
    
    for attr_name in dir(current_module):
        attr = getattr(current_module, attr_name)
        if (isinstance(attr, type) and 
            hasattr(attr, 'opcode') and
            attr.opcode == opcode):
            return attr
    return None


# --- Here are parsers by AI (BS, but sometimes works) ---

class ChannelSendMessageParser:
    """Parser for Channel::OP::CHANNEL_SEND message"""
    opcode = ChannelChannelSend.opcode
    
    def __init__(self, slice: Slice):
        # Based on channelSend handler in channel/handler.fc
        # Receives MdObj(lzSend, defaultEpConfig)
        self.md_obj_cell = slice.load_ref()
        
        # Parse MdObj structure
        obj_slice = self.md_obj_cell.begin_parse()
        self.lz_send_cell = obj_slice.load_ref()  # LzSend
        self.default_config_cell = obj_slice.load_ref()  # SendEpConfig
        
        # Parse LzSend
        self.lz_send = LayerZeroMDLzSend(self.lz_send_cell)

class MsglibSendCallbackParser:
    """Parser for Channel::OP::MSGLIB_SEND_CALLBACK message"""
    opcode = ChannelMsglibSendCallback.opcode
    
    def __init__(self, slice: Slice):
        # Simple approach: extract what we can from the available data
        # Based on test showing 55 bits + 0 refs remaining after lz_send_cell
        
        # Try to extract coins/fees first (these might be in different format)
        try:
            self.native_quote = slice.load_coins()
            self.zro_quote = slice.load_coins() 
        except:
            # Fallback to simpler parsing
            self.native_quote = 0
            self.zro_quote = 0
        
        # Load lz_send_cell first (this seems to work from test)
        self.lz_send_cell = slice.load_ref()
        self.lz_send = parse_md_obj_in_lz_send(self.lz_send_cell)


class ChannelCommitPacketParser:
    """Parser for Channel::OP::CHANNEL_COMMIT_PACKET message"""
    opcode = ChannelChannelCommitPacket.opcode
    
    def __init__(self, slice: Slice):
        # Based on channelCommitPacket handler
        # Receives ExtendedMd(packet, defaultEpConfig, callerMsglibConnection)
        self.extended_md_cell = slice.load_ref()
        
        # Parse ExtendedMd structure
        md_slice = self.extended_md_cell.begin_parse()
        self.packet_cell = md_slice.load_ref()
        self.default_config_cell = md_slice.load_ref()
        self.caller_msglib = md_slice.load_uint(256)  # Forwarding address
        
        # Parse packet
        self.packet = LayerZeroPacket(self.packet_cell)

class LzReceivePrepareParser:
    """Parser for Channel::OP::LZ_RECEIVE_PREPARE message"""
    opcode = ChannelLzReceivePrepare.opcode
    
    def __init__(self, slice: Slice):
        # Based on lzReceivePrepare handler
        # Receives LzReceivePrepare(nonce, nanotons)
        self.nonce = slice.load_uint(64)
        self.nanotons = slice.load_coins()

class LzReceiveLockParser:
    """Parser for Channel::OP::LZ_RECEIVE_LOCK message"""
    opcode = ChannelLzReceiveLock.opcode
    
    def __init__(self, slice: Slice):
        # Based on lzReceiveLock handler
        # Receives Nonce(nonce)
        self.nonce = slice.load_uint(64)

class LzReceiveExecuteCallbackParser:
    """Parser for Channel::OP::LZ_RECEIVE_EXECUTE_CALLBACK message"""
    opcode = ChannelLzReceiveExecuteCallback.opcode
    
    def __init__(self, slice: Slice):
        # Based on lzReceiveExecuteCallback handler
        # Receives LzReceiveStatus(success, nonce, value, extraData, reason)
        self.success = slice.load_bool()
        self.nonce = slice.load_uint(64)
        self.value = slice.load_coins()
        self.extra_data_cell = slice.load_maybe_ref()
        self.reason_cell = slice.load_maybe_ref()

class PacketBurnParser:
    """Parser for Channel::OP::BURN message"""
    opcode = ChannelBurn.opcode
    
    def __init__(self, slice: Slice):
        # Based on burn handler
        # Receives PacketId(path, nonce)
        self.packet_id_cell = slice.load_ref()
        
        # Parse PacketId
        packet_id_slice = self.packet_id_cell.begin_parse()
        self.path_cell = packet_id_slice.load_ref()
        self.nonce = packet_id_slice.load_uint(64)
        
        # Parse path
        path_slice = self.path_cell.begin_parse()
        self.src_eid = path_slice.load_uint(32)
        self.src_oapp = path_slice.load_uint(256)
        self.dst_eid = path_slice.load_uint(32)
        self.dst_oapp = path_slice.load_uint(256)
        if path_slice.remaining_bits > 0: # end_parse()
            raise Exception("PacketBurnParser slice remaining bits > 0")

class PacketNilifyParser:
    """Parser for Channel::OP::NILIFY message"""
    opcode = ChannelNilify.opcode
    
    def __init__(self, slice: Slice):
        # Based on nilify handler - same structure as burn
        # Receives PacketId(path, nonce)
        self.packet_id_cell = slice.load_ref()
        
        # Parse PacketId
        packet_id_slice = self.packet_id_cell.begin_parse()
        self.path_cell = packet_id_slice.load_ref()
        self.nonce = packet_id_slice.load_uint(64)
        
        # Parse path
        path_slice = self.path_cell.begin_parse()
        self.src_eid = path_slice.load_uint(32)
        self.src_oapp = path_slice.load_uint(256)
        self.dst_eid = path_slice.load_uint(32)
        self.dst_oapp = path_slice.load_uint(256)

class LayerZeroPacketIdParser:
    """Parser for LayerZero PacketId structure"""
    
    def __init__(self, packet_id_cell: Cell):
        slice = packet_id_cell.begin_parse()
        self.path_cell = slice.load_ref()
        self.nonce = slice.load_uint(64)
        if slice.remaining_bits > 0:
            raise Exception("PacketIdParser slice remaining bits > 0")

class LayerZeroDvnFeesPaidEventParser:
    """Parser for DvnFeesPaidEvent structure"""

    def __init__(self, event_body_cell: Cell):
        slice = event_body_cell.begin_parse()
        self.required_dvns_cell = slice.load_ref()
        self.optional_dvns_cell = slice.load_ref()
        combined_cell = slice.load_ref()
        combined_slice = combined_cell.begin_parse()
        self.serialized_payees_cell = combined_slice.load_ref()
        if slice.remaining_bits > 0 or combined_slice.remaining_bits > 0:
            raise Exception("DvnFeesPaidEventParser slice remaining bits > 0")

class LayerZeroExecutorFeePaidEventParser:
    """Parser for ExecutorFeePaidEvent structure"""

    def __init__(self, event_body_cell: Cell):
        slice = event_body_cell.begin_parse()
        self.executor_address = slice.load_uint(256)
        self.fee_paid = slice.load_uint(128)
        if slice.remaining_bits > 0:
            raise Exception("ExecutorFeePaidEventParser slice remaining bits > 0")