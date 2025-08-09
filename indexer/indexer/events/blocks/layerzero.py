from __future__ import annotations

import base64
import logging
from dataclasses import dataclass

from indexer.events.blocks.basic_blocks import CallContractBlock, TonTransferBlock
from indexer.events.blocks.basic_matchers import (
    BlockMatcher,
    BlockTypeMatcher,
    ContractMatcher,
)
from indexer.events.blocks.core import Block
from indexer.events.blocks.jettons import JettonTransferBlock
from indexer.events.blocks.labels import labeled
from indexer.events.blocks.utils import AccountId, Asset, Amount
from indexer.events.blocks.utils.block_utils import get_labeled
from indexer.events.blocks.messages.layerzero import (
    # Import LayerZero opcode classes
    DvnVerify,
    EndpointEndpointSend,
    ProxyCallContract,
    UlnUlnCommitPacket,
    UlnUlnVerify,
    UlnconnectionUlnConnectionCommitPacket,
    EndpointEndpointCommitPacket,
    MsglibconnectionMsglibConnectionCommitPacketCallback,
    LayerZeroEventMsgBody,
    LayerzeroLzReceiveExecute,
    LayerzeroLzReceivePrepare,
    MsglibconnectionMsglibConnectionSend,
    LayerzeroChannelSendCallback,
    ChannelSendMessageParser,
    MsglibSendCallbackParser,
    ChannelCommitPacket,
    LzReceivePrepareParser,
    LzReceiveLockParser,
    LayerZeroOappExecuteCallback,
    UlnUlnSend,
    UlnconnectionUlnConnectionVerify,
    UlnConnectionVerifyCallbackParser,
)
from loguru import logger as loguru_logger

logger = logging.getLogger(__name__)

@dataclass
class LayerZeroPacketData:
    src_oapp: str                  # source OApp
    dst_oapp: str                  # destination OApp (on another blockchain)
    src_eid: int                   # source endpoint ID (TON = 30343)
    dst_eid: int                   # destination endpoint ID (ETH = 30101)
    nonce: int                     # packet sequence number
    guid: str                      # global unique ID
    message: str                   # message

@dataclass
class LayerZeroSendData:
    initiator: AccountId           # who initiated
    packet_data: LayerZeroPacketData
    send_request_id: int           # request id
    native_fee: int                # fee in nanoTON
    zro_fee: int                   # fee in ZRO
    endpoint: AccountId            # endpoint contract address
    channel: AccountId             # channel contract address
    uln: AccountId                 # ULN contract address
    msglib_manager: str            # msglib manager
    msglib: str                    # used msglib

@dataclass
class LayerZeroSendTokensData:
    sender: AccountId              # token sender
    oapp: AccountId                # token recipient contract
    sender_wallet: AccountId       # sender's wallet
    oapp_wallet: AccountId         # recipient's wallet
    asset: Asset                   # token being sent
    amount: Amount                 # token amount
    layerzero_send_data: LayerZeroSendData  # LayerZero data

@dataclass  
class LayerZeroReceiveData:
    sender: AccountId
    channel: AccountId
    oapp: AccountId
    packet_data: LayerZeroPacketData

@dataclass
class LayerZeroDvnVerifyData:
    sender: AccountId
    nonce: int
    status: str  # "succeeded" | "nonce_out_of_range" | "dvn_not_configured" | "unknown_<code>"
    dvn: AccountId
    proxy: AccountId
    uln: AccountId
    uln_connection: AccountId


@dataclass
class LayerZeroCommitPacketData:
    sender: AccountId
    uln: AccountId
    uln_connection: AccountId
    endpoint: AccountId
    channel: AccountId
    msglib_connection: AccountId
    packet_data: LayerZeroPacketData

class LayerZeroSendBlock(Block):
    data: LayerZeroSendData

    def __init__(self, data: LayerZeroSendData):
        super().__init__('layerzero_send', [], data)
    
    def __repr__(self):
        return f"layerzero_send({self.data.packet_data.src_oapp} → {self.data.packet_data.dst_oapp}, nonce={self.data.packet_data.nonce}, guid={self.data.packet_data.guid}, msg={self.data.packet_data.message})"

class LayerZeroReceiveBlock(Block):
    data: LayerZeroReceiveData

    def __init__(self, data: LayerZeroReceiveData):
        super().__init__('layerzero_receive', [], data)
        

class LayerZeroSendTokensBlock(Block):
    data: LayerZeroSendTokensData

    def __init__(self, data: LayerZeroSendTokensData):
        super().__init__('layerzero_send_tokens', [], data)
        
    def __repr__(self):
        return f"layerzero_send_tokens({self.data.sender} → {self.data.oapp}, {self.data.amount} {self.data.asset})"


class LayerZeroCommitPacketBlock(Block):
    data: LayerZeroCommitPacketData

    def __init__(self, data: LayerZeroCommitPacketData):
        super().__init__('layerzero_commit_packet', [], data)


class LayerZeroDvnVerifyBlock(Block):
    data: LayerZeroDvnVerifyData

    def __init__(self, data: LayerZeroDvnVerifyData):
        super().__init__('layerzero_dvn_verify', [], data)



class LayerZeroSendMatcher(BlockMatcher):
    """
    A user calls the ‘lzSend’
    method inside the Sender OApp
    Contract, specifying a message,
    a destination LayerZero Endpoint,
    the destination OApp address, and
    other protocol handling options.

    The source Endpoint generates a
    packet based on the Sender
    OApp’s message data, assigning
    each packet a unique, sequentially
    increasing number (i.e. nonce).

    The Endpoint encodes the packet
    using the OApp’s specified MessageLib
    to emit the message to
    the selected Security Stack
    and Executor, completing the
    send transaction with a
    PacketSent event.
"""
    
    def __init__(self):
        super().__init__(
            child_matcher =
                labeled("channel_send",
                    ContractMatcher(ChannelSendMessageParser.opcode,
                        child_matcher=labeled("msglib_send",
                            ContractMatcher(MsglibconnectionMsglibConnectionSend.opcode,
                                child_matcher=labeled("uln_send",
                                    ContractMatcher(UlnUlnSend.opcode,
                                        child_matcher=labeled("send_callback",
                                            ContractMatcher(
                                                MsglibSendCallbackParser.opcode,
                                                children_matchers=[
                                                    labeled("msglib_send_callback_event", ContractMatcher(LayerZeroEventMsgBody.opcode)),
                                                    labeled("excess_transfer_1", BlockTypeMatcher('ton_transfer', optional=True)),
                                                    labeled("excess_transfer_2", BlockTypeMatcher('ton_transfer', optional=True)),
                                                    labeled("excess_transfer_3", BlockTypeMatcher('ton_transfer', optional=True)),
                                                    labeled("excess_transfer_4", BlockTypeMatcher('ton_transfer', optional=True)),
                                                    labeled("excess_transfer_5", BlockTypeMatcher('ton_transfer', optional=True)),
                                                    labeled("oapp_callback",
                                                        ContractMatcher(
                                                            LayerzeroChannelSendCallback.opcode,
                                                            children_matchers=[
                                                                labeled("packet_sent_event",
                                                                    ContractMatcher(LayerZeroEventMsgBody.opcode, optional=True)
                                                                ),
                                                                labeled("excess_transfer_final",
                                                                    BlockTypeMatcher('ton_transfer', optional=True),
                                                                )
                                                            ]
                                                        )
                                                    ),
                                            ]))))))))
        )
    
    def test_self(self, block: Block) -> bool:
        # Start with OApp calling endpoint send
        return (isinstance(block, CallContractBlock) 
                and block.opcode == EndpointEndpointSend.opcode)
    
    async def build_block(self, block: Block, other_blocks: list[Block]):
        try:
            # Extract labeled blocks
            channel_send_block = get_labeled("channel_send", other_blocks, CallContractBlock)
            msglib_send_block = get_labeled("msglib_send", other_blocks, CallContractBlock)
            uln_send_block = get_labeled("uln_send", other_blocks, CallContractBlock)
            send_callback_block = get_labeled("send_callback", other_blocks, CallContractBlock)
            oapp_callback_block = get_labeled("oapp_callback", other_blocks, CallContractBlock)
            
            # Check if required blocks are found
            if not channel_send_block or not msglib_send_block or not uln_send_block or not send_callback_block or not oapp_callback_block:
                return []
            
            # Parse messages
            oapp_callback_msg = LayerzeroChannelSendCallback(oapp_callback_block.get_body())
            
            # Extract packet information
            lz_send = oapp_callback_msg.lz_send
            packet = lz_send.packet
            
            # Get sender from block message
            msg = block.get_message()
            sender = AccountId(msg.source)
            
            # Extract contract addresses
            endpoint = AccountId(msg.destination)  # endpoint address from initial call
            channel = AccountId(channel_send_block.get_message().destination)
            msglib = AccountId(msglib_send_block.get_message().destination)
            uln = AccountId(uln_send_block.get_message().destination)
            
            # Create structured data  
            data = LayerZeroSendData(
                initiator=sender,
                packet_data=LayerZeroPacketData(
                    src_oapp=packet.path.src_oapp,
                    dst_oapp=packet.path.dst_oapp,
                    src_eid=packet.path.src_eid,
                    dst_eid=packet.path.dst_eid,
                    nonce=packet.nonce,
                    guid=packet.guid,
                    message=packet.message
                ),
                send_request_id=lz_send.send_request_id,
                msglib_manager=lz_send.send_msglib_manager,
                native_fee=lz_send.native_fee,
                zro_fee=lz_send.zro_fee,
                endpoint=endpoint,
                channel=channel,
                msglib=lz_send.send_msglib,
                uln=uln,
            )
            
            # Create block and link with originals
            new_block = LayerZeroSendBlock(data)
            new_block.merge_blocks([block] + other_blocks)
            
            return [new_block]
            
        except Exception as e:
            logger.error(f"Failed to build LayerZero send block: {e}", exc_info=True)
            return []

class LayerZeroSendTokensMatcher(BlockMatcher):
    """Just a combination of jetton_transfer + layerzero_send.
    Better to show user that he sent tokens via LayerZero."""
    
    def __init__(self):
        super().__init__(
            parent_matcher=labeled("jetton_transfer",
                BlockTypeMatcher('jetton_transfer',
                    child_matcher=labeled("layerzero_event",
                        ContractMatcher(LayerZeroEventMsgBody.opcode)
                    )
                )
            )
        )
    
    def test_self(self, block: Block) -> bool:
        # start with layerzero_send block
        return hasattr(block, 'btype') and block.btype == 'layerzero_send'
    
    async def build_block(self, block: Block, other_blocks: list[Block]):
        try:
            # extract jetton transfer block
            jetton_transfer_block = get_labeled("jetton_transfer", other_blocks, JettonTransferBlock)
            
            if not jetton_transfer_block:
                return []
            
            # extract data from jetton transfer
            jetton_data = jetton_transfer_block.data
            sender = jetton_data['sender']
            sender_wallet = jetton_data['sender_wallet']
            oapp = jetton_data['receiver'] 
            oapp_wallet = jetton_data['receiver_wallet']
            asset = jetton_data['asset']
            amount = jetton_data['amount']
            
            # get layerzero data from the triggering block
            if not hasattr(block, 'data') or not isinstance(block.data, LayerZeroSendData):
                return []
                
            layerzero_data = block.data
            
            # create structured data
            data = LayerZeroSendTokensData(
                sender=sender,
                oapp=oapp,
                sender_wallet=sender_wallet,
                oapp_wallet=oapp_wallet,
                asset=asset,
                amount=amount,
                layerzero_send_data=layerzero_data
            )
            
            # create block and link with originals
            new_block = LayerZeroSendTokensBlock(data)
            new_block.merge_blocks([block] + other_blocks)
            
            return [new_block]
            
        except Exception as e:
            logger.error(f"Failed to build LayerZero send tokens block: {e}", exc_info=True)
            return []

class LayerZeroReceiveMatcher(BlockMatcher):
    """
    The Destination Endpoint enforces that
    the packet being delivered by the
    Executor matches the message verified
    by the DVNs.

    An Executor calls the ‘lzReceive’
    function on the committed message to
    process the packet using the Receiver
    OApp's logic.

    The message is received by the
    Receiver OApp Contract on the
    destination chain.
    """
    def __init__(self):
        super().__init__(
            children_matchers=
            [
                labeled("excesses_transfer", BlockTypeMatcher('ton_transfer', optional=True)),
                labeled("receive_prepare", 
                    ContractMatcher(LayerzeroLzReceivePrepare.opcode, 
                        child_matcher=labeled("receive_lock", 
                            ContractMatcher(LzReceiveLockParser.opcode,
                                child_matcher=labeled("execute_callback", 
                                    ContractMatcher(LayerzeroLzReceiveExecute.opcode,
                                        children_matchers=[
                                            labeled("even_1",
                                                    ContractMatcher(LayerZeroEventMsgBody.opcode, optional=True)),
                                            labeled("execute_callback",
                                                    ContractMatcher(LayerZeroOappExecuteCallback.opcode, optional=False,
                                                        children_matchers=[
                                                            labeled("event_2", ContractMatcher(LayerZeroEventMsgBody.opcode, optional=True)),
                                                            labeled("excess_transfer_2", BlockTypeMatcher('ton_transfer', optional=True)),
                                                        ]
                                                    )
                                                ),
                                            ]
                                        )
                                    )
                                )
                            )
                    )
                ),
            ]
        )
    
    def test_self(self, block: Block) -> bool:
        return (isinstance(block, CallContractBlock)
                and block.opcode == LzReceivePrepareParser.opcode)
    
    @loguru_logger.catch()
    async def build_block(self, block: Block, other_blocks: list[Block]):
        execute_callback_block = get_labeled("execute_callback", other_blocks, CallContractBlock)
        
        if not execute_callback_block:
            return []
        execute_callback_msg = execute_callback_block.get_message()
        
        execute_msg_data = LayerZeroOappExecuteCallback(execute_callback_block.get_body())
        packet = execute_msg_data.packet
        if execute_callback_msg.destination[2:].lower() != packet.path.dst_oapp[2:].lower():
            logger.warning(f"address from execute_callback doesn't match dst_oapp in packet")
            return []
        
        data = LayerZeroReceiveData(
            sender=AccountId(block.get_message().source),
            channel=AccountId(execute_callback_msg.source),
            oapp=AccountId(execute_callback_msg.destination),
            packet_data=LayerZeroPacketData(
                src_oapp=packet.path.src_oapp,
                dst_oapp=packet.path.dst_oapp,
                src_eid=packet.path.src_eid,
                dst_eid=packet.path.dst_eid,
                nonce=packet.nonce,
                guid=packet.guid,
                message=packet.message
            )
        )
        
        new_block = LayerZeroReceiveBlock(data)
        new_block.merge_blocks([block] + other_blocks)
        
        return [new_block]

class LayerZeroCommitPacketMatcher(BlockMatcher):
    """
    Once all of the DVNs in the OApp’s Security Stack
    have verified the message, the destination MessageLib
    marks the message as verifiable.

    The Executor commits this packet’s verification to the
    Endpoint, staging the packet for execution in 
    the Destination Endpoint.
    """

    def __init__(self):
        super().__init__(
            child_matcher=labeled('uln_connection_commit',
                ContractMatcher(UlnconnectionUlnConnectionCommitPacket.opcode,
                    child_matcher=labeled('endpoint_commit',
                        ContractMatcher(EndpointEndpointCommitPacket.opcode,
                            child_matcher=labeled('channel_commit',
                                ContractMatcher(ChannelCommitPacket.opcode,
                                    children_matchers=[
                                        labeled('lz_event',
                                            ContractMatcher(LayerZeroEventMsgBody.opcode, optional=True)),
                                        labeled('msglib_callback',
                                            ContractMatcher(MsglibconnectionMsglibConnectionCommitPacketCallback.opcode, optional=True,
                                                child_matcher=labeled("excesses", 
                                                    BlockTypeMatcher('ton_transfer', optional=True)))),
                                    ]
                                )
                            )
                        )
                    )
                )
            )
        )

    def test_self(self, block: Block) -> bool:
        return (isinstance(block, CallContractBlock)
                and block.opcode == UlnUlnCommitPacket.opcode)

    async def build_block(self, block: Block, other_blocks: list[Block]):
        uln_connection_commit_block = get_labeled("uln_connection_commit", other_blocks, CallContractBlock)
        endpoint_commit_block = get_labeled("endpoint_commit", other_blocks, CallContractBlock)
        channel_commit_block = get_labeled("channel_commit", other_blocks, CallContractBlock)
        msglib_callback_block = get_labeled("msglib_callback", other_blocks, CallContractBlock)

        if not uln_connection_commit_block or not endpoint_commit_block or not channel_commit_block or not msglib_callback_block:
            return []

        commit_msg_data = ChannelCommitPacket(channel_commit_block.get_body())
        packet = commit_msg_data.packet

        uln_connection_commit_msg = uln_connection_commit_block.get_message()
        uln_connection = AccountId(uln_connection_commit_msg.destination)
        endpoint_commit_msg = endpoint_commit_block.get_message()
        endpoint = AccountId(endpoint_commit_msg.destination)
        sender = AccountId(block.get_message().source)
        channel_commit_msg = channel_commit_block.get_message()
        channel = AccountId(channel_commit_msg.destination)
        msglib_callback_msg = msglib_callback_block.get_message()
        msglib_connection = AccountId(msglib_callback_msg.destination)

        data = LayerZeroCommitPacketData(
            sender=sender,
            uln=uln_connection,
            uln_connection=uln_connection,
            endpoint=endpoint,
            channel=channel,
            msglib_connection=msglib_connection,
            packet_data=LayerZeroPacketData(
                src_oapp=packet.path.src_oapp,
                dst_oapp=packet.path.dst_oapp,
                src_eid=packet.path.src_eid,
                dst_eid=packet.path.dst_eid,
                nonce=packet.nonce,
                guid=packet.guid,
                message=packet.message
            )
        )

        new_block = LayerZeroCommitPacketBlock(data)
        new_block.merge_blocks([block] + other_blocks)

        return [new_block]

class LayerZeroDvnVerifyMatcher(BlockMatcher):
    """
    The configured DVNs, each
    using unique verification methods,
    independently verify the message.
    The destination MessageLib enforces
    that only the DVNs
    configured by the OApp
    can submit a verification.
    """

    def __init__(self):
        super().__init__(
            child_matcher=labeled('proxy_call',
                ContractMatcher(ProxyCallContract.opcode,
                    child_matcher=labeled('uln_verify',
                        ContractMatcher(UlnUlnVerify.opcode,
                            child_matcher=labeled('uln_connection_verify',
                                ContractMatcher(UlnconnectionUlnConnectionVerify.opcode,
                                    children_matchers=[
                                        labeled('lz_event',
                                            ContractMatcher(LayerZeroEventMsgBody.opcode, optional=True)),
                                        labeled('uln_verify_callback',
                                            ContractMatcher(UlnConnectionVerifyCallbackParser.opcode, optional=True,
                                                child_matcher=labeled("excesses", 
                                                    BlockTypeMatcher('ton_transfer', optional=True)))),
                                    ]
                                )
                            )
                        )
                    )
                )
            )
        )

    def test_self(self, block: Block) -> bool:
        return (isinstance(block, CallContractBlock)
                and block.opcode == DvnVerify.opcode)

    async def build_block(self, block: Block, other_blocks: list[Block]):
        try:
            dvn_verify_block = block
            uln_verify_block = get_labeled("uln_verify", other_blocks, CallContractBlock)
            uln_connection_verify_block = get_labeled("uln_connection_verify", other_blocks, CallContractBlock)
            proxy_call_block = get_labeled("proxy_call", other_blocks, CallContractBlock)
            uln_verify_callback_block = get_labeled("uln_verify_callback", other_blocks, CallContractBlock)
            if not uln_verify_callback_block or not dvn_verify_block or not uln_verify_block or not uln_connection_verify_block or not proxy_call_block:
                return []

            parser = UlnConnectionVerifyCallbackParser(uln_verify_callback_block.get_body())
            dvn_verify_msg = dvn_verify_block.get_message()
            dvn = AccountId(dvn_verify_msg.destination)
            proxy_call_msg = proxy_call_block.get_message()
            proxy = AccountId(proxy_call_msg.destination)
            uln_verify_msg = uln_verify_block.get_message()
            uln = AccountId(uln_verify_msg.destination)
            uln_connection_verify_msg = uln_connection_verify_block.get_message()
            uln_connection = AccountId(uln_connection_verify_msg.destination)
            data = LayerZeroDvnVerifyData(
                sender=AccountId(block.get_message().source),
                nonce=parser.nonce,
                status=parser.status,
                dvn=dvn,
                proxy=proxy,
                uln=uln,
                uln_connection=uln_connection,
            )

            new_block = LayerZeroDvnVerifyBlock(data)
            new_block.merge_blocks([block] + other_blocks)

            return [new_block]
        except Exception as e:
            logger.error(f"Failed to build LayerZero dvn verify block: {e}", exc_info=True)
            return []