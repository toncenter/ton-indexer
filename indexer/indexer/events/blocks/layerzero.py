from __future__ import annotations

import base64
import logging
from dataclasses import dataclass

from indexer.events.blocks.basic_blocks import CallContractBlock, TonTransferBlock
from indexer.events.blocks.basic_matchers import (
    BlockMatcher,
    BlockTypeMatcher,
    ContractMatcher,
    OrMatcher,
    GenericMatcher,
    child_sequence_matcher,
)
from indexer.events.blocks.core import Block
from indexer.events.blocks.jettons import JettonTransferBlock
from indexer.events.blocks.labels import labeled
from indexer.events.blocks.utils import AccountId, Asset, Amount
from indexer.events.blocks.utils.block_utils import get_labeled
from indexer.events.blocks.messages.layerzero import (
    # Import LayerZero opcode classes
    EndpointEndpointSend,
    ChannelChannelSend,
    ChannelMsglibSendCallback,
    ChannelChannelCommitPacket,
    ChannelLzReceivePrepare,
    ChannelLzReceiveLock,
    ChannelLzReceiveExecuteCallback,
    ChannelBurn,
    ChannelNilify,
    BaseinterfaceEvent,
    LayerZeroEventMsgBody,
    MsglibconnectionMsglibConnectionSend,
    LayerzeroChannelSendCallback,
    # Import message parsers
    ChannelSendMessageParser,
    MsglibSendCallbackParser,
    ChannelCommitPacketParser,
    LzReceivePrepareParser,
    LzReceiveLockParser,
    LzReceiveExecuteCallbackParser,
    PacketBurnParser,
    PacketNilifyParser,
    LayerZeroPacketIdParser,
    LayerZeroDvnFeesPaidEventParser,
    LayerZeroExecutorFeePaidEventParser,
    UlnUlnSend,
)

logger = logging.getLogger(__name__)


@dataclass
class LayerZeroSendData:
    initiator: AccountId           # who initiated
    src_oapp: str                  # source OApp
    dst_oapp: str                  # destination OApp (on another blockchain)
    src_eid: int                   # source endpoint ID (TON = 30343)
    dst_eid: int                   # destination endpoint ID (ETH = 30101)
    nonce: int                     # packet sequence number
    guid: str                      # global unique ID
    send_request_id: int           # request id
    message: str                   # message
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
    receiver: AccountId            # OApp recipient
    sender: AccountId              # OApp sender (from another blockchain)
    src_eid: int                   # source endpoint ID
    dst_eid: int                   # destination endpoint ID (TON)
    nonce: int                     # packet number
    message_hash: str              # message hash
    execution_status: str          # committed/executed/failed
    decoded_message: str | None    # decoded message

@dataclass
class LayerZeroPacketManagementData:
    operator: AccountId            # who initiated
    nonce: int                     # packet number
    action_type: str               # "burn" / "nilify"
    reason: str | None             # action reason

@dataclass
class LayerZeroConfigurationData:
    actor: AccountId               # who configures
    config_type: str               # "endpoint" / "channel" / "msglib"
    changes: dict                  # config changes


class LayerZeroSendBlock(Block):
    data: LayerZeroSendData

    def __init__(self, data: LayerZeroSendData):
        super().__init__('layerzero_send', [], data)
    
    def __repr__(self):
        return f"layerzero_send({self.data.src_oapp} → {self.data.dst_oapp}, nonce={self.data.nonce}, guid={self.data.guid}, msg={self.data.message})"

class LayerZeroReceiveBlock(Block):
    data: LayerZeroReceiveData

    def __init__(self, data: LayerZeroReceiveData):
        super().__init__('layerzero_receive', [], data)
        
class LayerZeroPacketManagementBlock(Block):
    data: LayerZeroPacketManagementData

    def __init__(self, data: LayerZeroPacketManagementData):
        super().__init__('layerzero_packet_management', [], data)

class LayerZeroConfigurationBlock(Block):
    data: LayerZeroConfigurationData

    def __init__(self, data: LayerZeroConfigurationData):
        super().__init__('layerzero_configuration', [], data)

class LayerZeroSendTokensBlock(Block):
    data: LayerZeroSendTokensData

    def __init__(self, data: LayerZeroSendTokensData):
        super().__init__('layerzero_send_tokens', [], data)
        
    def __repr__(self):
        return f"layerzero_send_tokens({self.data.sender} → {self.data.oapp}, {self.data.amount} {self.data.asset})"



class LayerZeroSendMatcher(BlockMatcher):
    """Matcher for cross-chain send operations TON → other blockchains"""
    
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
                src_oapp=packet.path.src_oapp,
                dst_oapp=packet.path.dst_oapp,
                src_eid=packet.path.src_eid,
                dst_eid=packet.path.dst_eid,
                nonce=packet.nonce,
                guid=packet.guid,
                send_request_id=lz_send.send_request_id,
                msglib_manager=lz_send.send_msglib_manager,
                message=packet.message,
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
    """Matcher for LayerZero token sends (jetton_transfer + layerzero_send)"""
    
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
    """Matcher for cross-chain receive operations other blockchains → TON"""
    
    def __init__(self):
        super().__init__(
            child_matcher=child_sequence_matcher([
                # 1. Channel commits packet
                labeled("channel_commit",
                    ContractMatcher(ChannelChannelCommitPacket.opcode)),
                
                # 2. Someone prepares receive
                labeled("receive_prepare", 
                    ContractMatcher(ChannelLzReceivePrepare.opcode)),
                
                # 3. OApp locks for execution
                labeled("receive_lock",
                    ContractMatcher(ChannelLzReceiveLock.opcode)),
                
                # 4. OApp execution callback
                labeled("execute_callback",
                    ContractMatcher(ChannelLzReceiveExecuteCallback.opcode)),
                
                # 5. Optional: DELIVERED event
                labeled("delivered_event",
                    GenericMatcher(lambda b: isinstance(b, CallContractBlock)
                                  and b.opcode == BaseinterfaceEvent.opcode, optional=True)),
            ])
        )
    
    def test_self(self, block: Block) -> bool:
        # Start with endpoint committing a packet
        return (isinstance(block, CallContractBlock)
                and block.opcode == ChannelChannelCommitPacket.opcode)
    
    async def build_block(self, block: Block, other_blocks: list[Block]):
        try:
            # Extract labeled blocks
            receive_prepare_block = get_labeled("receive_prepare", other_blocks, CallContractBlock)
            execute_callback_block = get_labeled("execute_callback", other_blocks, CallContractBlock)
            
            # Check if required blocks are found
            if not receive_prepare_block or not execute_callback_block:
                return []
            
            # Parse messages
            commit_msg = ChannelCommitPacketParser(block.get_body())
            prepare_msg = LzReceivePrepareParser(receive_prepare_block.get_body())
            execute_msg = LzReceiveExecuteCallbackParser(execute_callback_block.get_body())
            
            # Determine execution status
            if execute_msg.success:
                execution_status = "executed"
            else:
                execution_status = "failed"
            
            # Create structured data
            data = LayerZeroReceiveData(
                receiver=receive_prepare_block.data['destination'],
                sender=AccountId(f"0:{commit_msg.packet.path.src_oapp:064x}"),
                src_eid=commit_msg.packet.path.src_eid,
                dst_eid=commit_msg.packet.path.dst_eid,
                nonce=commit_msg.packet.nonce,
                message_hash=base64.b64encode(commit_msg.packet.message.encode()).decode(),
                execution_status=execution_status,
                decoded_message=None  # Could decode message payload if needed
            )
            
            # Create block and link with originals
            new_block = LayerZeroReceiveBlock(data)
            new_block.merge_blocks([block] + other_blocks)
            
            return [new_block]
            
        except Exception as e:
            logger.error(f"Failed to build LayerZero receive block: {e}")
            return []

class LayerZeroPacketManagementMatcher(BlockMatcher):
    """Matcher for packet management operations (burn/nilify)"""
    
    def __init__(self):
        super().__init__(
            child_matcher=OrMatcher([
                labeled("burn_action", ContractMatcher(ChannelBurn.opcode)),
                labeled("nilify_action", ContractMatcher(ChannelNilify.opcode))
            ])
        )
    
    def test_self(self, block: Block) -> bool:
        return (isinstance(block, CallContractBlock) 
                and block.opcode in [ChannelBurn.opcode, ChannelNilify.opcode])
    
    async def build_block(self, block: Block, other_blocks: list[Block]):
        try:
            # Cast to CallContractBlock to access attributes
            if not isinstance(block, CallContractBlock):
                return []
            
            # Determine action type
            if block.opcode == ChannelBurn.opcode:
                action_type = "burn"
                parser = PacketBurnParser(block.get_body())
            else:  # ChannelNilify.opcode
                action_type = "nilify"
                parser = PacketNilifyParser(block.get_body())
            
            # Create structured data
            data = LayerZeroPacketManagementData(
                operator=block.data['source'],
                nonce=parser.nonce,
                action_type=action_type,
                reason=f"Packet {action_type} for nonce {parser.nonce}"
            )
            
            # Create block and link with originals
            new_block = LayerZeroPacketManagementBlock(data)
            new_block.merge_blocks([block] + other_blocks)
            
            return [new_block]
            
        except Exception as e:
            logger.error(f"Failed to build LayerZero packet management block: {e}")
            return []

class LayerZeroConfigurationMatcher(BlockMatcher):
    """Matcher for configuration changes"""
    
    def __init__(self):
        super().__init__()
    
    def test_self(self, block: Block) -> bool:
        # Match various configuration opcodes
        config_opcodes = [
            EndpointEndpointSend.opcode,  # Example - add more as needed
            # Add other configuration opcodes here
        ]
        return (isinstance(block, CallContractBlock) 
                and block.opcode in config_opcodes)
    
    async def build_block(self, block: Block, other_blocks: list[Block]):
        try:
            # Cast to CallContractBlock to access attributes
            if not isinstance(block, CallContractBlock):
                return []
            
            # For now, create basic configuration data
            # This can be expanded to parse specific configuration changes
            data = LayerZeroConfigurationData(
                actor=block.data['source'],
                config_type="unknown",
                changes={"opcode": hex(block.opcode)}
            )
            
            # Create block and link with originals
            new_block = LayerZeroConfigurationBlock(data)
            new_block.merge_blocks([block] + other_blocks)
            
            return [new_block]
            
        except Exception as e:
            logger.error(f"Failed to build LayerZero configuration block: {e}")
            return []


