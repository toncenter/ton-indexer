from indexer.database import *
from indexer.crud import *
import codecs
import json
import aiohttp
import asyncio
import logging
import time
from urllib.parse import urlparse
from tvm_valuetypes import deserialize_boc
from tvm_valuetypes.cell import CellData
from sqlalchemy.orm import Session
from sqlalchemy.future import select
from sqlalchemy import update
from parser.bitreader import BitReader
from dataclasses import dataclass
from aiokafka import AIOKafkaProducer
from parser.eventbus import Event


logging.getLogger("requests").setLevel(logging.WARNING)

def opt_apply(value, func):
    if value is not None:
        return func(value)
    else:
        return None

# Simple parser predicate
class ParserPredicate:
    def __init__(self, context_class):
        self.context_class = context_class

    def match(self, context: any) -> bool:
        if self.context_class and not isinstance(context, self.context_class):
            return False
        return self._internal_match(context)

    def _internal_match(self, context: any):
        raise Error("Not implemented")

class AlwaysTrue(ParserPredicate):
    def __init__(self, context_class):
        super(AlwaysTrue, self).__init__(context_class)

    def _internal_match(self, context):
        return True

class OpCodePredicate(ParserPredicate):
    def __init__(self, opcode):
        super(OpCodePredicate, self).__init__(MessageContext)
        self.opcode = opcode if opcode < 0x80000000 else -1 * (0x100000000 - opcode)

    def _internal_match(self, context: MessageContext):
        # logger.info(f"_internal_match: {context.source_tx} {context.message.op} == {self.opcode}")
        return context.source_tx is not None and context.message.op == self.opcode

"""
Requires existence of source_tx in the MessageContext. Otherwise raises an exception.
In some cases source_tx could be absent at the time we do parsing.  In this cases
it is better to reject processing and re-launch it later
"""
class SourceTxRequiredPredicate(ParserPredicate):
    def __init__(self, delegate: ParserPredicate):
        super(SourceTxRequiredPredicate, self).__init__(context_class=None)
        self.delegate = delegate

    def _internal_match(self, context: MessageContext):
        res = self.delegate.match(context)
        if res:
            if context.source_tx is None:
                logger.warning(f"source_tx is not found for message {context.message.msg_id}")
                raise Exception(f"No source_tx for {context.message.msg_id}")
        return res

"""
The same as SourceTxRequiredPredicate but for destination_tx
"""
class DestinationTxRequiredPredicate(ParserPredicate):
    def __init__(self, delegate: ParserPredicate):
        super(DestinationTxRequiredPredicate, self).__init__(context_class=None)
        self.delegate = delegate

    def _internal_match(self, context: MessageContext):
        res = self.delegate.match(context)
        if res:
            if context.destination_tx is None:
                logger.warning(f"destination_tx is not found for message {context.message.msg_id}")
                raise Exception(f"No destination_tx for {context.message.msg_id}")
        return res


class ContractPredicate(ParserPredicate):
    def __init__(self, supported):
        super(ContractPredicate, self).__init__(AccountContext)
        self.supported = supported

    def _internal_match(self, context):
        return context.code is not None and context.code.hash in self.supported


class ActiveAccountsPredicate(ParserPredicate):
    def __init__(self):
        super(ActiveAccountsPredicate, self).__init__(AccountContext)

    def _internal_match(self, context):
        return context.code is not None

"""
Wrapper around event. Parser could generate events
and return it in wrapped form. If event being generated
depends on db commit, provide waitCommit=True
"""
@dataclass
class GeneratedEvent:
    event: Event
    waitCommit: bool = False

class Parser:
    def __init__(self, predicate: ParserPredicate):
        self.predicate = predicate

    """
    Implementation could return events for the EventBus wrapped in GeneratedEvent.
    Return either a single GeneratedEvent or a list of GeneratedEvent  
    """
    async def parse(self, session: Session, context: any):
        raise Error("Not supported")

    def _parse_boc(self, b64data):
        raw = codecs.decode(codecs.encode(b64data, "utf-8"), "base64")
        return deserialize_boc(raw)

# in some cases wallet was not parsed due to empty data/code. in this case we will force it to update its state
async def check_empty_wallets(session: Session, address):
    state = (await session.execute(select(AccountState)
                                                .filter(AccountState.address == address)
                                                .order_by(AccountState.last_tx_lt.desc())
                                                )).first()
    if not state:
        # this is possible if someone sends transfer without state_init. rare, but possible
        logger.warning(f"State for {address} not found")
        return
    state = state[0]
    if state.code_hash is None and state.last_tx_hash == 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=':
        logger.warning(f"Empty state found for {address}: {state}")
        await session.execute(update(KnownAccounts).where(KnownAccounts.address == address) \
                           .values(last_check_time=None))


"""
TEP-74 jetton standard
transfer#0f8a7ea5 query_id:uint64 amount:(VarUInteger 16) destination:MsgAddress
                 response_destination:MsgAddress custom_payload:(Maybe ^Cell)
                 forward_ton_amount:(VarUInteger 16) forward_payload:(Either Cell ^Cell)
                 = InternalMsgBody;
"""
class JettonTransferParser(Parser):
    def __init__(self):
        super(JettonTransferParser, self).__init__(DestinationTxRequiredPredicate(OpCodePredicate(0x0f8a7ea5)))

    @staticmethod
    def parser_name() -> str:
        return "JettonTransfer"

    async def parse(self, session: Session, context: MessageContext):
        logger.info(f"Parsing jetton transfer for message {context.message.msg_id}")
        cell = self._parse_boc(context.content.body)
        reader = BitReader(cell.data.data)
        op_id = reader.read_uint(32) # transfer#0x0f8a7ea5
        query_id = reader.read_uint(64)
        amount = reader.read_coins()
        destination = reader.read_address()
        response_destination = reader.read_address()
        custom_payload = None
        # TODO move Maybe parsing to utils
        if reader.read_uint(1):
            custom_payload = cell.refs.pop(0)
        forward_ton_amount = reader.read_coins()
        # TODO move Either parsing to utils
        forward_payload = None
        try:
            if reader.read_uint(1):
                forward_payload = cell.refs.pop(0)
            else:
                if destination == 'EQB3ncyBUTjZUA5EnFKR5_EnOMI9V1tTEAAPaiU71gc4TiUt' and reader.slice_bits() < 32 and len(cell.refs) > 0:
                    # Some ston.fi users send message with wrong layout, but ston.fi router accepts it
                    forward_payload = cell.refs.pop(0)
                else:
                    # in-place, read the rest of the cell slice and refs
                    forward_payload = cell.copy()
                    slice = CellData()
                    slice.data = reader.read_remaining()
                    forward_payload.data = slice
        except Exception as e:
            logger.error(f"Unable to parse forward payload {e}")

        sub_op = None
        if forward_payload is not None:
            fp_reader = BitReader(forward_payload.data.data)
            if fp_reader.slice_bits() >= 32:
                sub_op = fp_reader.read_uint(32)
        """
        destination_tx for jetton transfer contains internal_transfer (it is not enforced by TEP-74)
        execution and it has to be successful
        """
        successful = context.destination_tx.action_result_code == 0 and context.destination_tx.compute_exit_code == 0
        transfer = JettonTransfer(
            msg_id = context.message.msg_id,
            created_lt = context.message.created_lt,
            utime = context.destination_tx.utime,
            successful = successful,
            originated_msg_id = await get_originated_msg_id(session, context.message),
            query_id = str(query_id),
            amount = amount,
            source_owner = context.message.source,
            source_wallet = context.message.destination,
            destination_owner = destination,
            response_destination = response_destination,
            custom_payload = custom_payload.serialize_boc() if custom_payload is not None else None,
            forward_ton_amount = forward_ton_amount,
            forward_payload = forward_payload.serialize_boc() if forward_payload is not None else None,
            sub_op = sub_op
        )
        logger.info(f"Adding jetton transfer {transfer}")
        await upsert_entity(session, transfer)

        if successful:
            wallet = await get_wallet(session, context.message.destination)
            if not wallet:
                await check_empty_wallets(session, context.message.destination)
                raise Exception(f"Wallet not inited yet {context.message.destination}")
            return GeneratedEvent(event=Event(
                event_scope="Jetton",
                event_target=wallet.jetton_master,
                finding_type="Info",
                event_type="Transfer",
                severity="Medium",
                data={
                    "master": wallet.jetton_master,
                    "amount": str(amount),
                    "query_id": query_id,
                    "source_wallet": context.message.destination,
                    "source_owner": context.message.source,
                    "destination_owner": destination
                }
            ))

"""
Special parser which only fires event with msg_id of jetton transfer in order to parse
it in the further steps of ETL pipeline. JettonTransferParser has possibility to 
fail during parsing so this parser provides guarantees event would be triggered (at least once).
"""
class JettonTransferEventParser(Parser):
    def __init__(self):
        super(JettonTransferEventParser, self).__init__(DestinationTxRequiredPredicate(OpCodePredicate(0x0f8a7ea5)))

    @staticmethod
    def parser_name() -> str:
        return "JettonTransferEvent"

    async def parse(self, session: Session, context: MessageContext):
        return GeneratedEvent(event=Event(
            event_scope="Jetton",
            event_target="",
            finding_type="Internal",
            event_type="TransferInternalEvent",
            severity="Medium",
            data={
                "db_ref": context.message.msg_id
            }
        ), waitCommit=True)


"""
TEP-74 jetton standard does not specify mint format but it has recommended form of internal_transfer message:

internal_transfer  query_id:uint64 amount:(VarUInteger 16) from:MsgAddress
                     response_address:MsgAddress
                     forward_ton_amount:(VarUInteger 16)
                     forward_payload:(Either Cell ^Cell)
                     = InternalMsgBody;

So mint is an internal_transfer without preceding transfer message
"""
class JettonMintParser(Parser):
    def __init__(self):
        super(JettonMintParser, self).__init__(DestinationTxRequiredPredicate(OpCodePredicate(0x178d4519)))

    @staticmethod
    def parser_name() -> str:
        return "JettonMinter"

    async def parse(self, session: Session, context: MessageContext):
        # ensure we have no transfer operation before
        prev_message = await get_messages_by_in_tx_id(session, context.source_tx.tx_id)
        if prev_message.op == 0x0f8a7ea5:
            # skip ordinary chain transfer => internal_transfer
            return
        cell = self._parse_boc(context.content.body)
        reader = BitReader(cell.data.data)
        op_id = reader.read_uint(32) # internal_transfer#0x178d4519
        query_id = reader.read_uint(64)
        amount = reader.read_coins()
        from_address = reader.read_address()
        response_destination = reader.read_address()
        forward_ton_amount = reader.read_coins()
        # TODO move Either parsing to utils
        forward_payload = None
        try:
            if reader.read_uint(1):
                forward_payload = cell.refs.pop(0)
            else:
                # in-place, read the rest of the cell slice and refs
                forward_payload = cell.copy()
                slice = CellData()
                slice.data = reader.read_remaining()
                forward_payload.data = slice
        except Exception as e:
            logger.error(f"Unable to parse forward payload {e}")

        sub_op = None
        if forward_payload is not None:
            fp_reader = BitReader(forward_payload.data.data)
            if fp_reader.slice_bits() >= 32:
                sub_op = fp_reader.read_uint(32)

        successful = context.destination_tx.action_result_code == 0 and context.destination_tx.compute_exit_code == 0
        mint = JettonMint(
            msg_id = context.message.msg_id,
            created_lt = context.message.created_lt,
            utime = context.destination_tx.utime,
            successful = successful,
            originated_msg_id = await get_originated_msg_id(session, context.message),
            query_id = str(query_id),
            amount = amount,
            minter = context.message.source,
            wallet = context.message.destination,
            from_address = from_address,
            response_destination = response_destination,
            forward_ton_amount = forward_ton_amount,
            forward_payload = forward_payload.serialize_boc() if forward_payload is not None else None,
            sub_op = sub_op
        )
        logger.info(f"Adding jetton mint {mint}")
        await upsert_entity(session, mint)

        if successful:
            wallet = await get_wallet(session, context.message.destination)
            if not wallet:
                await check_empty_wallets(session, context.message.destination)
                raise Exception(f"Wallet not inited yet {context.message.destination}")
            return GeneratedEvent(event=Event(
                event_scope="Jetton",
                event_target=wallet.jetton_master,
                finding_type="Info",
                event_type="Mint",
                severity="Medium",
                data={
                    "master": wallet.jetton_master,
                    "amount": str(amount),
                    "query_id": query_id,
                    "destination_wallet": context.message.destination,
                    "minter": context.message.source,
                    "destination_owner": wallet.owner
                }
            ))


"""
TEP-74:

burn#595f07bc query_id:uint64 amount:(VarUInteger 16)
              response_destination:MsgAddress custom_payload:(Maybe ^Cell)
              = InternalMsgBody;
"""
class JettonBurnParser(Parser):
    def __init__(self):
        super(JettonBurnParser, self).__init__(DestinationTxRequiredPredicate(OpCodePredicate(0x595f07bc)))

    @staticmethod
    def parser_name() -> str:
        return "JettonBurn"

    async def parse(self, session: Session, context: MessageContext):
        cell = self._parse_boc(context.content.body)
        reader = BitReader(cell.data.data)
        op_id = reader.read_uint(32) # burn#595f07bc
        query_id = reader.read_uint(64)
        amount = reader.read_coins()
        try:
            response_destination = reader.read_address()
        except:
            logger.warning("Wrong response destination format")
            response_destination = None
        custom_payload = None
        # TODO move Maybe parsing to utils
        try:
            if reader.read_uint(1):
                custom_payload = cell.refs.pop(0)
        except:
            logger.warning("Wrong custom payload format")
        successful = context.destination_tx.action_result_code == 0 and context.destination_tx.compute_exit_code == 0
        burn = JettonBurn(
            msg_id = context.message.msg_id,
            created_lt = context.message.created_lt,
            utime = context.destination_tx.utime,
            successful = successful,
            originated_msg_id = await get_originated_msg_id(session, context.message),
            query_id = str(query_id),
            amount = amount,
            owner = context.message.source,
            wallet = context.message.destination,
            response_destination = response_destination,
            custom_payload = custom_payload.serialize_boc() if custom_payload is not None else None
        )
        logger.info(f"Adding jetton burn {burn}")
        await upsert_entity(session, burn)

        if successful:
            wallet = await get_wallet(session, context.message.destination)
            if not wallet:
                jetton_master = await get_jetton_master(session, context.message.destination) # special case for some jettons
                if not jetton_master:
                    raise Exception(f"Wallet not inited yet {context.message.destination}")
                jetton_master = jetton_master.address
            else:
                jetton_master = wallet.jetton_master
            return GeneratedEvent(event=Event(
                event_scope="Jetton",
                event_target=jetton_master,
                finding_type="Info",
                event_type="Burn",
                severity="Medium",
                data={
                    "master": jetton_master,
                    "amount": str(amount),
                    "query_id": query_id,
                    "destination_wallet": context.message.destination,
                    "destination_owner": wallet.owner if wallet else None
                }
            ))


"""
Utility class to execute get methods remotely over contracts-executor (./contracts-executor)
"""
class ContractsExecutorParser(Parser):
    def __init__(self, predicate=ActiveAccountsPredicate()):
        super(ContractsExecutorParser, self).__init__(predicate)
        self.executor_url = settings.parser.executor.url

    async def _execute(self, code, data, method, types, address=None, arguments=[]):
        req = {'code': code, 'data': data, 'method': method,
               'expected': types, 'address': address, 'arguments': arguments}
        if address is not None:
            req[address] = address
        async with aiohttp.ClientSession() as session:
            resp = await session.post(self.executor_url, json=req)
            async with resp:
                assert resp.status == 200, "Error during contract executor call: %s" % resp
                res = await resp.json()
                if res['exit_code'] != 0:
                    # logger.debug("Non-zero exit code: %s" % res)
                    return None
                return res['result']

    """
    Parses content according to TEP-64, returns tuple of metadata_url and metadata dict
    """
    async def _parse_tep64content(self, content):
        if content is None or 'content_layout' not in content:
            logger.warning(f"No content layout extracted: {content}")
            metadata_url = None
            metadata = {}
        else:
            if content['content_layout'] == 'off-chain':
                metadata_url = content['content']
                metadata_json = await self.fetcher.fetch(metadata_url)
                try:
                    metadata = json.loads(metadata_json)
                except Exception as e:
                    logger.error(f"Failed to parse metadata for {metadata_url}: {e}")
                    metadata = {}
                metadata['content_layout'] = content['content_layout']
            else:
                metadata = content['content']
                if 'uri' in metadata:
                    logger.info(f"Semi on-chain layout detected, uri: {metadata['uri']}")
                    metadata_url = metadata['uri']
                    metadata_json = await self.fetcher.fetch(metadata_url)
                    try:
                        aux_metadata = json.loads(metadata_json)
                        for k, v in aux_metadata.items():
                            metadata[k] = v
                    except Exception as e:
                        logger.error(f"Failed to parse metadata for {metadata_url}: {e}")
                else:
                    metadata_url = None

        return metadata_url, metadata


class JettonWalletParser(ContractsExecutorParser):
    def __init__(self):
        super(JettonWalletParser, self).__init__()

    @staticmethod
    def parser_name() -> str:
        return "JettonWalletParser"

    async def parse(self, session: Session, context: AccountContext):
        wallet_data = await self._execute(context.code.code, context.account.data, 'get_wallet_data',
                                    ["int", "address", "address", "cell_hash"])
        try:
            balance, owner, jetton, _ = wallet_data
        except:
            # we invoke get method on all contracts so ignore errors
            return

        jetton_master = await get_jetton_master(session, jetton)
        if not jetton_master:
            raise Exception(f"Jetton master not inited yet {jetton}")

        jetton_context = await get_account_context(session, jetton_master.state_id)

        try:
            wallet_address_from_jetton, = await self._execute(jetton_context.code.code, jetton_context.account.data, 
                                                        'get_wallet_address', ["address"], jetton, [owner])
        except Exception as e:
            raise Exception(f"Get method from jetton master {jetton} was executed with an error: {e}")

        wallet = JettonWallet(
            state_id = context.account.state_id,
            address = context.account.address,
            owner = owner,
            jetton_master = jetton,
            balance = balance,
            is_scam = context.account.address != wallet_address_from_jetton,
        )
        logger.info(f"Adding jetton wallet {wallet}")

        await upsert_entity(session, wallet, constraint="address")
        if owner is not None:
            await ensure_account_known(session, owner)
        if jetton is not None:
            await ensure_account_known(session, jetton)

class RemoteDataFetcher:
    def __init__(self, ipfs_gateway='https://w3s.link/ipfs/', timeout=5, max_attempts=5):
        self.ipfs_gateway = ipfs_gateway
        self.timeout = timeout
        self.max_attempts = max_attempts

    async def fetch(self, url):
        logger.debug(f"Fetching {url}")
        parsed_url = urlparse(url)
        async with aiohttp.ClientSession() as session:
            if parsed_url.scheme == 'ipfs':
                # assert len(parsed_url.path) == 0, parsed_url
                async with session.get(self.ipfs_gateway + parsed_url.netloc + parsed_url.path, timeout=self.timeout) as resp:
                    return await resp.text()
            elif parsed_url.scheme is None or len(parsed_url.scheme) == 0:
                logger.error(f"No schema for URL: {url}")
                return None
            else:
                if parsed_url.netloc == 'localhost' or parsed_url.netloc in ["ton-metadata.fanz.ee", "startupmarket.io", "mashamimosa.ru", "server.tonguys.org"]:
                    return None
                retry = 0
                while retry < self.max_attempts:
                    try:
                        async with session.get(url, timeout=self.timeout) as resp:
                            return await resp.text()
                    except Exception as e:
                        logger.error(f"Unable to fetch data from {url}", e)
                        await asyncio.sleep(5)
                    retry += 1
                return None


class JettonMasterParser(ContractsExecutorParser):
    def __init__(self):
        super(JettonMasterParser, self).__init__()
        self.fetcher = RemoteDataFetcher()

    @staticmethod
    def parser_name() -> str:
        return "JettonMasterParser"

    async def parse(self, session: Session, context: AccountContext):
        wallet_data = await self._execute(context.code.code, context.account.data, 'get_jetton_data',
                                    ["int", "int", "address", "metadata", "cell_hash"],
                                    address=context.account.address)
        try:
            total_supply, mintable, admin_address, jetton_content, wallet_hash = wallet_data
        except:
            # we invoke get method on all contracts so ignore errors
            return

        metadata_url, metadata = await self._parse_tep64content(jetton_content)

        master = JettonMaster(
            state_id = context.account.state_id,
            address = context.account.address,
            total_supply = total_supply,
            mintable = mintable,
            admin_address = admin_address,
            jetton_wallet_code = wallet_hash,
            symbol = metadata.get('symbol', None),
            name = metadata.get('name', None),
            image = metadata.get('image', None),
            image_data = metadata.get('image_data', None),
            decimals = opt_apply(metadata.get('decimals', None), int),
            metadata_url = metadata_url,
            description = metadata.get('description', None)
        )
        logger.info(f"Adding jetton master {master}")

        await upsert_entity(session, master, constraint="address")


"""
TEP-62 NFT standard
    transfer#5fcc3d14 query_id:uint64 new_owner:MsgAddress response_destination:MsgAddress 
    custom_payload:(Maybe ^Cell) forward_amount:(VarUInteger 16) f
    orward_payload:(Either Cell ^Cell) = InternalMsgBody;
"""
class NFTTransferParser(Parser):
    def __init__(self):
        super(NFTTransferParser, self).__init__(DestinationTxRequiredPredicate(OpCodePredicate(0x5fcc3d14)))

    @staticmethod
    def parser_name() -> str:
        return "NFTTransfer"

    async def parse(self, session: Session, context: MessageContext):
        logger.info(f"Parsing NFT transfer for message {context.message.msg_id}")
        cell = self._parse_boc(context.content.body)
        reader = BitReader(cell.data.data)
        op_id = reader.read_uint(32) # transfer#5fcc3d14
        query_id = reader.read_uint(64)
        new_owner = reader.read_address()
        response_destination = reader.read_address()
        custom_payload = None
        if reader.read_uint(1):
            try:
                custom_payload = cell.refs.pop(0)
            except:
                logger.warning(f"Unable to get custom_payload for {context.message.msg_id}")
                pass
        forward_ton_amount = reader.read_coins()
        forward_payload = None
        try:
            if reader.read_uint(1):
                forward_payload = cell.refs.pop(0)
            else:
                # in-place, read the rest of the cell slice and refs
                forward_payload = cell.copy()
                slice = CellData()
                slice.data = reader.read_remaining()
                forward_payload.data = slice
        except Exception as e:
            logger.error(f"Unable to parse forward payload {e}")

        successful = context.destination_tx.action_result_code == 0 and context.destination_tx.compute_exit_code == 0
        try:
            fp = forward_payload.serialize_boc()
        except:
            fp = None
        transfer = NFTTransfer(
            msg_id=context.message.msg_id,
            created_lt=context.message.created_lt,
            utime=context.destination_tx.utime,
            successful=successful,
            originated_msg_id=await get_originated_msg_id(session, context.message),
            query_id=str(query_id),
            current_owner=context.message.source,
            nft_item=context.message.destination,
            new_owner=new_owner,
            response_destination=response_destination,
            custom_payload=custom_payload.serialize_boc() if custom_payload is not None else None,
            forward_ton_amount=forward_ton_amount,
            forward_payload=fp if forward_payload is not None else None
        )
        logger.info(f"Adding NFT transfer {transfer}")
        await upsert_entity(session, transfer)

        if successful:
            events = []
            nft = await get_nft(session, context.message.destination)
            if not nft:
                raise Exception(f"NFT not inited yet {context.message.destination}")
            
            await self._parse_nft_history(session, context, transfer, nft)

            events.append(GeneratedEvent(event=Event(
                event_scope="NFT",
                event_target=nft.collection,
                finding_type="Info",
                event_type="Transfer",
                severity="Medium",
                data={
                    "collection": nft.collection,
                    "name": nft.name,
                    "nft_item": context.message.destination,
                    "new_owner": new_owner,
                    "previous_owner": context.message.source,
                    "msg_hash": context.message.hash
                }
            )))

            prev_owner_sale = await get_nft_sale(session, context.message.source)

            # Force auction sale account to update state
            if prev_owner_sale and prev_owner_sale.is_auction and prev_owner_sale.owner != new_owner:
                await reset_account_check_time(session, prev_owner_sale.address)

            # TODO ensure we have already parsed it
            if prev_owner_sale is not None:
                # Exclude sales cancellation
                if prev_owner_sale.owner != new_owner:
                    events.append(GeneratedEvent(event=Event(
                        event_scope="NFT",
                        event_target=nft.collection,
                        finding_type="Info",
                        event_type="Sale",
                        severity="Medium",
                        data={
                            "collection": nft.collection,
                            "name": nft.name,
                            "nft_item": context.message.destination,
                            "new_owner": new_owner,
                            "previous_owner": prev_owner_sale.owner,
                            "price": int(prev_owner_sale.price) / 1000000000,
                            "marketplace": prev_owner_sale.marketplace,
                            "msg_hash": context.message.hash
                        }
                    )))
            return events

    async def _parse_nft_history(self, session: Session, context: MessageContext, transfer: NFTTransfer, nft: NFTItem):
        try:
            from parser.nft_contracts import SALE_CONTRACTS
        except Exception as e:
            logger.error("Unable to init sale contracts", e)
            SALE_CONTRACTS = {}

        current_owner_code_hash = await get_account_code_hash(session, transfer.current_owner)
        if not current_owner_code_hash:
            raise Exception(f"Current owner account not inited yet {transfer.current_owner}")

        new_owner_code_hash = await get_account_code_hash(session, transfer.new_owner)
        if not new_owner_code_hash and transfer.new_owner != 'EQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAM9c':
            raise Exception(f"New owner account not inited yet {transfer.new_owner}")

        if current_owner_code_hash in SALE_CONTRACTS.keys():
            current_owner_sale = await get_nft_sale(session, transfer.current_owner)
            if not current_owner_sale:
                raise Exception(f"NFT sale not parsed yet {transfer.current_owner}")

        if new_owner_code_hash in SALE_CONTRACTS.keys():
            new_owner_sale = await get_nft_sale(session, transfer.new_owner)
            if not new_owner_sale:
                raise Exception(f"NFT sale not parsed yet {transfer.new_owner}")

        sale_address = None
        code_hash = None
        marketplace = None
        current_owner = transfer.current_owner
        new_owner = transfer.new_owner
        price = None
        is_auction = None

        if new_owner_code_hash in SALE_CONTRACTS.keys():
            event_type = NftHistory.EVENT_TYPE_INIT_SALE
            sale_address = new_owner_sale.address
            code_hash = new_owner_code_hash
            marketplace = new_owner_sale.marketplace
            current_owner = new_owner_sale.owner
            new_owner = None
            price = 0 if SALE_CONTRACTS[new_owner_code_hash].is_auction else new_owner_sale.price
            is_auction = SALE_CONTRACTS[new_owner_code_hash].is_auction

        elif current_owner_code_hash in SALE_CONTRACTS.keys() and current_owner_sale.owner == transfer.new_owner:
            event_type = NftHistory.EVENT_TYPE_CANCEL_SALE
            sale_address = current_owner_sale.address
            code_hash = current_owner_code_hash
            marketplace = current_owner_sale.marketplace
            current_owner = current_owner_sale.owner
            new_owner = None
            price = 0 if SALE_CONTRACTS[current_owner_code_hash].is_auction else current_owner_sale.price
            is_auction = SALE_CONTRACTS[current_owner_code_hash].is_auction

        elif current_owner_code_hash in SALE_CONTRACTS.keys() and current_owner_sale.owner != transfer.new_owner:
            event_type = NftHistory.EVENT_TYPE_SALE
            sale_address = current_owner_sale.address
            code_hash = current_owner_code_hash
            marketplace = current_owner_sale.marketplace
            current_owner = current_owner_sale.owner
            price = current_owner_sale.price
            is_auction = SALE_CONTRACTS[current_owner_code_hash].is_auction

        elif transfer.new_owner == "EQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAM9c":
            event_type = NftHistory.EVENT_TYPE_BURN

        else:
            event_type = NftHistory.EVENT_TYPE_TRANSFER

        nft_history = NftHistory(
            msg_id=context.message.msg_id,
            created_lt=context.message.created_lt,
            utime=context.destination_tx.utime,
            hash=await get_originated_msg_hash(session, context.message),
            event_type=event_type,
            nft_item_id=nft.id,
            nft_item_address=transfer.nft_item,
            collection_address=nft.collection,
            sale_address=sale_address,
            code_hash=code_hash,
            marketplace=marketplace,
            current_owner=current_owner,
            new_owner=new_owner,
            price=price,
            is_auction=is_auction
        )
        logger.info(f"Adding NFT history event {nft_history}")
        await upsert_entity(session, nft_history)


class NFTCollectionParser(ContractsExecutorParser):
    def __init__(self):
        super(NFTCollectionParser, self).__init__()
        self.fetcher = RemoteDataFetcher(timeout=10, max_attempts=2)

    @staticmethod
    def parser_name() -> str:
        return "NFTCollectionParser"

    async def parse(self, session: Session, context: AccountContext):
        collection_data = await self._execute(context.code.code, context.account.data, 'get_collection_data',
                                 ["int", "metadata", "address"],
                                 address=context.account.address)
        try:
            next_item_index, collection_content, owner = collection_data
        except:
            # we invoke get method on all contracts so ignore errors
            return

        metadata_url, metadata = await self._parse_tep64content(collection_content)
        logger.info(f"NFT collection metadata {metadata}")

        collection = NFTCollection(
            state_id=context.account.state_id,
            address=context.account.address,
            owner=owner,
            next_item_index=int(next_item_index),
            name=metadata.get('name', None),
            image=metadata.get('image', None),
            image_data=metadata.get('image_data', None),
            metadata_url=metadata_url,
            description=str(metadata.get('description', ''))
        )
        logger.info(f"Adding NFT collection {collection}")

        res = await upsert_entity(session, collection, constraint="address")
        if res.rowcount > 0:
            logger.info(f"Discovered new NFT collection {context.account.address}")
            return GeneratedEvent(event=Event(
                event_scope="NFT",
                event_target=context.account.address,
                finding_type="Info",
                event_type="NewCollection",
                severity="Medium",
                data={
                    "collection": context.account.address,
                    "name": metadata.get('name', None),
                    "image": metadata.get('image', None),
                    "image_data": metadata.get('image_data', None),
                    "metadata_url": metadata_url,
                    "owner": owner
                }
            ), waitCommit=False)

class NFTItemParser(ContractsExecutorParser):
    def __init__(self):
        super(NFTItemParser, self).__init__()
        self.fetcher = RemoteDataFetcher(timeout=3, max_attempts=2)

    @staticmethod
    def parser_name() -> str:
        return "NFTItemParser"

    async def parse(self, session: Session, context: AccountContext):
        nft_data = await self._execute(context.code.code, context.account.data, 'get_nft_data',
                                 ["int", "int", "address", "address", "boc"],
                                 address=context.account.address)
        try:
            _, index, collection_address, owner_address, individual_content_boc = nft_data
        except:
            # we invoke get method on all contracts so ignore errors
            return

        try:
            royalty_params = await self._execute(context.code.code, context.account.data, 'royalty_params',
                                 ["int", "int", "address"],
                                 address=context.account.address)
            _, _, royalty_destination = royalty_params
            logger.debug(f"Royalty params have been found: {royalty_params}")
        except:
            royalty_destination = None

        if collection_address:
            """
            TON DNS is a special case of NFT. It metadata doesn't store initial domain (it rather stores subdomains)
            so we need to get domain by calling explicitly method get_domain
            """
            if collection_address == 'EQC3dNlesgVD8YbAazcauIrXBPfiVhMMr5YYk2in0Mtsz0Bz':
                logger.info(f"Calling get_domain for ton domain {context.account.address}")
                domain,  = await self._execute(context.code.code, context.account.data, 'get_domain',
                                               ["string"],
                                               address=context.account.address)
                if owner_address is None:
                    logger.info(f"No owner for {context.account.address}, checking auction info")
                    max_bid_address, max_bid_amount, auction_end_time = await self._execute(context.code.code, context.account.data, 'get_auction_info',
                                                   ["address", "int", "int"],
                                                   address=context.account.address)
                    if int(auction_end_time) < time.time():
                        logger.info(f"Discovered actual owner for {context.account.address}: {max_bid_address}")
                        owner_address = max_bid_address
                item = NFTItem(
                    state_id=context.account.state_id,
                    address=context.account.address,
                    index=index,
                    collection=collection_address,
                    owner=owner_address,
                    name=domain,
                    description=None,
                    image=None,
                    image_data=None,
                    attributes=None,
                    telemint_royalty_address=royalty_destination
                )
                logger.info(f"Adding TON DNS item {item}")

                res = await upsert_entity(session, item, constraint="address")
                if res.rowcount > 0:
                    logger.info(f"Discovered new TON DNS item {context.account.address}")
                    return GeneratedEvent(event=Event(
                        event_scope="NFT",
                        event_target=collection_address,
                        finding_type="Info",
                        event_type="NewItem",
                        severity="Medium",
                        data={
                            "nft_item": context.account.address,
                            "collection": collection_address,
                            "name": domain,
                            "image": None,
                            "image_data": None,
                            "metadata_url": None,
                            "owner": owner_address
                        }
                    ), waitCommit=False)
                return
            logger.info(f"Getting collection info for {collection_address}")
            # need to retry query and get content as BOC
            collection_account = (await session.execute(select(AccountState)
                                                        .filter(AccountState.address == collection_address)
                                                        .order_by(AccountState.last_tx_lt.desc())
                                                        )).first()
            if not collection_account:
                raise Exception(f"Collection contract {collection_address} not found!")
            collection_account = collection_account[0]
            collection_code = (await session.execute(select(Code).filter(Code.hash == collection_account.code_hash))).first()[0]
            individual_content = (await self._execute(collection_code.code, collection_account.data, 'get_nft_content',
                                               ["metadata"],
                                               arguments=[int(index), individual_content_boc],
                                               address=collection_address))[0]
        else:
            _, _, _, _, individual_content = await self._execute(context.code.code, context.account.data, 'get_nft_data',
                                                           ["int", "int", "address", "address", "metadata"],
                                                           address=context.account.address)


        metadata_url, metadata = await self._parse_tep64content(individual_content)


        item = NFTItem(
            state_id=context.account.state_id,
            address=context.account.address,
            index=index,
            collection=collection_address,
            owner=owner_address,
            name=metadata.get('name', None),
            description=metadata.get('description', None),
            image=metadata.get('image', None),
            image_data=metadata.get('image_data', None),
            attributes=json.dumps(metadata.get('attributes')) if 'attributes' in metadata else None,
            telemint_royalty_address=royalty_destination
        )
        # cast from list to string
        if item.description and type(item.description) == list:
            item.description = " ".join(item.description)
        logger.info(f"Adding NFT item {item}")

        res = await upsert_entity(session, item, constraint="address")

        history_mint = await get_nft_history_mint(session, item.address)
        if not history_mint:
            try:
                nft = await get_nft(session, context.account.address)
                message = await get_nft_mint_message(session, item.address, item.collection)
                message_context = await get_messages_context(session, message.msg_id)

                nft_history = NftHistory(
                    msg_id=message.msg_id,
                    created_lt=message.created_lt,
                    utime=message_context.destination_tx.utime,
                    hash=await get_originated_msg_hash(session, message),
                    event_type=NftHistory.EVENT_TYPE_MINT,
                    nft_item_id=nft.id,
                    nft_item_address=nft.address,
                    collection_address=nft.collection
                )
                logger.info(f"Adding NFT history event {nft_history}")
                await upsert_entity(session, nft_history)

            except Exception:
                logger.warning(f"No NFT mint message found for item {context.account.address}")

        if res.rowcount > 0:
            logger.info(f"Discovered new NFT item {context.account.address}")
            return GeneratedEvent(event=Event(
                event_scope="NFT",
                event_target=collection_address,
                finding_type="Info",
                event_type="NewItem",
                severity="Medium",
                data={
                    "nft_item": context.account.address,
                    "collection": collection_address,
                    "name": metadata.get('name', None),
                    "image": metadata.get('image', None),
                    "image_data": metadata.get('image_data', None),
                    "metadata_url": metadata_url,
                    "owner": owner_address
                }
            ), waitCommit=False)

@dataclass
class SaleContract:
    name: str
    signature: list
    marketplace_pos: int
    nft_pos: int
    price_pos: int
    owner_pos: int
    is_auction: bool

class NFTItemSaleParser(ContractsExecutorParser):

    def __init__(self):
        try:
            from parser.nft_contracts import SALE_CONTRACTS
        except Exception as e:
            logger.error("Unable to init sale contracts", e)
            SALE_CONTRACTS = {}
        """
        Supported sale contract should be listed in file nft_contracts, format as follows:
        from parser.parsers_collection import SaleContract # import it to support SaleContract container
        
        SALE_CONTRACTS = {
            'contract_code_hash': SaleContract(
                name='contract_name',
                signature=['int', 'int', 'int', 'address', 'address', 'address', 'int', 'address', 'int', 'address', 'int'], # list of returned value types
                marketplace_pos=3, # position of marketplace address in the list of returned value types
                nft_pos=4, # position of nft address in the list of returned value types
                price_pos=6, # position of price in the list of returned value types     
                owner_pos=5 # position of owner address in the list of returned value types
            ),
            ...
        }
        """
        super(NFTItemSaleParser, self).__init__(
            predicate=ContractPredicate(supported=SALE_CONTRACTS.keys()))
        self.SALE_CONTRACTS = SALE_CONTRACTS

    @staticmethod
    def parser_name() -> str:
        return "NFTItemSaleParser"

    async def parse(self, session: Session, context: AccountContext):
        contract = self.SALE_CONTRACTS[context.code.hash]
        sale_data = await self._execute(context.code.code, context.account.data, 'get_sale_data',
                                  contract.signature,
                                  address=context.account.address)
        if sale_data is None:
            logger.error("get_sale_data returned null")
            return

        item = NFTItemSale(
            state_id=context.account.state_id,
            address=context.account.address,
            nft_item=sale_data[contract.nft_pos],
            marketplace=sale_data[contract.marketplace_pos],
            price=sale_data[contract.price_pos],
            owner=sale_data[contract.owner_pos],
            is_auction=contract.is_auction
        )
        logger.info(f"Adding NFT item sale {item}")

        await upsert_entity(session, item, constraint="address")

        if item.is_auction:
            nft_history = await get_nft_history_sale(session, item.address)
            if nft_history:
                nft_history.price = int(item.price)
                logger.info(f"Updating sale price in NFT history {nft_history}")


class TelemintStartAuctionParser(Parser):
    def __init__(self):
        super(TelemintStartAuctionParser, self).__init__(DestinationTxRequiredPredicate(OpCodePredicate(0x487a8e81)))

    @staticmethod
    def parser_name() -> str:
        return "TelemintStartAuction"

    async def parse(self, session: Session, context: MessageContext):
        cell = self._parse_boc(context.content.body)
        reader = BitReader(cell.data.data)
        op = reader.read_uint(32)
        query_id = reader.read_uint(64)

        try:
            auction_config = cell.refs.pop(0)
            ac_reader = BitReader(auction_config.data.data)
            beneficiary_address = ac_reader.read_address()
            initial_min_bid = ac_reader.read_coins()
            max_bid = ac_reader.read_coins()
            min_bid_step = ac_reader.read_uint(8)
            min_extend_time = ac_reader.read_uint(32)
            duration = ac_reader.read_uint(32)

        except Exception as e:
            logger.error(f"Unable to parse auction config {e}, skipped")
            return

        event = TelemintStartAuction(
            msg_id = context.message.msg_id,
            utime = context.destination_tx.utime,
            created_lt=context.message.created_lt,
            originated_msg_hash = await get_originated_msg_hash(session, context.message),
            query_id = str(query_id),
            source = context.message.source,
            destination = context.message.destination,
            beneficiary_address = beneficiary_address,
            initial_min_bid = initial_min_bid,
            max_bid = max_bid,
            min_bid_step = min_bid_step,
            min_extend_time = min_extend_time,
            duration = duration
        )   
        logger.info(f"Adding telemint start auction event {event}")
        await upsert_entity(session, event)

        nft = await get_nft(session, event.destination)
        if not nft:
            raise Exception(f"NFT not inited yet {event.destination}")

        nft_history = NftHistory(
            msg_id=event.msg_id,
            created_lt=event.created_lt,
            utime=event.utime,
            hash=event.originated_msg_hash,
            event_type=NftHistory.EVENT_TYPE_INIT_SALE,
            nft_item_id=nft.id,
            nft_item_address=event.destination,
            collection_address=nft.collection,
            marketplace=nft.telemint_royalty_address,
            current_owner=event.source,
            price=0,
            is_auction=True
        )
        logger.info(f"Adding NFT history event {nft_history}")
        await upsert_entity(session, nft_history)


class TelemintCancelAuctionParser(Parser):
    def __init__(self):
        super(TelemintCancelAuctionParser, self).__init__(DestinationTxRequiredPredicate(OpCodePredicate(0x371638ae)))

    @staticmethod
    def parser_name() -> str:
        return "TelemintCancelAuction"

    async def parse(self, session: Session, context: MessageContext):
        cell = self._parse_boc(context.content.body)
        reader = BitReader(cell.data.data)
        op = reader.read_uint(32)
        query_id = reader.read_uint(64)

        event = TelemintCancelAuction(
            msg_id = context.message.msg_id,
            utime = context.destination_tx.utime,
            created_lt=context.message.created_lt,
            originated_msg_hash = await get_originated_msg_hash(session, context.message),
            query_id = str(query_id),
            source = context.message.source,
            destination = context.message.destination
        )
        logger.info(f"Adding telemint cancel auction event {event}")
        await upsert_entity(session, event)

        nft = await get_nft(session, event.destination)
        if not nft:
            raise Exception(f"NFT not inited yet {event.destination}")

        nft_history = NftHistory(
            msg_id=event.msg_id,
            created_lt=event.created_lt,
            utime=event.utime,
            hash=event.originated_msg_hash,
            event_type=NftHistory.EVENT_TYPE_CANCEL_SALE,
            nft_item_id=nft.id,
            nft_item_address=event.destination,
            collection_address=nft.collection,
            marketplace=nft.telemint_royalty_address,
            current_owner=event.source,
            price=0,
            is_auction=True
        )
        logger.info(f"Adding NFT history event {nft_history}")
        await upsert_entity(session, nft_history)

class TelemintOwnershipAssignedParser(Parser):
    def __init__(self):
        super(TelemintOwnershipAssignedParser, self).__init__(DestinationTxRequiredPredicate(OpCodePredicate(0x05138d91)))

    @staticmethod
    def parser_name() -> str:
        return "TelemintOwnershipAssigned"

    async def parse(self, session: Session, context: MessageContext):
        cell = self._parse_boc(context.content.body)
        reader = BitReader(cell.data.data)
        op = reader.read_uint(32)

        try:
            query_id = reader.read_uint(64)
            prev_owner = reader.read_address()
            is_right = reader.read_uint(1)
            sub_op = reader.read_uint(32)

            if sub_op == 0x38127de1:  # op::teleitem_bid_info
                bid = reader.read_coins()
                bid_ts = reader.read_uint(32)

                event = TelemintOwnershipAssigned(
                    msg_id = context.message.msg_id,
                    utime = context.destination_tx.utime,
                    created_lt=context.message.created_lt,
                    originated_msg_hash = await get_originated_msg_hash(session, context.message),
                    query_id = str(query_id),
                    source = context.message.source,
                    destination = context.message.destination,
                    prev_owner = prev_owner,
                    bid = bid,
                    bid_ts = bid_ts
                )
                logger.info(f"Adding telemint ownership assigned event {event}")
                await upsert_entity(session, event)

                if prev_owner:
                    nft = await get_nft(session, event.source)
                    if not nft:
                        raise Exception(f"NFT not inited yet {event.source}")

                    nft_history = NftHistory(
                        msg_id=event.msg_id,
                        created_lt=event.created_lt,
                        utime=event.utime,
                        hash=event.originated_msg_hash,
                        event_type=NftHistory.EVENT_TYPE_SALE,
                        nft_item_id=nft.id,
                        nft_item_address=event.source,
                        collection_address=nft.collection,
                        marketplace=nft.telemint_royalty_address,
                        current_owner=event.prev_owner,
                        new_owner=event.destination,
                        price=event.bid,
                        is_auction=True
                    )
                    logger.info(f"Adding NFT history event {nft_history}")
                    await upsert_entity(session, nft_history)

                return

        except Exception:
            pass

        logger.warning(f"Unable to parse forward payload for telemint ownership assigned event (msg_id = {context.message.msg_id}), skipped")

class DedustV2SwapExtOutParser(Parser):
    def __init__(self):
        super(DedustV2SwapExtOutParser, self).__init__(SourceTxRequiredPredicate(OpCodePredicate(0x9c610de3)))

    @staticmethod
    def parser_name() -> str:
        return "DedustV2SwapExtOut"

    async def parse(self, session: Session, context: MessageContext):
        logger.info(f"Parsing dedust swap ext_out message {context.message.msg_id}")
        if context.message.source == 'EQA0a6c40n_Kejx_Wj0vowdeYCFYG9XnLdLMRHihXc27cng5' or context.message.source == 'EQDpuDAY31FH2jM9PysFsmJ3aXMMReGYb_P65aDOXVYDcCJX':
            logger.warning(f"Skipping non-stable pool {context.message.source}")
            return
        cell = self._parse_boc(context.content.body)
        reader = BitReader(cell.data.data)
        op_id = reader.read_uint(32) # swap#9c610de3
        asset_in = reader.read_dedust_asset()
        asset_out = reader.read_dedust_asset()
        amount_in = reader.read_coins()
        amount_out = reader.read_coins()

        payload_reader = BitReader(cell.refs.pop(0).data.data)
        sender_addr = payload_reader.read_address()
        referral_addr = payload_reader.read_address()

        swap = DexSwapParsed(
            msg_id=context.message.msg_id,
            originated_msg_id=await get_originated_msg_id(session, context.message),
            platform="dedust",
            swap_utime=context.source_tx.utime,
            swap_user=sender_addr,
            swap_pool=context.message.source,
            swap_src_token=asset_in,
            swap_dst_token=asset_out,
            swap_src_amount=amount_in,
            swap_dst_amount=amount_out,
            referral_address=referral_addr
        )
        logger.info(f"Adding dedust swap {swap}")
        await upsert_entity(session, swap)

        return GeneratedEvent(event=Event(
            event_scope="DEX",
            event_target="dedust",
            finding_type="Info",
            event_type="Swap",
            severity="Medium",
            data={
                "pool": context.message.source,
                "asset_in": asset_in,
                "asset_out": asset_out,
                "amount_in": str(amount_in),
                "amount_out": str(amount_out),
                "swap_user": sender_addr,
                "db_ref": context.message.msg_id
            }
        ), waitCommit=True)


class HugeTonTransfersParser(Parser):
    class MessageValuePredicate(ParserPredicate):
        def __init__(self, min_value):
            super(HugeTonTransfersParser.MessageValuePredicate, self).__init__(MessageContext)
            self.min_value = min_value

        def _internal_match(self, context: MessageContext):
            return context.message.value > self.min_value

    def __init__(self):
        # 1000 TON
        super(HugeTonTransfersParser, self).__init__(HugeTonTransfersParser.MessageValuePredicate(min_value=1000 * 1000000000))

    @staticmethod
    def parser_name() -> str:
        return "HugeTonTransfersParser"

    async def parse(self, session: Session, context: MessageContext):
        logger.info(f"Detected new significant TON transfer {context.message.msg_id}")

        return GeneratedEvent(event=Event(
            event_scope="TON",
            event_target=context.message.destination,
            finding_type="Info",
            event_type="Transfer",
            severity="Medium",
            data={
                "amount": int(context.message.value / 1000000000),
                "source": context.message.source,
                "destination": context.message.destination,
                "msg_hash": context.message.hash
            }
        ))

class HasTextCommentParser(Parser):
    class HasTextCommentPredicate(ParserPredicate):
        def __init__(self):
            super(HasTextCommentParser.HasTextCommentPredicate, self).__init__(MessageContext)

        def _internal_match(self, context: MessageContext):
            return context.message.comment is not None and len(context.message.comment.strip()) > 0

    def __init__(self):
        super(HasTextCommentParser, self).__init__(HasTextCommentParser.HasTextCommentPredicate())

    @staticmethod
    def parser_name() -> str:
        return "HasTextCommentParser"

    async def parse(self, session: Session, context: MessageContext):
        logger.info(f"Detected new TON transfer with comment {context.message.msg_id}")

        return GeneratedEvent(event=Event(
            event_scope="TON",
            event_target=context.message.destination,
            finding_type="Info",
            event_type="Comment",
            severity="Medium",
            data={
                "amount": context.message.value / 1000000000,
                "source": context.message.source,
                "destination": context.message.destination,
                "msg_hash": context.message.hash,
                "comment": context.message.comment
            }
        ))


class EvaaRouterPredicate(ParserPredicate):
    def __init__(self, delegate: ParserPredicate, direction = "destination"):
        super(EvaaRouterPredicate, self).__init__(MessageContext)
        assert direction in ["destination", "source"]
        self.direction = direction
        self.delegate = delegate

    def _internal_match(self, context: MessageContext):
        if self.direction == "destination" and context.message.destination == 'EQC8rUZqR_pWV1BylWUlPNBzyiTYVoBEmQkMIQDZXICfnuRr':
            return self.delegate.match(context)
        if self.direction == "source" and context.message.source == 'EQC8rUZqR_pWV1BylWUlPNBzyiTYVoBEmQkMIQDZXICfnuRr':
            return self.delegate.match(context)
        return False

def evaa_asset_to_str(asset_id: int):
    addr = b'\x11\x00' + asset_id.to_bytes(32, "big")
    return codecs.decode(codecs.encode(addr + BitReader.calc_crc(addr), "base64"), "utf-8").strip() \
        .replace('/', '_').replace("+", '-')

class EvaaSupplyParser(Parser):
    def __init__(self):
        super(EvaaSupplyParser, self).__init__(EvaaRouterPredicate(DestinationTxRequiredPredicate(OpCodePredicate(0x11a))))

    @staticmethod
    def parser_name() -> str:
        return "EvaaSupply"

    async def parse(self, session: Session, context: MessageContext):
        logger.info(f"Parsing EVAA supply message {context.message.msg_id}")
        cell = self._parse_boc(context.content.body)
        reader = BitReader(cell.data.data)
        reader.read_uint(32) # 0x11a
        query_id = reader.read_uint(64)
        owner_address = reader.read_address()
        asset_id = evaa_asset_to_str(reader.read_uint(256))
        amount_supplied = reader.read_uint(64)
        repay_amount_principal = reader.read_int(64)
        supply_amount_principal = reader.read_int(64)

        successful = context.destination_tx.action_result_code == 0 and context.destination_tx.compute_exit_code == 0

        supply = EvaaSupply(
            msg_id=context.message.msg_id,
            created_lt=context.message.created_lt,
            utime=context.destination_tx.utime,
            successful=successful,
            originated_msg_id=await get_originated_msg_id(session, context.message),
            query_id=str(query_id),
            amount=amount_supplied,
            asset_id=asset_id,
            owner_address=owner_address,
            repay_amount_principal=repay_amount_principal,
            supply_amount_principal=supply_amount_principal
        )
        logger.info(f"Adding EVAA supply {supply}")
        await upsert_entity(session, supply)

class EvaaWithdrawCollateralizedParser(Parser):
    def __init__(self):
        super(EvaaWithdrawCollateralizedParser, self).__init__(EvaaRouterPredicate(DestinationTxRequiredPredicate(OpCodePredicate(0x211))))

    @staticmethod
    def parser_name() -> str:
        return "EvaaWithdrawCollateralized"

    async def parse(self, session: Session, context: MessageContext):
        logger.info(f"Parsing EVAA withdraw_collateralized message {context.message.msg_id}")
        cell = self._parse_boc(context.content.body)
        reader = BitReader(cell.data.data)
        reader.read_uint(32) # 0x211
        query_id = reader.read_uint(64)
        owner_address = reader.read_address()
        asset_id = evaa_asset_to_str(reader.read_uint(256))
        withdraw_amount_current = reader.read_uint(64)
        borrow_amount_principal = reader.read_int(64)
        reclaim_amount_principal = reader.read_int(64)

        successful = context.destination_tx.action_result_code == 0 and context.destination_tx.compute_exit_code == 0

        existing = await get_evaa_withdraw(session, msg_id=context.message.msg_id)
        if not existing:
            withdraw = EvaaWithdraw(
                msg_id=context.message.msg_id,
                created_lt=context.message.created_lt,
                utime=context.destination_tx.utime,
                successful=successful,
                originated_msg_id=await get_originated_msg_id(session, context.message),
                query_id=str(query_id),
                amount=withdraw_amount_current,
                asset_id=asset_id,
                owner_address=owner_address,
                borrow_amount_principal=borrow_amount_principal,
                reclaim_amount_principal=reclaim_amount_principal,
                approved=None
            )
            logger.info(f"Adding EVAA withdraw {withdraw}")
            await upsert_entity(session, withdraw)

class EvaaWithdrawSuccessParser(Parser):
    def __init__(self):
        super(EvaaWithdrawSuccessParser, self).__init__(EvaaRouterPredicate(DestinationTxRequiredPredicate(AlwaysTrue(context_class=MessageContext)),
                                                                            direction="source"))

    @staticmethod
    def parser_name() -> str:
        return "EvaaWithdrawSuccess"

    async def parse(self, session: Session, context: MessageContext):
        cell = self._parse_boc(context.content.body)
        reader = BitReader(cell.data.data)
        try:
            reader.read_coins() # version
            reader.read_uint(3) # flags
            op = reader.read_uint(32)
            query_id = reader.read_uint(64)
        except:
            logger.info("Not an EVAA message")
            return
        if op != 0x211a:
            logger.info(f"Skipping message with opcode {op}")
            return
        logger.info(f"Parsing possible EVAA withdraw_success message {context.message.msg_id}")
        collaterized_msg_id = await get_prev_msg_id(session, context.message)
        logger.info(f"Discovered collateralized msg_id for {context.message.msg_id}: {collaterized_msg_id}")
        if collaterized_msg_id and context.destination_tx.action_result_code == 0 and context.destination_tx.compute_exit_code == 0:
            existing = await get_evaa_withdraw(session, msg_id=collaterized_msg_id)
            if not existing:
                raise Exception("Unable to find existing withdraw_collateralized, may be it was not parsed yet")

            logger.info("Approving withdraw")
            await update_evaa_withdraw_approved(session, withdraw=existing, approved=True)

class EvaaWithdrawFailParser(Parser):
    def __init__(self):
        super(EvaaWithdrawFailParser, self).__init__(EvaaRouterPredicate(DestinationTxRequiredPredicate(AlwaysTrue(context_class=MessageContext)),
                                                                            direction="source"))

    @staticmethod
    def parser_name() -> str:
        return "EvaaWithdrawFail"

    async def parse(self, session: Session, context: MessageContext):
        cell = self._parse_boc(context.content.body)
        reader = BitReader(cell.data.data)
        try:
            reader.read_coins() # version
            reader.read_uint(3) # flags
            op = reader.read_uint(32)
            query_id = reader.read_uint(64)
        except:
            logger.info("Not an EVAA message")
            return
        if op != 0x211f:
            logger.info(f"Skipping message with opcode {op}")
            return
        logger.info(f"Parsing possible EVAA withdraw_fail message {context.message.msg_id}")
        collaterized_msg_id = await get_prev_msg_id(session, context.message)
        logger.info(f"Discovered collateralized msg_id for {context.message.msg_id}: {collaterized_msg_id}")
        if collaterized_msg_id and context.destination_tx.action_result_code == 0 and context.destination_tx.compute_exit_code == 0:
            existing = await get_evaa_withdraw(session, msg_id=collaterized_msg_id)
            if not existing:
                raise Exception("Unable to find existing withdraw_collateralized, may be it was not parsed yet")

            logger.info("Rejecting withdraw")
            await update_evaa_withdraw_approved(session, withdraw=existing, approved=False)
            
class EvaaLiquidationSatisfiedParser(Parser):
    def __init__(self):
        super(EvaaLiquidationSatisfiedParser, self).__init__(EvaaRouterPredicate(DestinationTxRequiredPredicate(OpCodePredicate(0x311))))

    @staticmethod
    def parser_name() -> str:
        return "EvaaLiquidationSatisfied"

    async def parse(self, session: Session, context: MessageContext):
        logger.info(f"Parsing EVAA liquidate_satisfied message {context.message.msg_id}")
        cell = self._parse_boc(context.content.body)
        reader = BitReader(cell.data.data)
        reader.read_uint(32) # 0x311
        query_id = reader.read_uint(64)
        owner_address = reader.read_address()
        liquidator_address = reader.read_address()
        transferred_asset_id = evaa_asset_to_str(reader.read_uint(256))
        ref = cell.refs.pop(0)
        reader = BitReader(ref.data.data)
        delta_loan_principal = reader.read_int(64)
        liquidatable_amount = reader.read_uint(64)
        protocol_gift = reader.read_uint(64)
        collateral_asset_id = evaa_asset_to_str(reader.read_uint(256))
        delta_collateral_principal = reader.read_int(64)
        collateral_reward = reader.read_uint(64)
        min_collateral_amount = reader.read_uint(64)

        successful = context.destination_tx.action_result_code == 0 and context.destination_tx.compute_exit_code == 0

        existing = await get_evaa_liquidation(session, msg_id=context.message.msg_id)
        if not existing:
            liqudation = EvaaLiquidation(
                msg_id=context.message.msg_id,
                created_lt=context.message.created_lt,
                utime=context.destination_tx.utime,
                successful=successful,
                originated_msg_id=await get_originated_msg_id(session, context.message),
                query_id=str(query_id),
                amount=liquidatable_amount,
                protocol_gift=protocol_gift,
                collateral_reward=collateral_reward,
                min_collateral_amount=min_collateral_amount,
                transferred_asset_id=transferred_asset_id,
                collateral_asset_id=collateral_asset_id,
                owner_address=owner_address,
                liquidator_address=liquidator_address,
                delta_loan_principal=delta_loan_principal,
                delta_collateral_principal=delta_collateral_principal,
                approved=None
            )
            logger.info(f"Adding EVAA liquidation {liqudation}")
            await upsert_entity(session, liqudation)

class EvaaLiquidationSuccessParser(Parser):
    def __init__(self):
        super(EvaaLiquidationSuccessParser, self).__init__(EvaaRouterPredicate(DestinationTxRequiredPredicate(AlwaysTrue(context_class=MessageContext)),
                                                                            direction="source"))

    @staticmethod
    def parser_name() -> str:
        return "EvaaLiquidationSuccess"

    async def parse(self, session: Session, context: MessageContext):
        cell = self._parse_boc(context.content.body)
        reader = BitReader(cell.data.data)
        try:
            reader.read_coins() # version
            reader.read_uint(3) # flags
            op = reader.read_uint(32)
            query_id = reader.read_uint(64)
        except:
            logger.info("Not an EVAA message")
            return
        if op != 0x311a:
            logger.info(f"Skipping message with opcode {op}")
            return
        logger.info(f"Parsing possible EVAA liquidation_success message {context.message.msg_id}")
        satisfied_msg_id = await get_prev_msg_id(session, context.message)
        logger.info(f"Discovered liquidation_satisfied msg_id for {context.message.msg_id}: {satisfied_msg_id}")
        if satisfied_msg_id and context.destination_tx.action_result_code == 0 and context.destination_tx.compute_exit_code == 0:
            existing = await get_evaa_liquidation(session, msg_id=satisfied_msg_id)
            if not existing:
                raise Exception("Unable to find existing liquidation_satisfied, may be it was not parsed yet")

            logger.info("Approving liquidation")
            await update_evaa_liquidation_approved(session, liquidation=existing, approved=True)

class EvaaLiquidationFailParser(Parser):
    def __init__(self):
        super(EvaaLiquidationFailParser, self).__init__(EvaaRouterPredicate(DestinationTxRequiredPredicate(AlwaysTrue(context_class=MessageContext)),
                                                                            direction="source"))

    @staticmethod
    def parser_name() -> str:
        return "EvaaLiquidationFail"

    async def parse(self, session: Session, context: MessageContext):
        cell = self._parse_boc(context.content.body)
        reader = BitReader(cell.data.data)
        try:
            reader.read_coins() # version
            reader.read_uint(3) # flags
            op = reader.read_uint(32)
            query_id = reader.read_uint(64)
        except:
            logger.info("Not an EVAA message")
            return
        if op != 0x311f:
            logger.info(f"Skipping message with opcode {op}")
            return
        logger.info(f"Parsing possible EVAA liquidation_fail message {context.message.msg_id}")
        satisfied_msg_id = await get_prev_msg_id(session, context.message)
        logger.info(f"Discovered liquidation_satisfied msg_id for {context.message.msg_id}: {satisfied_msg_id}")
        if satisfied_msg_id and context.destination_tx.action_result_code == 0 and context.destination_tx.compute_exit_code == 0:
            existing = await get_evaa_liquidation(session, msg_id=satisfied_msg_id)
            if not existing:
                raise Exception("Unable to find existing liquidation_satisfied, may be it was not parsed yet")

            logger.info("Rejecting liquidation")
            await update_evaa_liquidation_approved(session, liquidation=existing, approved=False)

## Forwards all messages to kafka
class MessagesToKafka(Parser):
    class SuccessfulPredicate(ParserPredicate):
        def __init__(self):
            super(MessagesToKafka.SuccessfulPredicate, self).__init__(MessageContext)
            self.enabled = settings.eventbus.messages.enabled


        def _internal_match(self, context: MessageContext):
            if not self.enabled or context.message.destination == '': # log message
                return False
            if context.destination_tx is None:
                logger.warning(f"destination_tx is not found for message {context.message.msg_id}")
                raise Exception(f"No destination_tx for {context.message.msg_id}")
            return context.destination_tx.action_result_code == 0 and context.destination_tx.compute_exit_code == 0

    def __init__(self):
        super(MessagesToKafka, self).__init__(MessagesToKafka.SuccessfulPredicate())
        self.producer = None
        self.topic = settings.eventbus.messages.topic

    @staticmethod
    def parser_name() -> str:
        return "MessagesToKafka"

    async def parse(self, session: Session, context: MessageContext):
        if not self.producer:
            self.producer = AIOKafkaProducer(bootstrap_servers=settings.eventbus.kafka.broker)
            await self.producer.start()
        msg = {
            'msg_id': context.message.msg_id,
            'source': context.message.source,
            'destination': context.message.destination,
            'value': context.message.value,
            'op': context.message.op,
            'hash': context.message.hash,
            'created_lt': context.message.created_lt,
            'fwd_fee': context.message.fwd_fee,
            'ihr_fee': context.message.ihr_fee,
            'import_fee': context.message.import_fee,
            'utime': context.destination_tx.utime,
            'content': context.content.body
        }
        # logger.info(json.dumps(msg))
        await self.producer.send_and_wait(self.topic, json.dumps(msg).encode("utf-8"))


### Storm Trade

class StormExecuteOrderParser(Parser):
    def __init__(self):
        super(StormExecuteOrderParser, self).__init__(DestinationTxRequiredPredicate(OpCodePredicate(0xde1ddbcc)))

    @staticmethod
    def parser_name() -> str:
        return "StormExecuteOrder"

    async def parse(self, session: Session, context: MessageContext):
        logger.info(f"Parsing storm execute_order message {context.message.msg_id}")
        cell = self._parse_boc(context.content.body)
        reader = BitReader(cell.data.data)
        reader.read_uint(32) # 0xde1ddbcc

        direction = reader.read_uint(1)
        order_index = reader.read_uint(3)
        trader_addr = reader.read_address()
        prev_addr = reader.read_address()
        ref_addr = reader.read_address()
        executor_index = reader.read_uint(32)

        order = cell.refs.pop(0)
        position = cell.refs.pop(0)
        oracle_payload = cell.refs.pop(0)

        reader = BitReader(order.data.data)
        order_type = reader.read_uint(4)

        if order_type == 0 or order_type == 1:
            ## stop_loss_order$0000 / take_profit_order$0001 expiration:uint32 direction:Direction amount:Coins triger_price:Coins = SLTPOrder
            expiration = reader.read_uint(32)
            direction_order = reader.read_uint(1)
            amount = reader.read_coins()
            triger_price = reader.read_coins()

            leverage, limit_price, stop_price, stop_triger_price, take_triger_price = None, None, None, None, None
        elif order_type == 2 or order_type == 3:
            """
            stop_limit_order$0010 / market_order$0011 expiration:uint32 direction:Direction
            amount:Coins leverage:uint64
            limit_price:Coins stop_price:Coins
            stop_triger_price:Coins take_triger_price:Coins = LimitMarketOrder
            """
            expiration = reader.read_uint(32)
            direction_order = reader.read_uint(1)
            amount = reader.read_coins()
            leverage = reader.read_uint(64)
            limit_price = reader.read_coins()
            stop_price = reader.read_coins()
            stop_triger_price = reader.read_coins()
            take_triger_price = reader.read_coins()

            triger_price = None
        else:
            raise Exception("order_type %s is not supported" % order_type)

        reader = BitReader(position.data.data)
        """
          position#_ size:int128 direction:Direction 
          margin:Coins open_notional:Coins 
          last_updated_cumulative_premium:int64
          fee:uint32 discount:uint32 rebate:uint32 
          last_updated_timestamp:uint32 = PositionData
        """
        position_size = reader.read_int(128)
        direction_position = reader.read_uint(1)
        margin = reader.read_coins()
        open_notional = reader.read_coins()
        last_updated_cumulative_premium = reader.read_int(64)
        fee = reader.read_uint(32)
        discount = reader.read_uint(32)
        rebate = reader.read_uint(32)
        last_updated_timestamp = reader.read_uint(32)

        reader = BitReader(oracle_payload.refs.pop(0).data.data)
        """
        price_data#_ price:Coins spread:Coins timestamp:uint32 asset_id:uint16 = OraclePriceData
        """
        oracle_price = reader.read_coins()
        spread = reader.read_coins()
        oracle_timestamp = reader.read_uint(32)
        asset_id = reader.read_uint(16)

        successful = context.destination_tx.action_result_code == 0 and context.destination_tx.compute_exit_code == 0

        execute_order = StormExecuteOrder(
            msg_id=context.message.msg_id,
            created_lt=context.message.created_lt,
            utime=context.destination_tx.utime,
            successful=successful,
            originated_msg_id=await get_originated_msg_id(session, context.message),
            direction=direction,
            order_index=order_index,
            trader_addr=trader_addr,
            prev_addr=prev_addr,
            ref_addr=ref_addr,
            executor_index=executor_index,
            order_type=order_type,
            expiration=expiration,
            direction_order=direction_order,
            amount=amount,
            triger_price=triger_price,
            leverage=leverage,
            limit_price=limit_price,
            stop_price=stop_price,
            stop_triger_price=stop_triger_price,
            take_triger_price=take_triger_price,
            position_size=position_size,
            direction_position=direction_position,
            margin=margin,
            open_notional=open_notional,
            last_updated_cumulative_premium=last_updated_cumulative_premium,
            fee=fee,
            discount=discount,
            rebate=rebate,
            last_updated_timestamp=last_updated_timestamp,
            oracle_price=oracle_price,
            spread=spread,
            oracle_timestamp=oracle_timestamp,
            asset_id=asset_id
        )
        logger.info(f"Adding Storm execute_order {execute_order}")
        await upsert_entity(session, execute_order)

class StormCompleteOrderParser(Parser):
    def __init__(self):
        super(StormCompleteOrderParser, self).__init__(DestinationTxRequiredPredicate(OpCodePredicate(0xcf90d618)))

    @staticmethod
    def parser_name() -> str:
        return "StormCompleteOrder"

    async def parse(self, session: Session, context: MessageContext):
        logger.info(f"Parsing storm complete_order message {context.message.msg_id}")
        cell = self._parse_boc(context.content.body)
        reader = BitReader(cell.data.data)
        reader.read_uint(32) # 0xcf90d618

        order_type = reader.read_uint(4)
        order_index = reader.read_uint(3)
        direction = reader.read_uint(1)
        origin_op = reader.read_uint(32)
        oracle_price = reader.read_coins()

        position = cell.refs.pop(0)
        reader = BitReader(position.data.data)
        """
          position#_ size:int128 direction:Direction 
          margin:Coins open_notional:Coins 
          last_updated_cumulative_premium:int64
          fee:uint32 discount:uint32 rebate:uint32 
          last_updated_timestamp:uint32 = PositionData
        """
        position_size = reader.read_int(128)
        direction_position = reader.read_uint(1)
        margin = reader.read_coins()
        open_notional = reader.read_coins()
        last_updated_cumulative_premium = reader.read_int(64)
        fee = reader.read_uint(32)
        discount = reader.read_uint(32)
        rebate = reader.read_uint(32)
        last_updated_timestamp = reader.read_uint(32)

        amm_state = cell.refs.pop(0)
        reader = BitReader(amm_state.data.data)
        quote_asset_reserve = reader.read_coins()
        quote_asset_weight = reader.read_coins()
        base_asset_reserve = reader.read_coins()

        successful = context.destination_tx.action_result_code == 0 and context.destination_tx.compute_exit_code == 0

        complete_order = StormCompleteOrder(
            msg_id=context.message.msg_id,
            created_lt=context.message.created_lt,
            utime=context.destination_tx.utime,
            successful=successful,
            originated_msg_id=await get_originated_msg_id(session, context.message),
            order_type=order_type,
            order_index=order_index,
            direction=direction,
            origin_op=origin_op,
            oracle_price=oracle_price,
            position_size=position_size,
            direction_position=direction_position,
            margin=margin,
            open_notional=open_notional,
            last_updated_cumulative_premium=last_updated_cumulative_premium,
            fee=fee,
            discount=discount,
            rebate=rebate,
            last_updated_timestamp=last_updated_timestamp,
            quote_asset_reserve=quote_asset_reserve,
            quote_asset_weight=quote_asset_weight,
            base_asset_reserve=base_asset_reserve
        )
        logger.info(f"Adding Storm complete_order {complete_order}")
        await upsert_entity(session, complete_order)

class StormUpdatePositionParser(Parser):
    def __init__(self):
        super(StormUpdatePositionParser, self).__init__(DestinationTxRequiredPredicate(OpCodePredicate(0x60dfc677)))

    @staticmethod
    def parser_name() -> str:
        return "StormUpdatePosition"

    async def parse(self, session: Session, context: MessageContext):
        logger.info(f"Parsing storm update_position message {context.message.msg_id}")
        cell = self._parse_boc(context.content.body)
        reader = BitReader(cell.data.data)
        reader.read_uint(32) # 0x60dfc677

        direction = reader.read_uint(1)
        origin_op = reader.read_uint(32)
        oracle_price = reader.read_coins()

        position = cell.refs.pop(0)
        reader = BitReader(position.data.data)
        """
          position#_ size:int128 direction:Direction 
          margin:Coins open_notional:Coins 
          last_updated_cumulative_premium:int64
          fee:uint32 discount:uint32 rebate:uint32 
          last_updated_timestamp:uint32 = PositionData
        """
        position_size = reader.read_int(128)
        direction_position = reader.read_uint(1)
        margin = reader.read_coins()
        open_notional = reader.read_coins()
        last_updated_cumulative_premium = reader.read_int(64)
        fee = reader.read_uint(32)
        discount = reader.read_uint(32)
        rebate = reader.read_uint(32)
        last_updated_timestamp = reader.read_uint(32)

        amm_state = cell.refs.pop(0)
        reader = BitReader(amm_state.data.data)
        quote_asset_reserve = reader.read_coins()
        quote_asset_weight = reader.read_coins()
        base_asset_reserve = reader.read_coins()

        successful = context.destination_tx.action_result_code == 0 and context.destination_tx.compute_exit_code == 0

        update_position = StormUpdatePosition(
            msg_id=context.message.msg_id,
            created_lt=context.message.created_lt,
            utime=context.destination_tx.utime,
            successful=successful,
            originated_msg_id=await get_originated_msg_id(session, context.message),
            direction=direction,
            origin_op=origin_op,
            oracle_price=oracle_price,
            stop_trigger_price=None,
            take_trigger_price=None,
            position_size=position_size,
            direction_position=direction_position,
            margin=margin,
            open_notional=open_notional,
            last_updated_cumulative_premium=last_updated_cumulative_premium,
            fee=fee,
            discount=discount,
            rebate=rebate,
            last_updated_timestamp=last_updated_timestamp,
            quote_asset_reserve=quote_asset_reserve,
            quote_asset_weight=quote_asset_weight,
            base_asset_reserve=base_asset_reserve
        )
        logger.info(f"Adding Storm update_position {update_position}")
        await upsert_entity(session, update_position)

class StormUpdateStopLossPositionParser(Parser):
    def __init__(self):
        super(StormUpdateStopLossPositionParser, self).__init__(DestinationTxRequiredPredicate(OpCodePredicate(0x5d1b17b8)))

    @staticmethod
    def parser_name() -> str:
        return "StormUpdatePositionStopLoss"

    async def parse(self, session: Session, context: MessageContext):
        logger.info(f"Parsing storm update_position_stop_loss message {context.message.msg_id}")
        cell = self._parse_boc(context.content.body)
        reader = BitReader(cell.data.data)
        reader.read_uint(32) # 0x5d1b17b8

        direction = reader.read_uint(1)
        stop_trigger_price = reader.read_coins()
        take_trigger_price = reader.read_coins()
        origin_op = reader.read_uint(32)
        oracle_price = reader.read_coins()

        position = cell.refs.pop(0)
        reader = BitReader(position.data.data)
        """
          position#_ size:int128 direction:Direction 
          margin:Coins open_notional:Coins 
          last_updated_cumulative_premium:int64
          fee:uint32 discount:uint32 rebate:uint32 
          last_updated_timestamp:uint32 = PositionData
        """
        position_size = reader.read_int(128)
        direction_position = reader.read_uint(1)
        margin = reader.read_coins()
        open_notional = reader.read_coins()
        last_updated_cumulative_premium = reader.read_int(64)
        fee = reader.read_uint(32)
        discount = reader.read_uint(32)
        rebate = reader.read_uint(32)
        last_updated_timestamp = reader.read_uint(32)

        amm_state = cell.refs.pop(0)
        reader = BitReader(amm_state.data.data)
        quote_asset_reserve = reader.read_coins()
        quote_asset_weight = reader.read_coins()
        base_asset_reserve = reader.read_coins()

        successful = context.destination_tx.action_result_code == 0 and context.destination_tx.compute_exit_code == 0

        update_position = StormUpdatePosition(
            msg_id=context.message.msg_id,
            created_lt=context.message.created_lt,
            utime=context.destination_tx.utime,
            successful=successful,
            originated_msg_id=await get_originated_msg_id(session, context.message),
            direction=direction,
            origin_op=origin_op,
            oracle_price=oracle_price,
            stop_trigger_price=stop_trigger_price,
            take_trigger_price=take_trigger_price,
            position_size=position_size,
            direction_position=direction_position,
            margin=margin,
            open_notional=open_notional,
            last_updated_cumulative_premium=last_updated_cumulative_premium,
            fee=fee,
            discount=discount,
            rebate=rebate,
            last_updated_timestamp=last_updated_timestamp,
            quote_asset_reserve=quote_asset_reserve,
            quote_asset_weight=quote_asset_weight,
            base_asset_reserve=base_asset_reserve
        )
        logger.info(f"Adding Storm update_position {update_position}")
        await upsert_entity(session, update_position)

class StormTradeNotificationParser(Parser):
    def __init__(self):
        super(StormTradeNotificationParser, self).__init__(DestinationTxRequiredPredicate(OpCodePredicate(0x3475fdd2)))

    @staticmethod
    def parser_name() -> str:
        return "StormTradeNotification"

    async def parse(self, session: Session, context: MessageContext):
        logger.info(f"Parsing storm trade_notification message {context.message.msg_id}")
        cell = self._parse_boc(context.content.body)
        reader = BitReader(cell.data.data)
        reader.read_uint(32) # 0x3475fdd2

        asset_id = reader.read_uint(16)
        free_amount = reader.read_int(64)
        locked_amount = reader.read_int(64)
        exchange_amount = reader.read_int(64)
        withdraw_locked_amount = reader.read_uint(64)
        fee_to_stakers = reader.read_uint(64)
        withdraw_amount = reader.read_uint(64)
        trader_addr = reader.read_address()
        origin_addr = reader.read_address()

        referral_amount, referral_addr = None, None
        if reader.read_uint(1):
            referral = cell.refs.pop(0)
            reader = BitReader(referral.data.data)
            referral_amount = reader.read_coins()
            referral_addr = reader.read_address()

        successful = context.destination_tx.action_result_code == 0 and context.destination_tx.compute_exit_code == 0

        trade_notification = StormTradeNotification(
            msg_id=context.message.msg_id,
            created_lt=context.message.created_lt,
            utime=context.destination_tx.utime,
            successful=successful,
            originated_msg_id=await get_originated_msg_id(session, context.message),
            asset_id=asset_id,
            free_amount=free_amount,
            locked_amount=locked_amount,
            exchange_amount=exchange_amount,
            withdraw_locked_amount=withdraw_locked_amount,
            fee_to_stakers=fee_to_stakers,
            withdraw_amount=withdraw_amount,
            trader_addr=trader_addr,
            origin_addr=origin_addr,
            referral_amount=referral_amount,
            referral_addr=referral_addr
        )
        logger.info(f"Adding Storm trade_notification {trade_notification}")
        await upsert_entity(session, trade_notification)


class TonRafflesLockParser(ContractsExecutorParser):
    def __init__(self):
        super(TonRafflesLockParser, self).__init__()

    @staticmethod
    def parser_name() -> str:
        return "TonRafflesLockParser"

    async def parse(self, session: Session, context: AccountContext):
        if context.account.code_hash != 'EteT+cRJvvRce7Q2hd4h1OA8cRi1048L88e5vrSXkA0=':
            return
        stats = await self._execute(context.code.code, context.account.data, 'get_contract_data',
                                          ["address", "address", "int", "int", "int", "int", "int", "int", "int"])

        owner_address = stats[0]
        jetton_wallet = stats[1]

        lock = TonRafflesLock(
            state_id=context.account.state_id,
            address=context.account.address,
            owner=owner_address,
            jetton_wallet=jetton_wallet
        )
        logger.info(f"Adding TonRaffles lock {lock}")

        await upsert_entity(session, lock, constraint="address")

# TONRaffles fairlaunch

class TonRafflesFairlaunchParser(ContractsExecutorParser):
    def __init__(self):
        super(TonRafflesFairlaunchParser, self).__init__()

    @staticmethod
    def parser_name() -> str:
        return "TonRafflesFairlaunchParser"

    async def parse(self, session: Session, context: AccountContext):
        if context.account.code_hash != 'mbztWDlndXJ7XLvQ1vWA549zTei73DDmPHyZl9IeYkk=':
            return

        cell = self._parse_boc(context.account.data)
        reader = BitReader(cell.data.data)
        jetton_wallet = reader.read_address()
        liquidity_pool = reader.read_address()
        affilate_percentage = reader.read_uint(16)

        fairlaunch = TonRafflesFairlaunch(
            state_id=context.account.state_id,
            address=context.account.address,
            jetton_wallet=jetton_wallet,
            liquidity_pool=liquidity_pool,
            affilate_percentage=affilate_percentage
        )
        logger.info(f"Adding TonRaffles fairlaunch {fairlaunch}")

        await upsert_entity(session, fairlaunch, constraint="address")

class TonRafflesFairlaunchWalletParser(ContractsExecutorParser):
    def __init__(self):
        super(TonRafflesFairlaunchWalletParser, self).__init__()

    @staticmethod
    def parser_name() -> str:
        return "TonRafflesFairlaunchWalletParser"

    async def parse(self, session: Session, context: AccountContext):
        if context.account.code_hash != 'BUNmybQWXuQjH7fJ0tN/6Y6FHtucnRBGw/qEMv5/jTA=':
            return

        cell = self._parse_boc(context.account.data)
        reader = BitReader(cell.data.data)
        sale = reader.read_address()
        owner = reader.read_address()
        reader.read_coins()
        purchased = reader.read_coins()
        reader = BitReader(cell.refs.pop(0).data.data)
        reader.read_bits(128 + 64)
        referrals_purchased = reader.read_coins()
        
        wallet = TonRafflesFairlaunchWallet(
            state_id=context.account.state_id,
            address=context.account.address,
            owner=owner,
            sale=sale,
            purchased=purchased,
            referrals_purchased=referrals_purchased
        )
        logger.info(f"Adding TonRaffles fairlaunch wallet {wallet}")

        await upsert_entity(session, wallet, constraint="address")

class TonRafflesFairlaunchPurchaseParser(Parser):
    def __init__(self):
        super(TonRafflesFairlaunchPurchaseParser, self).__init__(DestinationTxRequiredPredicate(OpCodePredicate(0x6691fda5)))

    @staticmethod
    def parser_name() -> str:
        return "TonRafflesFairlaunchPurchaseParser"

    async def parse(self, session: Session, context: MessageContext):
        if context.destination_tx.action_result_code != 0 or context.destination_tx.compute_exit_code != 0:
            logger.warning("Failed tx for TONRaffles fairlaunch purchase")
            return
        cell = self._parse_boc(context.content.body)
        reader = BitReader(cell.data.data)
        reader.read_bits(32) # opcode
        query_id = reader.read_uint(64)
        amount = reader.read_coins()

        purchase = TonRafflesFairlaunchPurchase(
            msg_id=context.message.msg_id,
            utime=context.destination_tx.utime,
            created_lt=context.message.created_lt,
            originated_msg_hash=await get_originated_msg_hash(session, context.message),
            wallet=context.message.destination,
            query_id=str(query_id),
            amount=amount
        )
        logger.info(f"Adding TONRaffles fairlaunch purchase {purchase}")
        await upsert_entity(session, purchase)


class TonRafflesFairlaunchRewardParser(Parser):
    def __init__(self):
        super(TonRafflesFairlaunchRewardParser, self).__init__(DestinationTxRequiredPredicate(OpCodePredicate(0x256c5472)))

    @staticmethod
    def parser_name() -> str:
        return "TonRafflesFairlaunchRewardParser"

    async def parse(self, session: Session, context: MessageContext):
        if context.destination_tx.action_result_code != 0 or context.destination_tx.compute_exit_code != 0:
            logger.warning("Failed tx for TONRaffles fairlaunch reward")
            return
        cell = self._parse_boc(context.content.body)
        reader = BitReader(cell.data.data)
        reader.read_bits(32) # opcode
        query_id = reader.read_uint(64)
        amount = reader.read_coins()
        user = reader.read_address()

        purchase = TonRafflesFairlaunchReward(
            msg_id=context.message.msg_id,
            utime=context.destination_tx.utime,
            created_lt=context.message.created_lt,
            originated_msg_hash=await get_originated_msg_hash(session, context.message),
            wallet=context.message.destination,
            query_id=str(query_id),
            amount=amount,
            user=user
        )
        logger.info(f"Adding TONRaffles fairlaunch purchase {purchase}")
        await upsert_entity(session, purchase)

class DaoLamaBorrowWalletParser(ContractsExecutorParser):
    def __init__(self):
        super(DaoLamaBorrowWalletParser, self).__init__()

    @staticmethod
    def parser_name() -> str:
        return "DaoLamaBorrowWalletParser"

    async def parse(self, session: Session, context: AccountContext):
        if context.account.code_hash != '0MkU/bYx8WDSPC0dx5g6/EwGBG2T9p/Gn/ZaHW86qF4=':
            return
        pool_address, owner, nft_item, borrowed_amount, amount_to_repay, time_to_repay, status, start_time = await self._execute(context.code.code, context.account.data, 'get_loan_data',
                                    ["address", "address", "address", "int", "int", "int", "int", "int"])

        borrow = DaoLamaBorrowWallet(
            state_id=context.account.state_id,
            address=context.account.address,
            pool_address=pool_address,
            owner=owner,
            nft_item=nft_item,
            borrowed_amount=int(borrowed_amount),
            amount_to_repay=int(amount_to_repay),
            time_to_repay=int(time_to_repay),
            status=int(status),
            start_time=int(start_time)
        )
        logger.info(f"Adding DaoLama borrow {borrow}")

        await upsert_entity(session, borrow, constraint="address")

class DaoLamaExtendLoanParser(Parser):
    def __init__(self):
        super(DaoLamaExtendLoanParser, self).__init__(DestinationTxRequiredPredicate(OpCodePredicate(0x9)))

    @staticmethod
    def parser_name() -> str:
        return "DaoLamaExtendLoanParser"

    async def parse(self, session: Session, context: MessageContext):
        if context.destination_tx.action_result_code != 0 or context.destination_tx.compute_exit_code != 0:
            logger.warning(f"Failed tx for DAOLama extend {context.message.msg_id}")
            return
        cell = self._parse_boc(context.content.body)
        reader = BitReader(cell.data.data)
        reader.read_bits(32) # opcode
        query_id = reader.read_uint(64)
        signed_msg = cell.refs.pop(0).refs.pop(0)
        reader = BitReader(signed_msg.data.data)

        to_addr = reader.read_address()
        from_addr = reader.read_address()
        valid_until = reader.read_uint(32)
        op_code = reader.read_uint(32)
        new_start_time = reader.read_uint(32)
        new_repay_time = reader.read_uint(32)

        reader = BitReader(signed_msg.refs.pop(0).data.data)
        new_loan_amount = reader.read_coins()
        new_repay_amount = reader.read_coins()

        extend = DaoLamaExtendLoan(
            msg_id=context.message.msg_id,
            utime=context.destination_tx.utime,
            created_lt=context.message.created_lt,
            originated_msg_hash=await get_originated_msg_hash(session, context.message),
            query_id=str(query_id),
            to_addr=to_addr,
            from_addr=from_addr,
            valid_until=valid_until,
            op_code=op_code,
            new_start_time=new_start_time,
            new_repay_time=new_repay_time,
            new_loan_amount=new_loan_amount,
            new_repay_amount=new_repay_amount
        )
        logger.info(f"Adding DAOLama extend loan {extend}")
        await upsert_entity(session, extend)

# Collect all declared parsers

def children_iterator(klass):
    if hasattr(klass, "parser_name"):
        yield klass()
    for child in klass.__subclasses__():
        for k in children_iterator(child):
            yield k

ALL_PARSERS = list(children_iterator(Parser))


