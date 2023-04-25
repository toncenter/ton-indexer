from indexer.database import *
from indexer.crud import *
import codecs
import json
import aiohttp
import asyncio
import logging
from urllib.parse import urlparse
from tvm_valuetypes import deserialize_boc
from tvm_valuetypes.cell import CellData
from sqlalchemy.orm import Session
from sqlalchemy.future import select
from parser.bitreader import BitReader
from dataclasses import dataclass


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

class OpCodePredicate(ParserPredicate):
    def __init__(self, opcode):
        super(OpCodePredicate, self).__init__(MessageContext)
        self.opcode = opcode

    def _internal_match(self, context: MessageContext):
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


class Parser:
    def __init__(self, predicate: ParserPredicate):
        self.predicate = predicate

    async def parse(self, session: Session, context: any):
        raise Error("Not supported")

    def _parse_boc(self, b64data):
        raw = codecs.decode(codecs.encode(b64data, "utf-8"), "base64")
        return deserialize_boc(raw)

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
        transfer = JettonTransfer(
            msg_id = context.message.msg_id,
            created_lt = context.message.created_lt,
            utime = context.destination_tx.utime,
            successful = context.destination_tx.action_result_code == 0 and context.destination_tx.compute_exit_code == 0,
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


        mint = JettonMint(
            msg_id = context.message.msg_id,
            created_lt = context.message.created_lt,
            utime = context.destination_tx.utime,
            successful = context.destination_tx.action_result_code == 0 and context.destination_tx.compute_exit_code == 0,
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
        response_destination = reader.read_address()
        custom_payload = None
        # TODO move Maybe parsing to utils
        try:
            if reader.read_uint(1):
                custom_payload = cell.refs.pop(0)
        except:
            logger.warning("Wrong custom payload format")

        burn = JettonBurn(
            msg_id = context.message.msg_id,
            created_lt = context.message.created_lt,
            utime = context.destination_tx.utime,
            successful = context.destination_tx.action_result_code == 0 and context.destination_tx.compute_exit_code == 0,
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
        wallet = JettonWallet(
            state_id = context.account.state_id,
            address = context.account.address,
            owner = owner,
            jetton_master = jetton,
            balance = balance
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
                if parsed_url.netloc == 'localhost':
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
            custom_payload = cell.refs.pop(0)
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

        transfer = NFTTransfer(
            msg_id=context.message.msg_id,
            created_lt=context.message.created_lt,
            utime=context.destination_tx.utime,
            successful=context.destination_tx.action_result_code == 0 and context.destination_tx.compute_exit_code == 0,
            originated_msg_id=await get_originated_msg_id(session, context.message),
            query_id=str(query_id),
            current_owner=context.message.source,
            nft_item=context.message.destination,
            new_owner=new_owner,
            response_destination=response_destination,
            custom_payload=custom_payload.serialize_boc() if custom_payload is not None else None,
            forward_ton_amount=forward_ton_amount,
            forward_payload=forward_payload.serialize_boc() if forward_payload is not None else None
        )
        logger.info(f"Adding NFT transfer {transfer}")

        await upsert_entity(session, transfer)

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
            description=metadata.get('description', None)
        )
        logger.info(f"Adding NFT collection {collection}")

        await upsert_entity(session, collection, constraint="address")

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
                    attributes=None
                )
                logger.info(f"Adding TON DNS item {item}")

                await upsert_entity(session, item, constraint="address")
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
            attributes=json.dumps(metadata.get('attributes')) if 'attributes' in metadata else None
        )
        logger.info(f"Adding NFT item {item}")

        await upsert_entity(session, item, constraint="address")

@dataclass
class SaleContract:
    name: str
    signature: list
    marketplace_pos: int
    nft_pos: int
    price_pos: int
    owner_pos: int

class NFTItemSaleParser(ContractsExecutorParser):

    def __init__(self):
        try:
            from parser.nft_contracts import SALE_CONTRACTS
        except Exception as e:
            logger.error("Unable to init sale contracts", e)
            raise e
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

    @staticmethod
    def parser_name() -> str:
        return "NFTItemSaleParser"

    async def parse(self, session: Session, context: AccountContext):
        contract = NFTItemSaleParser.SALE_CONTRACTS[context.code.hash]
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
            owner=sale_data[contract.owner_pos]
        )
        logger.info(f"Adding NFT item sale {item}")

        await upsert_entity(session, item, constraint="address")


# Collect all declared parsers

def children_iterator(klass):
    if hasattr(klass, "parser_name"):
        yield klass()
    for child in klass.__subclasses__():
        for k in children_iterator(child):
            yield k

ALL_PARSERS = list(children_iterator(Parser))


