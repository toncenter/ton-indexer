from indexer.database import *
from indexer.crud import *
import codecs
import json
import requests
import logging
from urllib.parse import urlparse
from tvm_valuetypes import deserialize_boc
from tvm_valuetypes.cell import CellData
from parser.bitreader import BitReader


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
        if not isinstance(context, self.context_class):
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
        super(JettonTransferParser, self).__init__(OpCodePredicate(0x0f8a7ea5))

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

        transfer = JettonTransfer(
            msg_id = context.message.msg_id,
            successful = context.source_tx.action_result_code == 0 and context.source_tx.compute_exit_code == 0, # TODO check
            originated_msg_id = await get_originated_msg_id(session, context.message),
            query_id = str(query_id),
            amount = amount,
            source_owner = context.message.source,
            source_wallet = context.message.destination,
            destination_owner = destination,
            response_destination = response_destination,
            custom_payload = custom_payload.serialize_boc() if custom_payload is not None else None,
            forward_ton_amount = forward_ton_amount,
            forward_payload = forward_payload.serialize_boc() if forward_payload is not None else None
        )
        logger.info(f"Adding jetton transfer {transfer}")
        
        await upsert_entity(session, transfer)


"""
Utility class to execute get methods remotely over contracts-executor
https://github.com/shuva10v/ton-analytics/tree/main/utils/contracts_executor
"""
class ContractsExecutorParser(Parser):
    def __init__(self, predicate=ActiveAccountsPredicate()):
        super(ContractsExecutorParser, self).__init__(predicate)
        self.executor_url = settings.parser.executor.url

    def _execute(self, code, data, method, types, address=None):
        req = {'code': code, 'data': data, 'method': method,
               'expected': types, 'address': address}
        if address is not None:
            req[address] = address
        res = requests.post(self.executor_url, json=req)
        assert res.status_code == 200, "Error during contract executor call: %s" % res
        res = res.json()
        if res['exit_code'] != 0:
            # logger.debug("Non-zero exit code: %s" % res)
            return None
        return res['result']


class JettonWalletParser(ContractsExecutorParser):
    def __init__(self):
        super(JettonWalletParser, self).__init__()

    @staticmethod
    def parser_name() -> str:
        return "JettonWalletParser"

    async def parse(self, session: Session, context: AccountContext):
        try:
            wallet_data = self._execute(context.code.code, context.account.data, 'get_wallet_data',
                                                        ["int", "address", "address", "cell_hash"])
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
    def __init__(self, ipfs_gateway='https://w3s.link/ipfs/'):
        self.ipfs_gateway = ipfs_gateway

    def fetch(self, url):
        logger.debug(f"Fetching {url}")
        parsed_url = urlparse(url)
        if parsed_url.scheme == 'ipfs':
            assert len(parsed_url.path) == 0, parsed_url
            return requests.get(self.ipfs_gateway + parsed_url.netloc).content
        elif parsed_url.scheme is None or len(parsed_url.scheme) == 0:
            logger.error(f"No schema for URL: {url}")
            return None
        else:
            if parsed_url.netloc == 'localhost':
                return None
            retry = 0
            while retry < 5:
                try:
                    return requests.get(url).content
                except Exception as e:
                    logger.error(f"Unable to fetch data from {url}", e)
                    time.sleep(5)
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
        try:
            wallet_data = self._execute(context.code.code, context.account.data, 'get_jetton_data',
                                        ["int", "int", "address", "metadata", "cell_hash"],
                                        address=context.account.address)
            total_supply, mintable, admin_address, jetton_content, wallet_hash = wallet_data
        except:
            # we invoke get method on all contracts so ignore errors
            return

        if jetton_content is None or 'content_layout' not in jetton_content:
            logger.warning(f"No content layout extracted: {jetton_content}")
            metadata_url = None
            metadata = {}
        else:
            if jetton_content['content_layout'] == 'off-chain':
                metadata_url = jetton_content['content']
                metadata_json = self.fetcher.fetch(metadata_url)
                try:
                    metadata = json.loads(metadata_json)
                except Exception as e:
                    logger.error(f"Failed to parse metadata for {metadata_url}: {e}")
                    metadata = {}
                metadata['content_layout'] = jetton_content['content_layout']
            else:
                metadata = jetton_content['content']
                metadata_url = None

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


def children_iterator(klass):
    if hasattr(klass, "parser_name"):
        yield klass()
    for child in klass.__subclasses__():
        for k in children_iterator(child):
            yield k

ALL_PARSERS = list(children_iterator(Parser))


