from __future__ import annotations

import difflib
import json
from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

import requests

DEFAULT_SWAGGER_URL = "https://toncenter.com/api/v3/doc.json"
DEFAULT_TIMEOUT_SECONDS = 30.0
DEFAULT_USER_AGENT = "ton-indexer-backcompat-tests/1.0"
UNSAFE_OPERATIONS = {
    ("POST", "/api/v3/message"),
}


class DataUnavailableError(RuntimeError):
    """Raised when the default case generator cannot derive stable input data."""


@dataclass(frozen=True)
class ResponseSnapshot:
    source: str
    method: str
    path: str
    url: str
    status_code: int
    text: str
    json_body: Any


@dataclass(frozen=True)
class ResolvedRequest:
    query: Mapping[str, Any] = field(default_factory=dict)
    body: Any = None
    expected_status: int | None = 200
    ignore_paths: tuple[str, ...] = ()


Resolver = Callable[["BackcompatContext"], ResolvedRequest]


@dataclass(frozen=True)
class CaseDefinition:
    case_id: str
    method: str
    path: str
    description: str
    resolver: Resolver
    unsafe: bool = False
    skip: bool = False


@dataclass(frozen=True)
class RuntimeConfig:
    swagger_url: str
    timeout_seconds: float
    verify_tls: bool
    allow_unsafe: bool


class ApiClient:
    def __init__(
        self,
        *,
        name: str,
        base_url: str,
        api_key: str | None,
        timeout_seconds: float,
        verify_tls: bool,
    ) -> None:
        self.name = name
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds
        self.verify_tls = verify_tls
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": DEFAULT_USER_AGENT,
            }
        )

    def request(
        self,
        method: str,
        path: str,
        *,
        query: Mapping[str, Any] | None = None,
        body: Any = None,
    ) -> ResponseSnapshot:
        url = self._build_url(path)
        headers: dict[str, str] = {}
        if self.api_key:
            headers["X-Api-Key"] = self.api_key
        headers["X-No-Metadata"] = "true"
        try:
            query = query or {}
            response = self.session.request(
                method=method.upper(),
                url=url,
                params=_encode_query_params(query or {}),
                json=body,
                headers=headers,
                timeout=self.timeout_seconds,
                verify=self.verify_tls,
            )
        except requests.RequestException as exc:
            raise RuntimeError(
                f"{self.name} request failed for {method.upper()} {path}: {exc}"
            ) from exc

        try:
            json_body = response.json()
        except ValueError:
            json_body = None

        return ResponseSnapshot(
            source=self.name,
            method=method.upper(),
            path=path,
            url=response.url,
            status_code=response.status_code,
            text=response.text,
            json_body=json_body,
        )

    def _build_url(self, path: str) -> str:
        normalized_path = path if path.startswith("/") else f"/{path}"
        if self.base_url.endswith("/api/v3") and normalized_path.startswith("/api/v3"):
            normalized_path = normalized_path[len("/api/v3") :]
            if not normalized_path:
                normalized_path = "/"
        return f"{self.base_url}{normalized_path}"


class BackcompatContext:
    def __init__(self, reference_client: ApiClient, devel_client: ApiClient) -> None:
        self.reference_client = reference_client
        self.devel_client = devel_client
        self._value_cache: dict[str, Any] = {}
        self._reference_json_cache: dict[str, Any] = {}

    def fetch_pair(
        self,
        method: str,
        path: str,
        *,
        query: Mapping[str, Any] | None = None,
        body: Any = None,
    ) -> tuple[ResponseSnapshot, ResponseSnapshot]:
        with ThreadPoolExecutor(max_workers=2) as executor:
            reference_future = executor.submit(
                self.reference_client.request,
                method,
                path,
                query=query,
                body=body,
            )
            devel_future = executor.submit(
                self.devel_client.request,
                method,
                path,
                query=query,
                body=body,
            )
            return reference_future.result(), devel_future.result()

    def reference_json(
        self,
        method: str,
        path: str,
        *,
        query: Mapping[str, Any] | None = None,
        body: Any = None,
        expected_status: int | None = 200,
    ) -> Any:
        cache_key = json.dumps(
            {
                "method": method.upper(),
                "path": path,
                "query": query or {},
                "body": body,
                "expected_status": expected_status,
            },
            sort_keys=True,
        )
        if cache_key in self._reference_json_cache:
            return self._reference_json_cache[cache_key]

        response = self.reference_client.request(method, path, query=query, body=body)
        if expected_status is not None and response.status_code != expected_status:
            raise DataUnavailableError(
                f"reference returned {response.status_code} for {method.upper()} {path}"
            )
        if response.json_body is None:
            raise DataUnavailableError(f"reference did not return JSON for {method.upper()} {path}")

        self._reference_json_cache[cache_key] = response.json_body
        return response.json_body

    def common_masterchain_seqno(self, depth: int = 2) -> int:
        def load() -> int:
            reference, devel = self.fetch_pair("GET", "/api/v3/masterchainInfo")
            for response in (reference, devel):
                if response.status_code != 200 or response.json_body is None:
                    raise DataUnavailableError(
                        f"{response.source} masterchainInfo is unavailable "
                        f"({response.status_code})"
                    )

            reference_first = int(reference.json_body["first"]["seqno"])
            reference_last = int(reference.json_body["last"]["seqno"])
            devel_first = int(devel.json_body["first"]["seqno"])
            devel_last = int(devel.json_body["last"]["seqno"])

            common_first = max(reference_first, devel_first)
            common_last = min(reference_last, devel_last)
            if common_last < common_first:
                raise DataUnavailableError("reference and devel do not overlap on masterchain seqno")

            candidate = common_last - depth
            if candidate < common_first:
                candidate = common_last
            return candidate

        return self._memoize(f"common_masterchain_seqno:{depth}", load)

    def seed_transaction(self) -> Mapping[str, Any]:
        def load() -> Mapping[str, Any]:
            seqno = self.common_masterchain_seqno()
            primary = self.reference_json(
                "GET",
                "/api/v3/transactionsByMasterchainBlock",
                query={"seqno": seqno, "limit": 50, "sort": "desc"},
            )
            transactions = list(primary.get("transactions") or [])
            if not transactions:
                fallback = self.reference_json(
                    "GET",
                    "/api/v3/transactions",
                    query={"mc_seqno": seqno, "limit": 50, "sort": "desc"},
                )
                transactions = list(fallback.get("transactions") or [])

            for transaction in transactions:
                if transaction.get("hash") and transaction.get("account") and transaction.get("block_ref", dict()).get('workchain') == 0:
                    return transaction
            raise DataUnavailableError("could not find a seed transaction")

        return self._memoize("seed_transaction", load)

    def seed_transaction_hash(self) -> str:
        return str(self.seed_transaction()["hash"])

    def seed_account(self) -> str:
        def load() -> str:
            top_accounts = self.reference_json(
                "GET",
                "/api/v3/topAccountsByBalance",
                query={"limit": 50, "offset": 0},
            )
            for row in top_accounts or []:
                account = row.get("account")
                if account and not account.startswith("-1:"):
                    return str(account)

            transaction = self.seed_transaction()
            account = transaction.get("account")
            if account:
                return str(account)

            raise DataUnavailableError("could not find a seed account")

        return self._memoize("seed_account", load)

    def seed_wallet_address(self) -> str:
        def load() -> str:
            candidates: list[str] = []
            top_accounts = self.reference_json(
                "GET",
                "/api/v3/topAccountsByBalance",
                query={"limit": 20, "offset": 0},
            )
            for row in top_accounts or []:
                account = row.get("account")
                if account:
                    candidates.append(str(account))

            transaction_account = self.seed_transaction().get("account")
            if transaction_account:
                tx_account = str(transaction_account)
                if tx_account not in candidates:
                    candidates.append(tx_account)

            for candidate in candidates:
                response = self.reference_client.request(
                    "GET",
                    "/api/v3/walletInformation",
                    query={"address": candidate},
                )
                if response.status_code == 200:
                    return candidate

            if candidates:
                return candidates[0]

            raise DataUnavailableError("could not find any wallet candidate")

        return self._memoize("seed_wallet_address", load)

    def seed_trace_id(self) -> str:
        def load() -> str:
            transaction = self.seed_transaction()
            trace_id = transaction.get("trace_id")
            if trace_id:
                return str(trace_id)

            seqno = self.common_masterchain_seqno()
            traces = self.reference_json(
                "GET",
                "/api/v3/traces",
                query={"mc_seqno": seqno, "limit": 20, "sort": "desc"},
            )
            for trace in traces.get("traces") or []:
                trace_id = trace.get("trace_id")
                if trace_id:
                    return str(trace_id)

            raise DataUnavailableError("could not find a seed trace id")

        return self._memoize("seed_trace_id", load)

    def seed_message_hash(self) -> str:
        def load() -> str:
            transaction = self.seed_transaction()
            for message in _iter_transaction_messages(transaction):
                message_hash = message.get("hash")
                if message_hash:
                    return str(message_hash)

            seqno = self.common_masterchain_seqno()
            messages = self.reference_json(
                "GET",
                "/api/v3/messages",
                query={"start_lt": 1, "limit": 20, "sort": "desc"},
            )
            for message in messages.get("messages") or []:
                message_hash = message.get("hash")
                if message_hash:
                    return str(message_hash)

            raise DataUnavailableError("could not find a seed message hash")

        return self._memoize("seed_message_hash", load)

    def first_item(
        self,
        path: str,
        array_key: str | None,
        *,
        query: Mapping[str, Any] | None = None,
        predicate: Callable[[Mapping[str, Any]], bool] | None = None,
        error_hint: str,
    ) -> Mapping[str, Any]:
        data = self.reference_json("GET", path, query=query or {})
        items = _extract_items(data, array_key)
        for item in items:
            if predicate is None or predicate(item):
                return item
        raise DataUnavailableError(error_hint)

    def _memoize(self, key: str, loader: Callable[[], Any]) -> Any:
        if key not in self._value_cache:
            self._value_cache[key] = loader()
        return self._value_cache[key]


def fetch_swagger(swagger_url: str, timeout_seconds: float, verify_tls: bool) -> dict[str, Any]:
    return _fetch_swagger(swagger_url, timeout_seconds, verify_tls)


@lru_cache(maxsize=8)
def _fetch_swagger(swagger_url: str, timeout_seconds: float, verify_tls: bool) -> dict[str, Any]:
    response = requests.get(
        swagger_url,
        headers={
            "Accept": "application/json",
            "User-Agent": DEFAULT_USER_AGENT,
        },
        timeout=timeout_seconds,
        verify=verify_tls,
    )
    response.raise_for_status()
    return response.json()


def validate_case(swagger: Mapping[str, Any], case: CaseDefinition, request: ResolvedRequest) -> None:
    operation = swagger.get("paths", {}).get(case.path, {}).get(case.method.lower())
    if not operation:
        raise ValueError(f"{case.method} {case.path} is not present in the remote Swagger")

    missing_fields: list[str] = []
    for parameter in operation.get("parameters", []):
        if not parameter.get("required"):
            continue

        location = parameter.get("in")
        name = parameter.get("name")
        if location == "query":
            if _is_missing_query_value(request.query, name):
                missing_fields.append(f"query:{name}")
        elif location == "body":
            if request.body is None:
                missing_fields.append(f"body:{name}")

    if missing_fields:
        raise ValueError(
            f"{case.case_id} is missing required parameters for {case.method} {case.path}: "
            f"{', '.join(missing_fields)}"
        )


def compare_responses(
    case: CaseDefinition,
    request: ResolvedRequest,
    reference: ResponseSnapshot,
    devel: ResponseSnapshot,
) -> None:
    if request.expected_status is None:
        assert reference.status_code == devel.status_code, _status_mismatch_message(
            case, request, reference, devel
        )
    else:
        assert reference.status_code == request.expected_status, (
            f"{case.case_id} expected reference to return {request.expected_status}, "
            f"got {reference.status_code}\nreference url: {reference.url}\n"
            f"reference body:\n{_truncate_text(reference.text)}"
        )
        assert devel.status_code == request.expected_status, (
            f"{case.case_id} expected devel to return {request.expected_status}, "
            f"got {devel.status_code}\ndevel url: {devel.url}\n"
            f"devel body:\n{_truncate_text(devel.text)}"
        )

    if reference.json_body is not None and devel.json_body is not None:
        normalized_reference = _remove_ignored_paths(reference.json_body, request.ignore_paths)
        normalized_devel = _remove_ignored_paths(devel.json_body, request.ignore_paths)
        assert normalized_reference == normalized_devel, _json_mismatch_message(
            case,
            request,
            reference,
            devel,
            normalized_reference,
            normalized_devel,
        )
        return

    assert reference.text == devel.text, _text_mismatch_message(case, request, reference, devel)


def assert_backcompat(
    *,
    case_id: str,
    method: str,
    path: str,
    backcompat_context: BackcompatContext,
    swagger_spec: Mapping[str, Any],
    query: Mapping[str, Any] | None = None,
    body: Any = None,
    expected_status: int | None = 200,
    ignore_paths: Sequence[str] = (),
    description: str | None = None,
) -> tuple[ResponseSnapshot, ResponseSnapshot]:
    request = ResolvedRequest(
        query=query or {},
        body=body,
        expected_status=expected_status,
        ignore_paths=tuple(ignore_paths),
    )
    case = CaseDefinition(
        case_id=case_id,
        method=method.upper(),
        path=path,
        description=description or case_id,
        resolver=lambda _: request,
    )
    validate_case(swagger_spec, case, request)
    reference, devel = backcompat_context.fetch_pair(
        case.method,
        case.path,
        query=request.query,
        body=request.body,
    )
    compare_responses(case, request, reference, devel)
    return reference, devel


def load_custom_cases(path: str | None) -> list[CaseDefinition]:
    if not path:
        return []

    payload = json.loads(Path(path).read_text())
    if not isinstance(payload, list):
        raise ValueError("custom case file must contain a JSON array")

    cases: list[CaseDefinition] = []
    for raw_case in payload:
        if not isinstance(raw_case, dict):
            raise ValueError("each custom case must be a JSON object")

        case_id = str(raw_case["id"])
        method = str(raw_case["method"]).upper()
        path_value = str(raw_case["path"])
        query = raw_case.get("query") or {}
        body = raw_case.get("body")
        description = str(raw_case.get("description") or case_id)
        expected_status = raw_case.get("expected_status", 200)
        ignore_paths = tuple(str(item) for item in raw_case.get("ignore_paths", []))
        unsafe = bool(raw_case.get("unsafe", False)) or (method, path_value) in UNSAFE_OPERATIONS

        cases.append(
            CaseDefinition(
                case_id=case_id,
                method=method,
                path=path_value,
                description=description,
                resolver=_static_resolver(
                    query=query,
                    body=body,
                    expected_status=expected_status,
                    ignore_paths=ignore_paths,
                ),
                unsafe=unsafe,
            )
        )

    return cases


def default_cases() -> list[CaseDefinition]:
    return [
        _case(
            "top_accounts_by_balance",
            "GET",
            "/api/v3/topAccountsByBalance",
            "Compare the top accounts endpoint with a bounded response size.",
            query={"limit": 1, "offset": 0},
        ),
        CaseDefinition(
            case_id="blocks_by_masterchain_seqno",
            method="GET",
            path="/api/v3/blocks",
            description="Compare blocks for the same already-indexed masterchain seqno.",
            resolver=_resolve_blocks_by_masterchain_seqno,
            skip=True,
        ),
        CaseDefinition(
            case_id="masterchain_block_shard_state",
            method="GET",
            path="/api/v3/masterchainBlockShardState",
            description="Compare shard state for the same stable masterchain seqno.",
            resolver=_resolve_masterchain_seqno_only,
        ),
        CaseDefinition(
            case_id="masterchain_block_shards",
            method="GET",
            path="/api/v3/masterchainBlockShards",
            description="Compare shard blocks for the same stable masterchain seqno.",
            resolver=_resolve_masterchain_block_shards,
        ),
        CaseDefinition(
            case_id="transactions_by_masterchain_block",
            method="GET",
            path="/api/v3/transactionsByMasterchainBlock",
            description="Compare transactions for the same masterchain block.",
            resolver=_resolve_transactions_by_masterchain_block,
        ),
        CaseDefinition(
            case_id="transaction_by_hash",
            method="GET",
            path="/api/v3/transactions",
            description="Compare transaction lookup by transaction hash.",
            resolver=_resolve_transaction_by_hash,
        ),
        CaseDefinition(
            case_id="adjacent_transactions_by_hash",
            method="GET",
            path="/api/v3/adjacentTransactions",
            description="Compare adjacent transaction lookup for the same transaction hash.",
            resolver=_resolve_adjacent_transactions,
            skip=False,
        ),
        CaseDefinition(
            case_id="trace_by_trace_id",
            method="GET",
            path="/api/v3/traces",
            description="Compare trace lookup by trace id.",
            resolver=_resolve_trace_by_id,
            skip=False,
        ),
        CaseDefinition(
            case_id="trace_by_tx_hash",
            method="GET",
            path="/api/v3/traces",
            description="Compare trace lookup by trace id.",
            resolver=_resolve_trace_by_tx_hash,
            skip=False,
        ),
        CaseDefinition(
            case_id="actions_by_trace_id",
            method="GET",
            path="/api/v3/actions",
            description="Compare actions lookup by trace id.",
            resolver=_resolve_actions_by_trace_id,
        ),
        CaseDefinition(
            case_id="message_by_hash",
            method="GET",
            path="/api/v3/messages",
            description="Compare message lookup by message hash.",
            resolver=_resolve_message_by_hash,
        ),
        CaseDefinition(
            case_id="transactions_by_message_hash",
            method="GET",
            path="/api/v3/transactionsByMessage",
            description="Compare transaction lookup by message hash.",
            resolver=_resolve_transactions_by_message_hash,
        ),
        CaseDefinition(
            case_id="account_states_by_address",
            method="GET",
            path="/api/v3/accountStates",
            description="Compare account state lookup by address.",
            resolver=_resolve_account_states,
        ),
        CaseDefinition(
            case_id="address_book_by_address",
            method="GET",
            path="/api/v3/addressBook",
            description="Compare address book lookup by address.",
            resolver=_resolve_address_book,
        ),
        CaseDefinition(
            case_id="metadata_by_address",
            method="GET",
            path="/api/v3/metadata",
            description="Compare metadata lookup by address.",
            resolver=_resolve_metadata,
        ),
        CaseDefinition(
            case_id="address_information_by_address",
            method="GET",
            path="/api/v3/addressInformation",
            description="Compare address information lookup by address.",
            resolver=_resolve_address_information,
            skip=True,
        ),
        CaseDefinition(
            case_id="wallet_states_by_address",
            method="GET",
            path="/api/v3/walletStates",
            description="Compare wallet state lookup by address.",
            resolver=_resolve_wallet_states,
        ),
        CaseDefinition(
            case_id="wallet_information_by_address",
            method="GET",
            path="/api/v3/walletInformation",
            description="Compare wallet information lookup by address.",
            resolver=_resolve_wallet_information,
        ),
        _case(
            "decode_get",
            "GET",
            "/api/v3/decode",
            "Compare GET decode for a deterministic opcode payload.",
            query={"opcodes": ["0x0"]},
        ),
        _case(
            "decode_post",
            "POST",
            "/api/v3/decode",
            "Compare POST decode for the same deterministic opcode payload.",
            body={"opcodes": ["0x0"], "bodies": []},
        ),
        CaseDefinition(
            case_id="dns_records_by_domain",
            method="GET",
            path="/api/v3/dns/records",
            description="Compare DNS record lookup using a domain discovered from reference data.",
            resolver=_resolve_dns_records,
        ),
        CaseDefinition(
            case_id="jetton_master_by_address",
            method="GET",
            path="/api/v3/jetton/masters",
            description="Compare jetton master lookup by address.",
            resolver=_resolve_jetton_master,
        ),
        CaseDefinition(
            case_id="jetton_wallet_by_address",
            method="GET",
            path="/api/v3/jetton/wallets",
            description="Compare jetton wallet lookup by address.",
            resolver=_resolve_jetton_wallet,
        ),
        CaseDefinition(
            case_id="jetton_transfer_by_wallet",
            method="GET",
            path="/api/v3/jetton/transfers",
            description="Compare jetton transfer lookup using a discovered wallet filter.",
            resolver=_resolve_jetton_transfer,
        ),
        CaseDefinition(
            case_id="jetton_burn_by_wallet",
            method="GET",
            path="/api/v3/jetton/burns",
            description="Compare jetton burn lookup using a discovered wallet filter.",
            resolver=_resolve_jetton_burn,
        ),
        CaseDefinition(
            case_id="multisig_wallet_by_address",
            method="GET",
            path="/api/v3/multisig/wallets",
            description="Compare multisig lookup by address.",
            resolver=_resolve_multisig_wallet,
        ),
        CaseDefinition(
            case_id="multisig_order_by_address",
            method="GET",
            path="/api/v3/multisig/orders",
            description="Compare multisig order lookup by address.",
            resolver=_resolve_multisig_order,
        ),
        CaseDefinition(
            case_id="nft_collection_by_address",
            method="GET",
            path="/api/v3/nft/collections",
            description="Compare NFT collection lookup by address.",
            resolver=_resolve_nft_collection,
        ),
        CaseDefinition(
            case_id="nft_item_by_address",
            method="GET",
            path="/api/v3/nft/items",
            description="Compare NFT item lookup by address.",
            resolver=_resolve_nft_item,
        ),
        CaseDefinition(
            case_id="nft_transfer_by_item_address",
            method="GET",
            path="/api/v3/nft/transfers",
            description="Compare NFT transfer lookup by item address.",
            resolver=_resolve_nft_transfer,
        ),
        CaseDefinition(
            case_id="nft_sale_by_sale_address",
            method="GET",
            path="/api/v3/nft/sales",
            description="Compare NFT sale lookup by a discovered sale or auction contract address.",
            resolver=_resolve_nft_sale,
        ),
        CaseDefinition(
            case_id="vesting_by_contract_address",
            method="GET",
            path="/api/v3/vesting",
            description="Compare vesting contract lookup by contract address.",
            resolver=_resolve_vesting,
        ),
    ]


def _case(
    case_id: str,
    method: str,
    path: str,
    description: str,
    *,
    query: Mapping[str, Any] | None = None,
    body: Any = None,
    expected_status: int | None = 200,
    ignore_paths: Sequence[str] = (),
    unsafe: bool = False,
) -> CaseDefinition:
    return CaseDefinition(
        case_id=case_id,
        method=method,
        path=path,
        description=description,
        resolver=_static_resolver(
            query=query or {},
            body=body,
            expected_status=expected_status,
            ignore_paths=tuple(ignore_paths),
        ),
        unsafe=unsafe or (method.upper(), path) in UNSAFE_OPERATIONS,
    )


def _static_resolver(
    *,
    query: Mapping[str, Any],
    body: Any,
    expected_status: int | None,
    ignore_paths: tuple[str, ...],
) -> Resolver:
    def resolve(_: BackcompatContext) -> ResolvedRequest:
        return ResolvedRequest(
            query=query,
            body=body,
            expected_status=expected_status,
            ignore_paths=ignore_paths,
        )

    return resolve


def _resolve_masterchain_seqno_only(context: BackcompatContext) -> ResolvedRequest:
    return ResolvedRequest(query={"seqno": context.common_masterchain_seqno()})


def _resolve_blocks_by_masterchain_seqno(context: BackcompatContext) -> ResolvedRequest:
    return ResolvedRequest(
        query={
            "mc_seqno": context.common_masterchain_seqno(),
            "limit": 20,
            "offset": 0,
            "sort": "asc",
        }
    )


def _resolve_masterchain_block_shards(context: BackcompatContext) -> ResolvedRequest:
    return ResolvedRequest(
        query={
            "seqno": context.common_masterchain_seqno(),
            "limit": 20,
            "offset": 0,
        }
    )


def _resolve_transactions_by_masterchain_block(context: BackcompatContext) -> ResolvedRequest:
    return ResolvedRequest(
        query={
            "seqno": context.common_masterchain_seqno(),
            "limit": 20,
            "offset": 0,
            "sort": "asc",
        }
    )


def _resolve_transaction_by_hash(context: BackcompatContext) -> ResolvedRequest:
    return ResolvedRequest(query={"hash": context.seed_transaction_hash()})


def _resolve_adjacent_transactions(context: BackcompatContext) -> ResolvedRequest:
    return ResolvedRequest(query={"hash": context.seed_transaction_hash()})


def _resolve_trace_by_id(context: BackcompatContext) -> ResolvedRequest:
    return ResolvedRequest(query={"trace_id": [context.seed_trace_id()], "limit": 10, "offset": 0})

def _resolve_trace_by_tx_hash(context: BackcompatContext) -> ResolvedRequest:
    return ResolvedRequest(query={"tx_hash": [context.seed_trace_id()], "limit": 10, "offset": 0})

def _resolve_actions_by_trace_id(context: BackcompatContext) -> ResolvedRequest:
    return ResolvedRequest(query={"trace_id": [context.seed_trace_id()], "limit": 10, "offset": 0})


def _resolve_message_by_hash(context: BackcompatContext) -> ResolvedRequest:
    return ResolvedRequest(query={"msg_hash": [context.seed_message_hash()], "limit": 10, "offset": 0})


def _resolve_transactions_by_message_hash(context: BackcompatContext) -> ResolvedRequest:
    return ResolvedRequest(query={"msg_hash": context.seed_message_hash(), "limit": 10, "offset": 0})


def _resolve_account_states(context: BackcompatContext) -> ResolvedRequest:
    return ResolvedRequest(query={"address": [context.seed_account()], "include_boc": False})


def _resolve_address_book(context: BackcompatContext) -> ResolvedRequest:
    return ResolvedRequest(query={"address": [context.seed_account()]})


def _resolve_metadata(context: BackcompatContext) -> ResolvedRequest:
    return ResolvedRequest(query={"address": [context.seed_account()]})


def _resolve_address_information(context: BackcompatContext) -> ResolvedRequest:
    return ResolvedRequest(query={"address": context.seed_account(), "use_v2": True})


def _resolve_wallet_states(context: BackcompatContext) -> ResolvedRequest:
    return ResolvedRequest(query={"address": [context.seed_wallet_address()]})


def _resolve_wallet_information(context: BackcompatContext) -> ResolvedRequest:
    return ResolvedRequest(
        query={"address": context.seed_wallet_address(), "use_v2": True},
        expected_status=None,
    )


def _resolve_dns_records(context: BackcompatContext) -> ResolvedRequest:
    record = context.first_item(
        "/api/v3/dns/records",
        "records",
        query={"limit": 20, "offset": 0},
        error_hint="reference does not have any DNS records to use as seed input",
    )
    if record.get("domain"):
        return ResolvedRequest(query={"domain": record["domain"], "limit": 20, "offset": 0})
    if record.get("dns_wallet"):
        return ResolvedRequest(query={"wallet": record["dns_wallet"], "limit": 20, "offset": 0})
    raise DataUnavailableError("reference DNS record did not contain a domain or wallet filter")


def _resolve_jetton_master(context: BackcompatContext) -> ResolvedRequest:
    master = context.first_item(
        "/api/v3/jetton/masters",
        "jetton_masters",
        query={"limit": 20, "offset": 0},
        error_hint="reference does not have jetton masters to compare",
    )
    return ResolvedRequest(query={"address": [master["address"]], "limit": 20, "offset": 0})


def _resolve_jetton_wallet(context: BackcompatContext) -> ResolvedRequest:
    wallet = context.first_item(
        "/api/v3/jetton/wallets",
        "jetton_wallets",
        query={"limit": 20, "offset": 0},
        error_hint="reference does not have jetton wallets to compare",
    )
    return ResolvedRequest(query={"address": [wallet["address"]], "limit": 20, "offset": 0})


def _resolve_jetton_transfer(context: BackcompatContext) -> ResolvedRequest:
    transfer = context.first_item(
        "/api/v3/jetton/transfers",
        "jetton_transfers",
        query={"limit": 20, "offset": 0, "sort": "desc"},
        error_hint="reference does not have jetton transfers to compare",
    )
    if transfer.get("source_wallet"):
        return ResolvedRequest(
            query={"jetton_wallet": [transfer["source_wallet"]], "limit": 20, "offset": 0, "sort": "desc"}
        )
    if transfer.get("jetton_master"):
        return ResolvedRequest(
            query={"jetton_master": transfer["jetton_master"], "limit": 20, "offset": 0, "sort": "desc"}
        )
    raise DataUnavailableError("reference jetton transfer did not contain a usable filter")


def _resolve_jetton_burn(context: BackcompatContext) -> ResolvedRequest:
    burn = context.first_item(
        "/api/v3/jetton/burns",
        "jetton_burns",
        query={"limit": 20, "offset": 0, "sort": "desc"},
        error_hint="reference does not have jetton burns to compare",
    )
    if burn.get("jetton_wallet"):
        return ResolvedRequest(
            query={"jetton_wallet": [burn["jetton_wallet"]], "limit": 20, "offset": 0, "sort": "desc"}
        )
    if burn.get("jetton_master"):
        return ResolvedRequest(
            query={"jetton_master": burn["jetton_master"], "limit": 20, "offset": 0, "sort": "desc"}
        )
    raise DataUnavailableError("reference jetton burn did not contain a usable filter")


def _resolve_multisig_wallet(context: BackcompatContext) -> ResolvedRequest:
    wallet = context.first_item(
        "/api/v3/multisig/wallets",
        "multisigs",
        query={"limit": 20, "offset": 0, "sort": "desc", "include_orders": True},
        error_hint="reference does not have multisig wallets to compare",
    )
    return ResolvedRequest(
        query={
            "address": [wallet["address"]],
            "limit": 20,
            "offset": 0,
            "sort": "desc",
            "include_orders": True,
        }
    )


def _resolve_multisig_order(context: BackcompatContext) -> ResolvedRequest:
    order = context.first_item(
        "/api/v3/multisig/orders",
        "orders",
        query={"limit": 20, "offset": 0, "sort": "desc"},
        error_hint="reference does not have multisig orders to compare",
    )
    return ResolvedRequest(
        query={
            "address": [order["address"]],
            "limit": 20,
            "offset": 0,
            "sort": "desc",
        }
    )


def _resolve_nft_collection(context: BackcompatContext) -> ResolvedRequest:
    collection = context.first_item(
        "/api/v3/nft/collections",
        "nft_collections",
        query={"limit": 20, "offset": 0},
        error_hint="reference does not have NFT collections to compare",
    )
    return ResolvedRequest(query={"collection_address": [collection["address"]], "limit": 20, "offset": 0})


def _resolve_nft_item(context: BackcompatContext) -> ResolvedRequest:
    item = context.first_item(
        "/api/v3/nft/items",
        "nft_items",
        query={"limit": 20, "offset": 0},
        error_hint="reference does not have NFT items to compare",
    )
    return ResolvedRequest(query={"address": [item["address"]], "limit": 20, "offset": 0})


def _resolve_nft_transfer(context: BackcompatContext) -> ResolvedRequest:
    transfer = context.first_item(
        "/api/v3/nft/transfers",
        "nft_transfers",
        query={"limit": 20, "offset": 0, "sort": "desc"},
        error_hint="reference does not have NFT transfers to compare",
    )
    return ResolvedRequest(
        query={"item_address": [transfer["nft_address"]], "limit": 20, "offset": 0, "sort": "desc"}
    )


def _resolve_nft_sale(context: BackcompatContext) -> ResolvedRequest:
    item = context.first_item(
        "/api/v3/nft/items",
        "nft_items",
        query={"limit": 100, "offset": 0},
        predicate=lambda candidate: bool(
            candidate.get("sale_contract_address") or candidate.get("auction_contract_address")
        ),
        error_hint="reference does not expose any NFT item with sale or auction contract data",
    )
    sale_address = item.get("sale_contract_address") or item.get("auction_contract_address")
    return ResolvedRequest(query={"address": [sale_address]})


def _resolve_vesting(context: BackcompatContext) -> ResolvedRequest:
    contract = context.first_item(
        "/api/v3/vesting",
        "vesting_contracts",
        query={"limit": 20, "offset": 0},
        error_hint="reference does not have vesting contracts to compare",
    )
    return ResolvedRequest(query={"contract_address": [contract["address"]], "limit": 20, "offset": 0})


def _encode_query_params(query: Mapping[str, Any]) -> list[tuple[str, str]]:
    encoded: list[tuple[str, str]] = []
    for key, value in query.items():
        if value is None:
            continue

        if isinstance(value, (list, tuple)):
            for item in value:
                if item is None:
                    continue
                encoded.append((key, _stringify_query_value(item)))
            continue

        encoded.append((key, _stringify_query_value(value)))

    return encoded


def _stringify_query_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _iter_transaction_messages(transaction: Mapping[str, Any]) -> Iterable[Mapping[str, Any]]:
    inbound = transaction.get("in_msg")
    if isinstance(inbound, Mapping):
        yield inbound

    out_messages = transaction.get("out_msgs") or []
    for message in out_messages:
        if isinstance(message, Mapping):
            yield message


def _extract_items(data: Any, key: str | None) -> list[Mapping[str, Any]]:
    if key is None:
        items = data
    else:
        items = data.get(key) if isinstance(data, Mapping) else None

    if not isinstance(items, list):
        return []
    return [item for item in items if isinstance(item, Mapping)]


def _is_missing_query_value(query: Mapping[str, Any], name: str) -> bool:
    if name not in query:
        return True
    value = query[name]
    if value is None:
        return True
    if isinstance(value, (list, tuple)) and not value:
        return True
    return False


def _remove_ignored_paths(payload: Any, ignore_paths: Sequence[str]) -> Any:
    if not ignore_paths:
        return payload

    parsed_patterns = [_parse_json_pointer(path) for path in ignore_paths]
    return _remove_paths(payload, current_path=(), parsed_patterns=parsed_patterns)


def _remove_paths(payload: Any, *, current_path: tuple[str, ...], parsed_patterns: Sequence[tuple[str, ...]]) -> Any:
    if any(_path_matches(pattern, current_path) for pattern in parsed_patterns):
        return _DropSentinel.VALUE

    if isinstance(payload, dict):
        result: dict[str, Any] = {}
        for key, value in payload.items():
            child = _remove_paths(
                value,
                current_path=current_path + (str(key),),
                parsed_patterns=parsed_patterns,
            )
            if child is not _DropSentinel.VALUE:
                result[key] = child
        return result

    if isinstance(payload, list):
        result: list[Any] = []
        for index, value in enumerate(payload):
            child = _remove_paths(
                value,
                current_path=current_path + (str(index),),
                parsed_patterns=parsed_patterns,
            )
            if child is not _DropSentinel.VALUE:
                result.append(child)
        return result

    return payload


def _parse_json_pointer(path: str) -> tuple[str, ...]:
    if not path.startswith("/"):
        raise ValueError(f"ignore path must be a JSON pointer starting with '/': {path}")
    if path == "/":
        return tuple()
    return tuple(segment.replace("~1", "/").replace("~0", "~") for segment in path[1:].split("/"))


def _path_matches(pattern: Sequence[str], current_path: Sequence[str]) -> bool:
    if len(pattern) != len(current_path):
        return False
    return all(expected == "*" or expected == actual for expected, actual in zip(pattern, current_path))


def _status_mismatch_message(
    case: CaseDefinition,
    request: ResolvedRequest,
    reference: ResponseSnapshot,
    devel: ResponseSnapshot,
) -> str:
    return (
        f"{case.case_id} returned different status codes for the same request.\n"
        f"method: {case.method}\n"
        f"path: {case.path}\n"
        f"query: {_compact_json(request.query)}\n"
        f"body: {_compact_json(request.body)}\n"
        f"reference: {reference.status_code} {reference.url}\n"
        f"devel: {devel.status_code} {devel.url}\n"
        f"reference body:\n{_truncate_text(reference.text)}\n"
        f"devel body:\n{_truncate_text(devel.text)}"
    )


def _json_mismatch_message(
    case: CaseDefinition,
    request: ResolvedRequest,
    reference: ResponseSnapshot,
    devel: ResponseSnapshot,
    normalized_reference: Any,
    normalized_devel: Any,
) -> str:
    reference_dump = _pretty_json(normalized_reference)
    devel_dump = _pretty_json(normalized_devel)
    diff = "".join(
        difflib.unified_diff(
            reference_dump.splitlines(keepends=True),
            devel_dump.splitlines(keepends=True),
            fromfile="reference",
            tofile="devel",
        )
    )
    return (
        f"{case.case_id} returned different JSON payloads.\n"
        f"method: {case.method}\n"
        f"path: {case.path}\n"
        f"query: {_compact_json(request.query)}\n"
        f"body: {_compact_json(request.body)}\n"
        f"reference url: {reference.url}\n"
        f"devel url: {devel.url}\n"
        f"diff:\n{diff or '(no textual diff generated)'}"
    )


def _text_mismatch_message(
    case: CaseDefinition,
    request: ResolvedRequest,
    reference: ResponseSnapshot,
    devel: ResponseSnapshot,
) -> str:
    diff = "".join(
        difflib.unified_diff(
            reference.text.splitlines(keepends=True),
            devel.text.splitlines(keepends=True),
            fromfile="reference",
            tofile="devel",
        )
    )
    return (
        f"{case.case_id} returned different non-JSON payloads.\n"
        f"method: {case.method}\n"
        f"path: {case.path}\n"
        f"query: {_compact_json(request.query)}\n"
        f"body: {_compact_json(request.body)}\n"
        f"reference url: {reference.url}\n"
        f"devel url: {devel.url}\n"
        f"diff:\n{diff or '(no textual diff generated)'}"
    )


def _compact_json(payload: Any) -> str:
    if payload is None:
        return "null"
    return json.dumps(payload, sort_keys=True, ensure_ascii=True, separators=(",", ":"))


def _pretty_json(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, ensure_ascii=True, indent=2)


def _truncate_text(text: str, max_chars: int = 4000) -> str:
    if len(text) <= max_chars:
        return text
    return f"{text[:max_chars]}\n... <truncated>"


class _DropSentinel:
    VALUE = object()
