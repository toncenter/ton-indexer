from __future__ import annotations

import base64
import logging

from pytoniq_core import Cell

from indexer.events import context
from indexer.events.blocks.basic_blocks import CallContractBlock
from indexer.events.blocks.basic_matchers import (
    BlockMatcher,
    BlockTypeMatcher,
    ContractMatcher,
    GenericMatcher,
    OrMatcher,
    excess_matcher,
)
from indexer.events.blocks.core import Block
from indexer.events.blocks.jettons import JettonTransferBlock
from indexer.events.blocks.labels import labeled
from indexer.events.blocks.messages import (
    DedustV2PayJetton,
    DedustV2PayNative,
    DedustV2PayoutMessage,
    DedustV2SwapEvent,
    DedustV2SwapPayload,
    DedustV2SwapStep,
)
from indexer.events.blocks.swaps import JettonSwapBlock
from indexer.events.blocks.utils import AccountId, Amount, Asset
from indexer.events.blocks.utils.block_utils import (
    find_call_contracts,
    get_labeled,
    get_labeled_sorted,
)

logger = logging.getLogger(__name__)


def _has_swap_event_child(block: Block) -> bool:
    return any(
        isinstance(b, CallContractBlock) and b.opcode == DedustV2SwapEvent.opcode
        for b in block.next_blocks
    )


def _is_jetton_swap_carrier(block: Block) -> bool:
    # jetton transfer into a pool that emits a Swap event
    return block.btype == "jetton_transfer" and _has_swap_event_child(block)


def _carrier_asset(block: Block) -> Asset:
    if isinstance(block, JettonTransferBlock):
        return block.data["asset"]
    return Asset(is_ton=True)


def _final_min_out(payload: DedustV2SwapPayload) -> Amount:
    # floor for the final output: the payload's own minimal_amount_out is only
    # hop 1's, the last SwapStep holds the final one
    min_out = payload.minimal_amount_out
    nxt = payload.next
    depth = 0
    while nxt is not None and depth < 16:  # depth cap
        step = DedustV2SwapStep(nxt.begin_parse())
        min_out = step.minimal_amount_out
        nxt = step.next
        depth += 1
    return Amount(min_out)


def _jetton_swap_payload(block: Block) -> DedustV2SwapPayload | None:
    if not isinstance(block, JettonTransferBlock):
        return None
    fp = block.data.get("forward_payload")
    if fp is None:
        return None
    try:
        pay_jetton = DedustV2PayJetton(Cell.one_from_boc(base64.b64decode(fp)).begin_parse())
        if pay_jetton.payment_payload_opcode != DedustV2SwapPayload.opcode:
            return None
        return DedustV2SwapPayload(pay_jetton.payment_payload.begin_parse())
    except Exception:
        return None


def _native_swap_payload(block: Block) -> tuple[DedustV2PayNative, DedustV2SwapPayload] | None:
    try:
        pay_native = DedustV2PayNative(block.get_body())
        if pay_native.payment_payload_opcode != DedustV2SwapPayload.opcode:
            return None
        return pay_native, DedustV2SwapPayload(pay_native.payment_payload.begin_parse())
    except Exception:
        return None


async def _pool_other_asset(pool: AccountId, source_asset: Asset) -> Asset | None:
    pool_info = await context.interface_repository.get().get_dedust_pool(pool.as_str())
    if pool_info is None:
        return None
    for a in pool_info.assets:
        cand = Asset(a["is_ton"], a["address"] if not a["is_ton"] else None)
        if cand != source_asset:
            return cand
    return None


def _swap_payload(block: Block) -> DedustV2SwapPayload | None:
    if isinstance(block, JettonTransferBlock):
        return _jetton_swap_payload(block)
    parsed = _native_swap_payload(block)
    return parsed[1] if parsed else None


class DedustV2SwapBlockMatcher(BlockMatcher):
    """DeDust CPMM V2 swap, single hop or SwapStep multi-hop.

    Each hop is carrier -> Swap event 0x78e79ba4 + continuation, where the
    continuation is the next pool->pool carrier or the final out leg
    (jetton transfer | PayoutMessage). Amounts come from the Swap events."""

    def __init__(self):
        swap_event = ContractMatcher(opcode=DedustV2SwapEvent.opcode, include_excess=False)

        out_leg = labeled("out", OrMatcher([
            BlockTypeMatcher(block_type="jetton_transfer"),
            ContractMatcher(opcode=DedustV2PayoutMessage.opcode, include_excess=False),
        ]))

        # pool->pool hop carriers, children wired to hop_children below
        native_inner = ContractMatcher(opcode=DedustV2PayNative.opcode, include_excess=False)
        jetton_inner = GenericMatcher(_is_jetton_swap_carrier)
        jetton_inner.include_excess = False
        native_hop = labeled("hop", native_inner)
        jetton_hop = labeled("hop", jetton_inner)

        # hops before out_leg: an intermediate jetton carrier is also a jetton_transfer
        continuation = OrMatcher([native_hop, jetton_hop, out_leg])

        hop_children = [swap_event, continuation, excess_matcher()]
        native_hop.children_matchers = hop_children
        jetton_hop.children_matchers = hop_children

        super().__init__(include_excess=False, children_matchers=hop_children)

    def test_self(self, block: Block):
        if (
            isinstance(block, CallContractBlock)
            and block.opcode == DedustV2PayNative.opcode
        ):
            return True
        return _is_jetton_swap_carrier(block)

    def _min_out_amount(self, block: Block) -> Amount | None:
        payload = _swap_payload(block)
        if payload is None:
            return None
        try:
            return _final_min_out(payload)
        except Exception as e:
            logger.debug("DeDust V2 min_out_amount parse failed: %s", e)
            return None

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        # PayNative also carries deposits and fund rewards, check for a Swap payload
        if (
            isinstance(block, CallContractBlock)
            and block.opcode == DedustV2PayNative.opcode
            and _native_swap_payload(block) is None
        ):
            return []

        swap_blocks = sorted(
            find_call_contracts([block] + other_blocks, DedustV2SwapEvent.opcode),
            key=lambda b: b.min_lt,
        )
        if not swap_blocks:
            return []
        swap_msgs = [DedustV2SwapEvent(b.get_body()) for b in swap_blocks]
        n = len(swap_msgs)

        out_leg = get_labeled("out", other_blocks)
        if out_leg is None:
            return self.bail(block, "no out leg matched")
        if isinstance(out_leg, JettonTransferBlock) and _jetton_swap_payload(out_leg) is not None:
            return self.bail(block, "out leg still carries a SwapPayload, route failed mid-hop")

        # on a linear chain min_lt sort makes carriers[i] feed swap_msgs[i]
        hops = get_labeled_sorted("hop", other_blocks)
        carriers = [block] + hops
        asset_seq = [_carrier_asset(c) for c in carriers]
        out_asset = (
            out_leg.data["asset"] if isinstance(out_leg, JettonTransferBlock)
            else Asset(is_ton=True)
        )
        asset_seq.append(out_asset)

        if isinstance(block, JettonTransferBlock):
            sender = block.data["sender"]
            in_source_wallet = block.data["sender_wallet"]
            in_destination = block.data["receiver"]
            in_destination_wallet = block.data["receiver_wallet"]
        else:
            in_msg = block.get_message()
            sender = AccountId(in_msg.source)
            in_source_wallet = None
            in_destination = AccountId(in_msg.destination)
            in_destination_wallet = None

        if isinstance(out_leg, JettonTransferBlock):
            out_source = out_leg.data["sender"]
            out_source_wallet = out_leg.data["sender_wallet"]
            out_destination = out_leg.data["receiver"]
            out_destination_wallet = out_leg.data["receiver_wallet"]
        else:
            out_msg = out_leg.get_message()
            out_source = AccountId(out_msg.source)
            out_source_wallet = None
            out_destination = AccountId(out_msg.destination)
            out_destination_wallet = None

        peer_swaps = []
        if len(asset_seq) == n + 1:
            for i in range(n):
                peer_swaps.append({
                    "in": {"amount": Amount(swap_msgs[i].amount_in), "asset": asset_seq[i]},
                    "out": {"amount": Amount(swap_msgs[i].amount_out), "asset": asset_seq[i + 1]},
                })
            # consecutive swap events must chain amounts
            for i in range(n - 1):
                if swap_msgs[i].amount_out != swap_msgs[i + 1].amount_in:
                    logger.warning(
                        "DeDust V2 swap hop amounts don't chain at hop %d: %s",
                        i, block.event_nodes[0].message.trace_id,
                    )
                    break
        else:
            logger.warning(
                "DeDust V2 swap carrier/hop count mismatch (assets=%d, hops=%d): %s",
                len(asset_seq), n, block.event_nodes[0].message.trace_id,
            )

        source_asset = asset_seq[0]
        dex_incoming_transfer = {
            "asset": source_asset,
            "amount": Amount(swap_msgs[0].amount_in),
            "source": sender,
            "source_jetton_wallet": in_source_wallet,
            "destination": in_destination,
            "destination_jetton_wallet": in_destination_wallet,
        }
        dex_outgoing_transfer = {
            "asset": out_asset,
            "amount": Amount(swap_msgs[-1].amount_out),
            "source": out_source,
            "source_jetton_wallet": out_source_wallet,
            "destination": out_destination,
            "destination_jetton_wallet": out_destination_wallet,
        }

        success = all(
            b.get_message().transaction.aborted is False
            and b.get_message().transaction.compute_exit_code in (None, 0)
            for b in swap_blocks
        )

        new_block = JettonSwapBlock({
            "dex": "dedust_v2",
            "sender": sender,
            "source_asset": source_asset,
            "destination_asset": out_asset,
            "dex_incoming_transfer": dex_incoming_transfer,
            "dex_outgoing_transfer": dex_outgoing_transfer,
            "peer_swaps": peer_swaps if n > 1 else [],
            "referral_amount": None,
            "referral_address": None,
            "min_out_amount": self._min_out_amount(block),
        })
        new_block.merge_blocks([block] + other_blocks)
        new_block.failed = not success
        return [new_block]


class DedustV2FailedSwapBlockMatcher(BlockMatcher):
    """DeDust CPMM V2 failed swap (slippage / deadline), no Swap event:

        TON-in:    PayNative 0xa5a7cbf8 (user -> Pool)        # payload = Swap 0xc442500f
                     PayoutMessage 0x3216ca09 (Pool -> user)  # exit_code != 0
                     excess
        Jetton-in: jetton_transfer, forward = PayJetton -> Swap (user -> Pool)
                     jetton_transfer (Pool -> user)           # same asset, refund
    """

    def __init__(self):
        super().__init__(
            include_excess=False,
            children_matchers=[
                labeled("refund", OrMatcher([
                    ContractMatcher(opcode=DedustV2PayoutMessage.opcode, include_excess=False),
                    BlockTypeMatcher(block_type="jetton_transfer"),
                ])),
                excess_matcher(),
            ],
        )

    def test_self(self, block: Block):
        if (
            isinstance(block, CallContractBlock)
            and block.opcode == DedustV2PayNative.opcode
        ):
            return True
        return _jetton_swap_payload(block) is not None

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        if _has_swap_event_child(block):
            return []  # swap executed, success matcher's case
        refund = get_labeled("refund", other_blocks)
        if refund is None:
            return self.bail(block, "no refund leg matched")

        if isinstance(block, JettonTransferBlock):
            parsed = self._jetton_in(block, refund)
        else:
            parsed = self._native_in(block, refund)
        if parsed is None:
            return []
        sender, pool, source_asset, amount_in, in_src_wallet, in_dst_wallet, min_out = parsed

        out_asset = await _pool_other_asset(pool, source_asset)

        dex_incoming_transfer = {
            "asset": source_asset,
            "amount": amount_in,
            "source": sender,
            "source_jetton_wallet": in_src_wallet,
            "destination": pool,
            "destination_jetton_wallet": in_dst_wallet,
        }
        dex_outgoing_transfer = {
            "asset": out_asset,
            "amount": Amount(0),
            "source": pool,
            "source_jetton_wallet": None,
            "destination": sender,
            "destination_jetton_wallet": None,
        }
        new_block = JettonSwapBlock({
            "dex": "dedust_v2",
            "sender": sender,
            "source_asset": source_asset,
            "destination_asset": out_asset,
            "dex_incoming_transfer": dex_incoming_transfer,
            "dex_outgoing_transfer": dex_outgoing_transfer,
            "peer_swaps": [],
            "referral_amount": None,
            "referral_address": None,
            "min_out_amount": min_out,
        })
        new_block.merge_blocks([block] + other_blocks)
        new_block.failed = True
        return [new_block]

    def _native_in(self, block: Block, refund: Block):
        parsed = _native_swap_payload(block)
        if parsed is None:
            return None  # carrier payload is not a swap (deposit etc.)
        pay_native, payload = parsed
        if not (isinstance(refund, CallContractBlock)
                and refund.opcode == DedustV2PayoutMessage.opcode):
            return self.bail_none(block, "TON-in refund is not a PayoutMessage")
        try:
            if DedustV2PayoutMessage(refund.get_body()).exit_code == 0:
                return None  # exit_code 0 is a normal payout, not a refund
        except Exception as e:
            return self.bail_none(block, f"refund parse failed: {e}")
        in_msg = block.get_message()
        # refund must go back to the carrier's sender, else leak (a pool->pool
        # carrier of a failed multi-hop refunds the route initiator instead)
        if refund.get_message().destination != in_msg.source:
            return self.bail_none(block, "refund not addressed to the carrier sender")
        try:
            min_out = _final_min_out(payload)
        except Exception:
            min_out = None
        return (AccountId(in_msg.source), AccountId(in_msg.destination),
                Asset(is_ton=True), Amount(pay_native.amount), None, None, min_out)

    def _jetton_in(self, block: Block, refund: Block):
        payload = _jetton_swap_payload(block)
        if payload is None:
            return None  # carrier payload is not a swap (deposit etc.)
        source_asset = block.data["asset"]
        sender = block.data["sender"]
        if not isinstance(refund, JettonTransferBlock):
            return self.bail_none(block, "jetton-in refund is not a jetton transfer")
        if refund.data["asset"] != source_asset or refund.data["receiver"] != sender:
            return self.bail_none(block, "refund asset/receiver mismatch")
        return (sender, block.data["receiver"], source_asset, block.data["amount"],
                block.data["sender_wallet"], block.data["receiver_wallet"],
                _final_min_out(payload))
