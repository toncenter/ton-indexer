# Multisig v2 create-order, approve, and execute matchers. Fully declarative:
# zero host fns, predicates, shapers, or imperative builders.
#
# TWO APPROVE MATCHERS, one per anchor arm
# ----------------------------------------
# Legacy accepts either the 0xA762230F call or a ton_transfer whose comment is
# "approve". A mixed `(op X ^ btype ton_transfer where ...)` anchor cannot carry
# the inline expression: C013_WHERE_EXPR_NOT_SYNC requires a named predicate on
# a group branch, creating duplicate Python/C++ host code for one string test.
# Splitting keeps the expression on a plain anchor. This is the same reasoning
# and construct documented in specs/coffee_mev.mch:11-27. The arms are disjoint
# by btype, produce the same data, and their relative order is unobservable.
#
# WHY THE COMMENT ARM WRITES signer_index = -1
# ------------------------------------------------
# Its body is exactly op32(0) + "approve": only 56 bits remain after the opcode.
# MultisigApprove then tries load_uint(64), which underflows every time, so the
# bare except at blocks/multisig.py:199 always preserves the initial -1. This is
# legacy behavior, not an approximation. The opcode arm parses normally.
#
# The response is an XOR child. On the accepted branch the `rejected` scalar
# capture is null; statement-parse on null is a no-op and field access
# null-propagates, so `rejected.body.exit_code ?? 0` is safely zero without a
# tolerant host fn. Neither branch present means no structural match, matching
# the legacy required OrMatcher child.
#
# EMULATED EXECUTE YIELD
# ----------------------
# MultisigOrder detection sees only trace-final state. An order destroyed by
# execute has no tier-1 interface, so the lookup rejects and this matcher emits
# no emulated row. It still ports for committed and fixture paths, where the
# historical/bundled interface exists; building a historical emulated store is
# explicitly out of scope.
#
# KNOWN DEVIATIONS FROM LEGACY
# ----------------------------
# 1. A malformed 0xA762230F approve body is swallowed by legacy and emitted with
#    signer_index=-1; `parse vote as MultisigApprove` rejects it here. Such a
#    truncated opcode message is unreachable in practice and the deviation is
#    accepted.
# 2. `b64()` serializes Cell BOCs with td, whose container bytes can differ from
#    pytoniq on multi-ref trees. Both decode to the same root hash and every gate
#    neutralizes order_boc through the existing boc_fields cellhash rendering.
# 3. Legacy's `order.signers or []` and this spec's `?? []` differ only for a
#    fixture carrying signers:null. Production MultisigOrderData always renders
#    its vector as a list, possibly empty, and no fixture carries null.

opcode MultisigNewOrder       = 0xF718510F
opcode MultisigInitOrder      = 0x9C73FBA2
opcode MultisigApprove        = 0xA762230F
opcode MultisigApproveAccepted = 0x82609BF6
opcode MultisigApproveRejected = 0xAFAF283E
opcode MultisigExecute        = 0x75097F5D

matcher multisig_create_order {
    produces multisig_create_order
    entry    @new

    op MultisigNewOrder @new -> op MultisigInitOrder @init

    parse new  as MultisigNewOrder
    parse init as MultisigInitOrder
    let order = lookup multisig_order(init.msg.destination)
    reject when order == null
    out {
        query_id:               new.body.query_id
        multisig:               account(new.msg.destination)
        order_seqno:            new.body.order_seqno
        created_by:             account(new.msg.source)
        is_created_by_signer:   new.body.is_signer
        creator_index:          new.body.singer_index
        order_contract_address: account(init.msg.destination)
        creator_approved:       init.body.approve_on_init
        expiration_date:        new.body.expiration_date
        order_boc_str:          b64(init.body.order)
        signers:                order.signers
    }
}

matcher multisig_approve {
    produces multisig_approve
    entry    @vote

    op MultisigApprove @vote
      -> ( op MultisigApproveAccepted @accepted
         ^ op MultisigApproveRejected @rejected )

    parse vote     as MultisigApprove
    parse rejected as MultisigApproveRejected
    let order = lookup multisig_order(vote.msg.destination)
    reject when order == null
    out {
        signer:       account(vote.msg.source)
        order:        account(vote.msg.destination)
        success:      accepted != null
        signer_index: vote.body.signer_index
        exit_code:    rejected.body.exit_code ?? 0
        signers:      order.signers ?? []
    }
}

matcher multisig_approve_comment {
    produces multisig_approve
    entry    @vote

    btype ton_transfer @vote where (.comment == "approve")
      -> ( op MultisigApproveAccepted @accepted
         ^ op MultisigApproveRejected @rejected )

    parse rejected as MultisigApproveRejected
    let order = lookup multisig_order(vote.msg.destination)
    reject when order == null
    out {
        signer:       account(vote.msg.source)
        order:        account(vote.msg.destination)
        success:      accepted != null
        # A comment body always underflows MultisigApprove's query_id read; see
        # the proof in this file's header and blocks/multisig.py:193-200.
        signer_index: -1
        exit_code:    rejected.body.exit_code ?? 0
        signers:      order.signers ?? []
    }
}

matcher multisig_execute {
    produces multisig_execute
    entry    @exec

    op MultisigExecute @exec

    parse exec as MultisigExecute
    let order = lookup multisig_order(exec.msg.source)
    reject when order == null
    out {
        query_id:               exec.body.query_id
        multisig:               account(exec.msg.destination)
        success:                not exec.failed
        order_contract_address: account(exec.msg.source)
        order_seqno:            exec.body.order_seqno
        expiration_date:        exec.body.expiration_date
        approvals_num:          exec.body.approvals_num
        signers_hash_str:       b64(bytes_of(exec.body.signers_hash))
        order_boc_str:          b64(exec.body.order)
        signers:                order.signers ?? []
    }
}
