//! before_tx and after_tx against the transactions and the boundary states of
//! the block. The executor gives an intermediate state only after a replay
//! checked by hashes; here adjacent points, provenance and public VM calls on
//! those states are checked too.

use std::iter::once;

use substreams::errors::Error;
use substreams_ton::pb::ton::state::v1::AccountSnapshot;
use substreams_ton::pb::ton::tvm::v1::{call_result::Outcome, CallResult, ContextOverrides, ErrorCode};
use substreams_ton::tvm::{self, GetMethodCall};
use substreams_ton::types::{account, AccountBlock, Block, BlockData, Transaction};
use substreams_ton::{Address, At};

use crate::{account_check::without_bocs, pb};

const MAX_ACCOUNTS: usize = 4;
const MAX_DETAILS: usize = 8;
/// Accounts with up to so many transactions in a block are preferred: they
/// are replayed, and busy exchange accounts would make the test long.
const MAX_TRANSACTIONS: usize = 12;

fn check(out: &mut pb::TxPointCheck, ok: bool, message: impl Into<String>) {
    if !ok {
        out.mismatches += 1;
        if out.details.len() < MAX_DETAILS {
            out.details.push(message.into());
        }
    }
}

/// The BOC of an external-in message to the address, of one cell with no
/// StateInit and an empty body. The fixture of emulate_check has a fixed
/// destination; this one is made for each account.
fn external(address: &Address) -> Vec<u8> {
    // ext_in_msg_info$10, src addr_none$00, dest addr_std$10 with no anycast,
    // import_fee 0, no init, an inline body: 277 bits, then the completion
    // tag of a cell whose data is not a whole number of bytes.
    let mut data = [0u8; 35];
    let mut bit = 0;
    let mut put = |value: u64, bits: usize| {
        for i in (0..bits).rev() {
            data[bit / 8] |= (((value >> i) & 1) as u8) << (7 - bit % 8);
            bit += 1;
        }
    };
    put(0b10, 2);
    put(0b00, 2);
    put(0b100, 3);
    put(address.workchain as u8 as u64, 8);
    for byte in address.account_id {
        put(byte as u64, 8);
    }
    put(0, 4 + 1 + 1);
    put(1, 1);
    // One root of one cell: d1 = 0 refs, d2 = 2 * 35 - 1 for 35 bytes with a
    // completion tag.
    let mut boc = vec![0xb5, 0xee, 0x9c, 0x72, 1, 1, 1, 1, 0, 37, 0, 0, 69];
    boc.extend(data);
    boc
}

struct Point<'a> {
    address: Address,
    block: &'a BlockData,
    account: &'a AccountBlock,
    tx: &'a Transaction,
    index: usize,
    after: bool,
}
impl Point<'_> {
    fn at(&self) -> At {
        if self.after {
            At::AfterTx(self.tx.lt)
        } else {
            At::BeforeTx(self.tx.lt)
        }
    }

    fn label(&self) -> String {
        format!("{} {:?}", self.address, self.at())
    }
}

#[substreams::handlers::map]
fn map_tx_point_check(block: Block) -> Result<pb::TxPointCheck, Error> {
    let mc = block.masterchain_block.as_ref().ok_or_else(|| Error::msg("no masterchain block"))?;
    let mut out = pb::TxPointCheck { mc_seqno: mc.id.as_ref().map_or(0, |id| id.seq_no), ..Default::default() };
    let mut candidates: Vec<_> = once(mc)
        .chain(block.shard_blocks.iter())
        .flat_map(|data| data.extra.iter().flat_map(|extra| extra.account_blocks.iter()).map(move |a| (data, a)))
        .filter(|(_, a)| !a.transactions.is_empty())
        .collect();
    // Several transactions first, tick/tock accounts among them.
    candidates.sort_by_key(|(data, a)| {
        let n = a.transactions.len();
        (!(n > 1 && n <= MAX_TRANSACTIONS), n > MAX_TRANSACTIONS, data.id.as_ref().map_or(0, |id| id.seq_no))
    });
    let mut points = Vec::new();
    for (data, a) in candidates.into_iter().take(MAX_ACCOUNTS) {
        let workchain = data.id.as_ref().and_then(|id| id.shard_id.as_ref()).map_or(0, |s| s.workchain_id);
        let address = Address::new(workchain, a.account_addr.as_slice().try_into()?);
        let last = a.transactions.len() - 1;
        let mut indices = vec![0, 1.min(last), a.transactions.len() / 2, last];
        indices.sort_unstable();
        indices.dedup();
        for index in indices {
            for after in [false, true] {
                points.push(Point { address, block: data, account: a, tx: &a.transactions[index], index, after });
            }
        }
    }
    let mut batch = tvm::batch();
    for point in &points {
        batch.add(tvm::get_account(&point.address).at(point.at()));
    }
    let invalid_address = points.first().map_or(Address::new(0, [0; 32]), |p| p.address);
    batch
        .add(tvm::get_account(&invalid_address).at(At::BeforeTx(0)))
        .add(tvm::get_account(&invalid_address).at(At::AfterTx(u64::MAX)))
        .add(tvm::get_method(&invalid_address, "seqno").at(At::BeforeTx(1)))
        .add(tvm::emulate(external(&invalid_address)).at(At::AfterTx(1)));
    let answers = batch.execute()?;
    out.calls += answers.len() as u32;
    for result in &answers[points.len()..] {
        let invalid = matches!(&result.outcome, Some(Outcome::Error(e)) if e.code == ErrorCode::InvalidRequest as i32);
        check(&mut out, invalid, format!("invalid transaction point: {:?}", result.outcome));
    }
    let mut snapshots: Vec<Option<&AccountSnapshot>> = Vec::new();
    for (point, result) in points.iter().zip(&answers) {
        let Some(Outcome::Account(snapshot)) = &result.outcome else {
            if matches!(&result.outcome, Some(Outcome::Error(e)) if e.code == ErrorCode::ReplayMismatch as i32) {
                out.replay_mismatches += 1;
            }
            check(&mut out, false, format!("{}: {:?}", point.label(), result.outcome));
            snapshots.push(None);
            continue;
        };
        out.snapshots += 1;
        let value = snapshot.value.as_ref().ok_or_else(|| Error::msg("missing AccountValue"))?;
        let none = matches!(value.account.as_ref().and_then(|a| a.kind.as_ref()), Some(account::Kind::AccountNone(_)));
        // An account that does not exist has no last transaction to tell.
        let (lt, hash) = if none {
            (0, vec![0; 32])
        } else if point.after {
            (point.tx.lt, point.tx.hash.clone())
        } else {
            (point.tx.prev_trans_lt, point.tx.prev_trans_hash.clone())
        };
        let same_last = value.last_trans_lt == lt && value.last_trans_hash == hash;
        check(&mut out, same_last, format!("{}: previous transaction", point.label()));
        let source = snapshot.source.as_ref();
        let info = point.block.info.as_ref().ok_or_else(|| Error::msg("missing BlockInfo"))?;
        let provenance = snapshot.block == point.block.id
            && snapshot.gen_utime == info.gen_utime
            && snapshot.gen_lt == info.end_lt
            && source.is_some_and(|s| s.address == Some(point.address.to_pb()) && s.at == Some(point.at().into()));
        check(&mut out, provenance, format!("{}: snapshot provenance", point.label()));
        let last = point.index + 1 == point.account.transactions.len();
        let boundary = match (point.after, point.index) {
            (false, 0) => point.account.state_before.as_ref(),
            (true, _) if last => point.account.state_after.as_ref(),
            _ => None,
        };
        if let Some(boundary) = boundary {
            out.boundaries += 1;
            let same = value.account == Some(without_bocs(boundary.clone()));
            check(&mut out, same, format!("{}: boundary state", point.label()));
        } else {
            out.intermediate += 1;
        }
        if let Some(previous) = snapshots.last().and_then(|s| *s) {
            let previous_point = &points[snapshots.len() - 1];
            if previous_point.address == point.address
                && previous_point.block.id == point.block.id
                && previous_point.after
                && !point.after
                && previous_point.tx.lt == point.tx.prev_trans_lt
            {
                check(&mut out, previous.value == snapshot.value, format!("{}: adjacent states differ", point.label()));
            }
        }
        snapshots.push(Some(snapshot));
    }
    // Public calls still use N's context even when their state required replay.
    // Deliberately different overrides must never affect historical replay.
    let context = ContextOverrides {
        now: Some(1_700_000_123),
        lt: Some(17_000_123),
        block_rand_seed: Some(vec![7; 32]),
        ..Default::default()
    };
    let mut batch = tvm::batch();
    let mut compared = Vec::new();
    for (point, snapshot) in points.iter().zip(snapshots).filter(|(p, _)| p.index == 1 && !p.after) {
        let Some(boc) = snapshot.and_then(|s| s.value.as_ref()).and_then(|v| v.shard_account_boc.clone()) else {
            continue;
        };
        batch
            .add(tvm::get_method(&point.address, "seqno").at(point.at()).context(context.clone()))
            .add(GetMethodCall::on_shard_account(boc.clone(), "seqno").context(context.clone()))
            .add(tvm::emulate(external(&point.address)).at(point.at()).context(context.clone()).return_state())
            .add(tvm::emulate(external(&point.address)).shard_account(boc).context(context.clone()).return_state());
        compared.push(point.label());
    }
    let results: Vec<CallResult> = batch.execute()?;
    out.calls += results.len() as u32;
    for (label, group) in compared.into_iter().zip(results.as_chunks::<4>().0) {
        out.method_pairs += 1;
        out.emulate_pairs += 1;
        check(&mut out, group[0] == group[1], format!("{label}: get-method differs on BOC"));
        check(&mut out, group[2] == group[3], format!("{label}: emulation differs on BOC"));
    }
    Ok(out)
}
