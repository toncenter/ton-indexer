//! get_account against the block itself, for the accounts that changed in
//! the round: needs a real backend, ton-tvm-executor.

use std::collections::BTreeMap;
use std::iter::once;

use substreams::errors::Error;
use substreams_ton::tvm;
use substreams_ton::types::{account, account_state, Account, Block};
use substreams_ton::{Address, At};

use crate::pb;

/// Of the accounts that changed in a block, so many are checked.
const MAX_ACCOUNTS: usize = 8;
const MAX_DETAILS: usize = 5;

/// An account through the round: its state before its first transaction in
/// the round, after its last one, and that last transaction.
struct Changed<'a> {
    address: Address,
    before: &'a Account,
    after: &'a Account,
    last_lt: u64,
    last_hash: &'a [u8],
}

#[substreams::handlers::map]
fn map_account_check(block: Block) -> Result<pb::AccountCheck, Error> {
    let mc = block.masterchain_block.as_ref().ok_or_else(|| Error::msg("no masterchain block"))?;
    let mc_seqno = mc.id.as_ref().map_or(0, |id| id.seq_no);

    // The blocks of a shard chain follow each other by seqno, across splits
    // and merges too; the block lists them by shard first.
    let mut blocks: Vec<_> = once(mc).chain(block.shard_blocks.iter()).collect();
    blocks.sort_by_key(|data| data.id.as_ref().map_or(0, |id| id.seq_no));
    let mut changed: Vec<Changed> = Vec::new();
    let mut index: BTreeMap<Address, usize> = BTreeMap::new();
    for data in blocks {
        let workchain = data.id.as_ref().and_then(|id| id.shard_id.as_ref()).map_or(0, |s| s.workchain_id);
        for account_block in data.extra.iter().flat_map(|extra| extra.account_blocks.iter()) {
            let (Some(before), Some(after), Some(last)) = (
                &account_block.state_before,
                &account_block.state_after,
                account_block.transactions.iter().max_by_key(|tx| tx.lt),
            ) else {
                continue;
            };
            let Ok(id) = account_block.account_addr.as_slice().try_into() else { continue };
            let address = Address::new(workchain, id);
            match index.get(&address) {
                Some(&i) => {
                    changed[i].after = after;
                    changed[i].last_lt = last.lt;
                    changed[i].last_hash = &last.hash;
                }
                None if changed.len() < MAX_ACCOUNTS => {
                    index.insert(address, changed.len());
                    changed.push(Changed { address, before, after, last_lt: last.lt, last_hash: &last.hash });
                }
                None => {}
            }
        }
    }

    let mut batch = tvm::batch();
    for account in &changed {
        batch.add(tvm::get_account(&account.address).at(At::BeforeBlock));
        batch.add(tvm::get_account(&account.address));
    }
    let mut results = batch.execute()?.into_iter();
    let mut out = pb::AccountCheck { mc_seqno, accounts: changed.len() as u32, ..Default::default() };
    for account in changed {
        let before = results.next().unwrap().account()?;
        let after = results.next().unwrap().account()?;
        let mut problems = Vec::new();
        if before.value.as_ref().and_then(|v| v.account.clone()) != Some(without_bocs(account.before.clone())) {
            problems.push("before_block is not the state before");
        }
        let value = after.value.unwrap_or_default();
        // An account that is none after its transactions, as after a message
        // bounced from a nonexistent one, is not stored: it has no last
        // transaction to tell.
        let none_after = matches!(account.after.kind, Some(account::Kind::AccountNone(_)));
        if value.account != Some(without_bocs(account.after.clone())) {
            problems.push("after_block is not the state after");
        }
        if !none_after && (value.last_trans_lt != account.last_lt || value.last_trans_hash != account.last_hash) {
            problems.push("after_block has another last transaction");
        }
        if !problems.is_empty() {
            out.mismatches += 1;
            if out.details.len() < MAX_DETAILS {
                out.details.push(format!("{}: {}", account.address, problems.join(", ")));
            }
        }
    }
    Ok(out)
}

/// The account as get_account gives it: code, data and library by their
/// hashes, which the block gives with their BOCs when they changed.
pub(crate) fn without_bocs(mut account: Account) -> Account {
    if let Some(account::Kind::Account(existing)) = account.kind.as_mut() {
        let state = existing.storage.as_mut().and_then(|storage| storage.state.as_mut());
        if let Some(account_state::Kind::AccountActive(init)) = state.and_then(|state| state.kind.as_mut()) {
            for cell in [init.code.as_mut(), init.data.as_mut(), init.library.as_mut()].into_iter().flatten() {
                cell.boc = None;
                cell.boc_omitted = false;
            }
        }
    }
    account
}
