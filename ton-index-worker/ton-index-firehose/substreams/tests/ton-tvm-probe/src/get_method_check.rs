//! get_method on the network state against the same get-method on the
//! ShardAccount BOC that get_account gives of it, for the accounts that
//! changed in the round, before it and after: both run in the VM context of
//! the block, so they must answer alike. Needs ton-tvm-executor.

use std::collections::BTreeSet;

use substreams::errors::Error;
use substreams_ton::tvm::{self, GetMethodCall, Outcome};
use substreams_ton::types::BlockLazyView;
use substreams_ton::At;

use crate::pb;

const MAX_ACCOUNTS: usize = 8;
const MAX_DETAILS: usize = 5;
/// Of wallets and jetton wallets, which most changed accounts are.
const METHODS: [&str; 3] = ["seqno", "get_public_key", "get_wallet_data"];

#[substreams::handlers::map]
fn map_get_method_check(block: &BlockLazyView<'_>) -> Result<pb::GetMethodCheck, Error> {
    let mc = block.masterchain()?.ok_or_else(|| Error::msg("no masterchain block"))?;
    let mut out = pb::GetMethodCheck { mc_seqno: mc.id.get()?.map_or(0, |id| id.seq_no), ..Default::default() };

    let mut addresses = Vec::new();
    let mut seen = BTreeSet::new();
    for account_block in block.account_blocks() {
        let Some(address) = account_block?.address() else { continue };
        if seen.insert(address.clone()) {
            addresses.push(address);
            if addresses.len() == MAX_ACCOUNTS {
                break;
            }
        }
    }

    // Per account and point: its state, then each method on the network.
    let points = [At::BeforeBlock, At::AfterBlock];
    let mut batch = tvm::batch();
    for address in &addresses {
        for at in points {
            batch.add(tvm::get_account(address).at(at));
            for method in METHODS {
                batch.add(tvm::get_method(address, method).at(at));
            }
        }
    }
    let onchain = batch.execute()?;

    // The same methods on the states given back.
    let mut given = tvm::batch();
    let mut compared = Vec::new();
    for (i, group) in onchain.chunks(1 + METHODS.len()).enumerate() {
        let snapshot = group[0].clone().account()?;
        let Some(boc) = snapshot.value.into_option().and_then(|value| value.shard_account_boc) else {
            out.omitted += 1;
            continue;
        };
        for (j, method) in METHODS.iter().enumerate() {
            given.add(GetMethodCall::on_shard_account(boc.clone(), method));
            compared.push((i, &group[1 + j]));
        }
    }
    let answers = given.execute()?;

    for ((i, ours), theirs) in compared.into_iter().zip(answers) {
        out.calls += 1;
        match &ours.outcome {
            Some(Outcome::GetMethod(result)) if result.exit_code == 0 => out.executed += 1,
            Some(Outcome::Error(_)) => out.errors += 1,
            _ => {}
        }
        if *ours != theirs {
            out.mismatches += 1;
            if out.details.len() < MAX_DETAILS {
                let at = if i % 2 == 0 { "before" } else { "after" };
                out.details.push(format!("{} {at}: {:?} vs {:?}", addresses[i / 2], ours.outcome, theirs.outcome));
            }
        }
    }
    Ok(out)
}
