//! Calls every kind of the ton_v1 extension on every block and tells what it
//! answered, a line per call. Made for the stub backend of ton-tvm-router,
//! whose answers are known (router/stub.go): a test of the way from a module
//! through fireton to the router and back.
//!
//! Two calls of the host per block: a batch of every kind of call, among them
//! invalid ones, then a get-method on the state the first batch gave, as a
//! module gives back a state it got.

mod pb {
    include!(concat!(env!("OUT_DIR"), "/probe.v1.rs"));
}

mod account_check;

use std::iter::once;

use substreams::errors::Error;
use substreams_ton::pb::ton::tvm::v1::{call_result::Outcome, CallResult, ContextOverrides};
use substreams_ton::stack::StackExt;
use substreams_ton::tvm::{self, EmulationOutcome, GetMethodCall};
use substreams_ton::types::Block;
use substreams_ton::{Address, At};

const ELECTOR: Address = Address::new(-1, [0x33; 32]);

/// An external message to the elector with an empty body.
const EXTERNAL_TO_ELECTOR: &str = "b5ee9c7201010101002500004589fe666666666666666666666666666666666666666666666666666666666666666604";

#[substreams::handlers::map]
fn map_probe(block: Block) -> Result<pb::Probe, Error> {
    let mc = block.masterchain_block.as_ref().ok_or_else(|| Error::msg("no masterchain block"))?;
    let mc_id = mc.id.as_ref().ok_or_else(|| Error::msg("no masterchain block id"))?;

    // Some account changed in this round, else the elector.
    let changed = once(mc)
        .chain(block.shard_blocks.iter())
        .find_map(|data| {
            let workchain = data.id.as_ref()?.shard_id.as_ref()?.workchain_id;
            let account = data.extra.as_ref()?.account_blocks.first()?;
            Some(Address::new(workchain, account.account_addr.as_slice().try_into().ok()?))
        })
        .unwrap_or(ELECTOR);
    let external = hex::decode(EXTERNAL_TO_ELECTOR)?;

    let mut batch = tvm::batch();
    batch
        .get_account(&ELECTOR)
        .add(tvm::get_account(&changed).at(At::BeforeBlock))
        .add(tvm::get_account(&ELECTOR).at(At::BeforeTx(1)))
        .get_method(&ELECTOR, "active_election_id")
        .add(tvm::emulate(external.clone()).ignore_chksig())
        .add(tvm::emulate(external).shard_account(vec![0xb5]))
        .add(tvm::get_method(&ELECTOR, "seqno").context(ContextOverrides {
            rand_seed: Some(vec![0; 32]),
            ..Default::default()
        }))
        .add(tvm::get_method(&Address::new(0, [0; 32]), "seqno").gas_limit(u64::MAX));
    let results = batch.execute()?;

    let clock_matches = match &results[0].outcome {
        Some(Outcome::Account(snapshot)) => snapshot
            .block
            .as_ref()
            .is_some_and(|b| b.seq_no == mc_id.seq_no && b.root_hash == mc_id.root_hash),
        _ => false,
    };
    let given_state = match &results[0].outcome {
        Some(Outcome::Account(snapshot)) => snapshot.value.as_ref().and_then(|v| v.shard_account_boc.clone()),
        _ => None,
    };

    let mut lines: Vec<String> = results.into_iter().map(describe).collect();
    match given_state {
        Some(boc) => lines.push(describe(tvm::execute(vec![GetMethodCall::on_shard_account(boc, "seqno").into()])?.remove(0))),
        None => lines.push("no state to give back".to_string()),
    }
    Ok(pb::Probe { mc_seqno: mc_id.seq_no, clock_matches, results: lines })
}

fn describe(result: CallResult) -> String {
    match &result.outcome {
        Some(Outcome::Error(error)) => format!("error {}: {}", error.code().as_str_name(), error.message),
        Some(Outcome::Account(snapshot)) => {
            let kind = match snapshot.value.as_ref().and_then(|v| v.account.as_ref()).and_then(|a| a.kind.as_ref()) {
                Some(substreams_ton::types::account::Kind::AccountNone(_)) => "none",
                Some(substreams_ton::types::account::Kind::Account(_)) => "existing",
                None => "unset",
            };
            let seqno = snapshot.block.as_ref().map_or(0, |b| b.seq_no);
            format!("account {kind} in block {seqno}")
        }
        Some(Outcome::GetMethod(_)) => match result.get_method() {
            Ok(out) => format!("get_method exit {} first {:?}", out.exit_code, out.stack.int(0).ok()),
            Err(e) => format!("get_method: {e}"),
        },
        Some(Outcome::Emulate(_)) => match result.emulate() {
            Ok(EmulationOutcome::Executed(executed)) => {
                format!("executed lt {}", executed.transaction.map_or(0, |t| t.lt))
            }
            Ok(EmulationOutcome::ExternalNotAccepted(rejected)) => {
                format!("external not accepted, exit {}", rejected.vm_exit_code)
            }
            Err(e) => format!("emulate: {e}"),
        },
        None => "no outcome".to_string(),
    }
}
