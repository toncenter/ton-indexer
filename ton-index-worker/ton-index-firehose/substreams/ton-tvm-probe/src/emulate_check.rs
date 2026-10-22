//! emulate end to end, with reproducible TON/Fift fixtures and on the network
//! state. The fixture contract accepts a message, stores NOW, BLOCKLT, LTIME
//! and RANDSEED, and returns them from a get-method. The fixtures are made by
//! fixtures/emulate.fif.

use sha2::{Digest, Sha256};
use substreams::errors::Error;
use substreams_ton::pb::ton::tvm::v1::{
    call_result::Outcome, emulate_result, stack_entry, Call, CallResult, ContextOverrides, ErrorCode,
    ExecutedTransaction, StackEntry,
};
use substreams_ton::tvm::{self, GetMethodCall};
use substreams_ton::types::Block;
use substreams_ton::{stack, Address, At};

use crate::pb;

/// The fixtures do not depend on the block: they run on one block in so many,
/// the calls on the network state on every block.
const FIXTURE_EVERY: u32 = 100;
const MAX_DETAILS: usize = 8;

const NOW: u32 = 1_700_000_000;
const LT: u64 = 7_000_123;
/// LT rounded down to a multiple of 1,000,000, as TON derives BLOCKLT.
const BLOCK_LT: u64 = 7_000_000;
/// The destination of the fixture messages.
const FIXTURE_ACCOUNT: [u8; 32] = {
    let mut id = [0u8; 32];
    let mut i = 0;
    while i < 32 {
        id[i] = i as u8;
        i += 1;
    }
    id
};

fn fixture(text: &str) -> Vec<u8> {
    hex::decode(text.trim()).expect("valid fixture")
}

/// The context of the fixtures, with config param 8 of a global version.
fn context(version: u32) -> ContextOverrides {
    let config = if version == 7 {
        include_str!("../fixtures/config_v7.boc.hex")
    } else {
        include_str!("../fixtures/config_v8.boc.hex")
    };
    ContextOverrides {
        now: Some(NOW),
        lt: Some(LT),
        block_rand_seed: Some(vec![1; 32]),
        config_params: [(8, fixture(config))].into_iter().collect(),
        ..Default::default()
    }
}

/// RANDSEED as TON derives it from the block seed of [`context`].
fn expected_seed(account_id: &[u8], version: u32) -> StackEntry {
    let mut input = vec![1; 32];
    if version >= 8 {
        input.extend_from_slice(account_id);
    } else {
        input.extend_from_slice(&account_id[..4]);
        input.extend_from_slice(&account_id[..28]);
    }
    let hash = Sha256::digest(input);
    let first = hash.iter().position(|&b| b != 0).unwrap_or(31);
    let mut integer = hash[first..].to_vec();
    if integer[0] & 0x80 != 0 {
        integer.insert(0, 0);
    }
    StackEntry { value: Some(stack_entry::Value::Integer(integer)) }
}

/// What the fixture contract stores and returns: NOW, BLOCKLT, LTIME, RANDSEED.
fn stored_context(block_lt: u64, account_id: &[u8], version: u32) -> Vec<StackEntry> {
    vec![stack::int(NOW), stack::int(block_lt), stack::int(LT), expected_seed(account_id, version)]
}

fn executed(result: &CallResult) -> Option<&ExecutedTransaction> {
    match result.outcome.as_ref()? {
        Outcome::Emulate(value) => match value.outcome.as_ref()? {
            emulate_result::Outcome::Executed(value) => Some(value),
            _ => None,
        },
        _ => None,
    }
}

fn rejected_with(result: &CallResult) -> Option<i32> {
    match result.outcome.as_ref()? {
        Outcome::Emulate(value) => match value.outcome.as_ref()? {
            emulate_result::Outcome::ExternalNotAccepted(rejected) => Some(rejected.vm_exit_code),
            _ => None,
        },
        _ => None,
    }
}

fn is_invalid(result: &CallResult) -> bool {
    matches!(&result.outcome, Some(Outcome::Error(e)) if e.code == ErrorCode::InvalidRequest as i32)
}

fn returns(result: &CallResult, expected: &[StackEntry]) -> bool {
    matches!(&result.outcome, Some(Outcome::GetMethod(r)) if r.exit_code == 0 && r.stack == expected)
}

/// A batch whose results are read by the names of their calls.
#[derive(Default)]
struct Named {
    names: Vec<&'static str>,
    calls: Vec<Call>,
}

impl Named {
    fn add(&mut self, name: &'static str, call: impl Into<Call>) -> &mut Self {
        self.names.push(name);
        self.calls.push(call.into());
        self
    }

    fn execute(self) -> Result<Answers, Error> {
        Ok(Answers { names: self.names, results: tvm::execute(self.calls)? })
    }
}

struct Answers {
    names: Vec<&'static str>,
    results: Vec<CallResult>,
}

impl Answers {
    fn get(&self, name: &str) -> &CallResult {
        let i = self.names.iter().position(|n| *n == name).expect("a call of this name");
        &self.results[i]
    }
}

struct Checks(pb::EmulateCheck);

impl Checks {
    fn check(&mut self, ok: bool, message: &str) {
        if !ok {
            self.0.mismatches += 1;
            if self.0.details.len() < MAX_DETAILS {
                self.0.details.push(message.into());
            }
        }
    }

    fn count(&mut self, results: &[CallResult]) {
        self.0.calls += results.len() as u32;
        for result in results {
            if let Some(tx) = executed(result) {
                self.0.executed += 1;
                self.0.outgoing_messages += tx.transaction.as_ref().map_or(0, |t| t.outmsg_cnt);
            } else if rejected_with(result).is_some() {
                self.0.rejected += 1;
            }
        }
    }
}

#[substreams::handlers::map]
fn map_emulate_check(block: Block) -> Result<pb::EmulateCheck, Error> {
    let mc_seqno = block.masterchain_block.as_ref().and_then(|b| b.id.as_ref()).map_or(0, |b| b.seq_no);
    let mut checks = Checks(pb::EmulateCheck { mc_seqno, ..Default::default() });
    let fixtures = mc_seqno % FIXTURE_EVERY == 0;

    let account = fixture(include_str!("../fixtures/account.boc.hex"));
    let none = fixture(include_str!("../fixtures/none.boc.hex"));
    let external = fixture(include_str!("../fixtures/external.boc.hex"));
    let internal = fixture(include_str!("../fixtures/internal.boc.hex"));

    let mut batch = Named::default();
    // The network state of an address, after the block and before it, in the
    // same public context of N; a missing account source is the destination.
    let address = Address::new(0, FIXTURE_ACCOUNT);
    batch
        .add("state after", tvm::get_account(&address))
        .add("emulate after", tvm::emulate(internal.clone()).context(context(8)).return_state())
        .add("state before", tvm::get_account(&address).at(At::BeforeBlock))
        .add(
            "emulate before",
            tvm::emulate(internal.clone()).context(context(8)).return_state().at(At::BeforeBlock),
        )
        .add("elector", tvm::emulate(fixture(include_str!("../fixtures/elector_external.boc.hex"))));
    if fixtures {
        // A valid BOC that cannot be a config address must be a call error,
        // never Unavailable (which would make the host retry this batch).
        let mut invalid_config = context(8);
        invalid_config.config_params.insert(0, fixture(include_str!("../fixtures/library_ref.boc.hex")));
        let accept = tvm::emulate(external.clone()).shard_account(account.clone()).context(context(8));
        let signed = tvm::emulate(external.clone())
            .shard_account(fixture(include_str!("../fixtures/signature_account.boc.hex")))
            .context(context(8));
        batch
            .add("accept", accept.clone().return_state().vm_log())
            .add("accept again", accept.clone().return_state().vm_log())
            .add("accept plain", accept.clone())
            .add(
                "reject",
                tvm::emulate(external.clone())
                    .shard_account(fixture(include_str!("../fixtures/rejected_account.boc.hex")))
                    .context(context(8))
                    .vm_log(),
            )
            .add(
                "bounce",
                tvm::emulate(internal.clone()).shard_account(none.clone()).context(context(8)).return_state(),
            )
            .add(
                "deploy",
                tvm::emulate(fixture(include_str!("../fixtures/deploy.boc.hex")))
                    .shard_account(none)
                    .context(context(8))
                    .return_state(),
            )
            .add("invalid BOC", tvm::emulate(vec![0xb5]))
            .add("invalid config address emulate", accept.clone().context(invalid_config.clone()))
            .add(
                "invalid config address get",
                GetMethodCall::on_shard_account(account.clone(), "").with_id(42).context(invalid_config),
            )
            .add("LT overflow", accept.clone().context(ContextOverrides { lt: Some(u64::MAX), ..context(8) }))
            .add(
                "zero seed",
                accept.clone().context(ContextOverrides { block_rand_seed: Some(vec![0; 32]), ..context(8) }),
            )
            .add("bad signature", signed.clone())
            .add("ignore chksig", signed.ignore_chksig())
            .add(
                "extra library",
                tvm::emulate(external.clone())
                    .shard_account(fixture(include_str!("../fixtures/library_account.boc.hex")))
                    .context(ContextOverrides {
                        libraries: vec![fixture(include_str!("../fixtures/library.boc.hex"))],
                        ..context(8)
                    })
                    .return_state(),
            )
            .add("version 7", accept.context(context(7)).return_state())
            .add("get version 7", GetMethodCall::on_shard_account(account.clone(), "").with_id(42).context(context(7)))
            .add("get version 8", GetMethodCall::on_shard_account(account, "").with_id(42).context(context(8)));
    }
    let first = batch.execute()?;
    checks.count(&first.results);

    checks.check(
        rejected_with(first.get("elector")).is_some(),
        "empty external message to the onchain elector did not reach VM rejection",
    );
    if fixtures {
        checks.check(first.get("accept") == first.get("accept again"), "identical independent emulations differ");
        checks.check(executed(first.get("accept")).is_some(), "accepting contract produced no transaction");
        checks.check(
            executed(first.get("accept"))
                .and_then(|e| e.transaction.as_ref())
                .and_then(|t| t.in_msg.as_ref())
                .is_some_and(|m| m.hash_norm.as_ref().is_some_and(|norm| *norm != m.hash)),
            "external message has no normalized hash",
        );
        if let (Some(full), Some(plain)) = (executed(first.get("accept")), executed(first.get("accept plain"))) {
            checks.check(full.transaction_boc == plain.transaction_boc, "return_state/vm_log changed transaction");
            checks.check(plain.account_after.is_none() && plain.vm_log.is_empty(), "optional output flags ignored");
            checks.check(!full.vm_log.is_empty(), "requested VM log is empty");
        }
        checks.check(
            rejected_with(first.get("reject")) == Some(42),
            "external rejection did not preserve exit code 42",
        );
        checks.check(
            executed(first.get("bounce")).and_then(|e| e.transaction.as_ref()).is_some_and(|t| t.outmsg_cnt == 1),
            "message to account_none did not return one bounce message",
        );
        checks.check(executed(first.get("deploy")).is_some(), "StateInit deployment produced no transaction");
        for name in [
            "invalid BOC", "LT overflow", "zero seed", "invalid config address emulate", "invalid config address get",
        ] {
            checks.check(is_invalid(first.get(name)), name);
        }
        checks.check(rejected_with(first.get("bad signature")) == Some(43), "invalid signature was not rejected");
        checks.check(executed(first.get("ignore chksig")).is_some(), "ignore_chksig did not accept the signature");
        checks.check(
            executed(first.get("extra library")).is_some(),
            "extra library was not available to the transaction VM",
        );
        checks.check(executed(first.get("version 7")).is_some(), "version 7 produced no transaction");
        for (name, version) in [("get version 7", 7), ("get version 8", 8)] {
            checks.check(
                returns(first.get(name), &stored_context(LT, &FIXTURE_ACCOUNT, version)),
                "get-method seed or LT differs from native semantics",
            );
        }
    }

    // States given back: the emulator's new states read by a get-method of
    // the contract and run on again, and the network states run on as BOCs.
    const READS: [(&str, &str, u32); 3] =
        [("accept", "read accept", 8), ("deploy", "read deploy", 8), ("version 7", "read version 7", 7)];
    let mut second = Named::default();
    if fixtures {
        for (emulated, read, _) in READS {
            let after = executed(first.get(emulated)).and_then(|e| e.account_after.as_ref());
            let Some(boc) = after.and_then(|a| a.shard_account_boc.clone()) else {
                checks.check(false, "emulated state BOC is absent");
                continue;
            };
            second.add(read, GetMethodCall::on_shard_account(boc.clone(), "state"));
            if emulated == "accept" {
                let again = tvm::emulate(external.clone()).shard_account(boc).context(context(8)).return_state();
                second.add("accept on state", again);
            }
        }
    }
    for (state, name) in [("state after", "emulate after on BOC"), ("state before", "emulate before on BOC")] {
        let Some(boc) = first.get(state).clone().account()?.value.and_then(|v| v.shard_account_boc) else {
            checks.check(false, "network state BOC is absent");
            continue;
        };
        second.add(name, tvm::emulate(internal.clone()).shard_account(boc).context(context(8)).return_state());
    }
    let then = second.execute()?;
    checks.count(&then.results);
    checks.0.state_checks += then.results.len() as u32;

    for (emulated, read, version) in READS {
        if !then.names.contains(&read) {
            continue;
        }
        let address = &executed(first.get(emulated)).unwrap().transaction.as_ref().unwrap().account_addr;
        checks.check(
            returns(then.get(read), &stored_context(BLOCK_LT, address, version)),
            "stored TVM context differs from overrides",
        );
    }
    if then.names.contains(&"accept on state") {
        let first_tx = executed(first.get("accept")).and_then(|e| e.transaction.as_ref());
        let next_tx = executed(then.get("accept on state")).and_then(|e| e.transaction.as_ref());
        checks.check(
            first_tx.zip(next_tx).is_some_and(|(a, b)| {
                b.prev_trans_hash == a.hash
                    && b.prev_trans_lt == a.lt
                    && b.lt > a.lt
                    && a.state_update.as_ref().zip(b.state_update.as_ref()).is_some_and(|(x, y)| {
                        x.new_hash == y.old_hash
                    })
            }),
            "returned state lost transaction metadata",
        );
    }
    for (onchain, given) in [("emulate after", "emulate after on BOC"), ("emulate before", "emulate before on BOC")] {
        if then.names.contains(&given) {
            checks.check(first.get(onchain) == then.get(given), "network state and its BOC emulate differently");
        }
    }
    Ok(checks.0)
}
