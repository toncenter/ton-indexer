//! rston, the TON library of acton, in a module: the BOCs ton_v1 gives are
//! parsed by it and compared with the protobuf conversion of the executor.
//! For the accounts that changed in the round: the ShardAccount of
//! get_account (address, balance, last transaction, state, code hash), the
//! address in its base64 forms and back, and an internal message rston makes,
//! emulated on the account: the BOC of the transaction (hash, lt, outbound
//! messages, fees, the inbound message itself).

use std::collections::BTreeSet;
use std::iter::once;

use rston::models::{
    Account, AccountState, CurrencyCollection, IntAddr, IntMsgInfo, Message, MsgInfo, ShardAccount, StdAddr,
    StdAddrFormat, Transaction,
};
use rston::prelude::*;
use substreams::errors::Error;
use substreams_ton::pb::ton::tvm::v1::__buffa::oneof::{call_result::Outcome, emulate_result};
use substreams_ton::tvm;
use substreams_ton::types::__buffa::oneof::{account, account_state, msg_address_int};
use substreams_ton::types::{account::Existing, BlockLazyView};
use substreams_ton::Address;

use crate::pb;

const MAX_ACCOUNTS: usize = 8;
const MAX_DETAILS: usize = 8;
/// The source of the internal messages.
const SOURCE: [u8; 32] = [0x22; 32];

/// A bounceable internal message of 1 TON with no body, made by rston.
fn internal(address: &Address) -> Result<Cell, Error> {
    let message = Message {
        info: MsgInfo::Int(IntMsgInfo {
            bounce: true,
            src: IntAddr::Std(StdAddr::new(0, HashBytes(SOURCE))),
            dst: IntAddr::Std(StdAddr::new(address.workchain as i8, HashBytes(address.account_id))),
            value: CurrencyCollection::new(1_000_000_000),
            ..Default::default()
        }),
        init: None,
        body: Default::default(),
        layout: None,
    };
    CellBuilder::build_from(message).map_err(|e| Error::msg(format!("cannot build a message: {e}")))
}

struct Checks(pb::RstonCheck);

impl Checks {
    fn check(&mut self, ok: bool, message: impl Into<String>) {
        if !ok {
            self.0.mismatches += 1;
            if self.0.details.len() < MAX_DETAILS {
                self.0.details.push(message.into());
            }
        }
    }
}

#[substreams::handlers::map]
fn map_rston_check(block: &BlockLazyView<'_>) -> Result<pb::RstonCheck, Error> {
    let block = substreams_ton::block(block)?;
    let mc = block.masterchain_block.as_option().ok_or_else(|| Error::msg("no masterchain block"))?;
    let mut checks =
        Checks(pb::RstonCheck { mc_seqno: mc.id.as_option().map_or(0, |id| id.seq_no), ..Default::default() });

    let mut addresses = Vec::new();
    let mut seen = BTreeSet::new();
    for data in once(mc).chain(block.shard_blocks.iter()) {
        let workchain = data.id.as_option().and_then(|id| id.shard_id.as_option()).map_or(0, |s| s.workchain_id);
        for account_block in data.extra.as_option().into_iter().flat_map(|extra| extra.account_blocks.iter()) {
            let Ok(id) = account_block.account_addr.as_slice().try_into() else { continue };
            let address = Address::new(workchain, id);
            if addresses.len() < MAX_ACCOUNTS && seen.insert(address) {
                addresses.push(address);
            }
        }
    }

    let mut batch = tvm::batch();
    let mut messages = Vec::new();
    for address in &addresses {
        let message = internal(address)?;
        batch.get_account(address);
        batch.add(tvm::emulate(Boc::encode(&message)).return_state());
        messages.push(message);
    }
    let results = batch.execute()?;
    for ((address, message), pair) in addresses.iter().zip(&messages).zip(results.chunks(2)) {
        let label = address.to_string();
        let snapshot = pair[0].clone().account()?;
        let value = snapshot.value.into_option().unwrap_or_default();
        if let Some(boc) = &value.shard_account_boc {
            checks.0.accounts += 1;
            check_shard_account(&mut checks, &label, address, boc, &value);
        }
        if let Some(Outcome::Emulate(emulate)) = &pair[1].outcome {
            if let Some(emulate_result::Outcome::Executed(executed)) = &emulate.outcome {
                checks.0.transactions += 1;
                check_transaction(&mut checks, &label, address, message, executed);
            }
        }
    }
    Ok(checks.0)
}

fn check_shard_account(
    checks: &mut Checks,
    label: &str,
    address: &Address,
    boc: &[u8],
    value: &substreams_ton::pb::ton::state::v1::AccountValue,
) {
    let cell = match Boc::decode(boc) {
        Ok(cell) => cell,
        Err(e) => return checks.check(false, format!("{label}: ShardAccount BOC: {e}")),
    };
    let shard_account = match cell.parse::<ShardAccount>() {
        Ok(shard_account) => shard_account,
        Err(e) => return checks.check(false, format!("{label}: ShardAccount does not parse: {e}")),
    };
    checks.check(
        shard_account.last_trans_lt == value.last_trans_lt
            && shard_account.last_trans_hash.as_slice() == value.last_trans_hash.as_slice(),
        format!("{label}: last transaction"),
    );
    let account = match shard_account.load_account() {
        Ok(account) => account,
        Err(e) => return checks.check(false, format!("{label}: Account does not parse: {e}")),
    };
    match (&account, value.account.as_option().and_then(|a| a.kind.as_ref())) {
        (None, Some(account::Kind::AccountNone(_))) => {}
        (Some(account), Some(account::Kind::Account(existing))) => {
            check_account(checks, label, address, account, existing)
        }
        _ => checks.check(false, format!("{label}: none in one and not in the other")),
    }
}

fn check_account(checks: &mut Checks, label: &str, address: &Address, account: &Account, existing: &Existing) {
    let IntAddr::Std(std) = &account.address else {
        return checks.check(false, format!("{label}: not a standard address"));
    };
    let proto_address = match existing.addr.as_option().and_then(|a| a.kind.as_ref()) {
        Some(msg_address_int::Kind::AddrStd(std)) => Some((std.workchain_id, std.address.clone())),
        _ => None,
    };
    checks.check(
        proto_address == Some((std.workchain as i32, std.address.as_slice().to_vec()))
            && std.workchain as i32 == address.workchain
            && std.address.as_slice() == address.account_id,
        format!("{label}: address"),
    );
    checks.check(
        std.to_string().eq_ignore_ascii_case(&address.to_string()),
        format!("{label}: raw form {std} vs {address}"),
    );
    // Both base64 forms, parsed back by rston with the flags they were made with.
    for bounceable in [true, false] {
        let text = std.display_base64_url(bounceable).to_string();
        let back = StdAddr::from_str_ext(&text, StdAddrFormat::any());
        checks.check(
            text.len() == 48 && back.as_ref().is_ok_and(|(a, flags)| a == std && flags.bounceable == bounceable),
            format!("{label}: base64 {text} does not parse back"),
        );
    }
    let storage = existing.storage.as_option();
    checks.check(
        storage.is_some_and(|s| {
            s.last_trans_lt == account.last_trans_lt
                && s.balance.as_option().map_or(0, |b| b.grams) as u128 == account.balance.tokens.into_inner()
        }),
        format!("{label}: storage"),
    );
    let proto_state = storage.and_then(|s| s.state.as_option()).and_then(|s| s.kind.as_ref());
    let same_state = match (&account.state, proto_state) {
        (AccountState::Uninit, Some(account_state::Kind::AccountUninit(_))) => true,
        (AccountState::Frozen(hash), Some(account_state::Kind::AccountFrozen(frozen))) => {
            hash.as_slice() == frozen.state_hash.as_slice()
        }
        (AccountState::Active(init), Some(account_state::Kind::AccountActive(proto))) => {
            let code = init.code.as_ref().map(|c| c.repr_hash().as_slice().to_vec());
            code == proto.code.as_option().map(|c| c.hash.clone())
        }
        _ => false,
    };
    checks.check(same_state, format!("{label}: state"));
}

fn check_transaction(
    checks: &mut Checks,
    label: &str,
    address: &Address,
    message: &Cell,
    executed: &substreams_ton::pb::ton::tvm::v1::ExecutedTransaction,
) {
    let cell = match Boc::decode(&executed.transaction_boc) {
        Ok(cell) => cell,
        Err(e) => return checks.check(false, format!("{label}: transaction BOC: {e}")),
    };
    let tx = match cell.parse::<Transaction>() {
        Ok(tx) => tx,
        Err(e) => return checks.check(false, format!("{label}: Transaction does not parse: {e}")),
    };
    let Some(proto) = executed.transaction.as_option() else {
        return checks.check(false, format!("{label}: no transaction"));
    };
    checks.check(cell.repr_hash().as_slice() == proto.hash.as_slice(), format!("{label}: transaction hash"));
    checks.check(
        tx.account.as_slice() == address.account_id
            && tx.lt == proto.lt
            && tx.prev_trans_lt == proto.prev_trans_lt
            && tx.now == proto.now
            && tx.out_msg_count.into_inner() as u32 == proto.outmsg_cnt
            && tx.total_fees.tokens.into_inner() == proto.total_fees.as_option().map_or(0, |f| f.grams) as u128,
        format!("{label}: transaction fields"),
    );
    checks.check(
        tx.in_msg.as_ref().is_some_and(|m| m.repr_hash() == message.repr_hash()),
        format!("{label}: inbound message is not the one rston made"),
    );
    // A BOC rston encodes again is the same tree.
    let again = Boc::decode(Boc::encode(&cell));
    checks.check(again.is_ok_and(|c| c.repr_hash() == cell.repr_hash()), format!("{label}: BOC round trip"));
}
