//! The `ton_v1` extension: account states, get-methods and the emulation of
//! one transaction, all on the masterchain block the module is processing.
//!
//! Calls go in batches: a batch is one call of the host, however many calls
//! it holds, which the host splits into requests to the router as it needs.
//! The calls of a batch are independent, and every call gets its own result,
//! in the order of the calls. A call the SDK cannot make, on an address with
//! anycast, gets INVALID_REQUEST in its place from the SDK itself.

use std::fmt;

use buffa::{DecodeOptions, EnumValue, Enumeration, Inline, Message, MessageField};
use rston::boc::Boc;
use rston::cell::{Cell, Load};
use rston::models::{ShardAccount, StdAddr, Transaction};

use crate::address;
use crate::pb::ton::state::v1::{self as state, __buffa::oneof::at::Point};
use crate::pb::ton::tvm::v1::__buffa::oneof::{account_source, call, emulate_result, get_method};
use crate::pb::ton::tvm::v1::{self as pb, ExecuteRequest, ExecuteResponse};

/// What a call gave: the outcome of a [`pb::CallResult`].
pub use crate::pb::ton::tvm::v1::__buffa::oneof::call_result::Outcome;

/// A point of the block, see `ton.state.v1.At`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum At {
    AfterBlock,
    BeforeBlock,
    BeforeTx(u64),
    AfterTx(u64),
}

impl From<At> for state::At {
    fn from(at: At) -> Self {
        let point = match at {
            At::AfterBlock => Point::AfterBlock(Box::default()),
            At::BeforeBlock => Point::BeforeBlock(Box::default()),
            At::BeforeTx(lt) => Point::BeforeTx(lt),
            At::AfterTx(lt) => Point::AfterTx(lt),
        };
        state::At { point: Some(point), ..Default::default() }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Error {
    /// The call was answered with a `CallError`: by the service, or by the
    /// SDK for a call it cannot make (INVALID_REQUEST).
    Call { code: EnumValue<pb::ErrorCode>, message: String },
    /// The answer does not follow the protocol, or is not the one asked for.
    Protocol(String),
    /// A BOC of the answer does not decode.
    Boc(rston::boc::de::Error),
    /// A cell of the answer is not the TL-B it must be.
    Tlb(rston::error::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Call { code, message } => match code.as_known() {
                Some(code) => write!(f, "{}: {message}", code.proto_name()),
                None => write!(f, "error code {}: {message}", code.to_i32()),
            },
            Error::Protocol(message) => write!(f, "ton_v1 protocol: {message}"),
            Error::Boc(error) => write!(f, "BOC of the answer: {error}"),
            Error::Tlb(error) => write!(f, "TL-B of the answer: {error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Boc(error) => Some(error),
            Error::Tlb(error) => Some(error),
            _ => None,
        }
    }
}

/// What an emulation gave.
// Returned once per call: a box of the large variant would only add an
// allocation.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq)]
pub enum EmulationOutcome {
    Executed(pb::ExecutedTransaction),
    ExternalNotAccepted(pb::ExternalNotAccepted),
}

impl pb::CallResult {
    pub fn get_method(self) -> Result<pb::GetMethodResult, Error> {
        match self.outcome {
            Some(Outcome::GetMethod(result)) => Ok(*result),
            other => Err(unexpected(other, "get_method")),
        }
    }

    pub fn emulate(self) -> Result<EmulationOutcome, Error> {
        let result = match self.outcome {
            Some(Outcome::Emulate(result)) => result,
            other => return Err(unexpected(other, "emulate")),
        };
        match result.outcome {
            Some(emulate_result::Outcome::Executed(executed)) => Ok(EmulationOutcome::Executed(*executed)),
            Some(emulate_result::Outcome::ExternalNotAccepted(rejected)) => {
                Ok(EmulationOutcome::ExternalNotAccepted(*rejected))
            }
            None => Err(Error::Protocol("emulate result without outcome".to_string())),
        }
    }

    pub fn account(self) -> Result<state::AccountSnapshot, Error> {
        match self.outcome {
            Some(Outcome::Account(snapshot)) => Ok(*snapshot),
            other => Err(unexpected(other, "account")),
        }
    }
}

fn unexpected(outcome: Option<Outcome>, expected: &str) -> Error {
    match outcome {
        Some(Outcome::Error(error)) => Error::Call { code: error.code, message: error.message },
        Some(Outcome::GetMethod(_)) => Error::Protocol(format!("get_method result, not {expected}")),
        Some(Outcome::Emulate(_)) => Error::Protocol(format!("emulate result, not {expected}")),
        Some(Outcome::Account(_)) => Error::Protocol(format!("account result, not {expected}")),
        None => Error::Protocol("call result without outcome".to_string()),
    }
}

impl state::AccountValue {
    /// The TL-B ShardAccount, parsed by rston; its `load_account()` is none
    /// for an account that does not exist. None when the service did not give
    /// the BOC, which is over its limit (`boc_omitted`).
    pub fn parse_shard_account(&self) -> Result<Option<ShardAccount>, Error> {
        match &self.shard_account_boc {
            Some(boc) => parse(boc).map(Some),
            None if self.boc_omitted => Ok(None),
            None => Err(Error::Protocol("account value without its BOC".to_string())),
        }
    }
}

impl pb::ExecutedTransaction {
    /// The cell of the transaction, whose hash is that of the transaction.
    pub fn transaction_cell(&self) -> Result<Cell, Error> {
        Boc::decode(&self.transaction_boc).map_err(Error::Boc)
    }

    /// The TL-B Transaction, parsed by rston.
    pub fn parse_transaction(&self) -> Result<Transaction, Error> {
        parse(&self.transaction_boc)
    }
}

fn parse<T: for<'a> Load<'a>>(boc: &[u8]) -> Result<T, Error> {
    Boc::decode(boc).map_err(Error::Boc)?.parse::<T>().map_err(Error::Tlb)
}

/// A call to execute, or why the SDK cannot make it.
#[derive(Debug, Clone)]
pub struct Call(Result<pb::Call, String>);

impl From<pb::Call> for Call {
    fn from(call: pb::Call) -> Self {
        Call(Ok(call))
    }
}

/// Executes calls in one call of the host. The results are those of the
/// calls, in their order.
pub fn execute(calls: Vec<Call>) -> Result<Vec<pb::CallResult>, Error> {
    let mut request = Vec::new();
    let mut local = Vec::with_capacity(calls.len());
    for Call(call) in calls {
        match call {
            Ok(call) => {
                request.push(call);
                local.push(None);
            }
            Err(message) => local.push(Some(invalid(message))),
        }
    }
    let mut answers = execute_on_host(request)?.into_iter();
    Ok(local.into_iter().map(|result| result.unwrap_or_else(|| answers.next().expect("an answer per call"))).collect())
}

/// The answer of the service to a call it cannot take.
fn invalid(message: String) -> pb::CallResult {
    let error = pb::CallError { code: pb::ErrorCode::InvalidRequest.into(), message, ..Default::default() };
    pb::CallResult { outcome: Some(Outcome::Error(Box::new(error))), ..Default::default() }
}

fn execute_on_host(calls: Vec<pb::Call>) -> Result<Vec<pb::CallResult>, Error> {
    let count = calls.len();
    if count == 0 {
        return Ok(Vec::new());
    }
    let request = ExecuteRequest { calls, ..Default::default() }.encode_to_vec();
    // The host bounds every result (MaxCallResultSize of ton_v1). The budget
    // that buffa keeps against untrusted input would only refuse valid
    // answers: a stack of many small values takes far more memory than bytes.
    let response: ExecuteResponse = DecodeOptions::new()
        .with_element_memory_limit(usize::MAX)
        .decode_from_slice(&host::execute(&request))
        .map_err(|e| Error::Protocol(format!("undecodable response: {e}")))?;
    if response.results.len() != count {
        return Err(Error::Protocol(format!("{} results for {count} calls", response.results.len())));
    }
    Ok(response.results)
}

#[cfg(target_arch = "wasm32")]
mod host {
    #[link(wasm_import_module = "ton_v1")]
    extern "C" {
        #[link_name = "execute"]
        fn execute_extern(request_ptr: *const u8, request_len: u32, output_ptr: u32);
    }

    /// One call of the host: the encoded ExecuteRequest in, the encoded
    /// ExecuteResponse out.
    pub fn execute(request: &[u8]) -> Vec<u8> {
        unsafe {
            let output_ptr = substreams::memory::alloc(8);
            execute_extern(request.as_ptr(), request.len() as u32, output_ptr as u32);
            let response = substreams::memory::get_output_data(output_ptr);
            substreams::memory::dealloc(output_ptr, 8);
            response
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
mod host {
    pub fn execute(_request: &[u8]) -> Vec<u8> {
        panic!("ton_v1 is an import of substreams modules, compiled for wasm32")
    }
}

/// Calls collected to be executed together.
#[derive(Debug, Default, Clone)]
pub struct Batch {
    calls: Vec<Call>,
}

pub fn batch() -> Batch {
    Batch::default()
}

impl Batch {
    pub fn add(&mut self, call: impl Into<Call>) -> &mut Self {
        self.calls.push(call.into());
        self
    }

    /// A get-method of the account after the block.
    pub fn get_method(&mut self, address: &StdAddr, name: &str) -> &mut Self {
        self.add(get_method(address, name))
    }

    /// The account after the block.
    pub fn get_account(&mut self, address: &StdAddr) -> &mut Self {
        self.add(get_account(address))
    }

    pub fn len(&self) -> usize {
        self.calls.len()
    }

    pub fn is_empty(&self) -> bool {
        self.calls.is_empty()
    }

    pub fn execute(self) -> Result<Vec<pb::CallResult>, Error> {
        execute(self.calls)
    }
}

/// The accounts after the block, in the order of the addresses.
pub fn get_accounts(addresses: &[StdAddr]) -> Result<Vec<state::AccountSnapshot>, Error> {
    let calls = addresses.iter().map(|address| get_account(address).into()).collect();
    execute(calls)?.into_iter().map(pb::CallResult::account).collect()
}

/// A read of an account, after the block unless [`GetAccountCall::at`] is set.
#[derive(Debug, Clone)]
pub struct GetAccountCall(Result<pb::GetAccount, String>);

pub fn get_account(address: &StdAddr) -> GetAccountCall {
    GetAccountCall(
        address::to_pb(address)
            .map(|address| pb::GetAccount { account: account_ref(Some(address), None).into(), ..Default::default() }),
    )
}

impl GetAccountCall {
    pub fn at(mut self, at: At) -> Self {
        if let Ok(call) = &mut self.0 {
            if let Some(account) = call.account.as_option_mut() {
                account.at = MessageField::some(at.into());
            }
        }
        self
    }

    pub fn execute(self) -> Result<state::AccountSnapshot, Error> {
        execute_one(self)?.account()
    }
}

impl From<GetAccountCall> for Call {
    fn from(call: GetAccountCall) -> Self {
        Call(call.0.map(|call| pb::Call { kind: Some(call::Kind::GetAccount(Box::new(call))), ..Default::default() }))
    }
}

/// A get-method, on the network state of the account after the block unless
/// [`GetMethodCall::at`] is set, or on a given state.
#[derive(Debug, Clone)]
pub struct GetMethodCall(Result<pb::GetMethod, String>);

pub fn get_method(address: &StdAddr, name: &str) -> GetMethodCall {
    GetMethodCall(address::to_pb(address).map(|address| pb::GetMethod {
        account: onchain(Some(address), None).into(),
        method: Some(get_method::Method::Name(name.to_string())),
        ..Default::default()
    }))
}

impl GetMethodCall {
    /// On a full TL-B ShardAccount instead of the network state.
    pub fn on_shard_account(shard_account_boc: Vec<u8>, name: &str) -> Self {
        GetMethodCall(Ok(pb::GetMethod {
            account: given(shard_account_boc).into(),
            method: Some(get_method::Method::Name(name.to_string())),
            ..Default::default()
        }))
    }

    pub fn with_id(self, id: i32) -> Self {
        self.edit(|call| call.method = Some(get_method::Method::Id(id)))
    }

    /// The point of the network state. Panics on a call on a given state, to
    /// which no point applies.
    pub fn at(self, at: At) -> Self {
        self.edit(|call| set_at(&mut call.account, at))
    }

    /// Pushes an argument: the first one is at the bottom of the stack.
    pub fn arg(self, value: pb::StackEntry) -> Self {
        self.edit(|call| call.stack.push(value))
    }

    pub fn gas_limit(self, gas: u64) -> Self {
        self.edit(|call| call.gas_limit = Some(gas))
    }

    /// Overrides the context at N. `lt` sets both BLOCKLT and LTIME;
    /// `block_rand_seed` is a nonzero 32-byte block seed, from which the
    /// service derives this account's TVM seed using the config's version.
    pub fn context(self, context: pb::ContextOverrides) -> Self {
        self.edit(|call| call.context = MessageField::some(context))
    }

    pub fn execute(self) -> Result<pb::GetMethodResult, Error> {
        execute_one(self)?.get_method()
    }

    fn edit(mut self, edit: impl FnOnce(&mut pb::GetMethod)) -> Self {
        if let Ok(call) = &mut self.0 {
            edit(call);
        }
        self
    }
}

impl From<GetMethodCall> for Call {
    fn from(call: GetMethodCall) -> Self {
        Call(call.0.map(|call| pb::Call { kind: Some(call::Kind::GetMethod(Box::new(call))), ..Default::default() }))
    }
}

/// An emulation of one transaction of the message destination, on its network
/// state after the block unless set otherwise.
#[derive(Debug, Clone)]
pub struct EmulateCall(pb::Emulate);

pub fn emulate(message_boc: Vec<u8>) -> EmulateCall {
    EmulateCall(pb::Emulate { message_boc, ..Default::default() })
}

/// An emulation of a message cell, such as one rston builds.
pub fn emulate_message(message: &Cell) -> EmulateCall {
    emulate(Boc::encode(message))
}

impl EmulateCall {
    /// The point of the network state of the destination; the service finds the
    /// destination in the message. Panics after [`EmulateCall::shard_account`].
    pub fn at(mut self, at: At) -> Self {
        if self.0.account.is_unset() {
            self.0.account = MessageField::some(onchain(None, None));
        }
        set_at(&mut self.0.account, at);
        self
    }

    /// On a full TL-B ShardAccount instead of the network state.
    pub fn shard_account(mut self, shard_account_boc: Vec<u8>) -> Self {
        self.0.account = MessageField::some(given(shard_account_boc));
        self
    }

    pub fn ignore_chksig(mut self) -> Self {
        self.0.ignore_chksig = true;
        self
    }

    pub fn return_state(mut self) -> Self {
        self.0.return_state = true;
        self
    }

    /// Overrides the public execution context, never historical replay.
    /// TON rounds the requested `lt` down to a multiple of 1,000,000 for
    /// BLOCKLT and may raise the transaction LT to follow prior events.
    /// `block_rand_seed` is a nonzero 32-byte block seed, not a final TVM seed.
    pub fn context(mut self, context: pb::ContextOverrides) -> Self {
        self.0.context = MessageField::some(context);
        self
    }

    pub fn execute(self) -> Result<EmulationOutcome, Error> {
        execute_one(self)?.emulate()
    }
}

impl From<EmulateCall> for Call {
    fn from(call: EmulateCall) -> Self {
        Call(Ok(pb::Call { kind: Some(call::Kind::Emulate(Box::new(call.0))), ..Default::default() }))
    }
}

fn execute_one(call: impl Into<Call>) -> Result<pb::CallResult, Error> {
    let mut results = execute(vec![call.into()])?;
    Ok(results.remove(0))
}

fn account_ref(address: Option<state::Address>, at: Option<At>) -> state::AccountRef {
    state::AccountRef { address: address.into(), at: at.map(Into::into).into(), ..Default::default() }
}

fn onchain(address: Option<state::Address>, at: Option<At>) -> pb::AccountSource {
    pb::AccountSource {
        source: Some(account_source::Source::Onchain(Box::new(account_ref(address, at)))),
        ..Default::default()
    }
}

fn given(shard_account_boc: Vec<u8>) -> pb::AccountSource {
    pb::AccountSource { source: Some(account_source::Source::ShardAccountBoc(shard_account_boc)), ..Default::default() }
}

fn set_at(account: &mut MessageField<pb::AccountSource, Inline<pb::AccountSource>>, at: At) {
    match account.as_option_mut().and_then(|account| account.source.as_mut()) {
        Some(account_source::Source::Onchain(account)) => account.at = MessageField::some(at.into()),
        _ => panic!("At applies to network states only, not to a given ShardAccount"),
    }
}

#[cfg(test)]
mod tests {
    use rston::cell::{CellBuilder, HashBytes, Lazy};
    use rston::models::{Anycast, OptionalAccount};
    use rston::num::SplitDepth;

    use super::*;

    #[test]
    fn emulate_at_leaves_the_destination_to_the_service() {
        let Call(Ok(call)) = emulate(vec![1]).at(At::BeforeTx(7)).into() else { panic!() };
        let Some(call::Kind::Emulate(emulate)) = call.kind else { panic!() };
        let Some(account_source::Source::Onchain(account)) = emulate.account.unwrap().source else { panic!() };
        assert!(account.address.is_unset());
        assert_eq!(account.at.as_option(), Some(&At::BeforeTx(7).into()));
    }

    #[test]
    fn errors_of_calls_are_kept() {
        let result = invalid("none".to_string());
        let error = result.get_method().unwrap_err();
        assert!(
            matches!(&error, Error::Call { code, message } if *code == pb::ErrorCode::InvalidRequest && message == "none")
        );
        assert_eq!(error.to_string(), "ERROR_CODE_INVALID_REQUEST: none");
    }

    #[test]
    fn anycast_is_answered_by_the_sdk() {
        let anycast = Anycast { depth: SplitDepth::new(1).unwrap(), rewrite_prefix: vec![0x80] };
        let address = StdAddr { anycast: Some(Box::new(anycast)), ..StdAddr::new(0, HashBytes([1; 32])) };
        // No call of the host, which panics off wasm32.
        let results =
            execute(vec![get_account(&address).at(At::BeforeBlock).into(), get_method(&address, "seqno").into()])
                .unwrap();
        for result in results {
            let error = result.account().unwrap_err();
            assert!(matches!(error, Error::Call { code, .. } if code == pb::ErrorCode::InvalidRequest));
        }
    }

    #[test]
    #[should_panic]
    fn at_does_not_apply_to_given_states() {
        let _ = GetMethodCall::on_shard_account(vec![1], "seqno").at(At::BeforeBlock);
    }

    #[test]
    fn shard_accounts_parse() {
        let shard_account = ShardAccount {
            account: Lazy::new(&OptionalAccount::EMPTY).unwrap(),
            last_trans_hash: HashBytes([7; 32]),
            last_trans_lt: 42,
        };
        let boc = Boc::encode(CellBuilder::build_from(&shard_account).unwrap());
        let value = state::AccountValue { shard_account_boc: Some(boc), ..Default::default() };
        let parsed = value.parse_shard_account().unwrap().unwrap();
        assert_eq!((parsed.last_trans_lt, parsed.last_trans_hash), (42, HashBytes([7; 32])));
        assert!(parsed.load_account().unwrap().is_none());

        let omitted = state::AccountValue { boc_omitted: true, ..Default::default() };
        assert!(omitted.parse_shard_account().unwrap().is_none());
        assert!(matches!(state::AccountValue::default().parse_shard_account(), Err(Error::Protocol(_))));
        let broken = state::AccountValue { shard_account_boc: Some(vec![0xb5]), ..Default::default() };
        assert!(matches!(broken.parse_shard_account(), Err(Error::Boc(_))));
    }
}
