//! The `ton_v1` extension: account states, get-methods and the emulation of
//! one transaction, all on the masterchain block the module is processing.
//!
//! Calls go in batches: a batch is one call of the host, however many calls
//! it holds, which the host splits into requests to the router as it needs.
//! The calls of a batch are independent, and every call gets its own result,
//! in the order of the calls.

use std::fmt;

use crate::pb::ton::state::v1 as state;
use crate::pb::ton::tvm::v1::{
    self as pb, account_source, call, call_result, emulate_result, get_method, ExecuteRequest, ExecuteResponse,
};
use crate::Address;

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
        use state::at::Point;
        let point = match at {
            At::AfterBlock => Point::AfterBlock(()),
            At::BeforeBlock => Point::BeforeBlock(()),
            At::BeforeTx(lt) => Point::BeforeTx(lt),
            At::AfterTx(lt) => Point::AfterTx(lt),
        };
        state::At { point: Some(point) }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    /// The call was answered with a `CallError`.
    Call { code: pb::ErrorCode, message: String },
    /// The answer does not follow the protocol, or is not the one asked for.
    Protocol(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Call { code, message } => write!(f, "{}: {message}", code.as_str_name()),
            Error::Protocol(message) => write!(f, "ton_v1 protocol: {message}"),
        }
    }
}

impl std::error::Error for Error {}

/// What an emulation gave.
#[derive(Debug, Clone, PartialEq)]
pub enum EmulationOutcome {
    Executed(pb::ExecutedTransaction),
    ExternalNotAccepted(pb::ExternalNotAccepted),
}

impl pb::CallResult {
    pub fn get_method(self) -> Result<pb::GetMethodResult, Error> {
        match self.outcome {
            Some(call_result::Outcome::GetMethod(result)) => Ok(result),
            other => Err(unexpected(other, "get_method")),
        }
    }

    pub fn emulate(self) -> Result<EmulationOutcome, Error> {
        let result = match self.outcome {
            Some(call_result::Outcome::Emulate(result)) => result,
            other => return Err(unexpected(other, "emulate")),
        };
        match result.outcome {
            Some(emulate_result::Outcome::Executed(executed)) => Ok(EmulationOutcome::Executed(executed)),
            Some(emulate_result::Outcome::ExternalNotAccepted(rejected)) => {
                Ok(EmulationOutcome::ExternalNotAccepted(rejected))
            }
            None => Err(Error::Protocol("emulate result without outcome".to_string())),
        }
    }

    pub fn account(self) -> Result<state::AccountSnapshot, Error> {
        match self.outcome {
            Some(call_result::Outcome::Account(snapshot)) => Ok(snapshot),
            other => Err(unexpected(other, "account")),
        }
    }
}

fn unexpected(outcome: Option<call_result::Outcome>, expected: &str) -> Error {
    match outcome {
        Some(call_result::Outcome::Error(error)) => Error::Call { code: error.code(), message: error.message },
        Some(call_result::Outcome::GetMethod(_)) => Error::Protocol(format!("get_method result, not {expected}")),
        Some(call_result::Outcome::Emulate(_)) => Error::Protocol(format!("emulate result, not {expected}")),
        Some(call_result::Outcome::Account(_)) => Error::Protocol(format!("account result, not {expected}")),
        None => Error::Protocol("call result without outcome".to_string()),
    }
}

/// Executes calls in one call of the host. The results are those of the
/// calls, in their order.
pub fn execute(calls: Vec<pb::Call>) -> Result<Vec<pb::CallResult>, Error> {
    let count = calls.len();
    if count == 0 {
        return Ok(Vec::new());
    }
    let request = prost::Message::encode_to_vec(&ExecuteRequest { calls });
    let response: ExecuteResponse = prost::Message::decode(host::execute(&request).as_slice())
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
    calls: Vec<pb::Call>,
}

pub fn batch() -> Batch {
    Batch::default()
}

impl Batch {
    pub fn add(&mut self, call: impl Into<pb::Call>) -> &mut Self {
        self.calls.push(call.into());
        self
    }

    /// A get-method of the account after the block.
    pub fn get_method(&mut self, address: &Address, name: &str) -> &mut Self {
        self.add(get_method(address, name))
    }

    /// The account after the block.
    pub fn get_account(&mut self, address: &Address) -> &mut Self {
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
pub fn get_accounts(addresses: &[Address]) -> Result<Vec<state::AccountSnapshot>, Error> {
    let calls = addresses.iter().map(|address| get_account(address).into()).collect();
    execute(calls)?.into_iter().map(pb::CallResult::account).collect()
}

/// A read of an account, after the block unless [`GetAccountCall::at`] is set.
#[derive(Debug, Clone)]
pub struct GetAccountCall(pb::GetAccount);

pub fn get_account(address: &Address) -> GetAccountCall {
    GetAccountCall(pb::GetAccount { account: Some(account_ref(Some(address), None)) })
}

impl GetAccountCall {
    pub fn at(mut self, at: At) -> Self {
        if let Some(account) = self.0.account.as_mut() {
            account.at = Some(at.into());
        }
        self
    }

    pub fn execute(self) -> Result<state::AccountSnapshot, Error> {
        execute_one(self)?.account()
    }
}

impl From<GetAccountCall> for pb::Call {
    fn from(call: GetAccountCall) -> Self {
        pb::Call { kind: Some(call::Kind::GetAccount(call.0)) }
    }
}

/// A get-method, on the network state of the account after the block unless
/// [`GetMethodCall::at`] is set, or on a given state.
#[derive(Debug, Clone)]
pub struct GetMethodCall(pb::GetMethod);

pub fn get_method(address: &Address, name: &str) -> GetMethodCall {
    GetMethodCall(pb::GetMethod {
        account: Some(onchain(Some(address), None)),
        method: Some(get_method::Method::Name(name.to_string())),
        ..Default::default()
    })
}

impl GetMethodCall {
    /// On a full TL-B ShardAccount instead of the network state.
    pub fn on_shard_account(shard_account_boc: Vec<u8>, name: &str) -> Self {
        GetMethodCall(pb::GetMethod {
            account: Some(pb::AccountSource { source: Some(account_source::Source::ShardAccountBoc(shard_account_boc)) }),
            method: Some(get_method::Method::Name(name.to_string())),
            ..Default::default()
        })
    }

    pub fn with_id(mut self, id: i32) -> Self {
        self.0.method = Some(get_method::Method::Id(id));
        self
    }

    /// The point of the network state. Panics on a call on a given state, to
    /// which no point applies.
    pub fn at(mut self, at: At) -> Self {
        set_at(&mut self.0.account, at);
        self
    }

    /// Pushes an argument: the first one is at the bottom of the stack.
    pub fn arg(mut self, value: pb::StackEntry) -> Self {
        self.0.stack.push(value);
        self
    }

    pub fn gas_limit(mut self, gas: u64) -> Self {
        self.0.gas_limit = Some(gas);
        self
    }

    /// Overrides the context at N. `lt` sets both BLOCKLT and LTIME;
    /// `block_rand_seed` is a nonzero 32-byte block seed, from which the
    /// service derives this account's TVM seed using the config's version.
    pub fn context(mut self, context: pb::ContextOverrides) -> Self {
        self.0.context = Some(context);
        self
    }

    pub fn vm_log(mut self) -> Self {
        self.0.vm_log = true;
        self
    }

    pub fn execute(self) -> Result<pb::GetMethodResult, Error> {
        execute_one(self)?.get_method()
    }
}

impl From<GetMethodCall> for pb::Call {
    fn from(call: GetMethodCall) -> Self {
        pb::Call { kind: Some(call::Kind::GetMethod(call.0)) }
    }
}

/// An emulation of one transaction of the message destination, on its network
/// state after the block unless set otherwise.
#[derive(Debug, Clone)]
pub struct EmulateCall(pb::Emulate);

pub fn emulate(message_boc: Vec<u8>) -> EmulateCall {
    EmulateCall(pb::Emulate { message_boc, ..Default::default() })
}

impl EmulateCall {
    /// The point of the network state of the destination; the service finds the
    /// destination in the message. Panics after [`EmulateCall::shard_account`].
    pub fn at(mut self, at: At) -> Self {
        if self.0.account.is_none() {
            self.0.account = Some(onchain(None, None));
        }
        set_at(&mut self.0.account, at);
        self
    }

    /// On a full TL-B ShardAccount instead of the network state.
    pub fn shard_account(mut self, shard_account_boc: Vec<u8>) -> Self {
        self.0.account = Some(pb::AccountSource { source: Some(account_source::Source::ShardAccountBoc(shard_account_boc)) });
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
        self.0.context = Some(context);
        self
    }

    pub fn vm_log(mut self) -> Self {
        self.0.vm_log = true;
        self
    }

    pub fn execute(self) -> Result<EmulationOutcome, Error> {
        execute_one(self)?.emulate()
    }
}

impl From<EmulateCall> for pb::Call {
    fn from(call: EmulateCall) -> Self {
        pb::Call { kind: Some(call::Kind::Emulate(call.0)) }
    }
}

fn execute_one(call: impl Into<pb::Call>) -> Result<pb::CallResult, Error> {
    let mut results = execute(vec![call.into()])?;
    Ok(results.remove(0))
}

fn account_ref(address: Option<&Address>, at: Option<At>) -> state::AccountRef {
    state::AccountRef { address: address.map(Address::to_pb), at: at.map(Into::into) }
}

fn onchain(address: Option<&Address>, at: Option<At>) -> pb::AccountSource {
    pb::AccountSource { source: Some(account_source::Source::Onchain(account_ref(address, at))) }
}

fn set_at(account: &mut Option<pb::AccountSource>, at: At) {
    match account.as_mut().and_then(|account| account.source.as_mut()) {
        Some(account_source::Source::Onchain(account)) => account.at = Some(at.into()),
        _ => panic!("At applies to network states only, not to a given ShardAccount"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emulate_at_leaves_the_destination_to_the_service() {
        let call: pb::Call = emulate(vec![1]).at(At::BeforeTx(7)).into();
        let Some(call::Kind::Emulate(emulate)) = call.kind else { panic!() };
        let Some(account_source::Source::Onchain(account)) = emulate.account.unwrap().source else { panic!() };
        assert_eq!(account.address, None);
        assert_eq!(account.at, Some(At::BeforeTx(7).into()));
    }

    #[test]
    fn errors_of_calls_are_kept() {
        let result = pb::CallResult {
            outcome: Some(call_result::Outcome::Error(pb::CallError {
                code: pb::ErrorCode::AccountNotActive as i32,
                message: "none".to_string(),
            })),
        };
        assert_eq!(
            result.get_method(),
            Err(Error::Call { code: pb::ErrorCode::AccountNotActive, message: "none".to_string() })
        );
    }

    #[test]
    #[should_panic]
    fn at_does_not_apply_to_given_states() {
        let _ = GetMethodCall::on_shard_account(vec![1], "seqno").at(At::BeforeBlock);
    }
}
