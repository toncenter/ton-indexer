//! TON traces: every trace given once, with its transactions, in the
//! masterchain block where it ended.
//!
//! The transactions of a trace are spread over the blocks it took. Every
//! transaction of the TON Firehose carries its trace_id, and every block lists
//! the traces that ended in it (`Block.traces`). So:
//!
//! - `store_trace_size` counts the transactions of every trace not ended yet;
//! - `store_pending` keeps the first `max_transactions` transactions of every
//!   trace under `<trace_id>/<lt>/<hash>` and deletes them when it ends;
//! - `map_traces` reads the deltas of both stores: the deletions carry the
//!   deleted values, which are the kept transactions of the traces that ended
//!   and how many transactions of them the modules saw.
//!
//! There is no time limit: a trace waits for its end however long it takes,
//! and the store never holds more than `max_transactions` of it.

mod pb {
    pub mod ton {
        // `type` is a Rust keyword, so prost names the file after r#type.
        include!(concat!(env!("OUT_DIR"), "/sf.ton.r#type.v1.rs"));
    }
    pub mod traces {
        include!(concat!(env!("OUT_DIR"), "/traces.v1.rs"));
    }
}

use std::collections::{BTreeMap, HashMap};

use pb::ton::{Block, Transaction};
use pb::traces::{trace::State, Node, PendingTransaction, Trace, TraceDigest, TraceDigests, Traces};
use substreams::errors::Error;
use substreams::pb::substreams::store_delta::Operation;
use substreams::store::{
    DeltaInt64, DeltaProto, Deltas, StoreAdd, StoreAddInt64, StoreDelete, StoreGet, StoreGetInt64, StoreNew,
    StoreSet, StoreSetProto,
};

/// Deletions come after everything else of the block.
const DELETE_ORDINAL: i64 = 1 << 40;

/// The number of transactions of every trace not ended yet.
#[substreams::handlers::store]
fn store_trace_size(block: Block, store: StoreAddInt64) {
    for (index, tx) in transactions(&block).enumerate() {
        if let Some(trace_id) = &tx.trace_id {
            store.add(index as u64 + 1, hex::encode(trace_id), 1);
        }
    }
    for trace in &block.traces {
        store.delete_prefix(DELETE_ORDINAL, &hex::encode(&trace.trace_id));
    }
}

/// The first `max_transactions` transactions of every trace, by block and
/// then by lt, until it ends. A parent has a lower lt than its children and
/// is in the same block or an earlier one, so the parent of every kept
/// transaction is kept too: they form the tree of the trace from its root.
#[substreams::handlers::store]
fn store_pending(params: String, block: Block, size: StoreGetInt64, store: StoreSetProto<PendingTransaction>) {
    let max = max_transactions(&params);
    let seqno = seqno(&block);
    // the transactions of each trace in this block, with their places in it
    let mut by_trace: HashMap<&[u8], Vec<(usize, &Transaction)>> = HashMap::new();
    for (index, tx) in transactions(&block).enumerate() {
        if let Some(trace_id) = &tx.trace_id {
            by_trace.entry(trace_id.as_slice()).or_default().push((index, tx));
        }
    }
    let mut kept: Vec<(usize, &Transaction)> = vec![];
    for (trace_id, mut txs) in by_trace {
        // how many the earlier blocks have taken
        let before = size.get_first(hex::encode(trace_id)).unwrap_or(0).max(0) as u64;
        txs.sort_by(|(_, a), (_, b)| (a.lt, &a.hash).cmp(&(b.lt, &b.hash)));
        txs.truncate(max.saturating_sub(before) as usize);
        kept.extend(txs);
    }
    kept.sort_by_key(|(index, _)| *index);
    for (index, tx) in kept {
        let trace_id = tx.trace_id.as_deref().unwrap_or_default();
        let key = format!("{}/{:020}/{}", hex::encode(trace_id), tx.lt, hex::encode(&tx.hash));
        store.set(index as u64, key, &PendingTransaction { mc_seqno: seqno, transaction: Some(tx.clone()) });
    }
    for trace in &block.traces {
        store.delete_prefix(DELETE_ORDINAL, &format!("{}/", hex::encode(&trace.trace_id)));
    }
}

/// The traces that ended in this block, with their transactions.
#[substreams::handlers::map]
fn map_traces(
    block: Block,
    pending: Deltas<DeltaProto<PendingTransaction>>,
    size: Deltas<DeltaInt64>,
) -> Result<Traces, Error> {
    // trace_id (hex) to its kept transactions, as the deletions give them back
    let mut kept: HashMap<String, Vec<PendingTransaction>> = HashMap::new();
    for delta in pending.deltas {
        if delta.operation == Operation::Delete {
            let trace_id = delta.key.split('/').next().unwrap_or_default().to_string();
            kept.entry(trace_id).or_default().push(delta.old_value);
        }
    }
    // trace_id (hex) to the number of its transactions since initialBlock
    let seen: HashMap<String, i64> = size
        .deltas
        .into_iter()
        .filter(|delta| delta.operation == Operation::Delete)
        .map(|delta| (delta.key, delta.old_value))
        .collect();
    let mut out = Traces { seqno: seqno(&block), utime: utime(&block), traces: vec![] };
    for ended in &block.traces {
        let trace_id = hex::encode(&ended.trace_id);
        let txs = kept.remove(&trace_id).unwrap_or_default();
        let state = if txs.len() as u32 == ended.transactions {
            State::Complete
        } else if seen.get(&trace_id).copied().unwrap_or(0) < ended.transactions as i64 {
            // the beginning is not there, cut or not
            State::Incomplete
        } else {
            State::Truncated
        };
        out.traces.push(Trace {
            trace_id: ended.trace_id.clone(),
            external_hash: ended.external_hash.clone(),
            external_hash_norm: ended.external_hash_norm.clone(),
            mc_seqno_start: ended.mc_seqno_start,
            mc_seqno_end: ended.mc_seqno_end,
            start_lt: ended.start_lt,
            end_lt: ended.end_lt,
            start_utime: ended.start_utime,
            end_utime: ended.end_utime,
            transactions_total: ended.transactions,
            messages_total: ended.messages,
            state: state as i32,
            transactions: nodes(txs),
        });
    }
    Ok(out)
}

/// map_traces without the transactions: ids and the shape of each trace.
#[substreams::handlers::map]
fn map_trace_digests(traces: Traces) -> Result<TraceDigests, Error> {
    Ok(TraceDigests {
        seqno: traces.seqno,
        traces: traces
            .traces
            .iter()
            .map(|trace| TraceDigest {
                trace_id: hex::encode(&trace.trace_id),
                state: State::try_from(trace.state).map_or("?", |s| s.as_str_name()).to_string(),
                mc_seqno_start: trace.mc_seqno_start,
                mc_seqno_end: trace.mc_seqno_end,
                start_lt: trace.start_lt,
                end_lt: trace.end_lt,
                transactions_total: trace.transactions_total,
                transactions: trace
                    .transactions
                    .iter()
                    .map(|n| n.transaction.as_ref().map_or(String::new(), |t| hex::encode(&t.hash)))
                    .collect(),
                parents: trace.transactions.iter().map(|n| n.parent.map_or(-1, |p| p as i32)).collect(),
                depths: trace.transactions.iter().map(|n| n.depth).collect(),
            })
            .collect(),
    })
}

/// The transactions by lt, linked into their tree.
fn nodes(mut txs: Vec<PendingTransaction>) -> Vec<Node> {
    txs.sort_by_key(|p| p.transaction.as_ref().map(|t| (t.lt, t.hash.clone())));
    // outbound message hash to (index of the transaction that sent it, its index in out_msgs)
    let mut senders: BTreeMap<Vec<u8>, (u32, u32)> = BTreeMap::new();
    for (index, p) in txs.iter().enumerate() {
        for (out_index, msg) in p.transaction.iter().flat_map(|t| t.out_msgs.iter()).enumerate() {
            senders.insert(msg.hash.clone(), (index as u32, out_index as u32));
        }
    }
    let mut nodes: Vec<Node> = Vec::with_capacity(txs.len());
    for (index, p) in txs.into_iter().enumerate() {
        let sender = p
            .transaction
            .as_ref()
            .and_then(|t| t.in_msg.as_ref())
            .and_then(|msg| senders.get(&msg.hash).copied());
        // A message is created before it is received, so a parent has the
        // lower lt and is already in `nodes`.
        let depth = match sender {
            Some((parent, _)) => {
                let parent = &mut nodes[parent as usize];
                parent.children.push(index as u32);
                parent.depth + 1
            }
            None => 0,
        };
        nodes.push(Node {
            parent: sender.map(|(parent, _)| parent),
            out_msg_index: sender.map(|(_, out_index)| out_index),
            children: vec![],
            depth,
            mc_seqno: p.mc_seqno,
            transaction: p.transaction,
        });
    }
    nodes
}

fn transactions(block: &Block) -> impl Iterator<Item = &Transaction> {
    block
        .masterchain_block
        .iter()
        .chain(block.shard_blocks.iter())
        .filter_map(|data| data.extra.as_ref())
        .flat_map(|extra| extra.account_blocks.iter())
        .flat_map(|account| account.transactions.iter())
}

fn seqno(block: &Block) -> u32 {
    block.masterchain_block.as_ref().and_then(|data| data.info.as_ref()).map_or(0, |info| info.seq_no)
}

fn utime(block: &Block) -> u32 {
    block.masterchain_block.as_ref().and_then(|data| data.info.as_ref()).map_or(0, |info| info.gen_utime)
}

/// `max_transactions=<n>`, 1000 by default.
fn max_transactions(params: &str) -> u64 {
    params
        .split('&')
        .find_map(|pair| pair.trim().strip_prefix("max_transactions=")?.parse().ok())
        .unwrap_or(1000)
}
