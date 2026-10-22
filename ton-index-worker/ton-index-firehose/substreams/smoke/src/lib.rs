//! A smoke test of the TON Firehose: counts what every block holds and keeps
//! running totals in a store, so a run goes through both substreams tiers.
mod pb;

use pb::smoke::{BlockStats, Totals};
use pb::ton::{Block, BlockData};
use substreams::errors::Error;
use substreams::store::{StoreAdd, StoreAddInt64, StoreGet, StoreGetInt64, StoreNew};

#[substreams::handlers::map]
fn map_block_stats(block: Block) -> Result<BlockStats, Error> {
    let master = block.masterchain_block.as_ref().ok_or_else(|| Error::msg("no masterchain block"))?;
    let info = master.info.as_ref().ok_or_else(|| Error::msg("no block info"))?;
    let mut stats = BlockStats {
        seqno: info.seq_no,
        gen_utime: info.gen_utime,
        shard_blocks: block.shard_blocks.len() as u32,
        traces: block.traces.len() as u32,
        ..Default::default()
    };
    for data in std::iter::once(master).chain(block.shard_blocks.iter()) {
        count(data, &mut stats);
    }
    Ok(stats)
}

fn count(data: &BlockData, stats: &mut BlockStats) {
    for account in data.extra.iter().flat_map(|extra| &extra.account_blocks) {
        stats.transactions += account.transactions.len() as u64;
        stats.out_messages += account.transactions.iter().map(|tx| tx.outmsg_cnt as u64).sum::<u64>();
    }
}

#[substreams::handlers::store]
fn store_totals(stats: BlockStats, store: StoreAddInt64) {
    store.add(0, "blocks", 1);
    store.add(0, "transactions", stats.transactions as i64);
    store.add(0, "traces", stats.traces as i64);
}

// A store cannot be the output of a run: this map reads it.
#[substreams::handlers::map]
fn map_totals(stats: BlockStats, totals: StoreGetInt64) -> Result<Totals, Error> {
    let get = |key: &str| totals.get_last(key).unwrap_or(0);
    Ok(Totals { seqno: stats.seqno, blocks: get("blocks"), transactions: get("transactions"), traces: get("traces") })
}
