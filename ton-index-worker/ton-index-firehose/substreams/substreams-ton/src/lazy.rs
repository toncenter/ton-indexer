//! Reading a block through its lazy view, without decoding the rest of it.
//!
//! [`BlockLazyView`] has the order of the block: the masterchain block, then
//! the shard blocks it commits ([`BlockLazyView::blocks`]), their account
//! blocks ([`BlockLazyView::account_blocks`]) and the transactions of those
//! ([`BlockLazyView::transactions`]). A view decodes a field when it is read,
//! so the iterators give `Result`s; a view's `to_owned_message()` (of
//! `buffa::view::LazyMessageView`) gives the owned message, of a transaction
//! for instance.

use buffa::DecodeError;
use rston::cell::HashBytes;
use rston::models::StdAddr;

use crate::types::{
    AccountBlockLazyView, BlockDataLazyView, BlockExtraLazyView, BlockInfoLazyView, BlockLazyView, TransactionLazyView,
};

impl<'a> BlockLazyView<'a> {
    /// The masterchain block; the TON Firehose gives every block one.
    pub fn masterchain(&self) -> Result<Option<BlockDataLazyView<'a>>, DecodeError> {
        self.masterchain_block.get()
    }

    /// The seqno of the masterchain block, 0 without one.
    pub fn seqno(&self) -> Result<u32, DecodeError> {
        Ok(self.info()?.map_or(0, |info| info.seq_no))
    }

    /// The gen_utime of the masterchain block, 0 without one.
    pub fn gen_utime(&self) -> Result<u32, DecodeError> {
        Ok(self.info()?.map_or(0, |info| info.gen_utime))
    }

    fn info(&self) -> Result<Option<BlockInfoLazyView<'a>>, DecodeError> {
        match self.masterchain()? {
            Some(masterchain) => masterchain.info.get(),
            None => Ok(None),
        }
    }

    /// The masterchain block, then the shard blocks it commits, in their order.
    pub fn blocks(&self) -> Blocks<'_, 'a> {
        Blocks { block: self, masterchain: true, next: 0 }
    }

    /// The account blocks of [`BlockLazyView::blocks`], in their order.
    pub fn account_blocks(&self) -> AccountBlocks<'_, 'a> {
        AccountBlocks { blocks: self.blocks(), current: None, next: 0 }
    }

    /// The transactions of [`BlockLazyView::account_blocks`], in their order:
    /// by block, by account, by lt.
    pub fn transactions(&self) -> Transactions<'_, 'a> {
        Transactions { accounts: self.account_blocks(), current: None, next: 0 }
    }
}

/// See [`BlockLazyView::blocks`].
pub struct Blocks<'v, 'a> {
    block: &'v BlockLazyView<'a>,
    masterchain: bool,
    next: usize,
}

impl<'a> Iterator for Blocks<'_, 'a> {
    type Item = Result<BlockDataLazyView<'a>, DecodeError>;

    fn next(&mut self) -> Option<Self::Item> {
        if std::mem::take(&mut self.masterchain) {
            if let Some(masterchain) = self.block.masterchain_block.get().transpose() {
                return Some(masterchain);
            }
        }
        let data = self.block.shard_blocks.get(self.next)?;
        self.next += 1;
        Some(data)
    }
}

/// An account block, with the workchain of its shard block.
#[derive(Clone, Debug)]
pub struct AccountBlock<'a> {
    pub workchain: i32,
    pub view: AccountBlockLazyView<'a>,
}

impl AccountBlock<'_> {
    /// The address of the account: none for a workchain or an account_addr
    /// that a standard address cannot hold.
    pub fn address(&self) -> Option<StdAddr> {
        Some(StdAddr::new(self.workchain.try_into().ok()?, HashBytes(self.view.account_addr.try_into().ok()?)))
    }
}

/// See [`BlockLazyView::account_blocks`].
pub struct AccountBlocks<'v, 'a> {
    blocks: Blocks<'v, 'a>,
    /// The workchain and the extra of the block being read.
    current: Option<(i32, BlockExtraLazyView<'a>)>,
    next: usize,
}

impl<'a> Iterator for AccountBlocks<'_, 'a> {
    type Item = Result<AccountBlock<'a>, DecodeError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some((workchain, extra)) = &self.current {
                if let Some(view) = extra.account_blocks.get(self.next) {
                    self.next += 1;
                    return Some(view.map(|view| AccountBlock { workchain: *workchain, view }));
                }
            }
            let data = match self.blocks.next()? {
                Ok(data) => data,
                Err(error) => return Some(Err(error)),
            };
            match extra_of(&data) {
                Ok(current) => (self.current, self.next) = (current, 0),
                Err(error) => return Some(Err(error)),
            }
        }
    }
}

/// The workchain and the extra of a block; none for a block without extra.
fn extra_of<'a>(data: &BlockDataLazyView<'a>) -> Result<Option<(i32, BlockExtraLazyView<'a>)>, DecodeError> {
    let Some(extra) = data.extra.get()? else { return Ok(None) };
    let workchain = match data.id.get()? {
        Some(id) => id.shard_id.get()?.map_or(0, |shard| shard.workchain_id),
        None => 0,
    };
    Ok(Some((workchain, extra)))
}

/// See [`BlockLazyView::transactions`].
pub struct Transactions<'v, 'a> {
    accounts: AccountBlocks<'v, 'a>,
    current: Option<AccountBlockLazyView<'a>>,
    next: usize,
}

impl<'a> Iterator for Transactions<'_, 'a> {
    type Item = Result<TransactionLazyView<'a>, DecodeError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(account) = &self.current {
                if let Some(transaction) = account.transactions.get(self.next) {
                    self.next += 1;
                    return Some(transaction);
                }
            }
            match self.accounts.next()? {
                Ok(account) => (self.current, self.next) = (Some(account.view), 0),
                Err(error) => return Some(Err(error)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use buffa::view::LazyMessageView;
    use buffa::Message;

    use crate::types::{
        AccountBlock as Account, Block, BlockData, BlockExtra, BlockIdExt, BlockInfo, ShardIdent, Transaction,
    };

    use super::*;

    fn data(workchain: i32, seq_no: u32, accounts: &[(u8, &[u64])]) -> BlockData {
        let account_blocks = accounts
            .iter()
            .map(|(id, lts)| Account {
                account_addr: vec![*id; 32],
                transactions: lts.iter().map(|&lt| Transaction { lt, ..Default::default() }).collect(),
                ..Default::default()
            })
            .collect();
        BlockData {
            id: BlockIdExt {
                shard_id: ShardIdent { workchain_id: workchain, ..Default::default() }.into(),
                seq_no,
                ..Default::default()
            }
            .into(),
            info: BlockInfo { seq_no, gen_utime: 1_700_000_000, ..Default::default() }.into(),
            extra: BlockExtra { account_blocks, ..Default::default() }.into(),
            ..Default::default()
        }
    }

    #[test]
    fn the_order_of_the_block() {
        let mut shard_without_extra = data(0, 11, &[]);
        shard_without_extra.extra = Default::default();
        let block = Block {
            masterchain_block: data(-1, 7, &[(0x33, &[1, 2])]).into(),
            shard_blocks: vec![data(0, 10, &[(1, &[3]), (2, &[])]), shard_without_extra, data(0, 12, &[(3, &[4, 5])])],
            ..Default::default()
        };
        let bytes = block.encode_to_vec();
        let view = BlockLazyView::decode_lazy(&bytes).unwrap();

        assert_eq!((view.seqno().unwrap(), view.gen_utime().unwrap()), (7, 1_700_000_000));
        assert_eq!(view.blocks().count(), 4);
        let accounts: Vec<_> = view.account_blocks().map(Result::unwrap).collect();
        let addresses: Vec<_> = accounts.iter().map(|account| account.address().unwrap()).collect();
        assert_eq!(
            addresses,
            [(-1, 0x33), (0, 1), (0, 2), (0, 3)].map(|(workchain, id)| StdAddr::new(workchain, HashBytes([id; 32])))
        );
        let lts: Vec<_> = view.transactions().map(|tx| tx.unwrap().lt).collect();
        assert_eq!(lts, [1, 2, 3, 4, 5]);
        let owned: Transaction = view.transactions().nth(2).unwrap().unwrap().to_owned_message().unwrap();
        assert_eq!(owned.lt, 3);

        let empty = BlockLazyView::decode_lazy(&[]).unwrap();
        assert_eq!((empty.seqno().unwrap(), empty.blocks().count(), empty.transactions().count()), (0, 0, 0));
    }
}
