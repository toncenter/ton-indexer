begin;

-- blocks
drop index if exists blocks_index_2;
drop index if exists blocks_index_3;
drop index if exists blocks_index_3a;
drop index if exists blocks_index_4;
drop index if exists blocks_index_5;

-- transactions
drop index if exists transactions_index_1;
drop index if exists event_detector__transaction_index_1;
drop index if exists transactions_index_2;
drop index if exists transactions_index_2a;
drop index if exists transactions_index_3;
drop index if exists transactions_index_4;
drop index if exists transactions_index_5;
drop index if exists transactions_index_6;
drop index if exists transactions_index_8;

-- messages
-- create index if not exists messages_index_0 on messages (tx_hash, tx_lt);
drop index if exists messages_index_1;
drop index if exists messages_index_2;
drop index if exists messages_index_3;
drop index if exists messages_index_4;
drop index if exists messages_index_5;

-- account states
drop index if exists latest_account_states_index_1;
drop index if exists latest_account_states_address_book_index;

-- jettons
drop index if exists jetton_masters_index_2;

drop index if exists jetton_wallets_index_2;
drop index if exists jetton_wallets_index_3;

drop index if exists jetton_transfers_index_2;
drop index if exists jetton_transfers_index_3;
drop index if exists jetton_transfers_index_4;

drop index if exists jetton_burns_index_2;
drop index if exists jetton_burns_index_3;

-- nfts
drop index if exists nft_collections_index_2;

drop index if exists nft_items_index_2;
drop index if exists nft_items_index_3;

drop index if exists nft_transfers_index_2;
drop index if exists nft_transfers_index_3;
drop index if exists nft_transfers_index_4;

commit;
