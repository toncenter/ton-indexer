begin;

-- blocks
drop index if exists blocks_index_1;
drop index if exists blocks_index_2;
drop index if exists blocks_index_3;
drop index if exists blocks_index_4;

-- transactions
drop index if exists transactions_index_1;
drop index if exists transactions_index_2;
drop index if exists transactions_index_3;
drop index if exists transactions_index_4;
drop index if exists transactions_index_5;
drop index if exists transactions_index_6;
drop index if exists transactions_index_7;
drop index if exists transactions_index_8;

-- messages
drop index if exists messages_index_1;
drop index if exists messages_index_2;
drop index if exists messages_index_3;
drop index if exists messages_index_4;
drop index if exists messages_index_5;
drop index if exists messages_index_6;

-- account states
drop index if exists latest_account_states_index_1;
drop index if exists latest_account_states_index_2;
drop index if exists latest_account_states_address_book_index;

-- jettons
drop index if exists jetton_masters_index_1;
drop index if exists jetton_masters_index_2;

drop index if exists jetton_wallets_index_1;
drop index if exists jetton_wallets_index_2;
drop index if exists jetton_wallets_index_3;
drop index if exists jetton_wallets_index_4;
drop index if exists jetton_wallets_index_5;

drop index if exists jetton_transfers_index_1;
drop index if exists jetton_transfers_index_2;
drop index if exists jetton_transfers_index_3;
drop index if exists jetton_transfers_index_4;
drop index if exists jetton_transfers_index_5;
drop index if exists jetton_transfers_index_6;
drop index if exists jetton_transfers_index_7;
drop index if exists jetton_transfers_index_8;
drop index if exists jetton_transfers_index_9;
drop index if exists jetton_transfers_index_10;

drop index if exists jetton_burns_index_1;
drop index if exists jetton_burns_index_2;
drop index if exists jetton_burns_index_3;
drop index if exists jetton_burns_index_4;
drop index if exists jetton_burns_index_5;
drop index if exists jetton_burns_index_6;
drop index if exists jetton_burns_index_7;
drop index if exists jetton_burns_index_8;

-- nfts
drop index if exists nft_collections_index_1;
drop index if exists nft_collections_index_2;

drop index if exists nft_items_index_1;
drop index if exists nft_items_index_2;
drop index if exists nft_items_index_3;

drop index if exists nft_transfers_index_1;
drop index if exists nft_transfers_index_2;
drop index if exists nft_transfers_index_3;
drop index if exists nft_transfers_index_4;
drop index if exists nft_transfers_index_5;
drop index if exists nft_transfers_index_6;
drop index if exists nft_transfers_index_7;
drop index if exists nft_transfers_index_8;
drop index if exists nft_transfers_index_9;
drop index if exists nft_transfers_index_10;

-- traces
drop index if exists traces_index_1;
drop index if exists traces_index_2;
drop index if exists traces_index_3;
drop index if exists traces_index_4;
drop index if exists traces_index_5;
drop index if exists traces_index_6;

drop index if exists trace_edges_index_1;
drop index if exists trace_edges_index_2;

drop index if exists trace_unclassified_index;
commit;
