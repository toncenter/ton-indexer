begin;

-- blocks
create index if not exists blocks_index_1 on blocks (gen_utime asc);
create index if not exists blocks_index_2 on blocks (mc_block_seqno asc);
create index if not exists blocks_index_3 on blocks (seqno asc) where (workchain = '-1'::integer);
create index if not exists blocks_index_4 on blocks (start_lt asc);

-- transactions
create index if not exists transactions_index_1 on transactions (block_workchain, block_shard, block_seqno);
create index if not exists transactions_index_2 on transactions (lt asc);
create index if not exists transactions_index_3 on transactions (now asc, lt asc);
create index if not exists transactions_index_4 on transactions (account, lt asc);
create index if not exists transactions_index_5 on transactions (account, now asc, lt asc);
create index if not exists transactions_index_6 on transactions (hash);
create index if not exists transactions_index_7 on transactions (trace_id, lt asc);
create index if not exists transactions_index_8 on transactions (mc_block_seqno asc, lt asc);

-- messages
create index if not exists messages_index_1 on messages (msg_hash);
create index if not exists messages_index_2 on messages (source, created_lt asc);
create index if not exists messages_index_3 on messages (destination, created_lt asc);
create index if not exists messages_index_4 on messages (body_hash);
create index if not exists messages_index_5 on messages (trace_id, tx_lt asc);
create index if not exists messages_index_6 on messages (opcode, created_lt);

-- account states
create index if not exists latest_account_states_index_1 on latest_account_states (balance desc);
create index if not exists latest_account_states_index_2 on latest_account_states (id asc);
create index if not exists latest_account_states_address_book_index on latest_account_states (account) include (account_friendly, code_hash, account_status);

-- jettons
create index if not exists jetton_masters_index_1 on jetton_masters (admin_address, id asc);
create index if not exists jetton_masters_index_2 on jetton_masters (id asc);

create index if not exists jetton_wallets_index_1 on jetton_wallets (owner, id asc);
create index if not exists jetton_wallets_index_2 on jetton_wallets (jetton, id asc);
create index if not exists jetton_wallets_index_3 on jetton_wallets (id asc);
create index if not exists jetton_wallets_index_4 on jetton_wallets (jetton, balance desc);
create index if not exists jetton_wallets_index_5 on jetton_wallets (owner, balance desc);

create index if not exists jetton_transfers_index_1 on jetton_transfers (source, tx_now asc);
create index if not exists jetton_transfers_index_2 on jetton_transfers (source, tx_lt asc);
create index if not exists jetton_transfers_index_3 on jetton_transfers (destination, tx_lt asc);
create index if not exists jetton_transfers_index_4 on jetton_transfers (destination, tx_now asc);
create index if not exists jetton_transfers_index_6 on jetton_transfers (jetton_wallet_address, tx_lt asc);
create index if not exists jetton_transfers_index_7 on jetton_transfers (jetton_master_address, tx_now asc);
create index if not exists jetton_transfers_index_8 on jetton_transfers (jetton_master_address, tx_lt asc);
create index if not exists jetton_transfers_index_9 on jetton_transfers (tx_now asc, tx_lt asc);
create index if not exists jetton_transfers_index_10 on jetton_transfers (tx_lt asc);

create index if not exists jetton_burns_index_1 on jetton_burns (owner, tx_now asc, tx_lt asc);
create index if not exists jetton_burns_index_2 on jetton_burns (owner, tx_lt asc);
create index if not exists jetton_burns_index_3 on jetton_burns (jetton_wallet_address, tx_now asc, tx_lt asc);
create index if not exists jetton_burns_index_4 on jetton_burns (jetton_wallet_address, tx_lt asc);
create index if not exists jetton_burns_index_5 on jetton_burns (jetton_master_address, tx_now asc, tx_lt asc);
create index if not exists jetton_burns_index_6 on jetton_burns (jetton_master_address, tx_lt asc);
create index if not exists jetton_burns_index_7 on jetton_burns (tx_now asc, tx_lt asc);
create index if not exists jetton_burns_index_8 on jetton_burns (tx_lt asc);

-- nfts
create index if not exists nft_collections_index_1 on nft_collections (owner_address, id asc);
create index if not exists nft_collections_index_2 on nft_collections (id asc);

create index if not exists nft_items_index_1 on nft_items (collection_address, index asc);
create index if not exists nft_items_index_2 on nft_items (owner_address, collection_address asc, index asc);
create index if not exists nft_items_index_3 on nft_items (id asc);

-- create index if not exists nft_transfers_index_1 on nft_transfers (nft_item_address, tx_now asc, tx_lt asc);
create index if not exists nft_transfers_index_2 on nft_transfers (nft_item_address, tx_lt asc);
create index if not exists nft_transfers_index_3 on nft_transfers (nft_collection_address, tx_now asc);
create index if not exists nft_transfers_index_4 on nft_transfers (nft_collection_address, tx_lt asc);
create index if not exists nft_transfers_index_5 on nft_transfers (old_owner, tx_lt asc);
-- create index if not exists nft_transfers_index_6 on nft_transfers (old_owner, tx_now asc, tx_lt asc);
create index if not exists nft_transfers_index_7 on nft_transfers (new_owner, tx_lt asc);
-- create index if not exists nft_transfers_index_8 on nft_transfers (new_owner, tx_now asc, tx_lt asc);
create index if not exists nft_transfers_index_9 on nft_transfers (tx_lt asc);
create index if not exists nft_transfers_index_10 on nft_transfers (tx_now asc, tx_lt asc);

-- traces
create index if not exists traces_index_1 on traces (state);
create index if not exists trace_index_2a on traces (mc_seqno_end asc);
-- create index if not exists traces_index_3 on traces (end_lt asc);
-- create index if not exists traces_index_4 on traces (end_utime asc);
-- create index if not exists traces_index_5 on traces (external_hash, end_lt asc);
-- create index if not exists traces_index_6 on traces (external_hash, end_utime asc);
create index if not exists traces_index_7 on traces (classification_state);

create index if not exists trace_edges_index_1 on trace_edges (incomplete);
-- create index if not exists trace_edges_index_2 on trace_edges (msg_hash);

-- create index if not exists actions_index_1 on actions (trace_id, start_lt, end_lt);
create index if not exists actions_index_2 on actions (action_id);
commit;
