begin;

-- blocks
create index if not exists blocks_index_2 on blocks (gen_utime);
create index if not exists blocks_index_3 on blocks(mc_block_seqno);
create index if not exists blocks_index_4 on blocks (seqno) where (workchain = '-1'::integer);
create index if not exists blocks_index_5 on blocks (start_lt);

-- transactions
create index if not exists transactions_index_1 on transactions (block_workchain, block_shard, block_seqno);
create index if not exists transactions_index_2 on transactions (account, lt);
create index if not exists transactions_index_3 on transactions (account, now);
create index if not exists transactions_index_4 on transactions (lt, hash);
create index if not exists transactions_index_5 on transactions (now, hash);
create index if not exists transactions_index_6 on transactions (hash);
create index if not exists transactions_index_7 on transactions (trace_id);
create index if not exists transactions_index_8 on transactions (mc_block_seqno);
create index if not exists event_detector__transaction_index_1 on transactions (lt) where (trace_id IS NULL);

-- messages
create index if not exists messages_index_1 on messages (msg_hash);
create index if not exists messages_index_2 on messages (source, created_lt);
create index if not exists messages_index_3 on messages (destination, created_lt);
create index if not exists messages_index_4 on messages (created_lt);
create index if not exists messages_index_5 on messages (body_hash);

-- account states
create index if not exists latest_account_states_index_1 on latest_account_states (balance);
create index if not exists latest_account_states_address_book_index on latest_account_states (account) include (account_friendly, code_hash, account_status);

-- jettons
create index if not exists jetton_masters_index_2 on jetton_masters (admin_address);

create index if not exists jetton_wallets_index_2 on jetton_wallets (owner);
create index if not exists jetton_wallets_index_3 on jetton_wallets (jetton);

create index if not exists jetton_transfers_index_2 on jetton_transfers (source);
create index if not exists jetton_transfers_index_3 on jetton_transfers (destination);
create index if not exists jetton_transfers_index_4 on jetton_transfers (jetton_wallet_address);

create index if not exists jetton_burns_index_2 on jetton_burns (owner);
create index if not exists jetton_burns_index_3 on jetton_burns (jetton_wallet_address);

-- nfts
create index if not exists nft_collections_index_2 on nft_collections (owner_address);

create index if not exists nft_items_index_2 on nft_items (collection_address);
create index if not exists nft_items_index_3 on nft_items (owner_address);

create index if not exists nft_transfers_index_2 on nft_transfers (nft_item_address);
create index if not exists nft_transfers_index_3 on nft_transfers (old_owner);
create index if not exists nft_transfers_index_4 on nft_transfers (new_owner);

commit;
