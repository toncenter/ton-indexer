
#include <iostream>
#include "td/utils/port/signals.h"
#include "td/utils/OptionParser.h"
#include "td/utils/logging.h"
#include "pqxx/pqxx"
#include "version.h"

bool migration_needed(std::optional<Version> current_version, Version migration_version, bool rerun_last_migration) {
  if (rerun_last_migration && migration_version == latest_version) {
    return true;
  }
  if (current_version.has_value() && *current_version < migration_version) {
    return true;
  }
  return false;
}

void run_1_2_0_migrations(const std::string& connection_string, bool custom_types, bool dry_run) {
  LOG(INFO) << "Running migrations to version 1.2.0";

  LOG(INFO) << "Creating required types...";
  {
    auto exec_query = [&] (const std::string& query) {
      if (dry_run) {
        std::cout << query << std::endl;
        return;
      }

      try {
        pqxx::connection c(connection_string);
        pqxx::work txn(c);

        txn.exec(query).no_rows();
        txn.commit();
      } catch (const std::exception &e) {
        LOG(INFO) << "Skipping query '" << query << "': " << e.what();
      }
    };
    if (custom_types) {
      exec_query("create extension if not exists pgton;");
    } else {
      exec_query("create domain tonhash as char(44);");
      exec_query("create domain tonaddr as varchar;");
    }

    exec_query("create type account_status_type as enum ('uninit', 'frozen', 'active', 'nonexist');");
    exec_query("create type blockid as (workchain integer, shard bigint, seqno integer);");
    exec_query("create type blockidext as (workchain integer, shard bigint, seqno integer, root_hash tonhash, file_hash tonhash);");
    exec_query("create type bounce_type as enum ('negfunds', 'nofunds', 'ok');");
    exec_query("create type descr_type as enum ('ord', 'storage', 'tick_tock', 'split_prepare', 'split_install', 'merge_prepare', 'merge_install');");
    exec_query("create type msg_direction as enum ('out', 'in');");
    exec_query("create type skipped_reason_type as enum ('no_state', 'bad_state', 'no_gas', 'suspended');");
    exec_query("create type status_change_type as enum ('unchanged', 'frozen', 'deleted');");
    exec_query("create type trace_classification_state as enum ('unclassified', 'failed', 'ok', 'broken');");
    exec_query("create type trace_state as enum ('complete', 'pending', 'broken');");
    exec_query("create type change_dns_record_details as (key varchar, value_schema varchar, value varchar, flags integer);");
    exec_query("create type liquidity_vault_excess_details as (asset varchar, amount NUMERIC);");
    exec_query("create type dex_deposit_liquidity_details as (dex varchar, amount1 numeric, amount2 numeric, asset1 varchar, asset2 varchar, user_jetton_wallet_1 varchar, user_jetton_wallet_2 varchar, lp_tokens_minted numeric, target_asset_1 varchar, target_asset_2 varchar, target_amount_1 numeric, target_amount_2 numeric, vault_excesses liquidity_vault_excess_details[], tick_lower numeric, tick_upper numeric, nft_index numeric, nft_address varchar);");
    exec_query("create type dex_transfer_details as (amount numeric, asset tonaddr, source tonaddr, destination tonaddr, source_jetton_wallet tonaddr, destination_jetton_wallet tonaddr);");
    exec_query("create type dex_withdraw_liquidity_details as (dex varchar, amount1 numeric, amount2 numeric, asset1_out varchar, asset2_out varchar, user_jetton_wallet_1 varchar, user_jetton_wallet_2 varchar, dex_jetton_wallet_1 varchar, dex_jetton_wallet_2 varchar, lp_tokens_burnt numeric, dex_wallet_1 varchar, dex_wallet_2 varchar, burned_nft_index numeric, burned_nft_address varchar, tick_lower numeric, tick_upper numeric);");
    exec_query("create type jetton_transfer_details as(response_destination tonaddr, forward_amount numeric, query_id numeric, custom_payload text, forward_payload text, comment text, is_encrypted_comment boolean);");
    exec_query("create type nft_mint_details as (nft_item_index numeric);");
    exec_query("create type nft_transfer_details as(is_purchase boolean, price numeric, query_id numeric, custom_payload text, forward_payload text, forward_amount numeric, response_destination tonaddr, nft_item_index numeric, marketplace varchar);");
    exec_query("create type peer_swap_details as(asset_in tonaddr, amount_in numeric, asset_out tonaddr, amount_out numeric);");
    exec_query("create type jetton_swap_details as (dex varchar, sender tonaddr, dex_incoming_transfer dex_transfer_details, dex_outgoing_transfer dex_transfer_details, peer_swaps peer_swap_details[], min_out_amount numeric);");
    exec_query("create type staking_details as (provider varchar, ts_nft varchar, tokens_burnt numeric, tokens_minted numeric);");
    exec_query("create type ton_transfer_details as (content text, encrypted boolean);");
    exec_query("create type multisig_create_order_details as (query_id numeric, order_seqno numeric, is_created_by_signer boolean, is_signed_by_creator boolean, creator_index numeric, expiration_date numeric, order_boc varchar);");
    exec_query("create type multisig_approve_details as (signer_index numeric, exit_code numeric);");
    exec_query("create type multisig_execute_details as (query_id numeric, order_seqno numeric, expiration_date numeric, approvals_num numeric, signers_hash varchar, order_boc varchar);");
    exec_query("create type vesting_send_message_details as (query_id numeric, message_boc varchar);");
    exec_query("create type vesting_add_whitelist_details as (query_id numeric, accounts_added varchar[]);");
    exec_query("create type evaa_supply_details as (sender_jetton_wallet varchar, recipient_jetton_wallet varchar, master_jetton_wallet varchar, master varchar, asset_id varchar, is_ton boolean);");
    exec_query("create type evaa_withdraw_details as (sender_jetton_wallet varchar, recipient_jetton_wallet varchar, master_jetton_wallet varchar, master varchar, fail_reason varchar, asset_id varchar);");
    exec_query("create type evaa_liquidate_details as (fail_reason text, debt_amount numeric, asset_id varchar);");
    exec_query("create type jvault_claim_details as (claimed_jettons varchar[], claimed_amounts numeric[]);");
    exec_query("create type jvault_stake_details as (period numeric, minted_stake_jettons numeric, stake_wallet varchar);");
    exec_query("create type tonco_deploy_pool_details as (jetton0_router_wallet varchar, jetton1_router_wallet varchar, jetton0_minter varchar, jetton1_minter varchar, tick_spacing integer, initial_price_x96 numeric, protocol_fee integer, lp_fee_base integer, lp_fee_current integer, pool_active boolean);");
  }

  LOG(INFO) << "Creating tables...";
  try {
    pqxx::connection c(connection_string);
    pqxx::work txn(c);

    std::string query = "";
    query += (
      "create table if not exists blocks ("
      "workchain integer not null, "
      "shard bigint  not null, "
      "seqno integer not null, "
      "root_hash tonhash, "
      "file_hash tonhash, "
      "mc_block_workchain integer, "
      "mc_block_shard bigint, "
      "mc_block_seqno integer, "
      "global_id integer, "
      "version integer, "
      "after_merge boolean, "
      "before_split boolean, "
      "after_split boolean, "
      "want_merge boolean, "
      "want_split boolean, "
      "key_block boolean, "
      "vert_seqno_incr boolean, "
      "flags integer, "
      "gen_utime bigint, "
      "start_lt bigint, "
      "end_lt bigint, "
      "validator_list_hash_short integer, "
      "gen_catchain_seqno integer, "
      "min_ref_mc_seqno integer, "
      "prev_key_block_seqno integer, "
      "vert_seqno integer, "
      "master_ref_seqno integer, "
      "rand_seed tonhash, "
      "created_by tonhash, "
      "tx_count integer, "
      "prev_blocks blockid[], "
      "primary key (workchain, shard, seqno), "
      "foreign key (mc_block_workchain, mc_block_shard, mc_block_seqno) references blocks);\n"
    );

    query += (
      "create table if not exists shard_state ("
      "mc_seqno integer not null, "
      "workchain integer not null, "
      "shard bigint not null, "
      "seqno integer not null, "
      "primary key (mc_seqno, workchain, shard, seqno));"
    );

    query += (
      "create table if not exists transactions ("
      "account tonaddr not null, "
      "hash tonhash not null, "
      "lt bigint not null, "
      "block_workchain integer, "
      "block_shard bigint, "
      "block_seqno integer, "
      "mc_block_seqno integer, "
      "trace_id tonhash, "
      "prev_trans_hash tonhash, "
      "prev_trans_lt bigint, "
      "now integer, "
      "orig_status account_status_type, "
      "end_status account_status_type, "
      "total_fees bigint, "
      "total_fees_extra_currencies jsonb, "
      "account_state_hash_before tonhash, "
      "account_state_hash_after tonhash, "
      "descr descr_type, "
      "aborted boolean, "
      "destroyed boolean, "
      "credit_first boolean, "
      "is_tock boolean, "
      "installed boolean, "
      "storage_fees_collected bigint, "
      "storage_fees_due bigint, "
      "storage_status_change status_change_type, "
      "credit_due_fees_collected bigint, "
      "credit bigint, "
      "credit_extra_currencies jsonb, "
      "compute_skipped boolean, "
      "skipped_reason skipped_reason_type, "
      "compute_success boolean, "
      "compute_msg_state_used boolean, "
      "compute_account_activated boolean, "
      "compute_gas_fees bigint, "
      "compute_gas_used bigint, "
      "compute_gas_limit bigint, "
      "compute_gas_credit bigint, "
      "compute_mode smallint, "
      "compute_exit_code integer,"
      "compute_exit_arg integer,"
      "compute_vm_steps bigint,"
      "compute_vm_init_state_hash tonhash,"
      "compute_vm_final_state_hash tonhash,"
      "action_success boolean, "
      "action_valid boolean, "
      "action_no_funds boolean, "
      "action_status_change status_change_type, "
      "action_total_fwd_fees bigint, "
      "action_total_action_fees bigint, "
      "action_result_code int, "
      "action_result_arg int, "
      "action_tot_actions int, "
      "action_spec_actions int, "
      "action_skipped_actions int, "
      "action_msgs_created int, "
      "action_action_list_hash tonhash, "
      "action_tot_msg_size_cells bigint, "
      "action_tot_msg_size_bits bigint, "
      "bounce bounce_type, "
      "bounce_msg_size_cells bigint, "
      "bounce_msg_size_bits bigint, "
      "bounce_req_fwd_fees bigint, "
      "bounce_msg_fees bigint, "
      "bounce_fwd_fees bigint, "
      "split_info_cur_shard_pfx_len int, "
      "split_info_acc_split_depth int, "
      "split_info_this_addr tonaddr, "
      "split_info_sibling_addr tonaddr, "
      "primary key (hash, lt), "
      "foreign key (block_workchain, block_shard, block_seqno) references blocks);\n"
    );

    query += (
      "create table if not exists messages ("
      "tx_hash tonhash, "
      "tx_lt bigint, "
      "mc_seqno integer, "
      "msg_hash tonhash, "
      "msg_hash_norm tonhash, "
      "direction msg_direction, "
      "trace_id tonhash, "
      "source tonaddr, "
      "destination tonaddr, "
      "value bigint, "
      "value_extra_currencies jsonb, "
      "fwd_fee bigint, "
      "ihr_fee bigint, "
      "extra_flags numeric,"
      "created_lt bigint, "
      "created_at bigint, "
      "opcode integer, "
      "ihr_disabled boolean, "
      "bounce boolean, "
      "bounced boolean, "
      "import_fee bigint, "
      "body_hash tonhash, "
      "init_state_hash tonhash, "
      "primary key (tx_hash, tx_lt, msg_hash, direction), "
      "foreign key (tx_hash, tx_lt) references transactions);\n"
    );

    query += (
      "create table if not exists message_contents ("
      "hash tonhash not null primary key, "
      "body text);"
    );

    query += (
      "create table if not exists account_states ("
      "hash tonhash not null primary key, "
      "account tonaddr, "
      "balance bigint, "
      "balance_extra_currencies jsonb, "
      "account_status account_status_type, "
      "frozen_hash tonhash, "
      "data_hash tonhash, "
      "code_hash tonhash"
      ");\n"
    );

    query += (
      "create table if not exists latest_account_states ("
      "id bigserial not null, "
      "account tonaddr not null primary key, "
      "account_friendly tonaddr, "
      "hash tonhash not null, "
      "balance bigint, "
      "balance_extra_currencies jsonb, "
      "account_status account_status_type, "
      "timestamp integer, "
      "last_trans_hash tonhash, "
      "last_trans_lt bigint, "
      "frozen_hash tonhash, "
      "data_hash tonhash, "
      "code_hash tonhash, "
      "data_boc text, "
      "code_boc text) with (autovacuum_vacuum_scale_factor = 0.03);\n"
    );

    query += (
      "create table if not exists address_book ("
      "address tonaddr not null primary key, "
      "code_hash tonhash, "
      "domain varchar);\n"
    );

    query += (
      "create table if not exists nft_collections ("
      "id bigserial not null, "
      "address tonaddr not null primary key, "
      "next_item_index numeric, "
      "owner_address tonaddr, "
      "collection_content jsonb, "
      "last_transaction_lt bigint, "
      "code_hash tonhash, "
      "data_hash tonhash);\n"
    );

    query += (
      "create table if not exists nft_items ("
      "id bigserial not null, "
      "address tonaddr not null primary key, "
      "init boolean, "
      "index numeric, "
      "collection_address tonaddr, "
      "owner_address tonaddr, "
      "content jsonb, "
      "last_transaction_lt bigint, "
      "code_hash tonhash, "
      "data_hash tonhash);\n"
    );

    query += (
      "create table if not exists nft_transfers ("
      "tx_hash tonhash not null, "
      "tx_lt bigint not null, "
      "tx_now integer not null, "
      "tx_aborted boolean not null, "
      "mc_seqno integer, "
      "query_id numeric, "
      "nft_item_address tonaddr, "
      "nft_item_index numeric, "
      "nft_collection_address tonaddr, "
      "old_owner tonaddr, "
      "new_owner tonaddr, "
      "response_destination tonaddr, "
      "custom_payload text, "
      "forward_amount numeric, "
      "forward_payload text, "
      "trace_id tonhash, "
      "primary key (tx_hash, tx_lt), "
      "foreign key (tx_hash, tx_lt) references transactions);\n"
    );

    query += (
      "create table if not exists jetton_masters ("
      "id bigserial not null, "
      "address tonaddr not null primary key, "
      "total_supply numeric, "
      "mintable boolean, "
      "admin_address tonaddr, "
      "jetton_content jsonb, "
      "jetton_wallet_code_hash tonhash, "
      "last_transaction_lt bigint, "
      "code_hash tonhash, "
      "data_hash tonhash);\n"
    );

    query += (
      "create table if not exists mintless_jetton_masters ("
      "id bigserial,"
      "address tonaddr not null primary key,"
      "is_indexed boolean,"
      "custom_payload_api_uri character varying[]);\n"
    );

    query += (
      "create table if not exists jetton_wallets ("
      "id bigserial not null, "
      "address tonaddr not null primary key, "
      "balance numeric, "
      "owner tonaddr, "
      "jetton tonaddr, "
      "last_transaction_lt bigint, "
      "code_hash tonhash, "
      "data_hash tonhash, "
      "mintless_is_claimed boolean, "
      "mintless_amount numeric, "
      "mintless_start_from bigint, "
      "mintless_expire_at bigint);\n"
    );

    query += (
      "create table if not exists jetton_burns ( "
      "tx_hash tonhash not null, "
      "tx_lt bigint not null, "
      "tx_now integer not null, "
      "tx_aborted boolean not null, "
      "mc_seqno integer, "
      "query_id numeric, "
      "owner tonaddr, "
      "jetton_wallet_address tonaddr, "
      "jetton_master_address tonaddr, "
      "amount numeric, "
      "response_destination tonaddr, "
      "custom_payload text, "
      "trace_id tonhash, "
      "primary key (tx_hash, tx_lt), "
      "foreign key (tx_hash, tx_lt) references transactions);\n"
    );

    query += (
      "create table if not exists jetton_transfers ("
      "tx_hash tonhash not null, "
      "tx_lt bigint not null, "
      "tx_now integer not null, "
      "tx_aborted boolean not null, "
      "mc_seqno integer, "
      "query_id numeric, "
      "amount numeric, "
      "source tonaddr, "
      "destination tonaddr, "
      "jetton_wallet_address tonaddr, "
      "jetton_master_address tonaddr, "
      "response_destination tonaddr, "
      "custom_payload text, "
      "forward_ton_amount numeric, "
      "forward_payload text, "
      "trace_id tonhash, "
      "primary key (tx_hash, tx_lt), "
      "foreign key (tx_hash, tx_lt) references transactions);\n"
    );

    query += (
      "create table if not exists getgems_nft_sales ("
      "id bigserial not null, "
      "address tonaddr not null primary key, "
      "is_complete boolean, "
      "created_at bigint, "
      "marketplace_address tonaddr, "
      "nft_address tonaddr, "
      "nft_owner_address tonaddr, "
      "full_price numeric, "
      "marketplace_fee_address tonaddr, "
      "marketplace_fee numeric, "
      "royalty_address tonaddr, "
      "royalty_amount numeric, "
      "last_transaction_lt bigint, "
      "code_hash tonhash, "
      "data_hash tonhash);\n"
    );

    query += (
      "create table if not exists getgems_nft_auctions ("
      "id bigserial not null, "
      "address tonaddr not null primary key, "
      "end_flag boolean, "
      "end_time bigint, "
      "mp_addr tonaddr, "
      "nft_addr tonaddr, "
      "nft_owner tonaddr, "
      "last_bid numeric, "
      "last_member tonaddr, "
      "min_step bigint, "
      "mp_fee_addr tonaddr, "
      "mp_fee_factor bigint, "
      "mp_fee_base bigint, "
      "royalty_fee_addr tonaddr, "
      "royalty_fee_factor bigint, "
      "royalty_fee_base bigint, "
      "max_bid numeric, "
      "min_bid numeric, "
      "created_at bigint, "
      "last_bid_at bigint, "
      "is_canceled boolean, "
      "last_transaction_lt bigint, "
      "code_hash tonhash, "
      "data_hash tonhash);\n"
    );

    query += (
      "create table if not exists multisig ("
      "id bigserial not null, "
      "address tonaddr not null primary key, "
      "next_order_seqno numeric, "
      "threshold bigint, "
      "signers tonaddr[], "
      "proposers tonaddr[], "
      "last_transaction_lt bigint, "
      "code_hash tonhash, "
      "data_hash tonhash);\n"
    );

    query += (
      "create table if not exists multisig_orders ("
      "id bigserial not null, "
      "address tonaddr not null primary key, "
      "multisig_address tonaddr, "
      "order_seqno numeric, "
      "threshold bigint, "
      "sent_for_execution boolean, "
      "approvals_mask numeric, "
      "approvals_num bigint, "
      "expiration_date bigint, "
      "order_boc text, "
      "signers tonaddr[], "
      "last_transaction_lt bigint, "
      "code_hash tonhash, "
      "data_hash tonhash);\n"
    );

    query += (
      "create table if not exists traces ("
      "trace_id tonhash not null primary key, "
      "external_hash tonhash, "
      "external_hash_norm tonhash, "
      "mc_seqno_start integer, "
      "mc_seqno_end integer, "
      "start_lt bigint, "
      "start_utime integer, "
      "end_lt bigint, "
      "end_utime integer, "
      "state trace_state, "
      "pending_edges_ bigint, "
      "edges_ bigint, "
      "nodes_ bigint, "
      "classification_state trace_classification_state default 'unclassified'"
      ");\n"
    );
    
    query += (
      "create table if not exists actions ("
      "trace_id tonhash not null, "
      "action_id tonhash not null, "
      "start_lt bigint, "
      "end_lt bigint, "
      "start_utime bigint, "
      "end_utime bigint, "
      "source tonaddr, "
      "source_secondary tonaddr, "
      "destination tonaddr, "
      "destination_secondary tonaddr, "
      "asset tonaddr, "
      "asset_secondary tonaddr, "
      "asset2 tonaddr, "
      "asset2_secondary tonaddr, "
      "opcode bigint, "
      "tx_hashes tonhash[], "
      "type varchar, "
      "ton_transfer_data ton_transfer_details, "
      "value numeric, "
      "amount numeric, "
      "jetton_transfer_data jetton_transfer_details, "
      "nft_transfer_data nft_transfer_details, "
      "jetton_swap_data jetton_swap_details, "
      "change_dns_record_data change_dns_record_details, "
      "nft_mint_data nft_mint_details, "
      "success boolean default true, "
      "dex_withdraw_liquidity_data dex_withdraw_liquidity_details, "
      "dex_deposit_liquidity_data dex_deposit_liquidity_details, "
      "staking_data staking_details, "
      "tonco_deploy_pool_data tonco_deploy_pool_details, "
      "trace_end_lt bigint, "
      "trace_external_hash tonhash, "
      "trace_external_hash_norm tonhash, "
      "trace_end_utime integer, "
      "mc_seqno_end integer, "
      "trace_mc_seqno_end integer, "
      "multisig_create_order_data multisig_create_order_details, "
      "multisig_approve_data multisig_approve_details, "
      "multisig_execute_data multisig_execute_details, "
      "vesting_send_message_data vesting_send_message_details, "
      "vesting_add_whitelist_data vesting_add_whitelist_details, "
      "evaa_supply_data evaa_supply_details, "
      "evaa_withdraw_data evaa_withdraw_details, "
      "evaa_liquidate_data evaa_liquidate_details, "
      "jvault_claim_data jvault_claim_details, "
      "jvault_stake_data jvault_stake_details, "
      "parent_action_id varchar, "
      "ancestor_type varchar[] default '{}', "
      "value_extra_currencies jsonb default '{}'::jsonb, "
      "primary key (trace_id, action_id)"
      ") with (autovacuum_vacuum_scale_factor = 0.03);\n"
    );

    query += (
      "create table if not exists action_accounts ("
      "action_id tonhash not null, "
      "trace_id tonhash not null, "
      "account tonaddr not null, "
      "trace_end_lt bigint not null, "
      "action_end_lt bigint not null, "
      "trace_end_utime integer, "
      "action_end_utime bigint, "
      "primary key (account, trace_end_lt, trace_id, action_end_lt, action_id)"
      ") with (autovacuum_vacuum_scale_factor = 0.03);\n"
    );

    query += (
      "create table if not exists dns_entries ("
      "nft_item_address tonaddr not null primary key, "
      "nft_item_owner tonaddr, "
      "domain varchar, "
      "dns_next_resolver tonaddr, "
      "dns_wallet tonaddr, "
      "dns_site_adnl varchar(64), "
      "dns_storage_bag_id varchar(64), "
      "last_transaction_lt bigint);\n"
    );

    query += (
    "create table if not exists vesting_contracts ("
      "id bigserial not null, "
      "address                tonaddr not null primary key, "
      "vesting_start_time     integer, "
      "vesting_total_duration integer, "
      "unlock_period          integer, "
      "cliff_duration         integer, "
      "vesting_total_amount   numeric, "
      "vesting_sender_address tonaddr, "
      "owner_address          tonaddr, "
      "last_transaction_lt    bigint, "
      "code_hash              tonhash, "
      "data_hash              tonhash);\n"
    );

    query += (
      "create table if not exists vesting_whitelist ("
      "vesting_contract_address tonaddr not null, "
      "wallet_address           tonaddr not null, "
      "primary key (vesting_contract_address, wallet_address));\n"
    );

    query += "create table if not exists blocks_classified (mc_seqno integer not null primary key);\n";

    query += (
      "create unlogged table if not exists _classifier_tasks ("
      "id serial, "
      "mc_seqno integer, "
      "trace_id tonhash, "
      "pending boolean, "
      "claimed_at timestamp, "
      "start_after timestamp);\n"
    );

    query += (
      "create unlogged table if not exists _classifier_failed_traces ("
      "id serial, "
      "trace_id tonhash, "
      "broken boolean, "
      "error varchar);\n"
    );

    query += (
      "create table if not exists contract_methods ("
      "code_hash tonhash not null primary key, "
      "methods bigint[]);\n"
    );

    query += (
      "create unlogged table if not exists background_tasks ("
			"id bigint generated always as identity constraint background_tasks_pk primary key, "
			"type varchar, "
			"status varchar, "
			"retries integer default 0 not null, "
			"retry_at bigint, "
			"started_at bigint, "
			"data jsonb, "
			"error varchar"
		");\n"
    );

    query += (
      "create table if not exists address_metadata ("
			"address varchar not null, "
			"type varchar not null, "
			"valid boolean default true, "
			"name varchar, "
			"description varchar, "
			"extra jsonb, "
			"symbol varchar, "
			"image varchar, "
			"updated_at bigint, "
			"expires_at bigint, "
			"constraint address_metadata_pk primary key (address, type));\n"
    );

    query += (
      "create unlogged table if not exists ton_indexer_leader ("
      "id integer primary key check (id = 1),"
      "leader_worker_id varchar not null, "
      "last_heartbeat timestamptz not null, "
      "started_at timestamptz not null);\n"

      "insert into ton_indexer_leader (id, leader_worker_id, last_heartbeat, started_at)"
      "values (1, 'none', NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour');\n"
    );

    // create required indexes
    query += (
      "create index if not exists trace_unclassified_index on traces (state, start_lt) include (trace_id, nodes_) where (classification_state = 'unclassified'::trace_classification_state);\n"
      "create index if not exists _classifier_tasks_mc_seqno_idx on _classifier_tasks (mc_seqno desc);\n"
    );

    query += (
      "create or replace function on_new_mc_block_func() "
      "returns trigger language plpgsql as $$ "
      "begin\n"
      "insert into _classifier_tasks(mc_seqno, start_after)\n"
      "values (NEW.seqno, now() + interval '1 seconds');\n"
      "return null; \n"
      "end; $$;\n"
      "create or replace trigger on_new_mc_block "
      "after insert on blocks for each row when (new.workchain = '-1'::integer) "
      "execute procedure on_new_mc_block_func();\n"
    );

    query += (
      "create table if not exists ton_db_version ("
      "id                INTEGER PRIMARY KEY CHECK (id = 1),"
      "major             INTEGER NOT NULL, "
      "minor             INTEGER NOT NULL, "
      "patch             INTEGER NOT NULL);\n"
      
      "INSERT INTO ton_db_version (id, major, minor, patch) "
      "VALUES (1, 1, 2, 0) ON CONFLICT DO NOTHING;\n"
    );

    if (dry_run) {
      std::cout << query << std::endl;
      return;
    }

    LOG(DEBUG) << query;
    txn.exec(query).no_rows();
    txn.commit();
  } catch (const std::exception &e) {
    LOG(ERROR) << "Error while migrating database: " << e.what();
    std::exit(1);
  }

  LOG(INFO) << "Migration to version 1.2.0 completed successfully.";
}

void run_1_2_1_migrations(const std::string& connection_string, bool dry_run) {
  LOG(INFO) << "Running migrations to version 1.2.1";

  LOG(INFO) << "Altering types...";
  {
    auto exec_query = [&] (const std::string& query) {
      if (dry_run) {
        std::cout << query << std::endl;
        return;
      }

      try {
        pqxx::connection c(connection_string);
        pqxx::work txn(c);

        txn.exec(query).no_rows();
        txn.commit();
      } catch (const std::exception &e) {
        LOG(INFO) << "Skipping query '" << query << "': " << e.what();
      }
    };

    exec_query("alter type nft_transfer_details add attribute marketplace varchar;");
    exec_query("alter type nft_transfer_details add attribute real_prev_owner tonaddr;");
    exec_query("create type coffee_create_pool_details as (amount_1 numeric, amount_2 numeric, initiator_1 tonaddr, initiator_2 tonaddr, provided_asset tonaddr, lp_tokens_minted numeric, pool_creator_contract tonaddr);");
    exec_query("create type coffee_staking_deposit_details as (minted_item_address tonaddr, minted_item_index numeric);");
    exec_query("create type coffee_staking_withdraw_details as (nft_address tonaddr, nft_index numeric, points numeric);");
  }

  LOG(INFO) << "Updating tables...";
  try {
    pqxx::connection c(connection_string);
    pqxx::work txn(c);

    std::string query = "";

    query += "ALTER TABLE actions ADD COLUMN IF NOT EXISTS trace_external_hash_norm tonhash;\n";
    query += "ALTER TABLE traces ADD COLUMN IF NOT EXISTS external_hash_norm tonhash;\n";
    query += "ALTER TABLE actions ADD COLUMN IF NOT EXISTS coffee_create_pool_data coffee_create_pool_details;\n";
    query += "ALTER TABLE actions ADD COLUMN IF NOT EXISTS coffee_staking_deposit_data coffee_staking_deposit_details;\n";
    query += "ALTER TABLE actions ADD COLUMN IF NOT EXISTS coffee_staking_withdraw_data coffee_staking_withdraw_details;\n";

    query += (
      "INSERT INTO ton_db_version (id, major, minor, patch) "
      "VALUES (1, 1, 2, 1) ON CONFLICT(id) DO UPDATE "
      "SET major = 1, minor = 2, patch = 1;\n"
    );

    if (dry_run) {
      std::cout << query << std::endl;
      return;
    }

    LOG(DEBUG) << query;
    txn.exec(query).no_rows();
    txn.commit();
  } catch (const std::exception &e) {
    LOG(ERROR) << "Error while migrating database: " << e.what();
    std::exit(1);
  }

  LOG(INFO) << "Migration to version 1.2.1 completed successfully.";
}

void run_1_2_2_migrations(const std::string& connection_string, bool dry_run) {
  LOG(INFO) << "Running migrations to version 1.2.2";

  LOG(INFO) << "Altering types...";
  {
    auto exec_query = [&] (const std::string& query) {
      if (dry_run) {
        std::cout << query << std::endl;
        return;
      }

      try {
        pqxx::connection c(connection_string);
        pqxx::work txn(c);

        txn.exec(query).no_rows();
        txn.commit();
      } catch (const std::exception &e) {
        LOG(INFO) << "Skipping query '" << query << "': " << e.what();
      }
    };

    exec_query("alter type nft_transfer_details add attribute marketplace_address tonaddr;");
    exec_query("alter type nft_transfer_details add attribute payout_amount numeric;");
    exec_query("alter type nft_transfer_details add attribute payout_comment_encrypted boolean;");
    exec_query("alter type nft_transfer_details add attribute payout_comment_encoded boolean;");
    exec_query("alter type nft_transfer_details add attribute payout_comment text;");
    exec_query("alter type nft_transfer_details add attribute royalty_amount numeric;");
    exec_query("create type nft_listing_details as (nft_item_index numeric, full_price numeric, marketplace_fee numeric, royalty_amount numeric, mp_fee_factor numeric, mp_fee_base numeric, royalty_fee_base numeric, max_bid numeric, min_bid numeric, marketplace_fee_address tonaddr, royalty_address tonaddr, marketplace varchar);");
  }

  LOG(INFO) << "Updating tables...";
  try {
    pqxx::connection c(connection_string);
    pqxx::work txn(c);

    std::string query = "";

    query += "ALTER TABLE actions ADD COLUMN IF NOT EXISTS nft_listing_data nft_listing_details;\n";

    query += (
      "CREATE TABLE IF NOT EXISTS marketplace_names ("
      "address tonaddr NOT NULL PRIMARY KEY, "
      "name varchar NOT NULL);\n"
    );

    query += (
      "INSERT INTO ton_db_version (id, major, minor, patch) "
      "VALUES (1, 1, 2, 2) ON CONFLICT(id) DO UPDATE "
      "SET major = 1, minor = 2, patch = 2;\n"
    );

    if (dry_run) {
      std::cout << query << std::endl;
      return;
    }

    LOG(DEBUG) << query;
    txn.exec(query).no_rows();
    txn.commit();
  } catch (const std::exception &e) {
    LOG(ERROR) << "Error while migrating database: " << e.what();
    std::exit(1);
  }

  LOG(INFO) << "Migration to version 1.2.2 completed successfully.";
}

void run_1_2_3_migrations(const std::string& connection_string, bool dry_run) {
  LOG(INFO) << "Running migrations to version 1.2.3";

  LOG(INFO) << "Altering types...";
  {
    auto exec_query = [&] (const std::string& query) {
      if (dry_run) {
        std::cout << query << std::endl;
        return;
      }

      try {
        pqxx::connection c(connection_string);
        pqxx::work txn(c);

        txn.exec(query).no_rows();
        txn.commit();
      } catch (const std::exception &e) {
        LOG(INFO) << "Skipping query '" << query << "': " << e.what();
      }
    };

    exec_query("create type layerzero_send_details as (send_request_id numeric, msglib_manager varchar, msglib varchar, uln tonaddr, native_fee numeric, zro_fee numeric, endpoint tonaddr, channel tonaddr);");
    exec_query("create type layerzero_packet_details as (src_oapp varchar, dst_oapp varchar, src_eid integer, dst_eid integer, nonce numeric, guid varchar, message varchar);");
    exec_query("create type layerzero_dvn_verify_details as (nonce numeric, status varchar, dvn tonaddr, proxy tonaddr, uln tonaddr, uln_connection tonaddr);");
    exec_query("create type pool_type as enum ('stable', 'volatile');");
    exec_query("create type dex_type as enum ('dedust');");
  }

  LOG(INFO) << "Updating tables...";
  try {
    pqxx::connection c(connection_string);
    pqxx::work txn(c);

    std::string query = "";

    query += "ALTER TABLE messages ADD COLUMN IF NOT EXISTS extra_flags numeric;\n";


    query += "ALTER TABLE actions ADD COLUMN IF NOT EXISTS layerzero_send_data layerzero_send_details;\n";
    query += "ALTER TABLE actions ADD COLUMN IF NOT EXISTS layerzero_packet_data layerzero_packet_details;\n";
    query += "ALTER TABLE actions ADD COLUMN IF NOT EXISTS layerzero_dvn_verify_data layerzero_dvn_verify_details;\n";

    query += (
      "INSERT INTO ton_db_version (id, major, minor, patch) "
      "VALUES (1, 1, 2, 3) ON CONFLICT(id) DO UPDATE "
      "SET major = 1, minor = 2, patch = 3;\n"
    );

    query += (
      "CREATE TABLE IF NOT EXISTS dex_pools ("
      "id bigserial not null, "
      "address tonaddr not null primary key, "
      "asset_1 tonaddr, "
      "asset_2 tonaddr, "
      "reserve_1 numeric, "
      "reserve_2 numeric, "
      "pool_type pool_type, "
      "dex dex_type, "
      "fee double precision, "
      "last_transaction_lt bigint, "
      "code_hash tonhash, "
      "data_hash tonhash);\n"
    );

    if (dry_run) {
      std::cout << query << std::endl;
      return;
    }

    LOG(DEBUG) << query;
    txn.exec(query).no_rows();
    txn.commit();
  } catch (const std::exception &e) {
    LOG(ERROR) << "Error while migrating database: " << e.what();
    std::exit(1);
  }

  LOG(INFO) << "Migration to version 1.2.3 completed successfully.";
}

void run_1_2_4_migrations(const std::string& connection_string, bool dry_run) {
  LOG(INFO) << "Running migrations to version 1.2.4";

  LOG(INFO) << "Altering types...";
  {
    auto exec_query = [&] (const std::string& query) {
      if (dry_run) {
        std::cout << query << std::endl;
        return;
      }

      try {
        pqxx::connection c(connection_string);
        pqxx::work txn(c);

        txn.exec(query).no_rows();
        txn.commit();
      } catch (const std::exception &e) {
        LOG(INFO) << "Skipping query '" << query << "': " << e.what();
      }
    };

    // TODO: add new migrations
  }

  LOG(INFO) << "Updating tables...";
  try {
    pqxx::connection c(connection_string);
    pqxx::work txn(c);

    std::string query = "";

    query += "alter table address_metadata add reindex_allowed boolean default true not null;\n"

    query += (
      "INSERT INTO ton_db_version (id, major, minor, patch) "
      "VALUES (1, 1, 2, 4) ON CONFLICT(id) DO UPDATE "
      "SET major = 1, minor = 2, patch = 4;\n"
    );

    if (dry_run) {
      std::cout << query << std::endl;
      return;
    }

    LOG(DEBUG) << query;
    txn.exec(query).no_rows();
    txn.commit();
  } catch (const std::exception &e) {
    LOG(ERROR) << "Error while migrating database: " << e.what();
    std::exit(1);
  }

  LOG(INFO) << "Migration to version 1.2.4 completed successfully.";
}

void create_indexes(std::string connection_string, bool dry_run) {
  try {
    pqxx::connection c(connection_string);
    pqxx::work txn(c);
    
    std::string query = (
      "create index if not exists blocks_index_1 on blocks (gen_utime);\n"
      "create index if not exists blocks_index_2 on blocks (mc_block_seqno);\n"
      "create index if not exists blocks_index_3 on blocks (seqno) where (workchain = '-1'::integer);\n"
      "create index if not exists blocks_index_4 on blocks (start_lt);\n"
      "create index if not exists blocks_index_5 on blocks (root_hash);\n"
      "create index if not exists blocks_index_6 on blocks (file_hash);\n"
      "create index if not exists dns_entries_index_3 on dns_entries (dns_wallet, length(domain));\n"
      "create index if not exists dns_entries_index_4 on dns_entries (nft_item_owner, length(domain)) include (domain) where ((nft_item_owner)::text = (dns_wallet)::text);\n"
      "create index if not exists jetton_masters_index_1 on jetton_masters (admin_address, id);\n"
      "create index if not exists jetton_masters_index_2 on jetton_masters (id);\n"
      "create index if not exists jetton_wallets_index_1 on jetton_wallets (owner, id);\n"
      "create index if not exists jetton_wallets_index_2 on jetton_wallets (jetton, id);\n"
      "create index if not exists jetton_wallets_index_3 on jetton_wallets (id);\n"
      "create index if not exists jetton_wallets_index_4 on jetton_wallets (jetton asc, balance desc);\n"
      "create index if not exists jetton_wallets_index_5 on jetton_wallets (owner asc, balance desc);\n"
      "create index if not exists latest_account_states_index_1 on latest_account_states (balance desc) include (account);\n"
      "create index if not exists latest_account_states_address_book_index on latest_account_states (account) include (account_friendly, code_hash, account_status);\n"
      "create index if not exists latest_account_states_index_2 on latest_account_states (id);\n"
      "create index if not exists nft_collections_index_1 on nft_collections (owner_address, id);\n"
      "create index if not exists nft_collections_index_2 on nft_collections (id);\n"
      "create index if not exists nft_items_index_1 on nft_items (collection_address, index);\n"
      "create index if not exists nft_items_index_2 on nft_items (owner_address, collection_address, index);\n"
      "create index if not exists nft_items_index_3 on nft_items (id);\n"
      "create index if not exists trace_unclassified_index on traces (state, start_lt) include (trace_id, nodes_) where (classification_state = 'unclassified'::trace_classification_state);\n"
      "create index if not exists traces_index_1 on traces (state);\n"
      "create index if not exists trace_index_2a on traces (mc_seqno_end);\n"
      "create index if not exists traces_index_7 on traces (classification_state);\n"
      "create index if not exists traces_index_3 on traces (end_lt desc, trace_id desc);\n"
      "create index if not exists traces_index_4 on traces (end_utime desc, trace_id desc);\n"
      "create index if not exists jetton_burns_index_1 on jetton_burns (owner, tx_now, tx_lt);\n"
      "create index if not exists jetton_burns_index_2 on jetton_burns (owner, tx_lt);\n"
      "create index if not exists jetton_burns_index_3 on jetton_burns (jetton_wallet_address, tx_now, tx_lt);\n"
      "create index if not exists jetton_burns_index_4 on jetton_burns (jetton_wallet_address, tx_lt);\n"
      "create index if not exists jetton_burns_index_5 on jetton_burns (jetton_master_address, tx_now, tx_lt);\n"
      "create index if not exists jetton_burns_index_6 on jetton_burns (jetton_master_address, tx_lt);\n"
      "create index if not exists jetton_burns_index_7 on jetton_burns (tx_now, tx_lt);\n"
      "create index if not exists jetton_burns_index_8 on jetton_burns (tx_lt);\n"
      "create index if not exists jetton_transfers_index_1 on jetton_transfers (source, tx_now);\n"
      "create index if not exists jetton_transfers_index_2 on jetton_transfers (source, tx_lt);\n"
      "create index if not exists jetton_transfers_index_3 on jetton_transfers (destination, tx_lt);\n"
      "create index if not exists jetton_transfers_index_4 on jetton_transfers (destination, tx_now);\n"
      "create index if not exists jetton_transfers_index_6 on jetton_transfers (jetton_wallet_address, tx_lt);\n"
      "create index if not exists jetton_transfers_index_7 on jetton_transfers (jetton_master_address, tx_now);\n"
      "create index if not exists jetton_transfers_index_8 on jetton_transfers (jetton_master_address, tx_lt);\n"
      "create index if not exists jetton_transfers_index_9 on jetton_transfers (tx_now, tx_lt);\n"
      "create index if not exists jetton_transfers_index_10 on jetton_transfers (tx_lt);\n"
      "create index if not exists messages_index_1 on messages (msg_hash);\n"
      "create index if not exists messages_index_5 on messages (trace_id, tx_lt);\n"
      "create index if not exists messages_index_2 on messages (source, created_lt);\n"
      "create index if not exists messages_index_6 on messages (opcode, created_lt);\n"
      "create index if not exists messages_index_8 on messages (created_at, msg_hash);\n"
      "create index if not exists messages_index_7 on messages (created_lt, msg_hash);\n"
      "create index if not exists messages_index_3 on messages (destination, created_lt);\n"
      "create index if not exists messages_index_4 on messages (body_hash);\n"
      "create index if not exists messages_index_9 on messages (msg_hash_norm) where msg_hash_norm is not null;\n"
      "create index if not exists nft_transfers_index_2 on nft_transfers (nft_item_address, tx_lt);\n"
      "create index if not exists nft_transfers_index_3 on nft_transfers (nft_collection_address, tx_now);\n"
      "create index if not exists nft_transfers_index_4 on nft_transfers (nft_collection_address, tx_lt);\n"
      "create index if not exists nft_transfers_index_5 on nft_transfers (old_owner, tx_lt);\n"
      "create index if not exists nft_transfers_index_7 on nft_transfers (new_owner, tx_lt);\n"
      "create index if not exists nft_transfers_index_9 on nft_transfers (tx_lt);\n"
      "create index if not exists nft_transfers_index_10 on nft_transfers (tx_now, tx_lt);\n"
      "create index if not exists transactions_index_1 on transactions (block_workchain, block_shard, block_seqno);\n"
      "create index if not exists transactions_index_2 on transactions (lt);\n"
      "create index if not exists transactions_index_3 on transactions (now, lt);\n"
      "create index if not exists transactions_index_4 on transactions (account, lt);\n"
      "create index if not exists transactions_index_5 on transactions (account, now, lt);\n"
      "create index if not exists transactions_index_6 on transactions (hash);\n"
      "create index if not exists transactions_index_8 on transactions (mc_block_seqno, lt);\n"
      "create index if not exists transactions_index_7 on transactions (trace_id, lt);\n"
      "create index if not exists transactions_index_9 on transactions (account, trace_id);\n"
      "create index if not exists _classifier_tasks_mc_seqno_idx on _classifier_tasks (mc_seqno desc);\n"
      "create index if not exists actions_index_3 on actions (end_lt);\n"
      "create index if not exists actions_index_2 on actions (action_id);\n"
      "create index if not exists actions_index_1 on actions (trace_id, start_lt, end_lt);\n"
      "create index if not exists actions_index_4 on actions (trace_end_lt);\n"
      "create index if not exists actions_index_5 on actions (trace_mc_seqno_end);\n"
      "create index if not exists action_accounts_index_1 on action_accounts (action_id);\n"
      "create index if not exists action_accounts_index_2 on action_accounts (trace_id, action_id);\n"
      "create index if not exists action_accounts_index_3 on action_accounts (account, trace_end_utime, trace_id, action_end_utime, action_id);\n"
      "create index if not exists multisig_index_1 on multisig (id);\n"
      "create index if not exists multisig_index_2 on multisig using gin(signers);\n"
      "create index if not exists multisig_index_3 on multisig using gin(proposers);\n"
      "create index if not exists multisig_orders_index_1 on multisig_orders (id);\n"
      "create index if not exists multisig_orders_index_2 on multisig_orders (multisig_address);\n"
      "create index if not exists multisig_orders_index_3 on multisig_orders using gin(signers);\n"
      "create index if not exists vesting_index_1 on vesting_whitelist (wallet_address);\n"
      "create index if not exists vesting_index_2 on vesting_contracts (vesting_sender_address, id);\n"
      "create index if not exists vesting_index_3 on vesting_contracts (owner_address, id);\n"
      "create index if not exists vesting_index_4 on vesting_contracts (id);\n"
      "create index if not exists nft_items_index_4 on nft_items (last_transaction_lt);\n"
      "create index if not exists nft_items_index_5 on nft_items (owner_address, last_transaction_lt);\n"
      "create index if not exists nft_items_index_6 on nft_items (collection_address, last_transaction_lt);\n"
    );
    if (dry_run) {
      std::cout << query << std::endl;
      return;
    }

    txn.exec(query).no_rows();
    txn.commit();
  } catch (const std::exception &e) {
    LOG(ERROR) << "Error while creating indexes in database: " << e.what();
    std::exit(1);
  }
}

int main(int argc, char *argv[]) {
  SET_VERBOSITY_LEVEL(verbosity_INFO);
  td::set_default_failure_signal_handler().ensure();

  td::OptionParser p;
  p.set_description("Create Postgres database for TON blockchain indexing and run migrations");
  p.add_option('\0', "help", "Print help", [&p]() {
    td::StringBuilder sb;
    sb << p;
    std::cout << sb.as_cslice().c_str();
    std::exit(0);
  });
  std::string pg_connection_string;
  p.add_option('\0', "pg", "PostgreSQL connection string",  [&](td::Slice value) { 
    pg_connection_string = value.str();
  });
  bool custom_types = false;
  p.add_option('\0', "custom-types", "Use pgton extension with custom types", [&]() {
    custom_types = true;
    LOG(WARNING) << "Using pgton extension!";
  });
  
  bool should_create_indexes = true;
  p.add_option('\0', "no-create-indexes", "Do not create indexes in database", [&]() {
    should_create_indexes = false;
    LOG(WARNING) << "Indexes will not be created.";
  });

  bool dry_run = false;
  p.add_option('\0', "dry-run", "Do not apply migrations, just print SQL queries", [&]() {
    dry_run = true;
    LOG(WARNING) << "Dry run mode enabled. No changes will be applied to the database.";
  });

  bool rerun_last_migration = false;
  p.add_option('\0', "rerun-last-migration", "Apply last migration regardless current version", [&]() {
    rerun_last_migration = true;
    LOG(WARNING) << "Rerun last migration mode enabled.";
  });

  auto S = p.run(argc, argv);
  if (S.is_error()) {
    LOG(ERROR) << "failed to parse options: " << S.move_as_error();
    std::exit(2);
  }
  if (pg_connection_string.empty()) {
    LOG(WARNING) << "Parameter `--pg` is not present, environment variables (PGHOST, PGPORT, PGUSER, etc.) are used.";
  }

  std::optional<Version> current_version{};
  try {
    current_version = get_current_db_version(pg_connection_string);
  } catch (const std::exception &e) {
    LOG(ERROR) << "Error getting database version: " << e.what();
    std::_Exit(2);
  }

  if (current_version.has_value()) {
    if (current_version->major != latest_version.major || current_version->minor != latest_version.minor) {
      LOG(ERROR) << "Migration to a different major.minor version is not supported. Existing DB version: "
                 << current_version->str() << ", latest version: " << latest_version.str();
      LOG(ERROR) << "Please create an new empty database to start indexing with the this version of indexer.";
      std::exit(1);
    }
  }

  if (current_version.has_value() && *current_version == latest_version && !rerun_last_migration) {
    LOG(INFO) << "Database is already at the latest version. No migrations needed.";
  } else {
    if (!current_version.has_value()) {
      run_1_2_0_migrations(pg_connection_string, custom_types, dry_run);
      current_version = Version{1, 2, 0};
    }
    if (migration_needed(current_version, Version{1, 2, 1}, rerun_last_migration)) {
      run_1_2_1_migrations(pg_connection_string, dry_run);
      current_version = Version{1, 2, 1};
    }
    if (migration_needed(current_version, Version{1, 2, 2}, rerun_last_migration)) {
      run_1_2_2_migrations(pg_connection_string, dry_run);
      current_version = Version{1, 2, 2};
    }
    if (migration_needed(current_version, Version{1, 2, 3}, rerun_last_migration)) {
      run_1_2_3_migrations(pg_connection_string, dry_run);
      current_version = Version{1, 2, 3};
    }

    if (migration_needed(current_version, Version{1, 2, 4}, rerun_last_migration)) {
      run_1_2_4_migrations(pg_connection_string, dry_run);
      current_version = Version{1, 2, 4};
    }

    // In future, more migrations will be added here
    // if (is_migration_needed(current_version, Version{1, 2, 2}, rerun_last_migration)) {
    //   run_1_2_2_migrations(pg_connection_string);
    //   current_version = Version{1, 2, 2};
    // }
    // if (is_migration_needed(current_version, Version{1, 2, 3}, rerun_last_migration)) {
    //   run_1_2_3_migrations(pg_connection_string);
    //   current_version = Version{1, 2, 3};
    // }
    // and so on...
  }
  
  if (should_create_indexes) {
    LOG(INFO) << "Creating indexes in DB.";
    create_indexes(pg_connection_string, dry_run);
  } else {
    LOG(INFO) << "Skipping index creation.";
  }
  
  LOG(INFO) << "Migration tool completed successfully.";

  return 0;
}
