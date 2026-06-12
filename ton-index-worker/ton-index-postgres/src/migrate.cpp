
#include <iostream>
#include "td/utils/port/signals.h"
#include "td/utils/OptionParser.h"
#include "td/utils/logging.h"
#include "pqxx/pqxx"
#include "version.h"

bool migration_needed(std::optional<Version> current_version, Version migration_version) {
  return current_version.has_value() && *current_version < migration_version;
}

std::string set_version_query(const Version& version) {
  std::stringstream ss;
  ss << "create table if not exists ton_db_version ("
     << "id                INTEGER PRIMARY KEY CHECK (id = 1),"
     << "major             INTEGER NOT NULL, "
     << "minor             INTEGER NOT NULL, "
     << "patch             INTEGER NOT NULL);\n";

  ss << "INSERT INTO ton_db_version(id, major, minor, patch) "
     << "VALUES (1, " << version.major << ", " << version.minor << ", " << version.patch << ") "
     << "ON CONFLICT (id) DO UPDATE SET major = EXCLUDED.major, minor = EXCLUDED.minor, patch = EXCLUDED.patch;\n";
  return ss.str();
}

void ensure_latest_version(const std::string& connection_string, bool dry_run) {
  const Version& version = latest_version;
  auto query = set_version_query(version);
  if (dry_run) {
    std::cout << query << std::endl;
    return;
  }
  try {
    pqxx::connection c(connection_string);
    pqxx::work txn(c);
    txn.exec(query).no_rows();
    LOG(INFO) << "Set latest version to " << version.major << "." << version.minor << "." << version.patch;
    txn.commit();
  } catch (const std::exception &e) {
    LOG(WARNING) << "Failed to ensure version: " << e.what();
  }
}

void run_1_3_0_migrations(const std::string& connection_string, bool custom_types, bool dry_run) {
  LOG(INFO) << "Running migrations to version 1.3.0";

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
      exec_query("create domain tonbytes as text;");
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
    exec_query("create type pool_type as enum ('stable', 'volatile');");
    exec_query("create type dex_type as enum ('dedust');");

    exec_query(R"SQL(create type change_dns_record_details as
(
    key          varchar,
    value_schema varchar,
    value        varchar,
    flags        integer
);)SQL");
    exec_query(R"SQL(create type liquidity_vault_excess_details as
(
    asset  tonaddr,
    amount NUMERIC
);)SQL");
    exec_query(R"SQL(create type dex_deposit_liquidity_details as
(
    dex                  varchar,
    amount1              numeric,
    amount2              numeric,
    asset1               tonaddr,
    asset2               tonaddr,
    user_jetton_wallet_1 tonaddr,
    user_jetton_wallet_2 tonaddr,
    lp_tokens_minted     numeric,
    target_asset_1       tonaddr,
    target_asset_2       tonaddr,
    target_amount_1      numeric,
    target_amount_2      numeric,
    vault_excesses       liquidity_vault_excess_details[],
    tick_lower           numeric,
    tick_upper           numeric,
    nft_index            numeric,
    nft_address          tonaddr
);)SQL");
    exec_query(R"SQL(create type dex_transfer_details as
(
    amount                    numeric,
    asset                     tonaddr,
    source                    tonaddr,
    destination               tonaddr,
    source_jetton_wallet      tonaddr,
    destination_jetton_wallet tonaddr
);)SQL");
    exec_query(R"SQL(create type dex_withdraw_liquidity_details as
(
    dex                  varchar,
    amount1              numeric,
    amount2              numeric,
    asset1_out           tonaddr,
    asset2_out           tonaddr,
    user_jetton_wallet_1 tonaddr,
    user_jetton_wallet_2 tonaddr,
    dex_jetton_wallet_1  tonaddr,
    dex_jetton_wallet_2  tonaddr,
    lp_tokens_burnt      numeric,
    dex_wallet_1         tonaddr,
    dex_wallet_2         tonaddr,
    burned_nft_index     numeric,
    burned_nft_address   tonaddr,
    tick_lower           numeric,
    tick_upper           numeric
);)SQL");
    exec_query(R"SQL(create type jetton_transfer_details as
(
    response_destination tonaddr,
    forward_amount       numeric,
    query_id             numeric,
    custom_payload       tonbytes,
    forward_payload      tonbytes,
    comment              text,
    is_encrypted_comment boolean
);)SQL");
    exec_query(R"SQL(create type nft_mint_details as
(
    nft_item_index numeric
);)SQL");
    exec_query(R"SQL(create type nft_transfer_details as
(
    is_purchase              boolean,
    price                    numeric,
    query_id                 numeric,
    custom_payload           tonbytes,
    forward_payload          tonbytes,
    forward_amount           numeric,
    response_destination     tonaddr,
    nft_item_index           numeric,
    marketplace              varchar,
    real_prev_owner          tonaddr,
    marketplace_address      tonaddr,
    payout_amount            numeric,
    payout_comment_encrypted boolean,
    payout_comment_encoded   boolean,
    payout_comment           text,
    royalty_amount           numeric
);)SQL");
    exec_query(R"SQL(create type peer_swap_details as
(
    asset_in   tonaddr,
    amount_in  numeric,
    asset_out  tonaddr,
    amount_out numeric
);)SQL");
    exec_query(R"SQL(create type jetton_swap_details as
(
    dex                   varchar,
    sender                tonaddr,
    dex_incoming_transfer dex_transfer_details,
    dex_outgoing_transfer dex_transfer_details,
    peer_swaps            peer_swap_details[],
    min_out_amount        numeric
);)SQL");
    exec_query(R"SQL(create type staking_details as
(
    provider      varchar,
    ts_nft        tonaddr,
    tokens_burnt  numeric,
    tokens_minted numeric
);)SQL");
    exec_query(R"SQL(create type ton_transfer_details as
(
    content   text,
    encrypted boolean
);)SQL");
    exec_query(R"SQL(create type multisig_create_order_details as
(
    query_id             numeric,
    order_seqno          numeric,
    is_created_by_signer boolean,
    is_signed_by_creator boolean,
    creator_index        numeric,
    expiration_date      numeric,
    order_boc            tonbytes
);)SQL");
    exec_query(R"SQL(create type multisig_approve_details as
(
    signer_index numeric,
    exit_code    numeric
);)SQL");
    exec_query(R"SQL(create type multisig_execute_details as
(
    query_id        numeric,
    order_seqno     numeric,
    expiration_date numeric,
    approvals_num   numeric,
    signers_hash    tonhash,
    order_boc       tonbytes
);)SQL");
    exec_query(R"SQL(create type vesting_send_message_details as
(
    query_id    numeric,
    message_boc tonbytes
);)SQL");
    exec_query(R"SQL(create type vesting_add_whitelist_details as
(
    query_id       numeric,
    accounts_added tonaddr[]
);)SQL");
    exec_query(R"SQL(create type evaa_supply_details as
(
    sender_jetton_wallet    tonaddr,
    recipient_jetton_wallet tonaddr,
    master_jetton_wallet    tonaddr,
    master                  tonaddr,
    asset_id                varchar,
    is_ton                  boolean
);)SQL");
    exec_query(R"SQL(create type evaa_withdraw_details as
(
    sender_jetton_wallet    tonaddr,
    recipient_jetton_wallet tonaddr,
    master_jetton_wallet    tonaddr,
    master                  tonaddr,
    fail_reason             varchar,
    asset_id                varchar
);)SQL");
    exec_query(R"SQL(create type evaa_liquidate_details as
(
    fail_reason text,
    debt_amount numeric,
    asset_id    varchar
);)SQL");
    exec_query(R"SQL(create type jvault_claim_details as
(
    claimed_jettons tonaddr[],
    claimed_amounts numeric[]
);)SQL");
    exec_query(R"SQL(create type jvault_stake_details as
(
    period               numeric,
    minted_stake_jettons numeric,
    stake_wallet         tonaddr
);)SQL");
    exec_query(R"SQL(create type tonco_deploy_pool_details as
(
    jetton0_router_wallet tonaddr,
    jetton1_router_wallet tonaddr,
    jetton0_minter        tonaddr,
    jetton1_minter        tonaddr,
    tick_spacing          integer,
    initial_price_x96     numeric,
    protocol_fee          integer,
    lp_fee_base           integer,
    lp_fee_current        integer,
    pool_active           boolean
);)SQL");
    exec_query(R"SQL(create type coffee_create_pool_details as
(
    amount_1              numeric,
    amount_2              numeric,
    initiator_1           tonaddr,
    initiator_2           tonaddr,
    provided_asset        tonaddr,
    lp_tokens_minted      numeric,
    pool_creator_contract tonaddr
);)SQL");
    exec_query(R"SQL(create type coffee_staking_deposit_details as
(
    minted_item_address tonaddr,
    minted_item_index   numeric
);)SQL");
    exec_query(R"SQL(create type coffee_staking_withdraw_details as
(
    nft_address tonaddr,
    nft_index   numeric,
    points      numeric
);)SQL");
    exec_query(R"SQL(create type nft_listing_details as
(
    nft_item_index          numeric,
    full_price              numeric,
    marketplace_fee         numeric,
    royalty_amount          numeric,
    mp_fee_factor           numeric,
    mp_fee_base             numeric,
    royalty_fee_base        numeric,
    max_bid                 numeric,
    min_bid                 numeric,
    marketplace_fee_address tonaddr,
    royalty_address         tonaddr,
    marketplace             varchar
);)SQL");
    exec_query(R"SQL(create type layerzero_send_details as
(
    send_request_id numeric,
    msglib_manager  varchar,
    msglib          varchar,
    uln             tonaddr,
    native_fee      numeric,
    zro_fee         numeric,
    endpoint        tonaddr,
    channel         tonaddr
);)SQL");
    exec_query(R"SQL(create type layerzero_packet_details as
(
    src_oapp varchar,
    dst_oapp varchar,
    src_eid  integer,
    dst_eid  integer,
    nonce    numeric,
    guid     varchar,
    message  varchar
);)SQL");
    exec_query(R"SQL(create type layerzero_dvn_verify_details as
(
    nonce          numeric,
    status         varchar,
    dvn            tonaddr,
    proxy          tonaddr,
    uln            tonaddr,
    uln_connection tonaddr
);)SQL");
    exec_query(R"SQL(create type cocoon_worker_payout_details as
(
    payout_type   varchar,
    query_id      numeric,
    new_tokens    numeric,
    worker_state  integer,
    worker_tokens numeric
);)SQL");
    exec_query(R"SQL(create type cocoon_proxy_payout_details as
(
    query_id numeric
);)SQL");
    exec_query(R"SQL(create type cocoon_proxy_charge_details as
(
    query_id         numeric,
    new_tokens_used  numeric,
    expected_address varchar
);)SQL");
    exec_query(R"SQL(create type cocoon_client_top_up_details as
(
    query_id numeric
);)SQL");
    exec_query(R"SQL(create type cocoon_register_proxy_details as
(
    query_id numeric
);)SQL");
    exec_query(R"SQL(create type cocoon_unregister_proxy_details as
(
    query_id numeric,
    seqno    integer
);)SQL");
    exec_query(R"SQL(create type cocoon_client_register_details as
(
    query_id numeric,
    nonce    numeric
);)SQL");
    exec_query(R"SQL(create type cocoon_client_change_secret_hash_details as
(
    query_id        numeric,
    new_secret_hash varchar
);)SQL");
    exec_query(R"SQL(create type cocoon_client_request_refund_details as
(
    query_id   numeric,
    via_wallet boolean
);)SQL");
    exec_query(R"SQL(create type cocoon_grant_refund_details as
(
    query_id         numeric,
    new_tokens_used  numeric,
    expected_address varchar
);)SQL");
    exec_query(R"SQL(create type cocoon_client_increase_stake_details as
(
    query_id  numeric,
    new_stake numeric
);)SQL");
    exec_query(R"SQL(create type cocoon_client_withdraw_details as
(
    query_id        numeric,
    withdraw_amount numeric
);)SQL");
  }

  LOG(INFO) << "Creating tables...";
  try {
    pqxx::connection c(connection_string);
    pqxx::work txn(c);

    std::string query = R"SQL(
create table if not exists blocks
(
    workchain                 integer not null,
    shard                     bigint  not null,
    seqno                     integer not null,
    root_hash                 tonhash,
    file_hash                 tonhash,
    mc_block_seqno            integer not null,
    global_id                 integer,
    version                   integer,
    after_merge               boolean,
    before_split              boolean,
    after_split               boolean,
    want_merge                boolean,
    want_split                boolean,
    key_block                 boolean,
    vert_seqno_incr           boolean,
    flags                     integer,
    gen_utime                 bigint,
    start_lt                  bigint,
    end_lt                    bigint,
    validator_list_hash_short integer,
    gen_catchain_seqno        integer,
    min_ref_mc_seqno          integer,
    prev_key_block_seqno      integer,
    vert_seqno                integer,
    master_ref_seqno          integer,
    rand_seed                 tonhash,
    created_by                tonhash,
    tx_count                  integer,
    prev_blocks               blockid[],
    primary key (workchain, shard, seqno, mc_block_seqno)
) partition by range (mc_block_seqno);
create table if not exists blocks_default partition of blocks default;

create table if not exists shard_state
(
    mc_seqno  integer not null,
    workchain integer not null,
    shard     bigint  not null,
    seqno     integer not null,
    primary key (workchain, shard, seqno, mc_seqno)
) partition by range (mc_seqno);
create table if not exists shard_state_default partition of shard_state default;

create table if not exists transactions
(
    account                      tonaddr not null,
    hash                         tonhash not null,
    lt                           bigint  not null,
    block_workchain              integer,
    block_shard                  bigint,
    block_seqno                  integer,
    mc_block_seqno               integer not null,
    trace_id                     tonhash,
    prev_trans_hash              tonhash,
    prev_trans_lt                bigint,
    now                          integer,
    orig_status                  account_status_type,
    end_status                   account_status_type,
    total_fees                   bigint,
    total_fees_extra_currencies  jsonb,
    account_state_hash_before    tonhash,
    account_state_hash_after     tonhash,
    descr                        descr_type,
    aborted                      boolean,
    destroyed                    boolean,
    credit_first                 boolean,
    is_tock                      boolean,
    installed                    boolean,
    storage_fees_collected       bigint,
    storage_fees_due             bigint,
    storage_status_change        status_change_type,
    credit_due_fees_collected    bigint,
    credit                       bigint,
    credit_extra_currencies      jsonb,
    compute_skipped              boolean,
    skipped_reason               skipped_reason_type,
    compute_success              boolean,
    compute_msg_state_used       boolean,
    compute_account_activated    boolean,
    compute_gas_fees             bigint,
    compute_gas_used             bigint,
    compute_gas_limit            bigint,
    compute_gas_credit           bigint,
    compute_mode                 smallint,
    compute_exit_code            integer,
    compute_exit_arg             integer,
    compute_vm_steps             bigint,
    compute_vm_init_state_hash   tonhash,
    compute_vm_final_state_hash  tonhash,
    action_success               boolean,
    action_valid                 boolean,
    action_no_funds              boolean,
    action_status_change         status_change_type,
    action_total_fwd_fees        bigint,
    action_total_action_fees     bigint,
    action_result_code           int,
    action_result_arg            int,
    action_tot_actions           int,
    action_spec_actions          int,
    action_skipped_actions       int,
    action_msgs_created          int,
    action_action_list_hash      tonhash,
    action_tot_msg_size_cells    bigint,
    action_tot_msg_size_bits     bigint,
    bounce                       bounce_type,
    bounce_msg_size_cells        bigint,
    bounce_msg_size_bits         bigint,
    bounce_req_fwd_fees          bigint,
    bounce_msg_fees              bigint,
    bounce_fwd_fees              bigint,
    split_info_cur_shard_pfx_len int,
    split_info_acc_split_depth   int,
    split_info_this_addr         tonhash,
    split_info_sibling_addr      tonhash,
    primary key (hash, lt, mc_block_seqno)
) partition by range (mc_block_seqno);
create table if not exists transactions_default partition of transactions default;

create table if not exists messages
(
    tx_hash                tonhash,
    tx_lt                  bigint,
    mc_seqno               integer not null,
    msg_hash               tonhash,
    msg_hash_norm          tonhash,
    direction              msg_direction,
    trace_id               tonhash,
    source                 tonaddr,
    destination            tonaddr,
    value                  bigint,
    value_extra_currencies jsonb,
    fwd_fee                bigint,
    ihr_fee                bigint,
    extra_flags            numeric,
    created_lt             bigint,
    created_at             bigint,
    opcode                 integer,
    ihr_disabled           boolean,
    bounce                 boolean,
    bounced                boolean,
    import_fee             bigint,
    body_hash              tonhash,
    init_state_hash        tonhash,
    primary key (tx_hash, tx_lt, msg_hash, direction, mc_seqno)
) partition by range (mc_seqno);
create table if not exists messages_default partition of messages default;

create table if not exists message_contents
(
    hash tonhash not null primary key,
    body tonbytes
);

create table if not exists account_states
(
    hash                     tonhash not null primary key,
    account                  tonaddr,
    balance                  bigint,
    balance_extra_currencies jsonb,
    account_status           account_status_type,
    frozen_hash              tonhash,
    data_hash                tonhash,
    code_hash                tonhash
);

create table if not exists latest_account_states
(
    id                       bigserial not null,
    account                  tonaddr   not null primary key,
    hash                     tonhash   not null,
    balance                  bigint,
    balance_extra_currencies jsonb,
    account_status           account_status_type,
    timestamp                integer,
    last_trans_hash          tonhash,
    last_trans_lt            bigint,
    frozen_hash              tonhash,
    data_hash                tonhash,
    code_hash                tonhash,
    data_boc                 tonbytes,
    code_boc                 tonbytes
) with (fillfactor = 70, autovacuum_vacuum_scale_factor = 0.03);

create table if not exists nft_collections
(
    id                  bigserial not null,
    address             tonaddr   not null primary key,
    next_item_index     numeric,
    owner_address       tonaddr,
    collection_content  jsonb,
    last_transaction_lt bigint,
    code_hash           tonhash,
    data_hash           tonhash,
    destroyed           boolean   not null default false
) with (fillfactor = 70);

create table if not exists nft_items
(
    id                  bigserial not null,
    address             tonaddr   not null primary key,
    init                boolean,
    index               numeric,
    collection_address  tonaddr,
    owner_address       tonaddr,
    real_owner          tonaddr,
    content             jsonb,
    last_transaction_lt bigint,
    code_hash           tonhash,
    data_hash           tonhash,
    destroyed           boolean   not null default false
) with (fillfactor = 70);

create table if not exists nft_transfers
(
    tx_hash                tonhash not null,
    tx_lt                  bigint  not null,
    tx_now                 integer not null,
    tx_aborted             boolean not null,
    mc_seqno               integer not null,
    query_id               numeric,
    nft_item_address       tonaddr,
    nft_item_index         numeric,
    nft_collection_address tonaddr,
    old_owner              tonaddr,
    new_owner              tonaddr,
    response_destination   tonaddr,
    custom_payload         tonbytes,
    forward_amount         numeric,
    forward_payload        tonbytes,
    trace_id               tonhash,
    primary key (tx_hash, tx_lt, mc_seqno)
) partition by range (mc_seqno);
create table if not exists nft_transfers_default partition of nft_transfers default;

create table if not exists jetton_masters
(
    id                      bigserial not null,
    address                 tonaddr   not null primary key,
    total_supply            numeric,
    mintable                boolean,
    admin_address           tonaddr,
    jetton_content          jsonb,
    jetton_wallet_code_hash tonhash,
    last_transaction_lt     bigint,
    code_hash               tonhash,
    data_hash               tonhash,
    destroyed               boolean   not null default false
) with (fillfactor = 70);

create table if not exists mintless_jetton_masters
(
    id                     bigserial,
    address                tonaddr not null primary key,
    is_indexed             boolean,
    custom_payload_api_uri varchar[]
);

create table if not exists jetton_wallets
(
    id                  bigserial not null,
    address             tonaddr   not null primary key,
    balance             numeric,
    owner               tonaddr,
    jetton              tonaddr,
    last_transaction_lt bigint,
    code_hash           tonhash,
    data_hash           tonhash,
    mintless_is_claimed boolean,
    mintless_amount     numeric,
    mintless_start_from bigint,
    mintless_expire_at  bigint,
    destroyed           boolean   not null default false
) with (fillfactor = 70, autovacuum_vacuum_scale_factor = 0.03);

create table if not exists jetton_burns
(
    tx_hash               tonhash not null,
    tx_lt                 bigint  not null,
    tx_now                integer not null,
    tx_aborted            boolean not null,
    mc_seqno              integer not null,
    query_id              numeric,
    owner                 tonaddr,
    jetton_wallet_address tonaddr,
    jetton_master_address tonaddr,
    amount                numeric,
    response_destination  tonaddr,
    custom_payload        tonbytes,
    trace_id              tonhash,
    primary key (tx_hash, tx_lt, mc_seqno)
) partition by range (mc_seqno);
create table if not exists jetton_burns_default partition of jetton_burns default;

create table if not exists jetton_transfers
(
    tx_hash               tonhash not null,
    tx_lt                 bigint  not null,
    tx_now                integer not null,
    tx_aborted            boolean not null,
    mc_seqno              integer not null,
    query_id              numeric,
    amount                numeric,
    source                tonaddr,
    destination           tonaddr,
    jetton_wallet_address tonaddr,
    jetton_master_address tonaddr,
    response_destination  tonaddr,
    custom_payload        tonbytes,
    forward_ton_amount    numeric,
    forward_payload       tonbytes,
    trace_id              tonhash,
    primary key (tx_hash, tx_lt, mc_seqno)
) partition by range (mc_seqno);
create table if not exists jetton_transfers_default partition of jetton_transfers default;

create table if not exists getgems_nft_sales
(
    id                      bigserial not null,
    address                 tonaddr   not null primary key,
    is_complete             boolean,
    created_at              bigint,
    marketplace_address     tonaddr,
    nft_address             tonaddr,
    nft_owner_address       tonaddr,
    full_price              numeric,
    marketplace_fee_address tonaddr,
    marketplace_fee         numeric,
    royalty_address         tonaddr,
    royalty_amount          numeric,
    last_transaction_lt     bigint,
    code_hash               tonhash,
    data_hash               tonhash,
    sold_at                 numeric,
    sold_query_id           numeric,
    jetton_price_dict       jsonb,
    destroyed               boolean   not null default false
) with (fillfactor = 70);

create table if not exists getgems_nft_auctions
(
    id                  bigserial not null,
    address             tonaddr   not null primary key,
    end_flag            boolean,
    end_time            bigint,
    mp_addr             tonaddr,
    nft_addr            tonaddr,
    nft_owner           tonaddr,
    last_bid            numeric,
    last_member         tonaddr,
    min_step            bigint,
    mp_fee_addr         tonaddr,
    mp_fee_factor       bigint,
    mp_fee_base         bigint,
    royalty_fee_addr    tonaddr,
    royalty_fee_factor  bigint,
    royalty_fee_base    bigint,
    max_bid             numeric,
    min_bid             numeric,
    created_at          bigint,
    last_bid_at         bigint,
    is_canceled         boolean,
    last_transaction_lt bigint,
    code_hash           tonhash,
    data_hash           tonhash,
    activated           boolean,
    step_time           bigint,
    last_query_id       numeric,
    jetton_wallet       tonaddr,
    jetton_master       tonaddr,
    is_broken_state     boolean,
    public_key          varchar,
    destroyed           boolean   not null default false
) with (fillfactor = 70);

create table if not exists multisig
(
    id                  bigserial not null,
    address             tonaddr   not null primary key,
    next_order_seqno    numeric,
    threshold           bigint,
    signers             tonaddr[],
    proposers           tonaddr[],
    last_transaction_lt bigint,
    code_hash           tonhash,
    data_hash           tonhash,
    destroyed           boolean   not null default false
) with (fillfactor = 70);

create table if not exists multisig_orders
(
    id                  bigserial not null,
    address             tonaddr   not null primary key,
    multisig_address    tonaddr,
    order_seqno         numeric,
    threshold           bigint,
    sent_for_execution  boolean,
    approvals_mask      numeric,
    approvals_num       bigint,
    expiration_date     bigint,
    order_boc           tonbytes,
    signers             tonaddr[],
    last_transaction_lt bigint,
    code_hash           tonhash,
    data_hash           tonhash,
    destroyed           boolean   not null default false
) with (fillfactor = 70);

create table if not exists traces
(
    trace_id             tonhash not null,
    external_hash        tonhash,
    external_hash_norm   tonhash,
    mc_seqno_start       integer,
    mc_seqno_end         integer not null,
    start_lt             bigint,
    start_utime          integer,
    end_lt               bigint,
    end_utime            integer,
    state                trace_state,
    pending_edges_       bigint,
    edges_               bigint,
    nodes_               bigint,
    classification_state trace_classification_state default 'unclassified',
    primary key (trace_id, mc_seqno_end)
) partition by range (mc_seqno_end);
create table if not exists traces_default partition of traces default;
create index if not exists traces_unclassified on traces (state, start_lt) include (trace_id, nodes_)
    where (classification_state = 'unclassified'::trace_classification_state);


create table if not exists actions
(
    trace_id                              tonhash not null,
    action_id                             tonhash not null,
    start_lt                              bigint,
    end_lt                                bigint,
    start_utime                           bigint,
    end_utime                             bigint,
    source                                tonaddr,
    source_secondary                      tonaddr,
    destination                           tonaddr,
    destination_secondary                 tonaddr,
    asset                                 tonaddr,
    asset_secondary                       tonaddr,
    asset2                                tonaddr,
    asset2_secondary                      tonaddr,
    opcode                                bigint,
    tx_hashes                             tonhash[],
    type                                  varchar,
    ton_transfer_data                     ton_transfer_details,
    value                                 numeric,
    amount                                numeric,
    jetton_transfer_data                  jetton_transfer_details,
    nft_transfer_data                     nft_transfer_details,
    jetton_swap_data                      jetton_swap_details,
    change_dns_record_data                change_dns_record_details,
    nft_mint_data                         nft_mint_details,
    success                               boolean   default true,
    dex_withdraw_liquidity_data           dex_withdraw_liquidity_details,
    dex_deposit_liquidity_data            dex_deposit_liquidity_details,
    staking_data                          staking_details,
    tonco_deploy_pool_data                tonco_deploy_pool_details,
    trace_end_lt                          bigint,
    trace_external_hash                   tonhash,
    trace_external_hash_norm              tonhash,
    trace_end_utime                       integer,
    mc_seqno_end                          integer,
    trace_mc_seqno_end                    integer not null,
    multisig_create_order_data            multisig_create_order_details,
    multisig_approve_data                 multisig_approve_details,
    multisig_execute_data                 multisig_execute_details,
    vesting_send_message_data             vesting_send_message_details,
    vesting_add_whitelist_data            vesting_add_whitelist_details,
    evaa_supply_data                      evaa_supply_details,
    evaa_withdraw_data                    evaa_withdraw_details,
    evaa_liquidate_data                   evaa_liquidate_details,
    jvault_claim_data                     jvault_claim_details,
    jvault_stake_data                     jvault_stake_details,
    parent_action_id                      varchar,
    ancestor_type                         varchar[] default '{}',
    value_extra_currencies                jsonb     default '{}'::jsonb,
    coffee_create_pool_data               coffee_create_pool_details,
    coffee_staking_deposit_data           coffee_staking_deposit_details,
    coffee_staking_withdraw_data          coffee_staking_withdraw_details,
    nft_listing_data                      nft_listing_details,
    layerzero_send_data                   layerzero_send_details,
    layerzero_packet_data                 layerzero_packet_details,
    layerzero_dvn_verify_data             layerzero_dvn_verify_details,
    cocoon_worker_payout_data             cocoon_worker_payout_details,
    cocoon_proxy_payout_data              cocoon_proxy_payout_details,
    cocoon_proxy_charge_data              cocoon_proxy_charge_details,
    cocoon_client_top_up_data             cocoon_client_top_up_details,
    cocoon_register_proxy_data            cocoon_register_proxy_details,
    cocoon_unregister_proxy_data          cocoon_unregister_proxy_details,
    cocoon_client_register_data           cocoon_client_register_details,
    cocoon_client_change_secret_hash_data cocoon_client_change_secret_hash_details,
    cocoon_client_request_refund_data     cocoon_client_request_refund_details,
    cocoon_grant_refund_data              cocoon_grant_refund_details,
    cocoon_client_increase_stake_data     cocoon_client_increase_stake_details,
    cocoon_client_withdraw_data           cocoon_client_withdraw_details,
    extra                                 jsonb,
    primary key (trace_id, action_id, trace_mc_seqno_end)
) partition by range (trace_mc_seqno_end);
create table if not exists actions_default partition of actions default;

create table if not exists action_accounts
(
    action_id          tonhash not null,
    trace_id           tonhash not null,
    account            tonaddr not null,
    trace_end_lt       bigint  not null,
    action_end_lt      bigint  not null,
    trace_end_utime    integer,
    trace_mc_seqno_end integer not null,
    action_end_utime   bigint,
    primary key (account, trace_end_lt, trace_id, action_end_lt, action_id, trace_mc_seqno_end)
) partition by range (trace_mc_seqno_end);
create table if not exists action_accounts_default partition of action_accounts default;

create table if not exists dns_entries
(
    nft_item_address    tonaddr not null primary key,
    nft_item_owner      tonaddr,
    domain              varchar,
    dns_next_resolver   tonaddr,
    dns_wallet          tonaddr,
    dns_site_adnl       tonhash,
    dns_storage_bag_id  tonhash,
    last_transaction_lt bigint,
    destroyed           boolean not null default false
) with (fillfactor = 70);

create table if not exists vesting_contracts
(
    id                     bigserial not null,
    address                tonaddr   not null primary key,
    vesting_start_time     integer,
    vesting_total_duration integer,
    unlock_period          integer,
    cliff_duration         integer,
    vesting_total_amount   numeric,
    vesting_sender_address tonaddr,
    owner_address          tonaddr,
    last_transaction_lt    bigint,
    code_hash              tonhash,
    data_hash              tonhash,
    destroyed              boolean   not null default false
) with (fillfactor = 70);

create table if not exists vesting_whitelist
(
    vesting_contract_address tonaddr not null,
    wallet_address           tonaddr not null,
    primary key (vesting_contract_address, wallet_address)
);

create table if not exists telemint_nft_items
(
    id                  bigserial not null,
    address             tonaddr   not null primary key,
    token_name          varchar,
    bidder_address      tonaddr,
    bid                 numeric,
    bid_ts              integer,
    min_bid             numeric,
    end_time            integer,
    beneficiary_address tonaddr,
    initial_min_bid     numeric,
    max_bid             numeric,
    min_bid_step        numeric,
    min_extend_time     bigint,
    duration            bigint,
    royalty_numerator   bigint,
    royalty_denominator bigint,
    royalty_destination tonaddr,
    last_transaction_lt bigint,
    code_hash           tonhash,
    data_hash           tonhash,
    destroyed           boolean   not null default false
) with (fillfactor = 70);

CREATE TABLE IF NOT EXISTS marketplace_names
(
    address tonaddr NOT NULL PRIMARY KEY,
    name    varchar NOT NULL
);

CREATE TABLE IF NOT EXISTS dex_pools
(
    id                  bigserial not null,
    address             tonaddr   not null primary key,
    asset_1             tonaddr,
    asset_2             tonaddr,
    reserve_1           numeric,
    reserve_2           numeric,
    pool_type           pool_type,
    dex                 dex_type,
    fee                 double precision,
    last_transaction_lt bigint,
    code_hash           tonhash,
    data_hash           tonhash,
    destroyed           boolean   not null default false
) with (fillfactor = 70);

create table if not exists nominator_pools
(
    id                     bigserial not null,
    address                tonaddr   not null primary key,
    state                  integer,
    nominators_count       integer,
    stake_amount_sent      numeric,
    validator_amount       numeric,
    validator_address      tonaddr,
    validator_reward_share integer,
    max_nominators_count   integer,
    min_validator_stake    numeric,
    min_nominator_stake    numeric,
    active_nominators      jsonb     not null default '[]'::jsonb,
    last_transaction_lt    bigint,
    code_hash              tonhash,
    data_hash              tonhash,
    destroyed              boolean   not null default false
) with (fillfactor = 70);

create table if not exists nominator_pool_events
(
    tx_hash                 tonhash not null,
    tx_lt                   bigint  not null,
    tx_now                  integer not null,
    mc_seqno                integer not null,
    trace_id                tonhash,
    pool_address            tonaddr not null,
    nominator_address       tonaddr not null,
    event_index             integer not null,
    event_type              varchar not null,
    amount                  numeric not null,
    balance_delta           numeric not null,
    pending_balance_delta   numeric not null,
    balance_before          numeric not null,
    balance_after           numeric not null,
    pending_balance_before  numeric not null,
    pending_balance_after   numeric not null,
    withdraw_request_before boolean not null,
    withdraw_request_after  boolean not null,
    primary key (tx_hash, tx_lt, event_index, mc_seqno)
)
    partition by range (mc_seqno);
create table if not exists nominator_pool_events_default partition of nominator_pool_events default;

create table if not exists nominator_pool_validator_events
(
    tx_hash                tonhash not null,
    tx_lt                  bigint  not null,
    tx_now                 integer not null,
    mc_seqno               integer not null,
    pool_address           tonaddr not null,
    validator_address      tonaddr not null,
    event_type             varchar not null,
    amount                 numeric not null,
    balance_delta          numeric not null,
    balance_before         numeric not null,
    balance_after          numeric not null,
    query_id               numeric,
    cycle_start            integer,
    validator_reward_share integer not null,
    primary key (tx_hash, tx_lt, mc_seqno)
)
    partition by range (mc_seqno);
create table if not exists nominator_pool_validator_events_default partition of nominator_pool_validator_events default;

create table if not exists validator_events
(
    tx_hash                  tonhash not null,
    tx_lt                    bigint  not null,
    tx_now                   integer not null,
    mc_seqno                 integer not null,
    event_type               varchar not null,
    stake_holder_address     tonaddr not null,
    validator_pubkey         varchar,
    adnl_addr                varchar,
    election_id              integer,
    query_id                 numeric,
    amount                   numeric not null,
    reason                   integer,
    primary key (tx_hash, tx_lt, mc_seqno)
)
    partition by range (mc_seqno);
create table if not exists validator_events_default partition of validator_events default;

create table if not exists validator_elections
(
    election_id     integer not null primary key,
    elect_close     integer not null,
    min_stake       numeric not null,
    total_stake     numeric not null,
    failed          boolean not null,
    finished        boolean not null,
    source_mc_seqno integer not null
);

create table if not exists validator_election_participants
(
    election_id          integer not null,
    validator_pubkey     varchar not null,
    stake                numeric not null,
    max_factor           integer not null,
    stake_holder_address tonaddr not null,
    adnl_addr            varchar not null,
    source_mc_seqno      integer not null,
    primary key (election_id, validator_pubkey, stake_holder_address)
);

create table if not exists validator_cycles
(
    election_id             integer,
    utime_since             integer not null,
    utime_until             integer not null,
    total                   integer not null,
    main                    integer not null,
    total_weight            numeric not null,
    total_stake             numeric,
    source_mc_seqno         integer not null,
    validators_elected_for  integer not null,
    elections_start_before  integer not null,
    elections_end_before    integer not null,
    stake_held_for          integer not null,
    max_validators          integer not null,
    max_main_validators     integer not null,
    min_validators          integer not null,
    min_stake               numeric not null,
    max_stake               numeric not null,
    min_total_stake         numeric not null,
    max_stake_factor        integer not null,
    primary key (utime_since)
);

create table if not exists validator_cycle_members
(
    utime_since         integer not null,
    validator_index     integer not null,
    validator_pubkey    varchar not null,
    adnl_addr           varchar not null,
    weight              numeric not null,
    source_mc_seqno     integer not null,
    primary key (utime_since, validator_index)
);

create table if not exists validator_complaints
(
    election_id          integer not null,
    complaint_hash       varchar not null,
    validator_pubkey     varchar not null,
    adnl_addr            varchar,
    description_boc      text not null,
    created_at           integer not null,
    severity             integer not null,
    reward_address       tonaddr not null,
    paid                 numeric not null,
    suggested_fine       numeric not null,
    suggested_fine_part  integer not null,
    voted_validators     jsonb not null default '[]'::jsonb,
    vset_id              varchar not null,
    weight_remaining     bigint not null,
    approved_percent     double precision not null,
    is_passed            boolean not null,
    source_mc_seqno      integer not null,
    primary key (election_id, complaint_hash)
);

create table if not exists contract_methods
(
    code_hash tonhash not null primary key,
    methods   bigint[]
);
create table if not exists address_metadata
(
    address     tonaddr not null,
    type        varchar not null,
    valid       boolean default true,
    name        varchar,
    description varchar,
    extra       jsonb,
    symbol      varchar,
    image       varchar,
    updated_at  bigint,
    expires_at  bigint,
    reindex_allowed boolean not null default true,
    constraint address_metadata_pk primary key (address, type)
);

--
-- operational tables
--
create table if not exists _blocks_classified
(
    mc_seqno integer not null primary key
);

create table if not exists _classifier_tasks
(
    id          serial primary key,
    mc_seqno    integer,
    trace_id    tonhash,
    pending     boolean,
    claimed_at  timestamp,
    start_after timestamp
);
create index if not exists _classifier_tasks_mc_seqno_idx on _classifier_tasks (mc_seqno desc);

create table if not exists _classifier_progress
(
    id integer primary key check (id = 1),
    last_scheduled_mc_seqno integer not null,
    updated_at timestamptz not null default now()
);
do $$
declare
    current_max integer;
    initial_progress integer;
    old_watermark integer;
begin
    select coalesce(max(seqno), 0)
    into current_max
    from blocks
    where workchain = -1;

    initial_progress := current_max;
    if to_regclass('classifier_watermark') is not null then
        execute 'select last_scheduled_mc_seqno from classifier_watermark where id = 1'
        into old_watermark;
        if old_watermark is not null then
            initial_progress := least(old_watermark, current_max);
        end if;
    end if;

    if current_max > 0 or old_watermark is not null then
        insert into _classifier_progress(id, last_scheduled_mc_seqno)
        values (1, initial_progress)
        on conflict (id) do nothing;
    end if;
end;
$$;

create table if not exists _classifier_failed_traces
(
    id       serial,
    trace_id tonhash,
    broken   boolean,
    error    varchar
);

-- TODO: rework this table
create table if not exists _background_tasks
(
    id         bigint generated always as identity
        constraint background_tasks_pk primary key,
    type       varchar,
    status     varchar,
    retries    integer default 0 not null,
    retry_at   bigint,
    started_at bigint,
    data       jsonb,
    error      varchar
);

create table if not exists _ton_indexer_leader
(
    id               integer primary key check (id = 1),
    leader_worker_id varchar     not null,
    last_heartbeat   timestamptz not null,
    started_at       timestamptz not null
);
insert into _ton_indexer_leader (id, leader_worker_id, last_heartbeat, started_at)
values (1, 'none', NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour')
on conflict (id) do nothing;

create table if not exists _ton_indexer_progress
(
    id                 integer primary key check (id = 1),
    finalized_mc_seqno integer     not null,
    updated_at         timestamptz not null default now()
);

-- triggers
do $$
begin
    if to_regclass('blocks_to_classify') is not null then
        execute 'drop trigger if exists on_block_buffered on blocks_to_classify';
    end if;
end;
$$;

drop function if exists drain_classifiable_prefix();

create or replace function drain_classifier_progress(max_count integer default 1000)
    returns integer
    language plpgsql as
$$
declare
    cur integer;
    watermark integer;
    scheduled_count integer := 0;
begin
    if max_count is null or max_count <= 0 then
        max_count := 1000;
    end if;

    select last_scheduled_mc_seqno
    into watermark
    from _classifier_progress
    where id = 1
    for update skip locked;

    if not found then
        return 0;
    end if;

    cur := watermark + 1;
    loop
        exit when scheduled_count >= max_count;
        exit when not exists (select 1
                              from blocks
                              where workchain = '-1'::integer
                                and seqno = cur);

        insert into _classifier_tasks(mc_seqno, start_after)
        values (cur, now() + interval '1 seconds');
        scheduled_count := scheduled_count + 1;
        cur := cur + 1;
    end loop;

    if scheduled_count > 0 then
        update _classifier_progress
        set last_scheduled_mc_seqno = cur - 1,
            updated_at = now()
        where id = 1
          and last_scheduled_mc_seqno < cur - 1;
    end if;

    return scheduled_count;
end;
$$;

create or replace function on_new_mc_block_func()
    returns trigger
    language plpgsql as
$$
begin
    insert into _classifier_progress(id, last_scheduled_mc_seqno)
    values (1, NEW.seqno - 1)
    on conflict (id) do nothing;

    if exists (select 1
               from _classifier_progress
               where id = 1
                 and last_scheduled_mc_seqno >= NEW.seqno) then
        insert into _classifier_tasks(mc_seqno, start_after)
        values (NEW.seqno, now() + interval '1 seconds');
    else
        perform drain_classifier_progress();
    end if;

    return null;
end;
$$;

create or replace trigger on_new_mc_block
    after insert
    on blocks
    for each row
    when (new.workchain = '-1'::integer)
execute procedure on_new_mc_block_func();

create or replace function advance_ton_indexer_progress(max_count integer, wait_for_lock boolean)
    returns integer
    language plpgsql as
$$
declare
    next_seqno      integer;
    finalized_seqno integer;
    advanced_count  integer := 0;
begin
    if wait_for_lock then
        perform 1 from _ton_indexer_progress where id = 1 for update;
    else
        perform 1 from _ton_indexer_progress where id = 1 for update skip locked;
    end if;

    if not found then
        select finalized_mc_seqno
        into finalized_seqno
        from _ton_indexer_progress
        where id = 1;

        return finalized_seqno;
    end if;

    loop
        exit when max_count is not null and advanced_count >= max_count;

        select finalized_mc_seqno + 1
        into next_seqno
        from _ton_indexer_progress
        where id = 1;

        exit when not exists (select 1
                              from blocks
                              where workchain = '-1'::integer
                                and seqno = next_seqno);

        update _ton_indexer_progress
        set finalized_mc_seqno = next_seqno,
            updated_at         = now()
        where id = 1;
        advanced_count := advanced_count + 1;
    end loop;

    select finalized_mc_seqno
    into finalized_seqno
    from _ton_indexer_progress
    where id = 1;

    return finalized_seqno;
end;
$$;

create or replace function advance_ton_indexer_progress()
    returns integer
    language sql as
$$
    select advance_ton_indexer_progress(null::integer, true);
$$;

create or replace function advance_ton_indexer_progress_func()
    returns trigger
    language plpgsql as
$$
begin
    if NEW.workchain <> '-1'::integer then
        return null;
    end if;

    if coalesce(current_setting('ton_indexer.advance_progress', true), 'on') = 'off' then
        return null;
    end if;

    perform advance_ton_indexer_progress(1000, false);
    return null;
end;
$$;

create or replace trigger advance_ton_indexer_progress
    after insert
    on blocks
    for each row
    when (new.workchain = '-1'::integer)
execute procedure advance_ton_indexer_progress_func();

create or replace function notify_dex_pools_change() returns trigger as
$$
declare
    target_dex dex_type;
begin
    if TG_OP = 'DELETE' then
        target_dex := OLD.dex;
    else
        target_dex := NEW.dex;
    end if;

    if target_dex = 'dedust' then
        perform pg_notify('dex_pools_changes', '');
    end if;

    return null;
end;
$$ language plpgsql;

drop trigger if exists dex_pools_change_trg on dex_pools;
create trigger dex_pools_change_trg
    after insert or delete
    on dex_pools
    for each row
execute function notify_dex_pools_change();

-- real owner of nft item
CREATE OR REPLACE FUNCTION update_nft_real_owner() RETURNS TRIGGER AS
$$
BEGIN
    IF NEW.destroyed THEN RETURN NEW; END IF;
    IF NEW.real_owner IS NULL OR NEW.real_owner = NEW.owner_address THEN
        SELECT nft_owner_address
        INTO NEW.real_owner
        FROM getgems_nft_sales
        WHERE address = NEW.owner_address
          AND NOT destroyed
          AND nft_address = NEW.address
        LIMIT 1;
        IF NEW.real_owner IS NULL THEN
            SELECT nft_owner
            INTO NEW.real_owner
            FROM getgems_nft_auctions
            WHERE address = NEW.owner_address
              AND NOT destroyed
              AND nft_addr = NEW.address
            LIMIT 1;
        END IF;
        IF NEW.real_owner IS NULL THEN NEW.real_owner := NEW.owner_address; END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER try_update_nft_real_owner
    BEFORE INSERT OR UPDATE OF owner_address
    ON nft_items
    FOR EACH ROW
EXECUTE FUNCTION update_nft_real_owner();

)SQL";

    query += set_version_query({1, 3, 0});
    if (dry_run) {
      std::cout << query << std::endl;
      return;
    }

    txn.exec(query).no_rows();
    txn.commit();
  } catch (const std::exception &e) {
    LOG(ERROR) << "Error while migrating database: " << e.what();
    std::exit(1);
  }

  LOG(INFO) << "Migration to version 1.3.0 completed successfully.";
}

void create_indexes(std::string connection_string, bool dry_run) {
  try {
    pqxx::connection c(connection_string);
    pqxx::work txn(c);
    
    std::string query = R"SQL(

create index if not exists blocks_index_1 on blocks (gen_utime);
create index if not exists blocks_index_2 on blocks (mc_block_seqno);
create index if not exists blocks_index_3 on blocks (seqno) where (workchain = '-1'::integer);
create index if not exists blocks_index_4 on blocks (start_lt);
create index if not exists blocks_index_5 on blocks (root_hash);
create index if not exists blocks_index_6 on blocks (file_hash);

create index if not exists dns_entries_index_1 on dns_entries (dns_wallet, length(domain));
create index if not exists dns_entries_index_2 on dns_entries (nft_item_owner, length(domain)) include (domain) where ((nft_item_owner)::text = (dns_wallet)::text);

create index if not exists jetton_masters_index_1 on jetton_masters (admin_address, id);
create index if not exists jetton_masters_index_2 on jetton_masters (id);

create index if not exists jetton_wallets_index_1 on jetton_wallets (owner, id);
create index if not exists jetton_wallets_index_2 on jetton_wallets (jetton, id);
create index if not exists jetton_wallets_index_3 on jetton_wallets (id);
create index if not exists jetton_wallets_index_4 on jetton_wallets (jetton asc, balance desc);
create index if not exists jetton_wallets_index_5 on jetton_wallets (owner asc, balance desc);

create index if not exists latest_account_states_index_1 on latest_account_states (balance desc) include (account);
create index if not exists latest_account_states_index_2 on latest_account_states (id);
create index if not exists latest_account_states_address_book_index on latest_account_states (account) include (code_hash, account_status);

create index if not exists nft_collections_index_1 on nft_collections (owner_address, id);
create index if not exists nft_collections_index_2 on nft_collections (id);

create index if not exists nft_items_index_1 on nft_items (collection_address, index);
create index if not exists nft_items_index_2 on nft_items (owner_address, collection_address, index);
create index if not exists nft_items_index_3 on nft_items (id);
create index if not exists nft_items_index_4 on nft_items (last_transaction_lt);
create index if not exists nft_items_index_5 on nft_items (owner_address, last_transaction_lt);
create index if not exists nft_items_index_6 on nft_items (collection_address, last_transaction_lt);
create index if not exists nft_items_index_7 on nft_items (real_owner, last_transaction_lt);
create index if not exists nft_items_index_8 on nft_items (real_owner, collection_address, index);

create index if not exists traces_index_1 on traces (state);
create index if not exists traces_index_2 on traces (mc_seqno_end);
create index if not exists traces_index_3 on traces (classification_state);
create index if not exists traces_index_4 on traces (end_lt desc, trace_id desc);
create index if not exists traces_index_5 on traces (end_utime desc, trace_id desc);

create index if not exists jetton_burns_index_1 on jetton_burns (owner, tx_now, tx_lt);
create index if not exists jetton_burns_index_2 on jetton_burns (owner, tx_lt);
create index if not exists jetton_burns_index_3 on jetton_burns (jetton_wallet_address, tx_now, tx_lt);
create index if not exists jetton_burns_index_4 on jetton_burns (jetton_wallet_address, tx_lt);
create index if not exists jetton_burns_index_5 on jetton_burns (jetton_master_address, tx_now, tx_lt);
create index if not exists jetton_burns_index_6 on jetton_burns (jetton_master_address, tx_lt);
create index if not exists jetton_burns_index_7 on jetton_burns (tx_now, tx_lt);
create index if not exists jetton_burns_index_8 on jetton_burns (tx_lt);

create index if not exists jetton_transfers_index_1 on jetton_transfers (source, tx_now);
create index if not exists jetton_transfers_index_2 on jetton_transfers (source, tx_lt);
create index if not exists jetton_transfers_index_3 on jetton_transfers (destination, tx_lt);
create index if not exists jetton_transfers_index_4 on jetton_transfers (destination, tx_now);
create index if not exists jetton_transfers_index_5 on jetton_transfers (jetton_wallet_address, tx_lt);
create index if not exists jetton_transfers_index_6 on jetton_transfers (jetton_master_address, tx_now);
create index if not exists jetton_transfers_index_7 on jetton_transfers (jetton_master_address, tx_lt);
create index if not exists jetton_transfers_index_8 on jetton_transfers (tx_now, tx_lt);
create index if not exists jetton_transfers_index_9 on jetton_transfers (tx_lt);

create index if not exists messages_index_1 on messages (msg_hash);
create index if not exists messages_index_2 on messages (trace_id, tx_lt);
create index if not exists messages_index_3 on messages (source, created_lt);
create index if not exists messages_index_4 on messages (opcode, created_lt);
create index if not exists messages_index_5 on messages (created_at, msg_hash);
create index if not exists messages_index_6 on messages (created_lt, msg_hash);
create index if not exists messages_index_7 on messages (destination, created_lt);
create index if not exists messages_index_8 on messages (body_hash);
create index if not exists messages_index_9 on messages (msg_hash_norm) where msg_hash_norm is not null;

create index if not exists nft_transfers_index_1 on nft_transfers (nft_item_address, tx_lt);
create index if not exists nft_transfers_index_2 on nft_transfers (nft_collection_address, tx_now);
create index if not exists nft_transfers_index_3 on nft_transfers (nft_collection_address, tx_lt);
create index if not exists nft_transfers_index_4 on nft_transfers (old_owner, tx_lt);
create index if not exists nft_transfers_index_5 on nft_transfers (new_owner, tx_lt);
create index if not exists nft_transfers_index_6 on nft_transfers (tx_lt);
create index if not exists nft_transfers_index_7 on nft_transfers (tx_now, tx_lt);

create index if not exists transactions_index_1 on transactions (block_workchain, block_shard, block_seqno);
create index if not exists transactions_index_2 on transactions (lt);
create index if not exists transactions_index_3 on transactions (now, lt);
create index if not exists transactions_index_4 on transactions (account, lt);
create index if not exists transactions_index_5 on transactions (account, now, lt);
create index if not exists transactions_index_6 on transactions (hash);
create index if not exists transactions_index_8 on transactions (mc_block_seqno, lt);
create index if not exists transactions_index_7 on transactions (trace_id, lt);
create index if not exists transactions_index_9 on transactions (account, trace_id);

create index if not exists action_accounts_index_1 on action_accounts (action_id);
create index if not exists action_accounts_index_2 on action_accounts (trace_id, action_id);
create index if not exists action_accounts_index_3 on action_accounts (account, trace_end_utime, trace_id, action_end_utime, action_id);

create index if not exists actions_index_3 on actions (end_lt);
create index if not exists actions_index_2 on actions (action_id);
create index if not exists actions_index_1 on actions (trace_id, start_lt, end_lt);
create index if not exists actions_index_4 on actions (trace_end_lt);
create index if not exists actions_index_5 on actions (trace_mc_seqno_end);

create index if not exists multisig_index_1 on multisig (id);
create index if not exists multisig_index_2 on multisig using gin(signers);
create index if not exists multisig_index_3 on multisig using gin(proposers);

create index if not exists multisig_orders_index_1 on multisig_orders (id);
create index if not exists multisig_orders_index_2 on multisig_orders (multisig_address);
create index if not exists multisig_orders_index_3 on multisig_orders using gin(signers);

create index if not exists vesting_index_1 on vesting_whitelist (wallet_address);
create index if not exists vesting_index_2 on vesting_contracts (vesting_sender_address, id);
create index if not exists vesting_index_3 on vesting_contracts (owner_address, id);
create index if not exists vesting_index_4 on vesting_contracts (id);

create index if not exists nominator_pool_events_nominator_idx on nominator_pool_events(nominator_address, pool_address, tx_now desc, tx_lt desc, event_index desc);
create index if not exists nominator_pool_events_pool_idx on nominator_pool_events(pool_address, tx_now desc, tx_lt desc, event_index desc);
create index if not exists nominator_pool_validator_events_validator_idx on nominator_pool_validator_events(validator_address, pool_address, tx_now desc, tx_lt desc);
create index if not exists nominator_pool_validator_events_pool_idx on nominator_pool_validator_events(pool_address, tx_now desc, tx_lt desc);
create index if not exists nominator_pool_validator_events_cycle_idx on nominator_pool_validator_events(cycle_start);
create index if not exists nominator_pools_index_1 on nominator_pools (id);
create index if not exists nominator_pools_active_nominators_idx on nominator_pools using gin(active_nominators);
create index if not exists validator_events_stake_holder_idx on validator_events(stake_holder_address, tx_now desc, tx_lt desc);
create index if not exists validator_events_pubkey_idx on validator_events(validator_pubkey, tx_now desc, tx_lt desc);
create index if not exists validator_events_election_idx on validator_events(election_id, tx_now desc, tx_lt desc);
create index if not exists validator_election_participants_stake_holder_idx on validator_election_participants(stake_holder_address);
create index if not exists validator_election_participants_pubkey_idx on validator_election_participants(validator_pubkey);
create index if not exists validator_cycles_election_idx on validator_cycles(election_id);
create index if not exists validator_cycle_members_pubkey_idx on validator_cycle_members(validator_pubkey);
create index if not exists validator_cycle_members_adnl_idx on validator_cycle_members(adnl_addr);
create index if not exists validator_complaints_pubkey_idx on validator_complaints(validator_pubkey);
)SQL";
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
    if (!current_version.has_value() || rerun_last_migration || migration_needed(current_version, Version{1, 3, 0})) {
      run_1_3_0_migrations(pg_connection_string, custom_types, dry_run);
      current_version = Version{1, 3, 0};
    }


    // In future, more migrations will be added here
    // not every version must have migrations
    // name of a function should have target version of migration,
    // f.e. run_1_3_1 sets version to 1.3.1
    // if (migration_needed(current_version, Version{1, 3, 1})) {
    //   run_1_3_1_migrations(pg_connection_string);
    //   current_version = Version{1, 3, 1};
    // }
    // and so on...

    // finally, we bump a version to the latest
    ensure_latest_version(pg_connection_string, dry_run);
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
