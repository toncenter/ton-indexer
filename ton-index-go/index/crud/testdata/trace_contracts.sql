CREATE TEMP TABLE traces (
    trace_id text, external_hash text, mc_seqno_start integer, mc_seqno_end integer,
    start_lt bigint, start_utime integer, end_lt bigint, end_utime integer,
    state text, edges_ bigint, nodes_ bigint, pending_edges_ bigint, classification_state text
);
CREATE TEMP TABLE transactions (
    account text, hash text, lt bigint, block_workchain integer, block_shard bigint,
    block_seqno integer, mc_block_seqno integer, trace_id text, prev_trans_hash text,
    prev_trans_lt bigint, now integer, orig_status text, end_status text, total_fees bigint,
    total_fees_extra_currencies jsonb, account_state_hash_before text, account_state_hash_after text,
    descr text, aborted boolean, destroyed boolean, credit_first boolean, is_tock boolean,
    installed boolean, storage_fees_collected bigint, storage_fees_due bigint, storage_status_change text,
    credit_due_fees_collected bigint, credit bigint, credit_extra_currencies jsonb,
    compute_skipped boolean, skipped_reason text, compute_success boolean, compute_msg_state_used boolean,
    compute_account_activated boolean, compute_gas_fees bigint, compute_gas_used bigint,
    compute_gas_limit bigint, compute_gas_credit bigint, compute_mode integer, compute_exit_code integer,
    compute_exit_arg integer, compute_vm_steps integer, compute_vm_init_state_hash text, compute_vm_final_state_hash text,
    action_success boolean, action_valid boolean, action_no_funds boolean, action_status_change text,
    action_total_fwd_fees bigint, action_total_action_fees bigint, action_result_code integer, action_result_arg integer,
    action_tot_actions integer, action_spec_actions integer, action_skipped_actions integer, action_msgs_created integer,
    action_action_list_hash text, action_tot_msg_size_cells bigint, action_tot_msg_size_bits bigint,
    bounce text, bounce_msg_size_cells bigint, bounce_msg_size_bits bigint, bounce_req_fwd_fees bigint,
    bounce_msg_fees bigint, bounce_fwd_fees bigint, split_info_cur_shard_pfx_len integer,
    split_info_acc_split_depth integer, split_info_this_addr text, split_info_sibling_addr text
);
CREATE TEMP TABLE account_states (
    hash text, account text, balance text, balance_extra_currencies jsonb,
    account_status text, frozen_hash text, data_hash text, code_hash text
);
CREATE TEMP TABLE messages (
    tx_hash text, tx_lt bigint, msg_hash text, direction text, trace_id text, source text,
    destination text, value bigint, value_extra_currencies jsonb, fwd_fee bigint,
    ihr_fee bigint, extra_flags text, created_lt bigint, created_at integer, opcode bigint,
    ihr_disabled boolean, bounce boolean, bounced boolean, import_fee bigint,
    body_hash text, init_state_hash text, msg_hash_norm text
);
CREATE TEMP TABLE message_contents (hash text, body text);
CREATE DOMAIN pg_temp.tonhash AS text;
CREATE TEMP TABLE latest_account_states (account text, code_hash text);
CREATE TEMP TABLE contract_methods (code_hash text, methods bigint[]);
CREATE TEMP TABLE dns_entries (nft_item_owner text, domain text, dns_wallet text, destroyed boolean);
CREATE TEMP TABLE _blocks_classified (mc_seqno integer);
