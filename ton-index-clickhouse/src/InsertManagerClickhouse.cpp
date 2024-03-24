#include "InsertManagerClickhouse.h"

#include "td/utils/StringBuilder.h"
#include "clickhouse/client.h"
#include "clickhouse/columns/nullable.h"
#include "clickhouse/columns/numeric.h"

#include "convert-utils.h"


#define TO_STD_OPTIONAL(x) ((x) ? std::optional(x.value()) : std::nullopt)

void InsertManagerClickhouse::start_up() {
    LOG(INFO) << "Clickhouse start_up";
    try {
        {
            clickhouse::ClientOptions default_options;
            default_options.SetHost(credential_.host);
            default_options.SetPort(credential_.port);
            default_options.SetUser(credential_.user);
            default_options.SetPassword(credential_.password);
            default_options.SetDefaultDatabase("default");

            clickhouse::Client client(default_options);
            td::StringBuilder builder;
            builder << "CREATE DATABASE IF NOT EXISTS " << credential_.dbname << ";";
            client.Execute(builder.as_cslice().str());
            LOG(INFO) << "Database " << credential_.dbname << " created";
        }
        auto options = credential_.get_clickhouse_options();
        clickhouse::Client client(options);
        {
            td::StringBuilder builder;
            builder << "CREATE TABLE IF NOT EXISTS blocks ("
                    << "workchain Int32, "
                    << "shard Int64, "
                    << "seqno Int32, "
                    << "root_hash FixedString(44), "
                    << "file_hash FixedString(44), "
                    << "mc_block_seqno Int32, "
                    << "global_id Int32, "
                    << "version Int32, "
                    << "after_merge Bool, "
                    << "before_split Bool, "
                    << "after_split Bool, "
                    << "want_merge Bool, "
                    << "want_split Bool, "
                    << "key_block Bool, "
                    << "vert_seqno_incr Bool, "
                    << "flags Int32, "
                    << "gen_utime Int32, "
                    << "start_lt UInt64, "
                    << "end_lt UInt64, "
                    << "validator_list_hash_short Int32, "
                    << "gen_catchain_seqno Int32, "
                    << "min_ref_mc_seqno Int32, "
                    << "prev_key_block_seqno Int32, "
                    << "vert_seqno Int32, "
                    << "master_ref_seqno Nullable(Int32), "
                    << "rand_seed FixedString(44), "
                    << "created_by FixedString(44), "
                    << "prev_blocks Array(Tuple(Int32, Int64, Int32)), "
                    << "shards Array(Tuple(Int32, Int64, Int32)), "
                    << "transaction_count Int32"
                    << ") ENGINE ReplacingMergeTree PRIMARY KEY(workchain, shard, seqno) ORDER BY (workchain, shard, seqno);\n";
            client.Execute(builder.as_cslice().str());
            LOG(INFO) << "Table blocks created";
        }
        {
            td::StringBuilder builder;    
            builder << "CREATE TABLE IF NOT EXISTS transactions ("
                    << "hash FixedString(44), "
                    << "lt UInt64, "
                    << "account FixedString(68), "
                    << "prev_hash FixedString(44), "
                    << "prev_lt UInt64, "
                    << "now UInt32, "
                    << "orig_status Enum('uninit' = 0, 'frozen' = 1, 'active' = 2, 'nonexist' = 3), "
                    << "end_status Enum('uninit' = 0, 'frozen' = 1, 'active' = 2, 'nonexist' = 3), "
                    << "total_fees UInt64, "
                    << "account_state_hash_before FixedString(44), "
                    << "account_state_hash_after FixedString(44), "
                    << "block_workchain Int32, "
                    << "block_shard Int64, "
                    << "block_seqno Int32, "
                    << "mc_block_seqno Int32, "
                    // flattened description
                    << "descr Enum('ord' = 0, 'storage' = 1, 'tick_tock' = 2, 'split_prepare' = 4, 'split_install' = 5, 'merge_prepare' = 6, 'merge_install' = 7), "

                    << "aborted Nullable(Bool), "
                    << "destroyed Nullable(Bool), "
                    << "ord__credit_first Nullable(Bool), "
                    << "tick_tock__is_tock Nullable(Bool), "
                    << "split_install__installed Nullable(Bool), "

                    << "storage_ph__storage_fees_collected Nullable(UInt64), "
                    << "storage_ph__storage_fees_due Nullable(UInt64), "
                    << "storage_ph__status_change Nullable( Enum('acst_unchanged' = 0, 'acst_frozen' = 2, 'acst_deleted' = 3) ), "

                    << "credit_ph__due_fees_collected Nullable(UInt64), "
                    << "credit_ph__credit Nullable(UInt64), "

                    << "compute_ph Nullable(Enum('skipped' = 0, 'vm' = 1)), "
                    << "compute_ph__skipped__reason Nullable(Enum('cskip_no_state' = 0, 'cskip_bad_state' = 1, 'cskip_no_gas' = 2, 'cskip_suspended' = 5)), "
                    << "compute_ph__vm__success Nullable(Bool), "
                    << "compute_ph__vm__msg_state_used Nullable(Bool), "
                    << "compute_ph__vm__account_activated Nullable(Bool), "
                    << "compute_ph__vm__gas_fees Nullable(UInt64), "
                    << "compute_ph__vm__gas_used Nullable(UInt64), "
                    << "compute_ph__vm__gas_limit Nullable(UInt64), "
                    << "compute_ph__vm__gas_credit Nullable(UInt64), "
                    << "compute_ph__vm__mode Nullable(Int8), "
                    << "compute_ph__vm__exit_code Nullable(Int32), "
                    << "compute_ph__vm__exit_arg Nullable(Int32), "
                    << "compute_ph__vm__vm_steps Nullable(UInt32), "
                    << "compute_ph__vm__vm_init_state_hash Nullable(FixedString(44)), "
                    << "compute_ph__vm__vm_final_state_hash Nullable(FixedString(44)), "

                    << "action__success Nullable(Bool), "
                    << "action__valid Nullable(Bool), "
                    << "action__no_funds Nullable(Bool), "
                    << "action__status_change Nullable(Enum('acst_unchanged' = 0, 'acst_frozen' = 2, 'acst_deleted' = 3)), "
                    << "action__total_fwd_fees Nullable(UInt64), "
                    << "action__total_action_fees Nullable(UInt64), "
                    << "action__result_code Nullable(Int32), "
                    << "action__result_arg Nullable(Int32), "
                    << "action__tot_actions Nullable(UInt16), "
                    << "action__spec_actions Nullable(UInt16), "
                    << "action__skipped_actions Nullable(UInt16), "
                    << "action__msgs_created Nullable(UInt16), "
                    << "action__action_list_hash Nullable(FixedString(44)), "
                    << "action__tot_msg_size__cells Nullable(UInt64), "
                    << "action__tot_msg_size__bits Nullable(UInt64), "

                    << "bounce Nullable(Enum('negfunds' = 0, 'nofunds' = 1, 'ok' = 2)), "
                    << "bounce__msg_size__cells Nullable(UInt64), "
                    << "bounce__msg_size__bits Nullable(UInt64), "
                    << "bounce__no_funds__req_fwd_fees Nullable(UInt64), "
                    << "bounce__ok__msg_fees Nullable(UInt64), "
                    << "bounce__ok__fwd_fees Nullable(UInt64), "

                    << "split_info__cur_shard_pfx_len Nullable(UInt8), "
                    << "split_info__acc_split_depth Nullable(UInt8), "
                    << "split_info__this_addr Nullable(FixedString(68)), "
                    << "split_info__sibling_addr Nullable(FixedString(68))"

                    << ") ENGINE ReplacingMergeTree PRIMARY KEY (lt, account, hash) ORDER BY (lt, account, hash);\n";
            client.Execute(builder.as_cslice().str());
            LOG(INFO) << "Table transactions created";
        }
        {
            td::StringBuilder builder;    
            builder << "CREATE TABLE IF NOT EXISTS messages ("
                    << "tx_hash FixedString(44), "
                    << "tx_lt UInt64, "
                    << "tx_account FixedString(68), "
                    << "tx_now UInt32, "
                    << "block_workchain Int32, "
                    << "block_shard Int64, "
                    << "block_seqno Int32, "
                    << "mc_block_seqno Int32, "
                    << "direction Enum('in' = 0, 'out' = 1), "
                    << "hash FixedString(68), "
                    << "source Nullable(FixedString(68)), "
                    << "destination Nullable(FixedString(68)), "
                    << "value Nullable(UInt64), "
                    << "fwd_fee Nullable(UInt64), "
                    << "ihr_fee Nullable(UInt64), "
                    << "created_lt Nullable(UInt64), "
                    << "created_at Nullable(UInt32), "
                    << "opcode Nullable(Int32), "
                    << "ihr_disabled Nullable(Bool), "
                    << "bounce Nullable(Bool), "
                    << "bounced Nullable(Bool), "
                    << "import_fee Nullable(UInt64), "
                    << "body_hash FixedString(44), "
                    << "init_state_hash Nullable(FixedString(44))"
                    << ") ENGINE ReplacingMergeTree PRIMARY KEY (tx_lt, tx_account, tx_hash, direction, hash) ORDER BY (tx_lt, tx_account, tx_hash, direction, hash);\n";
            client.Execute(builder.as_cslice().str());
            LOG(INFO) << "Table messages created";
        }
        {
            td::StringBuilder builder;    
            builder << "CREATE TABLE IF NOT EXISTS message_contents ("
                    << "hash FixedString(44), "
                    << "data String"
                    << ") ENGINE ReplacingMergeTree PRIMARY KEY(hash) ORDER BY (hash);\n";
            client.Execute(builder.as_cslice().str());
            LOG(INFO) << "Table message_contents created";
        }
        {
            td::StringBuilder builder;
            builder << "CREATE TABLE IF NOT EXISTS account_states ("
                    << "hash FixedString(44), "
                    << "account FixedString(68), "
                    << "timestamp UInt32, "
                    << "balance UInt64, "
                    << "account_status Enum('uninit' = 0, 'frozen' = 1, 'active' = 2, 'nonexist' = 3),"
                    << "frozen_hash Nullable(FixedString(44)), "
                    << "code_hash Nullable(FixedString(44)), "
                    << "data_hash Nullable(FixedString(44)), "
                    << "last_trans_lt UInt64, "
                    << ") ENGINE ReplacingMergeTree PRIMARY KEY(timestamp, account, hash) ORDER BY (timestamp, account, hash);\n";
            client.Execute(builder.as_cslice().str());
            LOG(INFO) << "Table account_states created";
        }
        {
            td::StringBuilder builder;
            builder << "CREATE TABLE IF NOT EXISTS latest_account_states ("
                    << "account FixedString(68), "
                    << "hash FixedString(44), "
                    << "timestamp UInt32, "
                    << "balance UInt64, "
                    << "account_status Enum('uninit' = 0, 'frozen' = 1, 'active' = 2, 'nonexist' = 3),"
                    << "frozen_hash Nullable(FixedString(44)), "
                    << "code_hash Nullable(FixedString(44)), "
                    << "code_boc Nullable(String), "
                    << "data_hash Nullable(FixedString(44)), "
                    << "data_boc Nullable(String), "
                    << "last_trans_lt UInt64, "
                    << ") ENGINE ReplacingMergeTree(timestamp) PRIMARY KEY(account) ORDER BY (account);\n";
            client.Execute(builder.as_cslice().str());
            LOG(INFO) << "Table latest_account_states created";
        }
        LOG(INFO) << "Clickhouse start_up finished";
    } catch (const std::exception& e) {
        LOG(FATAL) << "Clickhouse start_up error: " << e.what();
        std::_Exit(2);
    }

    alarm_timestamp() = td::Timestamp::in(1.0);
}

void InsertManagerClickhouse::create_insert_actor(std::vector<InsertTaskStruct> insert_tasks, td::Promise<td::Unit> promise) {
    td::actor::create_actor<InsertBatchClickhouse>("insert_batch_clickhouse", credential_.get_clickhouse_options(), std::move(insert_tasks), std::move(promise)).release();
}

void InsertManagerClickhouse::get_existing_seqnos(td::Promise<std::vector<std::uint32_t>> promise)
{
    LOG(INFO) << "Clickhouse get_existing_seqnos";
    try {
        auto options = credential_.get_clickhouse_options();
        clickhouse::Client client(options);
        
        std::vector<std::uint32_t> result;
        client.Select("SELECT seqno from blocks WHERE workchain = -1", [&result](const clickhouse::Block& block) {
            for (size_t i = 0; i < block.GetRowCount(); ++i) {
                result.push_back(block[0]->As<clickhouse::ColumnInt32>()->At(i));
            }
        });
        promise.set_result(std::move(result));
    } catch (std::exception& e) {
       promise.set_error(td::Status::Error(DB_ERROR, PSLICE() << "Failed to fetch existing seqnos: " << e.what()));
    }
    return;
}

void InsertManagerClickhouse::upsert_jetton_wallet(JettonWalletData jetton_wallet, td::Promise<td::Unit> promise) {
    LOG(ERROR) << "Unreachable code";
    promise.set_result(td::Unit());
}

void InsertManagerClickhouse::upsert_jetton_master(JettonMasterData jetton_wallet, td::Promise<td::Unit> promise) {
    LOG(ERROR) << "Unreachable code";
    promise.set_result(td::Unit());
}

void InsertManagerClickhouse::upsert_nft_collection(NFTCollectionData nft_collection, td::Promise<td::Unit> promise) {
    LOG(ERROR) << "Unreachable code";
    promise.set_result(td::Unit());
}

void InsertManagerClickhouse::upsert_nft_item(NFTItemData nft_item, td::Promise<td::Unit> promise) {
    LOG(ERROR) << "Unreachable code";
    promise.set_result(td::Unit());
}

clickhouse::ClientOptions InsertManagerClickhouse::Credential::get_clickhouse_options()
{
    clickhouse::ClientOptions options;
    options.SetHost(host);
    options.SetPort(port);
    options.SetUser(user);
    options.SetPassword(password);
    options.SetDefaultDatabase(dbname);
    return std::move(options);
}

void InsertBatchClickhouse::start_up() {
    try{
        // TODO: Insert more info
        clickhouse::Client client(client_options_);
        insert_transactions(client);
        insert_messages(client);
        insert_account_states(client);
        insert_blocks(client);
        
        // all done
        for(auto& task_ : insert_tasks_) {
            task_.promise_.set_result(td::Unit());
        }
        promise_.set_result(td::Unit());
    } catch (const std::exception &e) {
        // something failed
        for(auto& task_ : insert_tasks_) {
            task_.promise_.set_error(td::Status::Error(ErrorCode::DB_ERROR, PSLICE() << "Error inserting task to Clickhouse: " << e.what()));
        }
        promise_.set_error(td::Status::Error(ErrorCode::DB_ERROR, PSLICE() << "Error inserting batch to Clickhouse: " << e.what()));
    }
    stop();
}


void InsertBatchClickhouse::insert_blocks(clickhouse::Client &client){
    using namespace clickhouse;
    
    clickhouse::Block block;

    auto workchain_col = std::make_shared<ColumnInt32>();
    auto shard_col = std::make_shared<ColumnInt64>();
    auto seqno_col = std::make_shared<ColumnInt32>();
    auto root_hash_col = std::make_shared<ColumnFixedString>(44);
    auto file_hash_col = std::make_shared<ColumnFixedString>(44);
    auto mc_block_seqno_col = std::make_shared<ColumnInt32>();
    auto global_id_col = std::make_shared<ColumnInt32>();
    auto version_col = std::make_shared<ColumnInt32>();
    auto after_merge_col = std::make_shared<ColumnUInt8>();
    auto before_split_col = std::make_shared<ColumnUInt8>();
    auto after_split_col = std::make_shared<ColumnUInt8>();
    auto want_merge_col = std::make_shared<ColumnUInt8>();
    auto want_split_col = std::make_shared<ColumnUInt8>();
    auto key_block_col = std::make_shared<ColumnUInt8>();
    auto vert_seqno_incr_col = std::make_shared<ColumnUInt8>();
    auto flags_col = std::make_shared<ColumnInt32>();
    auto gen_utime_col = std::make_shared<ColumnInt32>();
    auto start_lt_col = std::make_shared<ColumnUInt64>();
    auto end_lt_col = std::make_shared<ColumnUInt64>();
    auto validator_list_hash_short_col = std::make_shared<ColumnInt32>();
    auto gen_catchain_seqno_col = std::make_shared<ColumnInt32>();
    auto min_ref_mc_seqno_col = std::make_shared<ColumnInt32>();
    auto prev_key_block_seqno_col = std::make_shared<ColumnInt32>();
    auto vert_seqno_col = std::make_shared<ColumnInt32>();
    auto master_ref_seqno_col = std::make_shared<ColumnNullableT<ColumnInt32>>();
    auto rand_seed_col = std::make_shared<ColumnFixedString>(44);
    auto created_by_col = std::make_shared<ColumnFixedString>(44);
    auto prev_blocks_col = std::make_shared<ColumnArray>(std::make_shared<ColumnTuple>(std::vector<ColumnRef>{
        std::make_shared<ColumnInt32>(),
        std::make_shared<ColumnInt64>(),
        std::make_shared<ColumnInt32>(),
    }));
    auto shards_col = std::make_shared<ColumnArray>(std::make_shared<ColumnTuple>(std::vector<ColumnRef>{
        std::make_shared<ColumnInt32>(),
        std::make_shared<ColumnInt64>(),
        std::make_shared<ColumnInt32>(),
    }));
    auto transaction_count_col = std::make_shared<ColumnInt32>();

    for(const auto& task_ : insert_tasks_) {
        for(const auto& blk_ :task_.parsed_block_->blocks_) {
            workchain_col->Append(blk_.workchain);
            shard_col->Append(blk_.shard);
            seqno_col->Append(blk_.seqno);
            root_hash_col->Append(blk_.root_hash);
            file_hash_col->Append(blk_.file_hash);
            mc_block_seqno_col->Append(blk_.mc_block_seqno.value());
            global_id_col->Append(blk_.global_id);
            version_col->Append(blk_.version);
            after_merge_col->Append(blk_.after_merge);
            before_split_col->Append(blk_.before_split);
            after_split_col->Append(blk_.after_split);
            want_merge_col->Append(blk_.want_merge);
            want_split_col->Append(blk_.after_split);
            key_block_col->Append(blk_.key_block);
            vert_seqno_incr_col->Append(blk_.vert_seqno_incr);
            flags_col->Append(blk_.flags);
            gen_utime_col->Append(blk_.gen_utime);
            start_lt_col->Append(blk_.start_lt);
            end_lt_col->Append(blk_.end_lt);
            validator_list_hash_short_col->Append(blk_.validator_list_hash_short);
            gen_catchain_seqno_col->Append(blk_.gen_catchain_seqno);
            min_ref_mc_seqno_col->Append(blk_.min_ref_mc_seqno);
            prev_key_block_seqno_col->Append(blk_.prev_key_block_seqno);
            vert_seqno_col->Append(blk_.vert_seqno);

            if (blk_.master_ref_seqno)
                master_ref_seqno_col->Append(blk_.master_ref_seqno.value());
            else
                master_ref_seqno_col->Append(std::nullopt);

            rand_seed_col->Append(blk_.rand_seed);
            created_by_col->Append(blk_.created_by);
            {
                auto loc_workchain = std::make_shared<ColumnInt32>();
                auto loc_shard = std::make_shared<ColumnInt64>();
                auto loc_seqno = std::make_shared<ColumnInt32>();
                
                for(auto & prev_blk_ : blk_.prev_blocks) {
                    loc_workchain->Append(prev_blk_.workchain);
                    loc_shard->Append(prev_blk_.shard);
                    loc_seqno->Append(prev_blk_.seqno);
                }
                prev_blocks_col->AppendAsColumn(std::make_shared<ColumnTuple>(std::vector<ColumnRef>{loc_workchain, loc_shard, loc_seqno}));
            }
            {
                auto loc_workchain = std::make_shared<ColumnInt32>();
                auto loc_shard = std::make_shared<ColumnInt64>();
                auto loc_seqno = std::make_shared<ColumnInt32>();
                
                if(blk_.workchain == -1) {
                    for(auto & shard_ : task_.parsed_block_->shard_state_) {
                        loc_workchain->Append(shard_.workchain);
                        loc_shard->Append(shard_.shard);
                        loc_seqno->Append(shard_.seqno);
                    }
                }
                shards_col->AppendAsColumn(std::make_shared<ColumnTuple>(std::vector<ColumnRef>{loc_workchain, loc_shard, loc_seqno}));
            }
            transaction_count_col->Append(blk_.transactions.size());
        }
    }
    block.AppendColumn("workchain", workchain_col);
    block.AppendColumn("shard", shard_col);
    block.AppendColumn("seqno", seqno_col);
    block.AppendColumn("root_hash", root_hash_col);
    block.AppendColumn("file_hash", file_hash_col);
    block.AppendColumn("mc_block_seqno", mc_block_seqno_col);
    block.AppendColumn("global_id", global_id_col);
    block.AppendColumn("version", version_col);
    block.AppendColumn("after_merge", after_merge_col);
    block.AppendColumn("before_split", before_split_col);
    block.AppendColumn("after_split", after_split_col);
    block.AppendColumn("want_merge", want_merge_col);
    block.AppendColumn("want_split", want_split_col);
    block.AppendColumn("key_block", key_block_col);
    block.AppendColumn("vert_seqno_incr", vert_seqno_incr_col);
    block.AppendColumn("flags", flags_col);
    block.AppendColumn("gen_utime", gen_utime_col);
    block.AppendColumn("start_lt", start_lt_col);
    block.AppendColumn("end_lt", end_lt_col);
    block.AppendColumn("validator_list_hash_short", validator_list_hash_short_col);
    block.AppendColumn("gen_catchain_seqno", gen_catchain_seqno_col);
    block.AppendColumn("min_ref_mc_seqno", min_ref_mc_seqno_col);
    block.AppendColumn("prev_key_block_seqno", prev_key_block_seqno_col);
    block.AppendColumn("vert_seqno", vert_seqno_col);
    block.AppendColumn("master_ref_seqno", master_ref_seqno_col);
    block.AppendColumn("rand_seed", rand_seed_col);
    block.AppendColumn("created_by", created_by_col);
    block.AppendColumn("prev_blocks", prev_blocks_col);
    block.AppendColumn("shards", shards_col);
    block.AppendColumn("transaction_count", transaction_count_col);

    client.Insert("blocks", block);
}


void InsertBatchClickhouse::insert_transactions(clickhouse::Client& client) {
    using namespace clickhouse;
    
    clickhouse::Block block;
    
    auto hash_col = std::make_shared<ColumnFixedString>(44);
    auto lt_col = std::make_shared<ColumnUInt64>();
    auto account_col = std::make_shared<ColumnFixedString>(68);
    auto prev_hash_col = std::make_shared<ColumnFixedString>(44);
    auto prev_lt_col = std::make_shared<ColumnUInt64>();
    auto now_col = std::make_shared<ColumnUInt32>();
    auto orig_status_col = std::make_shared<ColumnEnum8>(Type::CreateEnum8({{"uninit", 0}, {"frozen", 1}, {"active", 2}, {"nonexist", 3}}));
    auto end_status_col = std::make_shared<ColumnEnum8>(Type::CreateEnum8({{"uninit", 0}, {"frozen", 1}, {"active", 2}, {"nonexist", 3}}));
    auto total_fees_col = std::make_shared<ColumnUInt64>();
    auto account_state_hash_before_col = std::make_shared<ColumnFixedString>(44);
    auto account_state_hash_after_col = std::make_shared<ColumnFixedString>(44);
    auto block_workchain_col = std::make_shared<ColumnInt32>();
    auto block_shard_col = std::make_shared<ColumnInt64>();
    auto block_seqno_col = std::make_shared<ColumnInt32>();
    auto mc_block_seqno_col = std::make_shared<ColumnInt32>();
    auto descr_col = std::make_shared<ColumnEnum8>(Type::CreateEnum8({{"ord", 0}, {"storage", 1}, {"tick_tock", 2}, {"split_prepare", 3}, {"split_install", 5}, {"merge_prepare", 6}, {"merge_install", 7}}));
    auto aborted_col = std::make_shared<ColumnNullableT<ColumnUInt8>>(); // Bool is typically represented as UInt8
    auto destroyed_col = std::make_shared<ColumnNullableT<ColumnUInt8>>();
    auto ord__credit_first_col = std::make_shared<ColumnNullableT<ColumnUInt8>>();
    auto tick_tock__is_tock_col = std::make_shared<ColumnNullableT<ColumnUInt8>>();
    auto split_install__installed_col = std::make_shared<ColumnNullableT<ColumnUInt8>>();
    auto storage_ph__storage_fees_collected_col = std::make_shared<ColumnNullableT<ColumnUInt64>>();
    auto storage_ph__storage_fees_due_col = std::make_shared<ColumnNullableT<ColumnUInt64>>();
    auto storage_ph__status_change_col = std::make_shared<ColumnNullableT<ColumnEnum8>>(Type::CreateEnum8({{"acst_unchanged", 0}, {"acst_frozen", 2}, {"acst_deleted", 3}}));
    auto credit_ph__due_fees_collected_col = std::make_shared<ColumnNullableT<ColumnUInt64>>();
    auto credit_ph__credit_col = std::make_shared<ColumnNullableT<ColumnUInt64>>();
    auto compute_ph_col = std::make_shared<ColumnNullableT<ColumnEnum8>>(Type::CreateEnum8({{"skipped", 0}, {"vm", 1}}));
    auto compute_ph__skipped__reason_col = std::make_shared<ColumnNullableT<ColumnEnum8>>(Type::CreateEnum8({{"cskip_no_state", 0}, {"cskip_bad_state", 1}, {"cskip_no_gas", 2}, {"cskip_suspended", 5}}));
    auto compute_ph__vm__success_col = std::make_shared<ColumnNullableT<ColumnUInt8>>();
    auto compute_ph__vm__msg_state_used_col = std::make_shared<ColumnNullableT<ColumnUInt8>>();
    auto compute_ph__vm__account_activated_col = std::make_shared<ColumnNullableT<ColumnUInt8>>();
    auto compute_ph__vm__gas_fees_col = std::make_shared<ColumnNullableT<ColumnUInt64>>();
    auto compute_ph__vm__gas_used_col = std::make_shared<ColumnNullableT<ColumnUInt64>>();
    auto compute_ph__vm__gas_limit_col = std::make_shared<ColumnNullableT<ColumnUInt64>>();
    auto compute_ph__vm__gas_credit_col = std::make_shared<ColumnNullableT<ColumnUInt64>>();
    auto compute_ph__vm__mode_col = std::make_shared<ColumnNullableT<ColumnInt8>>();
    auto compute_ph__vm__exit_code_col = std::make_shared<ColumnNullableT<ColumnInt32>>();
    auto compute_ph__vm__exit_arg_col = std::make_shared<ColumnNullableT<ColumnInt32>>();
    auto compute_ph__vm__vm_steps_col = std::make_shared<ColumnNullableT<ColumnUInt32>>();
    auto compute_ph__vm__vm_init_state_hash_col = std::make_shared<ColumnNullableT<ColumnFixedString>>(44);
    auto compute_ph__vm__vm_final_state_hash_col = std::make_shared<ColumnNullableT<ColumnFixedString>>(44);
    auto action__success_col = std::make_shared<ColumnNullableT<ColumnUInt8>>();
    auto action__valid_col = std::make_shared<ColumnNullableT<ColumnUInt8>>();
    auto action__no_funds_col = std::make_shared<ColumnNullableT<ColumnUInt8>>();
    auto action__status_change_col = std::make_shared<ColumnNullableT<ColumnEnum8>>(Type::CreateEnum8({{"acst_unchanged", 0}, {"acst_frozen", 2}, {"acst_deleted", 3}}));
    auto action__total_fwd_fees_col = std::make_shared<ColumnNullableT<ColumnUInt64>>();
    auto action__total_action_fees_col = std::make_shared<ColumnNullableT<ColumnUInt64>>();
    auto action__result_code_col = std::make_shared<ColumnNullableT<ColumnInt32>>();
    auto action__result_arg_col = std::make_shared<ColumnNullableT<ColumnInt32>>();
    auto action__tot_actions_col = std::make_shared<ColumnNullableT<ColumnUInt16>>();
    auto action__spec_actions_col = std::make_shared<ColumnNullableT<ColumnUInt16>>();
    auto action__skipped_actions_col = std::make_shared<ColumnNullableT<ColumnUInt16>>();
    auto action__msgs_created_col = std::make_shared<ColumnNullableT<ColumnUInt16>>();
    auto action__action_list_hash_col = std::make_shared<ColumnNullableT<ColumnFixedString>>(44);
    auto action__tot_msg_size__cells_col = std::make_shared<ColumnNullableT<ColumnUInt64>>();
    auto action__tot_msg_size__bits_col = std::make_shared<ColumnNullableT<ColumnUInt64>>();
    auto bounce_col = std::make_shared<ColumnNullableT<ColumnEnum8>>(Type::CreateEnum8({{"negfunds", 0}, {"nofunds", 1}, {"ok", 2}}));
    auto bounce__msg_size__cells_col = std::make_shared<ColumnNullableT<ColumnUInt64>>();
    auto bounce__msg_size__bits_col = std::make_shared<ColumnNullableT<ColumnUInt64>>();
    auto bounce__no_funds__req_fwd_fees_col = std::make_shared<ColumnNullableT<ColumnUInt64>>();
    auto bounce__ok__msg_fees_col = std::make_shared<ColumnNullableT<ColumnUInt64>>();
    auto bounce__ok__fwd_fees_col = std::make_shared<ColumnNullableT<ColumnUInt64>>();
    auto split_info__cur_shard_pfx_len_col = std::make_shared<ColumnNullableT<ColumnUInt8>>();
    auto split_info__acc_split_depth_col = std::make_shared<ColumnNullableT<ColumnUInt8>>();
    auto split_info__this_addr_col = std::make_shared<ColumnNullableT<ColumnFixedString>>(68);
    auto split_info__sibling_addr_col = std::make_shared<ColumnNullableT<ColumnFixedString>>(68);

    auto store_storage_ph = [&](const schema::TrStoragePhase& storage_ph) {
        storage_ph__storage_fees_collected_col->Append(storage_ph.storage_fees_collected);
        storage_ph__storage_fees_due_col->Append(storage_ph.storage_fees_due);
        storage_ph__status_change_col->Append(storage_ph.status_change);
    };
    auto store_empty_storage_ph = [&]() {
        storage_ph__storage_fees_collected_col->Append(std::nullopt);
        storage_ph__storage_fees_due_col->Append(std::nullopt);
        storage_ph__status_change_col->Append(std::nullopt);
    };
    auto store_credit_ph = [&](const schema::TrCreditPhase& credit_ph) {
        credit_ph__due_fees_collected_col->Append(credit_ph.due_fees_collected);
        credit_ph__credit_col->Append(credit_ph.credit);
    };
    auto store_empty_credit_ph = [&]() {
        credit_ph__due_fees_collected_col->Append(std::nullopt);
        credit_ph__credit_col->Append(std::nullopt);
    };
    auto store_compute_ph = [&](const schema::TrComputePhase& compute_ph) {
        if (auto* v = std::get_if<schema::TrComputePhase_skipped>(&compute_ph)) {
            compute_ph_col->Append(std::optional<std::int8_t>(0));
            compute_ph__skipped__reason_col->Append(v->reason);
            compute_ph__vm__success_col->Append(std::nullopt);
            compute_ph__vm__msg_state_used_col->Append(std::nullopt);
            compute_ph__vm__account_activated_col->Append(std::nullopt);
            compute_ph__vm__gas_fees_col->Append(std::nullopt);
            compute_ph__vm__gas_used_col->Append(std::nullopt);
            compute_ph__vm__gas_limit_col->Append(std::nullopt);
            compute_ph__vm__gas_credit_col->Append(std::nullopt);
            compute_ph__vm__mode_col->Append(std::nullopt);
            compute_ph__vm__exit_code_col->Append(std::nullopt);
            compute_ph__vm__exit_arg_col->Append(std::nullopt);
            compute_ph__vm__vm_steps_col->Append(std::nullopt);
            compute_ph__vm__vm_init_state_hash_col->Append(std::nullopt);
            compute_ph__vm__vm_final_state_hash_col->Append(std::nullopt);
        }
        else if (auto* v = std::get_if<schema::TrComputePhase_vm>(&compute_ph)) {
            compute_ph_col->Append(std::optional<std::int8_t>(1));
            compute_ph__skipped__reason_col->Append(std::nullopt);
            compute_ph__vm__success_col->Append(v->success);
            compute_ph__vm__msg_state_used_col->Append(v->msg_state_used);
            compute_ph__vm__account_activated_col->Append(v->account_activated);
            compute_ph__vm__gas_fees_col->Append(v->gas_fees);
            compute_ph__vm__gas_used_col->Append(v->gas_used);
            compute_ph__vm__gas_limit_col->Append(v->gas_limit);
            compute_ph__vm__gas_credit_col->Append(v->gas_credit);
            compute_ph__vm__mode_col->Append(v->mode);
            compute_ph__vm__exit_code_col->Append(v->exit_code);
            compute_ph__vm__exit_arg_col->Append(v->exit_arg);
            compute_ph__vm__vm_steps_col->Append(v->vm_steps);
            compute_ph__vm__vm_init_state_hash_col->Append(td::base64_encode(v->vm_init_state_hash.as_slice()));
            compute_ph__vm__vm_final_state_hash_col->Append(td::base64_encode(v->vm_final_state_hash.as_slice()));
        }
    };
    auto store_empty_compute_ph = [&]() {
        compute_ph_col->Append(std::nullopt);
        compute_ph__skipped__reason_col->Append(std::nullopt);
        compute_ph__vm__success_col->Append(std::nullopt);
        compute_ph__vm__msg_state_used_col->Append(std::nullopt);
        compute_ph__vm__account_activated_col->Append(std::nullopt);
        compute_ph__vm__gas_fees_col->Append(std::nullopt);
        compute_ph__vm__gas_used_col->Append(std::nullopt);
        compute_ph__vm__gas_limit_col->Append(std::nullopt);
        compute_ph__vm__gas_credit_col->Append(std::nullopt);
        compute_ph__vm__mode_col->Append(std::nullopt);
        compute_ph__vm__exit_code_col->Append(std::nullopt);
        compute_ph__vm__exit_arg_col->Append(std::nullopt);
        compute_ph__vm__vm_steps_col->Append(std::nullopt);
        compute_ph__vm__vm_init_state_hash_col->Append(std::nullopt);
        compute_ph__vm__vm_final_state_hash_col->Append(std::nullopt);
    };
    auto store_action_ph = [&](const schema::TrActionPhase& action) {
        action__success_col->Append(action.success);
        action__valid_col->Append(action.valid);
        action__no_funds_col->Append(action.no_funds);
        action__status_change_col->Append(action.status_change);
        action__total_fwd_fees_col->Append(action.total_fwd_fees);
        action__total_action_fees_col->Append(action.total_action_fees);
        action__result_code_col->Append(action.result_code);
        action__result_arg_col->Append(action.result_arg);
        action__tot_actions_col->Append(action.tot_actions);
        action__spec_actions_col->Append(action.spec_actions);
        action__skipped_actions_col->Append(action.skipped_actions);
        action__msgs_created_col->Append(action.msgs_created);
        action__action_list_hash_col->Append(td::base64_encode(action.action_list_hash.as_slice()));
        action__tot_msg_size__cells_col->Append(action.tot_msg_size.cells);
        action__tot_msg_size__bits_col->Append(action.tot_msg_size.bits);
    };
    auto store_empty_action_ph = [&]() {
        action__success_col->Append(std::nullopt);
        action__valid_col->Append(std::nullopt);
        action__no_funds_col->Append(std::nullopt);
        action__status_change_col->Append(std::nullopt);
        action__total_fwd_fees_col->Append(std::nullopt);
        action__total_action_fees_col->Append(std::nullopt);
        action__result_code_col->Append(std::nullopt);
        action__result_arg_col->Append(std::nullopt);
        action__tot_actions_col->Append(std::nullopt);
        action__spec_actions_col->Append(std::nullopt);
        action__skipped_actions_col->Append(std::nullopt);
        action__msgs_created_col->Append(std::nullopt);
        action__action_list_hash_col->Append(std::nullopt);
        action__tot_msg_size__cells_col->Append(std::nullopt);
        action__tot_msg_size__bits_col->Append(std::nullopt);
    };
    auto store_bounce_ph = [&](const schema::TrBouncePhase& bounce) {
        if(auto* v = std::get_if<schema::TrBouncePhase_negfunds>(&bounce)) {
            bounce_col->Append(std::optional<std::int8_t>(0));
            bounce__msg_size__cells_col->Append(std::nullopt);
            bounce__msg_size__bits_col->Append(std::nullopt);
            bounce__no_funds__req_fwd_fees_col->Append(std::nullopt);
            bounce__ok__msg_fees_col->Append(std::nullopt);
            bounce__ok__fwd_fees_col->Append(std::nullopt);
        } else if (auto* v = std::get_if<schema::TrBouncePhase_nofunds>(&bounce)) {
            bounce_col->Append(std::optional<std::int8_t>(1));
            bounce__msg_size__cells_col->Append(v->msg_size.cells);
            bounce__msg_size__bits_col->Append(v->msg_size.bits);
            bounce__no_funds__req_fwd_fees_col->Append(v->req_fwd_fees);
            bounce__ok__msg_fees_col->Append(std::nullopt);
            bounce__ok__fwd_fees_col->Append(std::nullopt);
        } else if (auto* v = std::get_if<schema::TrBouncePhase_ok>(&bounce)) {
            bounce_col->Append(std::optional<std::int8_t>(2));
            bounce__msg_size__cells_col->Append(v->msg_size.cells);
            bounce__msg_size__bits_col->Append(v->msg_size.bits);
            bounce__no_funds__req_fwd_fees_col->Append(std::nullopt);
            bounce__ok__msg_fees_col->Append(v->msg_fees);
            bounce__ok__fwd_fees_col->Append(v->fwd_fees);
        }
    };
    auto store_empty_bounce_ph = [&]() {
        bounce_col->Append(std::nullopt);
        bounce__msg_size__cells_col->Append(std::nullopt);
        bounce__msg_size__bits_col->Append(std::nullopt);
        bounce__no_funds__req_fwd_fees_col->Append(std::nullopt);
        bounce__ok__msg_fees_col->Append(std::nullopt);
        bounce__ok__fwd_fees_col->Append(std::nullopt);
    };
    auto store_split_info = [&](const schema::SplitMergeInfo& split_info) {
        split_info__cur_shard_pfx_len_col->Append(split_info.cur_shard_pfx_len);
        split_info__acc_split_depth_col->Append(split_info.acc_split_depth);
        split_info__this_addr_col->Append(td::base64_encode(split_info.this_addr.as_slice()));
        split_info__sibling_addr_col->Append(td::base64_encode(split_info.sibling_addr.as_slice()));
    };
    auto store_empty_split_info = [&]() {
        split_info__cur_shard_pfx_len_col->Append(std::nullopt);
        split_info__acc_split_depth_col->Append(std::nullopt);
        split_info__this_addr_col->Append(std::nullopt);
        split_info__sibling_addr_col->Append(std::nullopt);
    };

    for(const auto& task_ : insert_tasks_) {
        for(const auto& blk_ : task_.parsed_block_->blocks_) {
            for(const auto& tx_ : blk_.transactions) {
                hash_col->Append(td::base64_encode(tx_.hash.as_slice()));
                lt_col->Append(tx_.lt);
                account_col->Append(convert::to_raw_address(tx_.account));
                prev_hash_col->Append(td::base64_encode(tx_.prev_trans_hash.as_slice()));
                prev_lt_col->Append(tx_.prev_trans_lt);
                now_col->Append(tx_.now);
                orig_status_col->Append(tx_.orig_status);
                end_status_col->Append(tx_.end_status);
                total_fees_col->Append(tx_.total_fees);
                account_state_hash_before_col->Append(td::base64_encode(tx_.account_state_hash_after.as_slice()));
                account_state_hash_after_col->Append(td::base64_encode(tx_.account_state_hash_after.as_slice()));
                block_workchain_col->Append(blk_.workchain);
                block_shard_col->Append(blk_.shard);
                block_seqno_col->Append(blk_.seqno);
                mc_block_seqno_col->Append(blk_.mc_block_seqno.value());
                
                if (auto* v = std::get_if<schema::TransactionDescr_ord>(&tx_.description)) {
                    descr_col->Append(0);
                    aborted_col->Append(v->aborted);
                    destroyed_col->Append(v->destroyed);
                    ord__credit_first_col->Append(v->credit_first);
                    tick_tock__is_tock_col->Append(std::nullopt);
                    split_install__installed_col->Append(std::nullopt);

                    store_storage_ph(v->storage_ph);
                    store_credit_ph(v->credit_ph);
                    store_compute_ph(v->compute_ph);
                    if (v->action) {
                        store_action_ph(v->action.value());
                    } else {
                        store_empty_action_ph();
                    }
                    if (v->bounce) {
                        store_bounce_ph(v->bounce.value());
                    } else {
                        store_empty_bounce_ph();
                    }
                    store_empty_split_info();
                } 
                else if (auto* v = std::get_if<schema::TransactionDescr_storage>(&tx_.description)) {
                    descr_col->Append(1);
                    aborted_col->Append(std::nullopt);
                    destroyed_col->Append(std::nullopt);
                    ord__credit_first_col->Append(std::nullopt);
                    tick_tock__is_tock_col->Append(std::nullopt);
                    split_install__installed_col->Append(std::nullopt);

                    store_storage_ph(v->storage_ph);
                    store_empty_credit_ph();
                    store_empty_compute_ph();
                    store_empty_action_ph();
                    store_empty_bounce_ph();
                    store_empty_split_info();
                } 
                else if (auto* v = std::get_if<schema::TransactionDescr_tick_tock>(&tx_.description)) {
                    descr_col->Append(2);
                    aborted_col->Append(v->aborted);
                    destroyed_col->Append(v->destroyed);
                    ord__credit_first_col->Append(std::nullopt);
                    tick_tock__is_tock_col->Append(v->is_tock);
                    split_install__installed_col->Append(std::nullopt);

                    store_storage_ph(v->storage_ph);
                    store_empty_credit_ph();
                    store_compute_ph(v->compute_ph);
                    if (v->action) {
                        store_action_ph(v->action.value());
                    } else {
                        store_empty_action_ph();
                    }
                    store_empty_bounce_ph();
                    store_empty_split_info();
                } 
                else if (auto* v = std::get_if<schema::TransactionDescr_split_prepare>(&tx_.description)) {
                    descr_col->Append(3);
                    aborted_col->Append(v->aborted);
                    destroyed_col->Append(v->destroyed);
                    ord__credit_first_col->Append(std::nullopt);
                    tick_tock__is_tock_col->Append(std::nullopt);
                    split_install__installed_col->Append(std::nullopt);

                    if (v->storage_ph) {
                        store_storage_ph(v->storage_ph.value());
                    } else {
                        store_empty_storage_ph();
                    }
                    store_empty_credit_ph();
                    store_compute_ph(v->compute_ph);
                    if (v->action) {
                        store_action_ph(v->action.value());
                    } else {
                        store_empty_action_ph();
                    }
                    store_empty_bounce_ph();
                    store_split_info(v->split_info);
                } 
                else if (auto* v = std::get_if<schema::TransactionDescr_split_install>(&tx_.description)) {
                    descr_col->Append(5);
                    aborted_col->Append(std::nullopt);
                    destroyed_col->Append(std::nullopt);
                    ord__credit_first_col->Append(std::nullopt);
                    tick_tock__is_tock_col->Append(std::nullopt);
                    split_install__installed_col->Append(v->installed);

                    store_empty_storage_ph();
                    store_empty_credit_ph();
                    store_empty_compute_ph();
                    store_empty_action_ph();
                    store_empty_bounce_ph();
                    store_split_info(v->split_info);
                } 
                else if (auto* v = std::get_if<schema::TransactionDescr_merge_prepare>(&tx_.description)) {
                    descr_col->Append(6);
                    aborted_col->Append(v->aborted);
                    destroyed_col->Append(std::nullopt);
                    ord__credit_first_col->Append(std::nullopt);
                    tick_tock__is_tock_col->Append(std::nullopt);
                    split_install__installed_col->Append(std::nullopt);

                    store_storage_ph(v->storage_ph);
                    store_empty_credit_ph();
                    store_empty_compute_ph();
                    store_empty_action_ph();
                    store_empty_bounce_ph();
                    store_split_info(v->split_info);
                } 
                else if (auto* v = std::get_if<schema::TransactionDescr_merge_install>(&tx_.description)) {
                    descr_col->Append(7);
                    aborted_col->Append(v->aborted);
                    destroyed_col->Append(v->destroyed);
                    ord__credit_first_col->Append(std::nullopt);
                    tick_tock__is_tock_col->Append(std::nullopt);
                    split_install__installed_col->Append(std::nullopt);

                    if (v->storage_ph) {
                        store_storage_ph(v->storage_ph.value());
                    } else {
                        store_empty_storage_ph();
                    }
                    if (v->credit_ph) {
                        store_credit_ph(v->credit_ph.value());
                    } else {
                        store_empty_credit_ph();
                    }
                    store_compute_ph(v->compute_ph);
                    if (v->action) {
                        store_action_ph(v->action.value());
                    } else {
                        store_empty_action_ph();
                    }
                    store_empty_bounce_ph();
                    store_split_info(v->split_info);
                }
            }
        }
    }

    // Appending columns to a block
    block.AppendColumn("hash", hash_col);
    block.AppendColumn("lt", lt_col);
    block.AppendColumn("account", account_col);
    block.AppendColumn("prev_hash", prev_hash_col);
    block.AppendColumn("prev_lt", prev_lt_col);
    block.AppendColumn("now", now_col);
    block.AppendColumn("orig_status", orig_status_col);
    block.AppendColumn("end_status", end_status_col);
    block.AppendColumn("total_fees", total_fees_col);
    block.AppendColumn("account_state_hash_before", account_state_hash_before_col);
    block.AppendColumn("account_state_hash_after", account_state_hash_after_col);
    block.AppendColumn("block_workchain", block_workchain_col);
    block.AppendColumn("block_shard", block_shard_col);
    block.AppendColumn("block_seqno", block_seqno_col);
    block.AppendColumn("mc_block_seqno", mc_block_seqno_col);
    block.AppendColumn("descr", descr_col);
    block.AppendColumn("aborted", aborted_col);
    block.AppendColumn("destroyed", destroyed_col);
    block.AppendColumn("ord__credit_first", ord__credit_first_col);
    block.AppendColumn("tick_tock__is_tock", tick_tock__is_tock_col);
    block.AppendColumn("split_install__installed", split_install__installed_col);
    block.AppendColumn("storage_ph__storage_fees_collected", storage_ph__storage_fees_collected_col);
    block.AppendColumn("storage_ph__storage_fees_due", storage_ph__storage_fees_due_col);
    block.AppendColumn("storage_ph__status_change", storage_ph__status_change_col);
    block.AppendColumn("credit_ph__due_fees_collected", credit_ph__due_fees_collected_col);
    block.AppendColumn("credit_ph__credit", credit_ph__credit_col);
    block.AppendColumn("compute_ph", compute_ph_col);
    block.AppendColumn("compute_ph__skipped__reason", compute_ph__skipped__reason_col);
    block.AppendColumn("compute_ph__vm__success", compute_ph__vm__success_col);
    block.AppendColumn("compute_ph__vm__msg_state_used", compute_ph__vm__msg_state_used_col);
    block.AppendColumn("compute_ph__vm__account_activated", compute_ph__vm__account_activated_col);
    block.AppendColumn("compute_ph__vm__gas_fees", compute_ph__vm__gas_fees_col);
    block.AppendColumn("compute_ph__vm__gas_used", compute_ph__vm__gas_used_col);
    block.AppendColumn("compute_ph__vm__gas_limit", compute_ph__vm__gas_limit_col);
    block.AppendColumn("compute_ph__vm__gas_credit", compute_ph__vm__gas_credit_col);
    block.AppendColumn("compute_ph__vm__mode", compute_ph__vm__mode_col);
    block.AppendColumn("compute_ph__vm__exit_code", compute_ph__vm__exit_code_col);
    block.AppendColumn("compute_ph__vm__exit_arg", compute_ph__vm__exit_arg_col);
    block.AppendColumn("compute_ph__vm__vm_steps", compute_ph__vm__vm_steps_col);
    block.AppendColumn("compute_ph__vm__vm_init_state_hash", compute_ph__vm__vm_init_state_hash_col);
    block.AppendColumn("compute_ph__vm__vm_final_state_hash", compute_ph__vm__vm_final_state_hash_col);
    block.AppendColumn("action__success", action__success_col);
    block.AppendColumn("action__valid", action__valid_col);
    block.AppendColumn("action__no_funds", action__no_funds_col);
    block.AppendColumn("action__status_change", action__status_change_col);
    block.AppendColumn("action__total_fwd_fees", action__total_fwd_fees_col);
    block.AppendColumn("action__total_action_fees", action__total_action_fees_col);
    block.AppendColumn("action__result_code", action__result_code_col);
    block.AppendColumn("action__result_arg", action__result_arg_col);
    block.AppendColumn("action__tot_actions", action__tot_actions_col);
    block.AppendColumn("action__spec_actions", action__spec_actions_col);
    block.AppendColumn("action__skipped_actions", action__skipped_actions_col);
    block.AppendColumn("action__msgs_created", action__msgs_created_col);
    block.AppendColumn("action__action_list_hash", action__action_list_hash_col);
    block.AppendColumn("action__tot_msg_size__cells", action__tot_msg_size__cells_col);
    block.AppendColumn("action__tot_msg_size__bits", action__tot_msg_size__bits_col);
    block.AppendColumn("bounce", bounce_col);
    block.AppendColumn("bounce__msg_size__cells", bounce__msg_size__cells_col);
    block.AppendColumn("bounce__msg_size__bits", bounce__msg_size__bits_col);
    block.AppendColumn("bounce__no_funds__req_fwd_fees", bounce__no_funds__req_fwd_fees_col);
    block.AppendColumn("bounce__ok__msg_fees", bounce__ok__msg_fees_col);
    block.AppendColumn("bounce__ok__fwd_fees", bounce__ok__fwd_fees_col);
    block.AppendColumn("split_info__cur_shard_pfx_len", split_info__cur_shard_pfx_len_col);
    block.AppendColumn("split_info__acc_split_depth", split_info__acc_split_depth_col);
    block.AppendColumn("split_info__this_addr", split_info__this_addr_col);
    block.AppendColumn("split_info__sibling_addr", split_info__sibling_addr_col);

    client.Insert("transactions", block);
}

void InsertBatchClickhouse::insert_messages(clickhouse::Client &client) {
    using namespace clickhouse;
    
    clickhouse::Block block;

    auto tx_hash_col = std::make_shared<ColumnFixedString>(44);
    auto tx_lt_col = std::make_shared<ColumnUInt64>();
    auto tx_account_col = std::make_shared<ColumnFixedString>(68);
    auto tx_now_col = std::make_shared<ColumnUInt32>();
    auto block_workchain_col = std::make_shared<ColumnInt32>();
    auto block_shard_col = std::make_shared<ColumnInt64>();
    auto block_seqno_col = std::make_shared<ColumnInt32>();
    auto mc_block_seqno_col = std::make_shared<ColumnInt32>();
    auto direction_col = std::make_shared<ColumnEnum8>(Type::CreateEnum8({{"in", 0}, {"out", 1}}));
    auto hash_col = std::make_shared<ColumnFixedString>(68);
    auto source_col = std::make_shared<ColumnNullableT<ColumnFixedString>>(68);
    auto destination_col = std::make_shared<ColumnNullableT<ColumnFixedString>>(68);
    auto value_col = std::make_shared<ColumnNullableT<ColumnUInt64>>();
    auto fwd_fee_col = std::make_shared<ColumnNullableT<ColumnUInt64>>();
    auto ihr_fee_col = std::make_shared<ColumnNullableT<ColumnUInt64>>();
    auto created_lt_col = std::make_shared<ColumnNullableT<ColumnUInt64>>();
    auto created_at_col = std::make_shared<ColumnNullableT<ColumnUInt32>>();
    auto opcode_col = std::make_shared<ColumnNullableT<ColumnInt32>>();
    auto ihr_disabled_col = std::make_shared<ColumnNullableT<ColumnUInt8>>();
    auto bounce_col = std::make_shared<ColumnNullableT<ColumnUInt8>>();
    auto bounced_col = std::make_shared<ColumnNullableT<ColumnUInt8>>();
    auto import_fee_col = std::make_shared<ColumnNullableT<ColumnUInt64>>();
    auto body_hash_col = std::make_shared<ColumnFixedString>(44);
    auto init_state_hash_col = std::make_shared<ColumnNullableT<ColumnFixedString>>(44);

    std::set<td::Bits256> body_hashes;
    std::vector<MsgBody> bodies;

    auto store_message = [&](const schema::Block& blk, const schema::Transaction& tx, const schema::Message& msg, std::int8_t direction = 1) {
        tx_hash_col->Append(td::base64_encode(tx.hash.as_slice()));
        tx_lt_col->Append(tx.lt);
        tx_account_col->Append(convert::to_raw_address(tx.account));
        tx_now_col->Append(tx.now);
        block_workchain_col->Append(blk.workchain);
        block_shard_col->Append(blk.shard);
        block_seqno_col->Append(blk.seqno);
        mc_block_seqno_col->Append(blk.mc_block_seqno.value());
        direction_col->Append(direction);
        hash_col->Append(td::base64_encode(msg.hash.as_slice()));
        source_col->Append(TO_STD_OPTIONAL(msg.source));
        destination_col->Append(TO_STD_OPTIONAL(msg.destination));
        value_col->Append(TO_STD_OPTIONAL(msg.value));
        fwd_fee_col->Append(TO_STD_OPTIONAL(msg.fwd_fee));
        ihr_fee_col->Append(TO_STD_OPTIONAL(msg.ihr_fee));
        created_lt_col->Append(TO_STD_OPTIONAL(msg.created_lt));
        created_at_col->Append(TO_STD_OPTIONAL(msg.created_at));
        opcode_col->Append(TO_STD_OPTIONAL(msg.opcode));
        ihr_disabled_col->Append(TO_STD_OPTIONAL(msg.ihr_disabled));
        bounce_col->Append(TO_STD_OPTIONAL(msg.bounce));
        bounced_col->Append(TO_STD_OPTIONAL(msg.bounced));
        import_fee_col->Append(TO_STD_OPTIONAL(msg.import_fee));

        td::Bits256 body_hash = msg.body->get_hash().bits();
        if(body_hashes.find(body_hash) == body_hashes.end()) {
            body_hashes.insert(body_hash);
            bodies.push_back({body_hash, msg.body_boc});
        }
        body_hash_col->Append(td::base64_encode(body_hash.as_slice()));

        if (msg.init_state_boc) {
            td::Bits256 init_state_hash = msg.init_state->get_hash().bits();
            if(body_hashes.find(init_state_hash) == body_hashes.end()) {
                body_hashes.insert(init_state_hash);
                bodies.push_back({init_state_hash, msg.init_state_boc.value()});
            }
            init_state_hash_col->Append(td::base64_encode(init_state_hash.as_slice()));
        } else {
            init_state_hash_col->Append(std::nullopt);
        }
    };

    for(const auto& task_ : insert_tasks_) {
        for(const auto& blk_ : task_.parsed_block_->blocks_) {
            for(const auto& tx_ : blk_.transactions) {
                if(tx_.in_msg) {
                    store_message(blk_, tx_, tx_.in_msg.value(), 0);
                }
                for(const auto& msg_ : tx_.out_msgs) {
                    store_message(blk_, tx_, msg_, 1);
                }
            }
        }
    }

    block.AppendColumn("tx_hash", tx_hash_col);
    block.AppendColumn("tx_lt", tx_lt_col);
    block.AppendColumn("tx_account", tx_account_col);
    block.AppendColumn("tx_now", tx_now_col);
    block.AppendColumn("block_workchain", block_workchain_col);
    block.AppendColumn("block_shard", block_shard_col);
    block.AppendColumn("block_seqno", block_seqno_col);
    block.AppendColumn("mc_block_seqno", mc_block_seqno_col);
    block.AppendColumn("direction", direction_col);
    block.AppendColumn("hash", hash_col);
    block.AppendColumn("source", source_col);
    block.AppendColumn("destination", destination_col);
    block.AppendColumn("value", value_col);
    block.AppendColumn("fwd_fee", fwd_fee_col);
    block.AppendColumn("ihr_fee", ihr_fee_col);
    block.AppendColumn("created_lt", created_lt_col);
    block.AppendColumn("created_at", created_at_col);
    block.AppendColumn("opcode", opcode_col);
    block.AppendColumn("ihr_disabled", ihr_disabled_col);
    block.AppendColumn("bounce", bounce_col);
    block.AppendColumn("bounced", bounced_col);
    block.AppendColumn("import_fee", import_fee_col);
    block.AppendColumn("body_hash", body_hash_col);
    block.AppendColumn("init_state_hash", init_state_hash_col);

    client.Insert("messages", block);

    // insert message bodies
    clickhouse::Block block2;
    auto msg_hash_col = std::make_shared<ColumnFixedString>(44);
    auto msg_body_col = std::make_shared<ColumnString>();

    for (const auto& body_ : bodies) {
        msg_hash_col->Append(td::base64_encode(body_.hash.as_slice()));
        msg_body_col->Append(body_.body);
    }

    block2.AppendColumn("hash", msg_hash_col);
    block2.AppendColumn("data", msg_body_col);

    client.Insert("message_contents", block2);
}

void InsertBatchClickhouse::insert_account_states(clickhouse::Client &client) {
    using namespace clickhouse;

    clickhouse::Block block, block_latest;

    auto hash_col = std::make_shared<ColumnFixedString>(44);
    auto account_col = std::make_shared<ColumnFixedString>(68);
    auto timestamp_col = std::make_shared<ColumnUInt32>();
    auto balance_col = std::make_shared<ColumnUInt64>();
    auto account_status_col = std::make_shared<ColumnEnum8>(Type::CreateEnum8({{"uninit", 0}, {"frozen", 1}, {"active", 2}, {"nonexist", 3}}));
    auto frozen_hash_col = std::make_shared<ColumnNullableT<ColumnFixedString>>(44);
    auto code_hash_col = std::make_shared<ColumnNullableT<ColumnFixedString>>(44);
    auto data_hash_col = std::make_shared<ColumnNullableT<ColumnFixedString>>(44);
    auto last_trans_lt_col = std::make_shared<ColumnUInt64>();
    auto code_boc_col = std::make_shared<ColumnNullableT<ColumnString>>();
    auto data_boc_col = std::make_shared<ColumnNullableT<ColumnString>>();

    for(const auto& task_ : insert_tasks_) {
        for (const auto& state_: task_.parsed_block_->account_states_) {
            hash_col->Append(td::base64_encode(state_.hash.as_slice()));
            account_col->Append(convert::to_raw_address(state_.account));
            timestamp_col->Append(state_.timestamp);
            balance_col->Append(state_.balance);
            account_status_col->Append(state_.account_status);
            frozen_hash_col->Append(TO_STD_OPTIONAL(state_.frozen_hash));
            code_hash_col->Append(TO_STD_OPTIONAL(state_.code_hash));
            data_hash_col->Append(TO_STD_OPTIONAL(state_.data_hash));
            last_trans_lt_col->Append(state_.last_trans_lt);

            if (state_.code.not_null()) {
                auto code_res = vm::std_boc_serialize(state_.code);
                if(code_res.is_error()) {
                    LOG(ERROR) << "Failed to convert code boc";
                    code_boc_col->Append(std::nullopt);
                } else {
                    code_boc_col->Append(td::base64_encode(code_res.move_as_ok().as_slice().str()));
                }
            } else {
                code_boc_col->Append(std::nullopt);
            }
            if (state_.data.not_null()) {
                auto data_res = vm::std_boc_serialize(state_.data);
                if(data_res.is_error()) {
                    LOG(ERROR) << "Failed to convert data boc";
                    data_boc_col->Append(std::nullopt);
                } else {
                    data_boc_col->Append(td::base64_encode(data_res.move_as_ok().as_slice().str()));
                }
            } else {
                data_boc_col->Append(std::nullopt);
            }
        }
    }

    block.AppendColumn("hash", hash_col);
    block.AppendColumn("account", account_col);
    block.AppendColumn("timestamp", timestamp_col);
    block.AppendColumn("balance", balance_col);
    block.AppendColumn("account_status", account_status_col);
    block.AppendColumn("frozen_hash", frozen_hash_col);
    block.AppendColumn("code_hash", code_hash_col);
    block.AppendColumn("data_hash", data_hash_col);
    block.AppendColumn("last_trans_lt", last_trans_lt_col);

    block_latest.AppendColumn("hash", hash_col);
    block_latest.AppendColumn("account", account_col);
    block_latest.AppendColumn("timestamp", timestamp_col);
    block_latest.AppendColumn("balance", balance_col);
    block_latest.AppendColumn("account_status", account_status_col);
    block_latest.AppendColumn("frozen_hash", frozen_hash_col);
    block_latest.AppendColumn("code_hash", code_hash_col);
    block_latest.AppendColumn("data_hash", data_hash_col);
    block_latest.AppendColumn("last_trans_lt", last_trans_lt_col);
    block_latest.AppendColumn("code_boc", code_boc_col);
    block_latest.AppendColumn("data_boc", data_boc_col);

    client.Insert("account_states", block);
    client.Insert("latest_account_states", block_latest);
}
