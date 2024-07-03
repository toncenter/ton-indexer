#include "BlockEmulator.h"
#include "TraceInterfaceDetector.h"

class BlockParser: public td::actor::Actor {
    td::Ref<ton::validator::BlockData> block_data_;
    td::Promise<std::vector<TransactionInfo>> promise_;
public:
    BlockParser(td::Ref<ton::validator::BlockData> block_data, td::Promise<std::vector<TransactionInfo>> promise)
        : block_data_(std::move(block_data)), promise_(std::move(promise)) {}

    void start_up() override {
        std::vector<TransactionInfo> res;

        block::gen::Block::Record blk;
        block::gen::BlockInfo::Record info;
        block::gen::BlockExtra::Record extra;
        if (!(tlb::unpack_cell(block_data_->root_cell(), blk) && tlb::unpack_cell(blk.info, info) && tlb::unpack_cell(blk.extra, extra))) {
            promise_.set_error(td::Status::Error("block data info extra unpack failed"));
            stop();
            return;
        }
        try {
            vm::AugmentedDictionary acc_dict{vm::load_cell_slice_ref(extra.account_blocks), 256, block::tlb::aug_ShardAccountBlocks};

            td::Bits256 cur_addr = td::Bits256::zero();
            bool eof = false;
            bool allow_same = true;
            while (!eof) {
                auto value = acc_dict.extract_value(
                    acc_dict.vm::DictionaryFixed::lookup_nearest_key(cur_addr.bits(), 256, true, allow_same));
                if (value.is_null()) {
                    eof = true;
                    break;
                }
                allow_same = false;
                block::gen::AccountBlock::Record acc_blk;
                if (!(tlb::csr_unpack(std::move(value), acc_blk) && acc_blk.account_addr == cur_addr)) {
                    promise_.set_error(td::Status::Error("invalid AccountBlock for account " + cur_addr.to_hex()));
                    stop();
                    return;
                }
                vm::AugmentedDictionary trans_dict{vm::DictNonEmpty(), std::move(acc_blk.transactions), 64,
                                                    block::tlb::aug_AccountTransactions};
                td::BitArray<64> cur_trans{(long long)0};
                while (true) {
                    auto tvalue = trans_dict.extract_value_ref(
                        trans_dict.vm::DictionaryFixed::lookup_nearest_key(cur_trans.bits(), 64, true));
                    if (tvalue.is_null()) {
                        break;
                    }
                    block::gen::Transaction::Record trans;
                    if (!tlb::unpack_cell(tvalue, trans)) {
                        promise_.set_error(td::Status::Error("Failed to unpack Transaction"));
                        stop();
                        return;
                    }
                    block::gen::TransactionDescr::Record_trans_ord descr;
                    if (!tlb::unpack_cell(trans.description, descr)) {
                        LOG(WARNING) << "Skipping non ord transaction " << tvalue->get_hash().to_hex();
                        continue;
                    }

                    TransactionInfo tx_info;

                    tx_info.account = block::StdAddress(block_data_->block_id().id.workchain, cur_addr);
                    tx_info.hash = tvalue->get_hash().bits();
                    tx_info.root = tvalue;
                    tx_info.lt = trans.lt;

                    if (trans.r1.in_msg->prefetch_long(1)) {
                        auto msg = trans.r1.in_msg->prefetch_ref();
                        tx_info.in_msg_hash = msg->get_hash().bits();
                        auto message_cs = vm::load_cell_slice(trans.r1.in_msg->prefetch_ref());
                        tx_info.is_first = block::gen::t_CommonMsgInfo.get_tag(message_cs) == block::gen::CommonMsgInfo::ext_in_msg_info;
                    } else {
                        LOG(ERROR) << "Ordinary transaction without in_msg, skipping";
                        continue;
                    }

                    // LOG(INFO) << "TX hash: " << tx_info.hash.to_hex();

                    if (trans.outmsg_cnt != 0) {
                        vm::Dictionary dict{trans.r1.out_msgs, 15};
                        for (int x = 0; x < trans.outmsg_cnt; x++) {
                            auto value = dict.lookup_ref(td::BitArray<15>{x});
                            OutMsgInfo out_msg_info;
                            out_msg_info.hash = value->get_hash().bits();
                            out_msg_info.root = value;
                            tx_info.out_msgs.push_back(std::move(out_msg_info));

                            // LOG(INFO) << "  out msg: " << out_msg_info.hash.to_hex();
                        }
                    }

                    res.push_back(tx_info);
                }
            }
        } catch (vm::VmError err) {
            promise_.set_error(td::Status::Error(PSLICE() << "error while parsing AccountBlocks : " << err.get_msg()));
            stop();
            return;
        }
        promise_.set_value(std::move(res));
        stop();
        return;
    }
};

McBlockEmulator::McBlockEmulator(MasterchainBlockDataState mc_data_state, std::function<void(std::unique_ptr<Trace>)> trace_processor, td::Promise<> promise)
    : mc_data_state_(std::move(mc_data_state)), trace_processor_(std::move(trace_processor)), promise_(std::move(promise)), blocks_left_to_parse_(mc_data_state_.shard_blocks_diff_.size()) {
        auto libraries_root = mc_data_state_.config_->get_libraries_root();
        emulator_ = std::make_shared<emulator::TransactionEmulator>(mc_data_state_.config_, 0);
        emulator_->set_libs(vm::Dictionary(libraries_root, 256));
}

void McBlockEmulator::start_up() {
    start_time_ = td::Timestamp::now();
    for (const auto& shard_state : mc_data_state_.shard_blocks_) {
        shard_states_.push_back(shard_state.block_state);
    }

    for (auto& block_data : mc_data_state_.shard_blocks_diff_) {
        LOG(INFO) << "Parsing block " << block_data.block_data->block_id().to_str();
        auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), blk_id = block_data.block_data->block_id().id](td::Result<std::vector<TransactionInfo>> R) {
            if (R.is_error()) {
                td::actor::send_closure(SelfId, &McBlockEmulator::parse_error, blk_id, R.move_as_error());
                return;
            }
            td::actor::send_closure(SelfId, &McBlockEmulator::block_parsed, blk_id, R.move_as_ok());
        });
        td::actor::create_actor<BlockParser>("BlockParser", block_data.block_data, std::move(P)).release();
    }
}

void McBlockEmulator::parse_error(ton::BlockId blkid, td::Status error) {
    LOG(ERROR) << "Failed to parse block " << blkid.to_str() << ": " << error;
    promise_.set_error(std::move(error));
    stop();
}

void McBlockEmulator::block_parsed(ton::BlockId blkid, std::vector<TransactionInfo> txs) {
    txs_.insert(txs_.end(), txs.begin(), txs.end());
    blocks_left_to_parse_--;
    if (blocks_left_to_parse_ == 0) {
        process_txs();
    }
}

void McBlockEmulator::process_txs() {
    std::sort(txs_.begin(), txs_.end(), [](const TransactionInfo& a, const TransactionInfo& b) {
        return a.lt < b.lt;
    });

    std::unordered_map<td::Bits256, TransactionInfo, BitArrayHasher> txs_by_out_msg_hash;
    for (const auto& tx : txs_) {
        tx_by_in_msg_hash_.insert({tx.in_msg_hash, std::ref(tx)});
        for (const auto& out_msg : tx.out_msgs) {
            txs_by_out_msg_hash.insert({out_msg.hash, tx});
        }
    }

    for (auto& tx : txs_) {
        if (tx.is_first) {
            tx.initial_msg_hash = tx.in_msg_hash;
        } else if (txs_by_out_msg_hash.find(tx.in_msg_hash) != txs_by_out_msg_hash.end() && txs_by_out_msg_hash[tx.in_msg_hash].initial_msg_hash.has_value()) {
            tx.initial_msg_hash = txs_by_out_msg_hash[tx.in_msg_hash].initial_msg_hash;
        } else if (interblock_trace_ids_.find(tx.in_msg_hash) != interblock_trace_ids_.end()) {
            tx.initial_msg_hash = interblock_trace_ids_[tx.in_msg_hash];
        } else {
            LOG(WARNING) << "Couldn't get initial_msg_hash for tx " << tx.hash.to_hex() << ". This tx will be skipped.";
        }

        // write trace_id for out_msgs for interblock chains
        if (tx.initial_msg_hash.has_value()) {
            for (const auto& out_msg : tx.out_msgs) {
                interblock_trace_ids_[out_msg.hash] = tx.initial_msg_hash.value();
            }
        }
    }

    emulate_traces();
}

void McBlockEmulator::db_error(td::Status error) {
    LOG(ERROR) << "Failed to lookup trace_ids: " << error;
    promise_.set_error(std::move(error));
    stop();
}

void McBlockEmulator::emulate_traces() {
    for (auto& tx : txs_) {
        if (!tx.initial_msg_hash.has_value()) {
            // we don't emulate traces for transactions that have no initial_msg_hash
            continue;
        }
        if (trace_ids_in_progress_.find(tx.initial_msg_hash.value()) != trace_ids_in_progress_.end()) {
            // we already emulating trace for this trace_id
            continue;
        }

        // for debugging
        // if (tx.hash.to_hex() != "36F63618F374305C85AAEF856E4EE055540FA6F199453F8F2D347C199F7FDBB2") {
        //     continue;
        // }
        // if (tx.initial_msg_hash.value().to_hex() != "A97D033AF645F4D1BE66EFFCCA441D570A1867BED666FC462A55FC620EE8EE18") {
        //     continue;
        // }
        
        auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), tx_hash = tx.hash, trace_id = tx.initial_msg_hash.value()](td::Result<Trace *> R) {
            if (R.is_error()) {
                td::actor::send_closure(SelfId, &McBlockEmulator::trace_error, tx_hash, trace_id, R.move_as_error());
                return;
            }

            td::actor::send_closure(SelfId, &McBlockEmulator::trace_received, tx_hash, R.move_as_ok());
        });
        td::actor::send_closure(actor_id(this), &McBlockEmulator::create_trace, tx, std::move(P));

        trace_ids_in_progress_.insert(tx.initial_msg_hash.value());
    }
}

td::Result<block::Account> McBlockEmulator::fetch_account(const block::StdAddress& addr, ton::UnixTime now) {
    for (const auto &shard_state : shard_states_) {
        block::gen::ShardStateUnsplit::Record sstate;
        if (!tlb::unpack_cell(shard_state, sstate)) {
            return td::Status::Error("Failed to unpack ShardStateUnsplit");
        }
        block::ShardId shard;
        if (!(shard.deserialize(sstate.shard_id.write()))) {
            return td::Status::Error("Failed to deserialize ShardId");
        }
        if (shard.workchain_id == addr.workchain && ton::shard_contains(ton::ShardIdFull(shard).shard, addr.addr)) {
            vm::AugmentedDictionary accounts_dict{vm::load_cell_slice_ref(sstate.accounts), 256, block::tlb::aug_ShardAccounts};
            auto account = block::Account(addr.workchain, addr.addr.cbits());
            auto account_cs = accounts_dict.lookup(addr.addr);
            if (account_cs.is_null()) {
                return td::Status::Error("Account not found in shard_states");
            } else if (!account.unpack(std::move(account_cs), now, 
                                    addr.workchain == ton::masterchainId && emulator_->get_config().is_special_smartcontract(addr.addr))) {
                return td::Status::Error("Failed to unpack account");
            }
            return account;
        }
    }
    return td::Status::Error("Account not found in shard_states");
}

void McBlockEmulator::create_trace(const TransactionInfo& tx, td::Promise<Trace *> promise) {
    Trace *trace = new Trace();
    trace->emulated = false;
    trace->workchain = tx.account.workchain;
    trace->transaction_root = tx.root;
    trace->id = tx.initial_msg_hash.value();
    trace->node_id = tx.in_msg_hash;
    auto account = fetch_account(tx.account, emulator_->get_unixtime());
    if (account.is_error()) {
        promise.set_error(account.move_as_error_prefix("Failed to fetch account from shard states: "));
        return;
    }
    trace->account = std::make_unique<block::Account>(account.move_as_ok());

    td::MultiPromise mp;
    auto ig = mp.init_guard();
    ig.add_promise([SelfId = actor_id(this), trace, promise = std::move(promise)](td::Result<td::Unit> R) mutable {
        if (R.is_error()) {
            promise.set_error(R.move_as_error());
            return;
        }
        promise.set_value(std::move(trace));
    });

    int children_cnt = 0;
    for (const auto out_msg : tx.out_msgs) {
        auto message_cs = vm::load_cell_slice(out_msg.root);
        if (block::gen::t_CommonMsgInfo.get_tag(message_cs) == block::gen::CommonMsgInfo::ext_out_msg_info) {
            // skip ext out msgs
            continue;
        }
        children_cnt++;
    }
    trace->children.resize(children_cnt);

    int child_ind = 0;
    for (const auto out_msg : tx.out_msgs) {
        // LOG(INFO) << "tx: " << tx.hash.to_hex() << " creating trace for msg " << out_msg.hash.to_hex();
        auto message_cs = vm::load_cell_slice(out_msg.root);
        if (block::gen::t_CommonMsgInfo.get_tag(message_cs) == block::gen::CommonMsgInfo::ext_out_msg_info) {
            // skip ext out msgs
            continue;
        }

        if (tx_by_in_msg_hash_.find(out_msg.hash) != tx_by_in_msg_hash_.end()) {
            const TransactionInfo& child_tx = tx_by_in_msg_hash_.at(out_msg.hash);
            auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), msg_hash = out_msg.hash, parent_trace = trace, child_ind, subpromise = ig.get_promise()](td::Result<Trace *> R) mutable {
                if (R.is_error()) {
                    subpromise.set_error(R.move_as_error());
                    return;
                }
                parent_trace->children[child_ind] = R.move_as_ok();
                subpromise.set_value(td::Unit());
            });
            td::actor::send_closure(actor_id(this), &McBlockEmulator::create_trace, child_tx, std::move(P));
        } else {
            // LOG(INFO) << "Emulating trace for tx " << tx.hash.to_hex() << " msg " << out_msg.hash.to_hex();
            auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), msg_hash = out_msg.hash, parent_trace = trace, child_ind, subpromise = ig.get_promise()](td::Result<Trace *> R) mutable {
                if (R.is_error()) {
                    subpromise.set_error(R.move_as_error());
                    return;
                }
                parent_trace->children[child_ind] = R.move_as_ok();
                subpromise.set_value(td::Unit());
            });
            std::unordered_map<block::StdAddress, std::shared_ptr<block::Account>, AddressHasher> shard_accounts;
            td::actor::create_actor<TraceEmulator>("TraceEmu", emulator_, shard_states_, std::move(shard_accounts), out_msg.root, 20, std::move(P)).release();
        }
        child_ind++;
    }
}

void McBlockEmulator::trace_error(td::Bits256 tx_hash, TraceId trace_id, td::Status error) {
    LOG(ERROR) << "Failed to emulate trace_id " << trace_id.to_hex() << " from tx " << tx_hash.to_hex() << ": " << error;
    trace_ids_in_progress_.erase(trace_id);
}

void McBlockEmulator::trace_received(td::Bits256 tx_hash, Trace *trace) {
    LOG(INFO) << "Emulated trace " << trace->id.to_hex() << " from tx " << tx_hash.to_hex() << ": " << trace->transactions_count() << " transactions, " << trace->depth() << " depth";
    if constexpr (std::variant_size_v<Trace::Detector::DetectedInterface> > 0) {
        auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), trace_id = trace->id](td::Result<std::unique_ptr<Trace>> R) {
            if (R.is_error()) {
                td::actor::send_closure(SelfId, &McBlockEmulator::trace_interfaces_error, trace_id, R.move_as_error());
                return;
            }
            td::actor::send_closure(SelfId, &McBlockEmulator::finish_processing, R.move_as_ok());
        });

        td::actor::create_actor<TraceInterfaceDetector>("TraceInterfaceDetector", shard_states_, mc_data_state_.config_, std::unique_ptr<Trace>(trace), std::move(P)).release();
    } else {
        finish_processing(std::unique_ptr<Trace>(trace));
    }
}

void McBlockEmulator::trace_interfaces_error(TraceId trace_id, td::Status error) {
    LOG(ERROR) << "Failed to detect interfaces on trace_id " << trace_id.to_hex() << ": " << error;
    trace_ids_in_progress_.erase(trace_id);
}

void McBlockEmulator::finish_processing(std::unique_ptr<Trace> trace) {
    LOG(INFO) << trace->to_string();
    auto trace_id = trace->id;
    
    trace_processor_(std::move(trace));

    trace_ids_in_progress_.erase(trace_id);
    traces_cnt_++;
    if (trace_ids_in_progress_.empty()) {
        auto blkid = mc_data_state_.shard_blocks_[0].block_data->block_id().id;
        LOG(INFO) << "Finished emulating block " << blkid.to_str() << ": " << traces_cnt_ << " traces in " << (td::Timestamp::now().at() - start_time_.at()) * 1000 << " ms";
        promise_.set_value(td::Unit());
        stop();
    }
}
