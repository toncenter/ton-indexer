#include "BlockEmulator.h"
#include "TraceInterfaceDetector.h"


class BlockParser: public td::actor::Actor {
    td::Ref<ton::validator::BlockData> block_data_;
    ton::BlockSeqno mc_block_seqno_;
    td::Promise<std::vector<TransactionInfo>> promise_;
public:
    BlockParser(td::Ref<ton::validator::BlockData> block_data, ton::BlockSeqno mc_block_seqno, td::Promise<std::vector<TransactionInfo>> promise)
        : block_data_(std::move(block_data)), mc_block_seqno_(mc_block_seqno), promise_(std::move(promise)) {}

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
                    tx_info.block_id = block_data_->block_id().id;
                    tx_info.mc_block_seqno = mc_block_seqno_;

                    if (trans.r1.in_msg->prefetch_long(1)) {
                        auto msg = trans.r1.in_msg->prefetch_ref();
                        tx_info.in_msg_hash = msg->get_hash().bits();
                        auto message_cs = vm::load_cell_slice(trans.r1.in_msg->prefetch_ref());
                        if (block::gen::t_CommonMsgInfo.get_tag(message_cs) == block::gen::CommonMsgInfo::ext_in_msg_info) {
                            tx_info.trace_ids = TraceIds{
                                .root_tx_hash = tx_info.hash,
                                .ext_in_msg_hash = msg->get_hash().bits(),
                                .ext_in_msg_hash_norm = ext_in_msg_get_normalized_hash(msg).move_as_ok()
                            };
                        }
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

McBlockEmulator::McBlockEmulator(MasterchainBlockDataState mc_data_state, std::function<void(Trace, td::Promise<td::Unit>)> trace_processor, td::Promise<> promise)
        : mc_data_state_(std::move(mc_data_state)), trace_processor_(std::move(trace_processor)), promise_(std::move(promise)), 
          blocks_left_to_parse_(mc_data_state_.shard_blocks_diff_.size()) {
}

void McBlockEmulator::start_up() {
    start_time_ = td::Timestamp::now();
    for (const auto& shard_state : mc_data_state_.shard_blocks_) {
        shard_states_.push_back(shard_state.block_state);
    }
    auto mc_block_seqno = mc_data_state_.shard_blocks_[0].handle->id().seqno();

    for (auto& block_data : mc_data_state_.shard_blocks_diff_) {
        LOG(INFO) << "Parsing block " << block_data.block_data->block_id().to_str();
        auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), blk_id = block_data.block_data->block_id().id](td::Result<std::vector<TransactionInfo>> R) {
            if (R.is_error()) {
                td::actor::send_closure(SelfId, &McBlockEmulator::parse_error, blk_id, R.move_as_error());
                return;
            }
            td::actor::send_closure(SelfId, &McBlockEmulator::block_parsed, blk_id, R.move_as_ok());
        });
        
        td::actor::create_actor<BlockParser>("BlockParser", block_data.block_data, mc_block_seqno, std::move(P)).release();
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

    for (auto& tx : txs_) {
        for (const auto& out_msg : tx.out_msgs) {
            tx_by_out_msg_hash_.insert({out_msg.hash, tx});
        }
    }

    for (auto& tx : txs_) {
        if (tx.trace_ids.has_value()) {
            // we already have trace_ids for this tx
        } else if (tx_by_out_msg_hash_.find(tx.in_msg_hash) != tx_by_out_msg_hash_.end() && 
                   tx_by_out_msg_hash_[tx.in_msg_hash].trace_ids.has_value()) {
            tx.trace_ids = tx_by_out_msg_hash_[tx.in_msg_hash].trace_ids;
        } else if (interblock_trace_ids_.find(tx.in_msg_hash) != interblock_trace_ids_.end()) {
            tx.trace_ids = interblock_trace_ids_[tx.in_msg_hash];
        } else {
            LOG(WARNING) << "Couldn't get ext_in_msg_hash_norm for tx " << tx.hash.to_hex() << ". This tx will be skipped.";
        }

        // write trace_id for out_msgs for interblock chains
        if (tx.trace_ids.has_value()) {
            for (const auto& out_msg : tx.out_msgs) {
                interblock_trace_ids_[out_msg.hash] = tx.trace_ids.value();
            }
        }

        tx_by_in_msg_hash_.insert({tx.in_msg_hash, tx});
    }

    emulate_traces();
}


std::unique_ptr<TraceNode> McBlockEmulator::construct_commited_trace(const TransactionInfo& tx, std::vector<td::Ref<vm::Cell>>& msgs_to_emulate) {
    auto trace_node = std::make_unique<TraceNode>();
    trace_node->emulated = false;
    trace_node->transaction_root = tx.root;
    trace_node->node_id = tx.in_msg_hash;
    trace_node->address = tx.account;
    trace_node->block_id = tx.block_id;
    trace_node->mc_block_seqno = tx.mc_block_seqno;

    for (const auto& out_msg : tx.out_msgs) {
        int type;
        auto destination_r = fetch_msg_dest_address(out_msg.root, type);
        if (type == block::gen::CommonMsgInfo::ext_out_msg_info) {
            continue;
        }
        if (destination_r.is_error()) {
            LOG(ERROR) << "Failed to fetch destination address for out_msg " << out_msg.hash.to_hex();
            continue;
        }
        auto destination = destination_r.move_as_ok();

        if (tx_by_in_msg_hash_.find(out_msg.hash) != tx_by_in_msg_hash_.end()) {
            TransactionInfo& child_tx = tx_by_in_msg_hash_.at(out_msg.hash);
            if (!child_tx.trace_ids.has_value()) {
                LOG(WARNING) << "No trace ids for child tx " << child_tx.hash.to_hex();
                child_tx.trace_ids = tx.trace_ids;
            }

            auto child_trace_node = construct_commited_trace(child_tx, msgs_to_emulate);
            trace_node->children.push_back(std::move(child_trace_node));
        } else {
            msgs_to_emulate.push_back(out_msg.root);
        }
    }
    return trace_node;
}

void McBlockEmulator::emulate_traces() {
    for (auto& tx : txs_) {
        if (!tx.trace_ids.has_value()) {
            // we don't emulate traces for transactions that have no trace_ids
            continue;
        }
        
        // if this tx.in_msg is some out_msg of another tx in the same block, we don't emulate it
        if (tx_by_out_msg_hash_.find(tx.in_msg_hash) != tx_by_out_msg_hash_.end()) {
            continue;
        }

        in_progress_cnt_++;

        std::vector<td::Ref<vm::Cell>> msgs_to_emulate;
        auto parent_node = construct_commited_trace(tx, msgs_to_emulate);
        
        if (msgs_to_emulate.empty()) {
            children_emulated(std::move(parent_node), {}, tx.trace_ids.value(), nullptr);
            continue;
        }
        
        // for now we emulate each trace independently
        auto context = std::make_unique<EmulationContext>(mc_data_state_.shard_blocks_[0].handle->id().id.seqno, mc_data_state_.config_);
        for (const auto& shard_state : mc_data_state_.shard_blocks_) {
            auto blkid = shard_state.handle->id().id;
            auto timestamp = shard_state.handle->unix_time();
            auto lt = shard_state.handle->logical_time();
            lt = lt - lt % block::ConfigInfo::get_lt_align();
            context->add_shard_state(blkid, timestamp, lt, shard_state.block_state);
        }
        context->increase_seqno(3);

        EmulationContext& context_ref = *context;

        auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), parent_node = std::move(parent_node), tx_info = tx, context = std::move(context)]
                (td::Result<std::vector<std::unique_ptr<TraceNode>>> R) mutable {
            if (R.is_error()) {
                td::actor::send_closure(SelfId, &McBlockEmulator::trace_error, tx_info.hash, tx_info.trace_ids->root_tx_hash, R.move_as_error());
            } else {
                td::actor::send_closure(SelfId, &McBlockEmulator::children_emulated, 
                    std::move(parent_node), R.move_as_ok(), tx_info.trace_ids.value(), std::move(context));
            }
        });

        td::actor::create_actor<MasterchainBlockEmulator>("MasterchainBlockEmulator", context_ref, std::move(msgs_to_emulate), std::move(P)).release();
    }
}

void McBlockEmulator::children_emulated(std::unique_ptr<TraceNode> parent_node, std::vector<std::unique_ptr<TraceNode>> child_nodes, 
                                     TraceIds trace_ids, std::unique_ptr<EmulationContext> context) {
    parent_node->children.insert(parent_node->children.end(),
        std::make_move_iterator(child_nodes.begin()),
        std::make_move_iterator(child_nodes.end()));

    Trace trace;
    trace.ext_in_msg_hash = trace_ids.ext_in_msg_hash;
    trace.ext_in_msg_hash_norm = trace_ids.ext_in_msg_hash_norm;
    trace.root_tx_hash = trace_ids.root_tx_hash;
    trace.root = std::move(parent_node);
    trace.root_tx_hash = trace.root->transaction_root->get_hash().bits();
    if (context) {
        trace.rand_seed = context->get_rand_seed();
        trace.emulated_accounts = context->release_account_states();
        trace.tx_limit_exceeded = context->is_limit_exceeded();
    }

    if constexpr (std::variant_size_v<Trace::Detector::DetectedInterface> > 0) {
        auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), trace_root_tx_hash = trace.root_tx_hash](td::Result<Trace> R) {
            if (R.is_error()) {
                td::actor::send_closure(SelfId, &McBlockEmulator::trace_interfaces_error, trace_root_tx_hash, R.move_as_error());
                return;
            }
            td::actor::send_closure(SelfId, &McBlockEmulator::trace_emulated, R.move_as_ok());
        });

        td::actor::create_actor<TraceInterfaceDetector>("TraceInterfaceDetector", shard_states_, mc_data_state_.config_, std::move(trace), std::move(P)).release();
    } else {
        trace_emulated(std::move(trace));
    }
}

void McBlockEmulator::trace_error(td::Bits256 tx_hash, td::Bits256 trace_root_tx_hash, td::Status error) {
    LOG(ERROR) << "Failed to emulate trace with root tx " << td::base64_encode(trace_root_tx_hash.as_slice()) << " from tx " << tx_hash.to_hex() << ": " << error;
    in_progress_cnt_--;
}

void McBlockEmulator::trace_interfaces_error(td::Bits256 trace_root_tx_hash, td::Status error) {
    LOG(ERROR) << "Failed to detect interfaces on trace with root tx " << td::base64_encode(trace_root_tx_hash.as_slice()) << ": " << error;
    in_progress_cnt_--;
}

void McBlockEmulator::trace_emulated(Trace trace) {
    // LOG(INFO) << trace.to_string();
    
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), trace_root_tx_hash = trace.root_tx_hash](td::Result<td::Unit> R) {
        if (R.is_error()) {
            LOG(ERROR) << "Failed to insert trace " << td::base64_encode(trace_root_tx_hash.as_slice()) << ": " << R.move_as_error();
        } else {
            LOG(DEBUG) << "Successfully inserted trace " << td::base64_encode(trace_root_tx_hash.as_slice());
        }
        td::actor::send_closure(SelfId, &McBlockEmulator::trace_finished, trace_root_tx_hash);
    });

    trace_processor_(std::move(trace), std::move(P));
}

void McBlockEmulator::trace_finished(td::Bits256 trace_root_tx_hash) {
    in_progress_cnt_--;
    traces_cnt_++;

    if (in_progress_cnt_ == 0) {
        auto blkid = mc_data_state_.shard_blocks_[0].block_data->block_id().id;
        LOG(INFO) << "Finished emulating block " << blkid.to_str() << ": " << traces_cnt_ << " traces in " << (td::Timestamp::now().at() - start_time_.at()) * 1000 << " ms";
        promise_.set_value(td::Unit());
        stop();
    }
}
