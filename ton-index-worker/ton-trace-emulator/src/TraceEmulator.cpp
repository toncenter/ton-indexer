#include <fstream>
#include <sstream>
#include <string>
#include <emulator/transaction-emulator.h>
#include "crypto/openssl/rand.hpp"
#include "TraceEmulator.h"
#include "Statistics.h"

td::Result<block::StdAddress> fetch_msg_dest_address(td::Ref<vm::Cell> msg, int& type) {
    auto message_cs = vm::load_cell_slice(msg);
    int msg_tag = block::gen::t_CommonMsgInfo.get_tag(message_cs);
    type = msg_tag;
    if (msg_tag == block::gen::CommonMsgInfo::ext_in_msg_info) {
        block::gen::CommonMsgInfo::Record_ext_in_msg_info info;
        if (!(tlb::unpack(message_cs, info))) {
            return td::Status::Error("Can't unpack external message");
        }
        block::StdAddress addr;
        if (!block::tlb::t_MsgAddressInt.extract_std_address(info.dest, addr)) {
            return td::Status::Error("Can't extract address from external message");
        }
        return addr;
    } else if (msg_tag == block::gen::CommonMsgInfo::int_msg_info) {
        block::gen::CommonMsgInfo::Record_int_msg_info info;
        if (!(tlb::unpack(message_cs, info))) {
            return td::Status::Error("Can't unpack internal message");
        }
        block::StdAddress addr;
        if (!block::tlb::t_MsgAddressInt.extract_std_address(info.dest, addr)) {
            return td::Status::Error("Can't extract address from internal message");
        }
        return addr;
    } else {
        return td::Status::Error("Ext out message found");
    }
}

void TraceEmulatorImpl::emulate(td::Ref<vm::Cell> in_msg, block::StdAddress address, ton::LogicalTime lt,
                                td::Promise<std::unique_ptr<TraceNode>> promise) {
    auto account_r = context_.get_account_state(address);
    if (account_r.is_error()) {
        promise.set_error(account_r.move_as_error());
        return;
    }
    auto account = account_r.move_as_ok();
    ton::UnixTime unixtime = 0;
    for (auto& shard_state : context_.get_shard_states()) {
        if (shard_state.blkid == block_id_) {
            unixtime = shard_state.timestamp;
            break;
        }
    }
    auto emulation_r = emulator_->emulate_transaction(std::move(account), in_msg, unixtime, lt, block::transaction::Transaction::tr_ord);
    if (emulation_r.is_error()) {
        promise.set_error(emulation_r.move_as_error());
        return;
    }
    auto emulation = emulation_r.move_as_ok();

    auto external_not_accepted = dynamic_cast<emulator::TransactionEmulator::EmulationExternalNotAccepted *>(emulation.get());
    if (external_not_accepted) {
        promise.set_error(td::Status::Error(PSLICE() << "EmulationExternalNotAccepted: " << external_not_accepted->vm_exit_code));
        return;
    }

    auto emulation_success = dynamic_cast<emulator::TransactionEmulator::EmulationSuccess&>(*emulation);
    
    auto result = std::make_unique<TraceNode>();
    result->node_id = in_msg->get_hash().bits();
    result->emulated = true;
    result->address = address;
    result->transaction_root = emulation_success.transaction;
    result->block_id = block_id_;
    result->mc_block_seqno = context_.get_mc_seqno();

    context_.insert_account_state(emulation_success.account);
    context_.increase_tx_count(1);
    
    block::gen::Transaction::Record trans;
    if (!tlb::unpack_cell(emulation_success.transaction, trans)) {
        promise.set_error(td::Status::Error("Failed to unpack emulated Transaction"));
        return;
    }
    size_t pending = 0;
    if (trans.outmsg_cnt > 0 && !context_.is_limit_exceeded()) {
        vm::Dictionary dict{trans.r1.out_msgs, 15};
        for (int ind = 0; ind < trans.outmsg_cnt; ind++) {
            auto out_msg = dict.lookup_ref(td::BitArray<15>{ind});
            if (out_msg.is_null()) {
                promise.set_error(td::Status::Error("Failed to lookup out_msg in emulated transaction"));
                return;
            }

            int type;
            auto out_msg_address_r = fetch_msg_dest_address(out_msg, type);
            if (type == block::gen::CommonMsgInfo::ext_out_msg_info) {
                continue;
            }
            if (out_msg_address_r.is_error()) {
                promise.set_error(out_msg_address_r.move_as_error());
                return;
            }
            auto out_msg_address = out_msg_address_r.move_as_ok();

            if (!ton::shard_contains(block_id_.shard_full(), ton::extract_addr_prefix(out_msg_address.workchain, out_msg_address.addr))) {
                // in this shard block we emulate only intra-shard messages
                continue;
            }
            
            {
                std::lock_guard<std::mutex> lock(emulator_actors_mutex_);
                if (emulator_actors_.find(out_msg_address) == emulator_actors_.end()) {
                    emulator_actors_[out_msg_address] = td::actor::create_actor<TraceEmulatorImpl>("TraceEmulatorImpl", block_id_, context_, emulator_actors_, emulator_actors_mutex_);
                }
            }

            auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), child_ind = pending, trace_raw = result.get()](td::Result<std::unique_ptr<TraceNode>> R) {
                if (R.is_error()) {
                    td::actor::send_closure(SelfId, &TraceEmulatorImpl::child_error, trace_raw, R.move_as_error());
                    return;
                }
                td::actor::send_closure(SelfId, &TraceEmulatorImpl::child_emulated, trace_raw, R.move_as_ok(), child_ind);
            });
            td::actor::send_closure(emulator_actors_[out_msg_address].get(), &TraceEmulatorImpl::emulate, out_msg, out_msg_address, lt + 1, std::move(P));
            pending++;
        }
        result->children.resize(pending);
    }
    if (pending == 0) {
        promise.set_value(std::move(result));
        return;
    }
    TraceNode* raw_ptr = result.get();
    result_promises_[raw_ptr] = { std::move(result), std::move(promise) };
}

void TraceEmulatorImpl::child_emulated(TraceNode *parent_node_raw, std::unique_ptr<TraceNode> child, size_t ind) {
    auto it = result_promises_.find(parent_node_raw);
    if (it == result_promises_.end()) {
        // one of children returned error and parent was already finished
        return;
    }

    auto& parent_entry = it->second;
    parent_entry.first->children[ind] = std::move(child);

    for (const auto& child_ptr : parent_entry.first->children) {
        if (!child_ptr) {
            return;
        }
    }
    parent_entry.second.set_value(std::move(parent_entry.first));
    result_promises_.erase(it);
}

void TraceEmulatorImpl::child_error(TraceNode *parent_node_raw, td::Status error) {
    auto it = result_promises_.find(parent_node_raw);
    if (it != result_promises_.end()) {
        it->second.second.set_error(std::move(error));
        result_promises_.erase(it);
    }
}

void ShardBlockEmulator::start_up() {
    ton::LogicalTime lt = 0;
    for (auto& shard_state : context_.get_shard_states()) {
        if (shard_state.blkid == block_id_) {
            lt = shard_state.lt;
            break;
        }
    }

    size_t pending = 0;
    for (auto& msg: in_msgs_) {
        int type;
        auto out_msg_address_r = fetch_msg_dest_address(msg, type);
        if (type == block::gen::CommonMsgInfo::ext_out_msg_info) {
            error(td::Status::Error("ShardBlockEmulator received ext_out_msg as input"));
            return;
        }
        if (out_msg_address_r.is_error()) {
            promise_.set_error(out_msg_address_r.move_as_error());
            stop();
            return;
        }
        auto out_msg_address = out_msg_address_r.move_as_ok();
        if (!ton::shard_contains(block_id_.shard_full(), ton::extract_addr_prefix(out_msg_address.workchain, out_msg_address.addr))) {
            error(td::Status::Error("ShardBlockEmulator received msg that does not belong to its shard"));
            return;
        }
        if (emulator_actors_.find(out_msg_address) == emulator_actors_.end()) {
            emulator_actors_[out_msg_address] = td::actor::create_actor<TraceEmulatorImpl>("TraceEmulatorImpl", block_id_, context_, emulator_actors_, emulator_actors_mutex_);
        }

        auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), child_ind = pending](td::Result<std::unique_ptr<TraceNode>> R) {
            if (R.is_error()) {
                td::actor::send_closure(SelfId, &ShardBlockEmulator::error, R.move_as_error());
                return;
            }
            td::actor::send_closure(SelfId, &ShardBlockEmulator::child_emulated, R.move_as_ok(), child_ind);
        });
        td::actor::send_closure(emulator_actors_[out_msg_address].get(), &TraceEmulatorImpl::emulate, msg, out_msg_address, lt, std::move(P));
        pending++;
    }
    result_.resize(pending);
}


void ShardBlockEmulator::error(td::Status error) {
    promise_.set_error(std::move(error));
    stop();
}

void ShardBlockEmulator::child_emulated(std::unique_ptr<TraceNode> node, size_t child_ind) {
    CHECK(node != nullptr);

    result_[child_ind] = std::move(node);
    for (const auto& child_ptr : result_) {
        if (!child_ptr) {
            return;
        }
    }
    promise_.set_value(std::move(result_));
    stop();
}

void MasterchainBlockEmulator::start_up() {
    std::map<ton::BlockId, std::vector<td::Ref<vm::Cell>>> msgs_by_shards;
    for (const auto& shard_state : context_.get_shard_states()) {        
        std::vector<td::Ref<vm::Cell>> cur_shard_msgs;
        for (auto& msg : in_msgs_) {
            int type;
            auto msg_dest_r = fetch_msg_dest_address(msg, type);
            if (msg_dest_r.is_error()) {
                promise_.set_error(msg_dest_r.move_as_error_prefix("in msg found with no destination: "));
                stop();
                return;
            }
            auto msg_dest = msg_dest_r.move_as_ok();
            if (ton::shard_contains(shard_state.blkid.shard_full(), ton::extract_addr_prefix(msg_dest.workchain, msg_dest.addr))) {
                cur_shard_msgs.emplace_back(msg);
            }
        }
        if (cur_shard_msgs.empty()) {
            continue;
        }
        msgs_by_shards[shard_state.blkid] = std::move(cur_shard_msgs);
    }

    td::MultiPromise mp;
    auto ig = mp.init_guard();

    auto ret_promise = td::PromiseCreator::lambda(
        [SelfId = actor_id(this)](td::Result<td::Unit> R) mutable {
          if (R.is_error()) {
            td::actor::send_closure(SelfId, &MasterchainBlockEmulator::error, R.move_as_error());
          } else {
            td::actor::send_closure(SelfId, &MasterchainBlockEmulator::all_shards_emulated);
          }
        });
    ig.add_promise(std::move(ret_promise));

    for (auto& [shard_blkid, shard_msgs] : msgs_by_shards) {
        auto P = td::PromiseCreator::lambda(
                [SelfId = actor_id(this), shard = shard_blkid.shard, promise = ig.get_promise()]
                (td::Result<std::vector<std::unique_ptr<TraceNode>>> R) mutable {
            if (R.is_error()) {
                promise.set_error(R.move_as_error_prefix("failed to emulate shard block: "));
            } else {
                td::actor::send_closure(SelfId, &MasterchainBlockEmulator::shard_emulated, shard, R.move_as_ok(), std::move(promise));
            }
        });

        td::actor::create_actor<ShardBlockEmulator>("ShardBlockEmulator", shard_blkid, context_, shard_msgs, std::move(P)).release();
    }
}

void MasterchainBlockEmulator::shard_emulated(block::ShardId shard, std::vector<std::unique_ptr<TraceNode>> shard_traces, td::Promise<> promise) {
    result_.insert(result_.end(),
        std::make_move_iterator(shard_traces.begin()),
        std::make_move_iterator(shard_traces.end()));

    promise.set_result(td::Unit());
}

td::Status find_out_msgs_to_emulate_next(const std::unique_ptr<TraceNode>& node, std::vector<td::Ref<vm::Cell>>& result) {
    block::gen::Transaction::Record trans;
    if (!tlb::unpack_cell(node->transaction_root, trans)) {
        return td::Status::Error("Failed to unpack Transaction");
    }
    std::unordered_set<td::Bits256> emulated_msgs;
    for (const auto& child : node->children) {
        emulated_msgs.insert(child->node_id);
    }
    if (trans.outmsg_cnt != 0) {
        vm::Dictionary dict{trans.r1.out_msgs, 15};
        for (int x = 0; x < trans.outmsg_cnt; x++) {
            auto msg = dict.lookup_ref(td::BitArray<15>{x});
            int type;
            auto msg_dest = fetch_msg_dest_address(msg, type);
            if (type == block::gen::CommonMsgInfo::ext_out_msg_info) {
                continue;
            }
            auto msg_hash = td::Bits256(msg->get_hash().as_bitslice().bits());
            if (emulated_msgs.find(msg_hash) == emulated_msgs.end()) {
                result.push_back(msg);
            }
        }
    }

    for (const auto& child : node->children) {
        TRY_STATUS(find_out_msgs_to_emulate_next(child, result));
    }
    return td::Status::OK();
}

void MasterchainBlockEmulator::all_shards_emulated() {
    if (context_.is_limit_exceeded()) {
        promise_.set_result(std::move(result_));
        stop();
        return;
    }
    std::vector<td::Ref<vm::Cell>> to_emulate_in_next_mc_block;
    for (const auto& node : result_) {       
        auto r = find_out_msgs_to_emulate_next(node, to_emulate_in_next_mc_block);
        if (r.is_error()) {
            error(r.move_as_ok());
            return;
        }
    }
    if (to_emulate_in_next_mc_block.empty()) {
        promise_.set_result(std::move(result_));
        stop();
        return;
    }

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<std::vector<std::unique_ptr<TraceNode>>> R) {
        if (R.is_error()) {
            td::actor::send_closure(SelfId, &MasterchainBlockEmulator::error, R.move_as_error());
        } else {
            td::actor::send_closure(SelfId, &MasterchainBlockEmulator::next_mc_emulated, R.move_as_ok());
        }
    });

    context_.increase_seqno(3);

    td::actor::create_actor<MasterchainBlockEmulator>("MasterchainBlockEmulator", 
        context_, to_emulate_in_next_mc_block, std::move(P)).release();
}

void MasterchainBlockEmulator::error(td::Status error) {
    promise_.set_error(std::move(error));
    stop();
}

void MasterchainBlockEmulator::next_mc_emulated(std::vector<std::unique_ptr<TraceNode>> children) {
    // now we need to attach these children to correct nodes in result_
    std::unordered_map<td::Bits256, std::unique_ptr<TraceNode>> children_map;
    for (auto& node : children) {
        children_map[node->node_id] = std::move(node);
    }

    for (auto& node: result_) {
        std::queue<TraceNode*> q;
        q.push(node.get());
        while (!q.empty()) {
            TraceNode* current = q.front();
            q.pop();

            // Add existing children to the queue
            for (auto& child : current->children) {
                if (child) {
                    q.push(child.get());
                }
            }
            
            block::gen::Transaction::Record trans;
            if (!tlb::unpack_cell(current->transaction_root, trans)) {
                error(td::Status::Error("Failed to unpack Transaction"));
                return;
            }
            
            if (trans.outmsg_cnt != 0) {
                vm::Dictionary dict{trans.r1.out_msgs, 15};
                for (int x = 0; x < trans.outmsg_cnt; x++) {
                    auto msg = dict.lookup_ref(td::BitArray<15>{x});
                    auto msg_hash = td::Bits256(msg->get_hash().as_bitslice().bits());
                    
                    auto it = children_map.find(msg_hash);
                    if (it != children_map.end()) {
                        current->children.push_back(std::move(it->second));
                        children_map.erase(it);
                    }
                }
            }
        }
    }

    if (children_map.size() != 0) {
        LOG(ERROR) << "Children left: " << children_map.size();
    }

    promise_.set_result(std::move(result_));
    stop();
}

TraceEmulator::TraceEmulator(MasterchainBlockDataState mc_data_state, td::Ref<vm::Cell> in_msg, bool ignore_chksig, td::Promise<Trace> promise)
    : mc_data_state_(std::move(mc_data_state)), in_msg_(std::move(in_msg)), ignore_chksig_(ignore_chksig), promise_(std::move(promise)) {
}

void TraceEmulator::start_up() {
    timer_.resume();
   
    context_ = std::make_unique<EmulationContext>(mc_data_state_.shard_blocks_[0].handle->id().id.seqno, mc_data_state_.config_, ignore_chksig_);
    for (const auto& shard_state : mc_data_state_.shard_blocks_) {
        auto blkid = shard_state.handle->id().id;
        auto timestamp = shard_state.handle->unix_time();
        auto lt = shard_state.handle->logical_time();
        lt = lt - lt % block::ConfigInfo::get_lt_align();
        context_->add_shard_state(blkid, timestamp, lt, shard_state.block_state);
    }
    context_->increase_seqno(1);
    
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<std::vector<std::unique_ptr<TraceNode>>> R) {
        td::actor::send_closure(SelfId, &TraceEmulator::finish, std::move(R));
    });
    
    std::vector<td::Ref<vm::Cell>> in_msgs = {in_msg_};

    td::actor::create_actor<MasterchainBlockEmulator>("MasterchainBlockEmulator", *context_.get(), std::move(in_msgs), std::move(P)).release();
}

void TraceEmulator::finish(td::Result<std::vector<std::unique_ptr<TraceNode>>> root_r) {
    if (root_r.is_error()) {
        g_statistics.record_count(EMULATE_TRACE_ERROR);
        promise_.set_error(root_r.move_as_error());
        stop();
        return;
    }
    if (root_r.ok().size() != 1) {
        promise_.set_error(td::Status::Error("Expected one root node"));
        stop();
        return;
    }

    Trace result;
    auto ext_in_msg_hash_norm = ext_in_msg_get_normalized_hash(in_msg_);
    if (ext_in_msg_hash_norm.is_ok()) {
        // in some cases we in_msg might not be the ext-in msg - if user wants to emulate trace starting from some internal message
        result.ext_in_msg_hash_norm = ext_in_msg_hash_norm.move_as_ok();
    }
    result.ext_in_msg_hash = in_msg_->get_hash().bits();
    result.root = std::move(root_r.move_as_ok()[0]);
    result.root_tx_hash = result.root->transaction_root->get_hash().bits();
    result.rand_seed = context_->get_rand_seed();
    result.emulated_accounts = context_->release_account_states();
    result.tx_limit_exceeded = context_->is_limit_exceeded();
    promise_.set_result(std::move(result));
    g_statistics.record_time(EMULATE_TRACE, timer_.elapsed() * 1e3);
    stop();
}