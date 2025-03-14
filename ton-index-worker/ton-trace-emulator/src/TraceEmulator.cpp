#include <fstream>
#include <sstream>
#include <string>
#include <emulator/transaction-emulator.h>
#include "crypto/openssl/rand.hpp"
#include "TraceEmulator.h"

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

void TraceEmulatorImpl::emulate(td::Ref<vm::Cell> in_msg, block::StdAddress address, size_t depth, td::Promise<std::unique_ptr<TraceNode>> promise) {
    {
        std::unique_lock<std::mutex> lock(emulated_accounts_mutex_);
        auto range = emulated_accounts_.equal_range(address);
        if (range.first != range.second) {
            auto it = std::prev(range.second);
            auto account = it->second;
            lock.unlock();
            emulate_transaction(std::move(account), address, in_msg, depth, std::move(promise));
            return;
        }
    }
    for (const auto &shard_state : shard_states_) {
        block::gen::ShardStateUnsplit::Record sstate;
        if (!tlb::unpack_cell(shard_state, sstate)) {
            promise.set_error(td::Status::Error("Failed to unpack ShardStateUnsplit"));
            return;
        }
        block::ShardId shard;
        if (!(shard.deserialize(sstate.shard_id.write()))) {
            promise.set_error(td::Status::Error("Failed to deserialize ShardId"));
            return;
        }
        if (shard.workchain_id == address.workchain && ton::shard_contains(ton::ShardIdFull(shard).shard, address.addr)) {
            vm::AugmentedDictionary accounts_dict{vm::load_cell_slice_ref(sstate.accounts), 256, block::tlb::aug_ShardAccounts};
            auto account = unpack_account(accounts_dict, address, emulator_->get_unixtime());
            if (account.is_error()) {
                promise.set_error(account.move_as_error());
                return;
            }
            {
                auto account_state = account.ok();
                std::lock_guard<std::mutex> lock(emulated_accounts_mutex_);
                emulated_accounts_.insert({address, std::move(account_state)});
            }
            emulate_transaction(account.move_as_ok(), address, in_msg, depth, std::move(promise));
            return;
        }
    }
    promise.set_error(td::Status::Error("Account not found in shard_states"));
}

td::Result<block::Account> TraceEmulatorImpl::unpack_account(vm::AugmentedDictionary& accounts_dict, const block::StdAddress& account_addr, uint32_t utime) {
    auto res = block::Account(account_addr.workchain, account_addr.addr.cbits());
    auto account = accounts_dict.lookup(account_addr.addr);
    if (account.is_null()) {
        if (!res.init_new(utime)) {
            return td::Status::Error("Failed to init new account");
        }
    } else if (!res.unpack(std::move(account), utime, 
                            account_addr.workchain == ton::masterchainId && emulator_->get_config().is_special_smartcontract(account_addr.addr))) {
        return td::Status::Error("Failed to unpack account");
    }
    res.block_lt = res.last_trans_lt_ - res.last_trans_lt_ % block::ConfigInfo::get_lt_align(); // TODO: check if it's correct
    return res;
}

void TraceEmulatorImpl::emulate_transaction(block::Account account, block::StdAddress address,
                td::Ref<vm::Cell> in_msg, size_t depth, td::Promise<std::unique_ptr<TraceNode>> promise) {
    auto emulation_r = emulator_->emulate_transaction(std::move(account), in_msg, 0, 0, block::transaction::Transaction::tr_ord);
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
    // LOG(INFO) << emulation_success.vm_log;

    {
        std::lock_guard<std::mutex> lock(emulated_accounts_mutex_);
        emulated_accounts_.insert({address, std::move(emulation_success.account)});
    }
    
    block::gen::Transaction::Record trans;
    if (!tlb::unpack_cell(emulation_success.transaction, trans)) {
        promise.set_error(td::Status::Error("Failed to unpack emulated Transaction"));
        return;
    }
    size_t pending = 0;
    if (depth > 0 && trans.outmsg_cnt > 0) {
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
            
            {
                std::lock_guard<std::mutex> lock(emulated_accounts_mutex_);
                if (emulator_actors_.find(out_msg_address) == emulator_actors_.end()) {
                    emulator_actors_[out_msg_address] = td::actor::create_actor<TraceEmulatorImpl>("TraceEmulatorImpl", emulator_, shard_states_, emulated_accounts_, emulated_accounts_mutex_, emulator_actors_);
                }
            }

            auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), child_ind = pending, trace_raw = result.get()](td::Result<std::unique_ptr<TraceNode>> R) {
                if (R.is_error()) {
                    td::actor::send_closure(SelfId, &TraceEmulatorImpl::child_error, trace_raw, R.move_as_error());
                    return;
                }
                td::actor::send_closure(SelfId, &TraceEmulatorImpl::child_emulated, trace_raw, R.move_as_ok(), child_ind);
            });
            td::actor::send_closure(emulator_actors_[out_msg_address].get(), &TraceEmulatorImpl::emulate, out_msg, out_msg_address, depth - 1, std::move(P));
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

TraceEmulator::TraceEmulator(MasterchainBlockDataState mc_data_state, td::Ref<vm::Cell> in_msg, bool ignore_chksig, td::Promise<Trace> promise)
    : mc_data_state_(std::move(mc_data_state)), in_msg_(std::move(in_msg)), ignore_chksig_(ignore_chksig), promise_(std::move(promise)) {
    prng::rand_gen().strong_rand_bytes(rand_seed_.data(), 32);
}

void TraceEmulator::start_up() {
    std::vector<td::Ref<vm::Cell>> shard_states;
    for (const auto& shard_state : mc_data_state_.shard_blocks_) {
        shard_states.push_back(shard_state.block_state);
    }
    
    emulator_ = std::make_shared<emulator::TransactionEmulator>(mc_data_state_.config_, 0);
    auto libraries_root = mc_data_state_.config_->get_libraries_root();
    emulator_->set_libs(vm::Dictionary(libraries_root, 256));
    emulator_->set_ignore_chksig(ignore_chksig_);
    emulator_->set_unixtime(td::Timestamp::now().at_unix());

    int type;
    TRY_RESULT_PROMISE(promise_, account_addr, fetch_msg_dest_address(in_msg_, type));
    emulator_actors_[account_addr] = td::actor::create_actor<TraceEmulatorImpl>("TraceEmulatorImpl", emulator_, shard_states, emulated_accounts_, emulated_accounts_mutex_, emulator_actors_);

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<std::unique_ptr<TraceNode>> R) {
        td::actor::send_closure(SelfId, &TraceEmulator::finish, std::move(R));
    });

    td::actor::send_closure(emulator_actors_[account_addr].get(), &TraceEmulatorImpl::emulate, in_msg_, account_addr, 20, std::move(P));
}

void TraceEmulator::finish(td::Result<std::unique_ptr<TraceNode>> root) {
    if (root.is_error()) {
        promise_.set_error(root.move_as_error());
        stop();
        return;
    }
    Trace result;
    auto ext_in_msg_norm_hash = ext_in_msg_get_normalized_hash(in_msg_);
    if (ext_in_msg_norm_hash.is_ok()) {
        result.id = ext_in_msg_norm_hash.move_as_ok();
    } else {
        result.id = in_msg_->get_hash().bits();
    }
    result.root = root.move_as_ok();
    result.rand_seed = rand_seed_;
    result.emulated_accounts = std::move(emulated_accounts_);
    promise_.set_result(std::move(result));
    stop();
}