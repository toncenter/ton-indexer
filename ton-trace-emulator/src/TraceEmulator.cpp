#include <fstream>
#include <sstream>
#include <string>
#include <emulator/transaction-emulator.h>
#include "TraceEmulator.h"


void TraceEmulator::set_error(td::Status error) {
    if (result_) {
        delete result_;
    }
    promise_.set_error(std::move(error));
    stop();
}

void TraceEmulator::start_up() {
    auto message_cs = vm::load_cell_slice(in_msg_);
    int msg_tag = block::gen::t_CommonMsgInfo.get_tag(message_cs);
    if (msg_tag == block::gen::CommonMsgInfo::ext_in_msg_info) {
        block::gen::CommonMsgInfo::Record_ext_in_msg_info info;
        if (!(tlb::unpack(message_cs, info) && block::tlb::t_MsgAddressInt.extract_std_address(info.dest, account_addr_))) {
            set_error(td::Status::Error(PSLICE() << "Can't unpack external message " << in_msg_->get_hash()));
            return;
        }
        is_external_ = true;
    } else if (msg_tag == block::gen::CommonMsgInfo::int_msg_info) {
        block::gen::CommonMsgInfo::Record_int_msg_info info;
        if (!(tlb::unpack(message_cs, info) && block::tlb::t_MsgAddressInt.extract_std_address(info.dest, account_addr_))) {
            set_error(td::Status::Error(PSLICE() << "Can't unpack internal message " << in_msg_->get_hash()));
            return;
        }
        is_external_ = false;
    } else {
        LOG(ERROR) << "Ext out message found " << in_msg_->get_hash();
        promise_.set_error(td::Status::Error("Ext out message found"));
        return;
    }

    auto account_it = emulated_accounts_.find(account_addr_);
    if (account_it != emulated_accounts_.end()) {
        auto account = std::make_unique<block::Account>(*account_it->second);
        emulate_transaction(std::move(account));
        return;
    }

    for (const auto &shard_state : shard_states_) {
        block::gen::ShardStateUnsplit::Record sstate;
        if (!tlb::unpack_cell(shard_state, sstate)) {
            set_error(td::Status::Error("Failed to unpack ShardStateUnsplit"));
            stop();
            return;
        }
        block::ShardId shard;
        if (!(shard.deserialize(sstate.shard_id.write()))) {
            set_error(td::Status::Error("Failed to deserialize ShardId"));
            return;
        }
        if (shard.workchain_id == account_addr_.workchain && ton::shard_contains(ton::ShardIdFull(shard).shard, account_addr_.addr)) {
            vm::AugmentedDictionary accounts_dict{vm::load_cell_slice_ref(sstate.accounts), 256, block::tlb::aug_ShardAccounts};
            auto account = unpack_account(accounts_dict);
            if (!account) {
                set_error(td::Status::Error("Failed to unpack account"));
                return;
            }

            emulate_transaction(std::move(account));
            return;
        }
    }
    set_error(td::Status::Error("Account not found in shard_states"));
}

std::unique_ptr<block::Account> TraceEmulator::unpack_account(vm::AugmentedDictionary& accounts_dict) {
    auto ptr = std::make_unique<block::Account>(account_addr_.workchain, account_addr_.addr.cbits());
    auto account = accounts_dict.lookup(account_addr_.addr);
    if (account.is_null()) {
        if (!ptr->init_new(emulator_->get_unixtime())) {
            return nullptr;
        }
    } else if (!ptr->unpack(std::move(account), emulator_->get_unixtime(), 
                            account_addr_.workchain == ton::masterchainId && emulator_->get_config().is_special_smartcontract(account_addr_.addr))) {
        return nullptr;
    }
    ptr->block_lt = ptr->last_trans_lt_ - ptr->last_trans_lt_ % block::ConfigInfo::get_lt_align(); // TODO: check if it's correct
    return ptr;
}

void TraceEmulator::emulate_transaction(std::unique_ptr<block::Account> account) {
    auto emulation_r = emulator_->emulate_transaction(std::move(*account), in_msg_, 0, 0, block::transaction::Transaction::tr_ord);
    if (emulation_r.is_error()) {
        set_error(emulation_r.move_as_error());
        return;
    }
    auto emulation = emulation_r.move_as_ok();

    auto external_not_accepted = dynamic_cast<emulator::TransactionEmulator::EmulationExternalNotAccepted *>(emulation.get());
    if (external_not_accepted) {
        set_error(td::Status::Error(PSLICE() << "EmulationExternalNotAccepted: " << external_not_accepted->vm_exit_code));
        return;
    }

    auto emulation_success = dynamic_cast<emulator::TransactionEmulator::EmulationSuccess&>(*emulation);
    
    result_ = new Trace();
    result_->node_id = in_msg_->get_hash().bits();
    if (is_external_) {
        result_->id = result_->node_id;
    }
    result_->emulated = true;
    result_->workchain = account->workchain;
    result_->transaction_root = emulation_success.transaction;
    result_->account = std::make_unique<block::Account>(emulation_success.account);

    // LOG(INFO) << emulation_success.vm_log;
    
    block::gen::Transaction::Record trans;
    if (!tlb::unpack_cell(emulation_success.transaction, trans)) {
        set_error(td::Status::Error("Failed to unpack emulated Transaction"));
        return;
    }
    
    if (depth_ > 0 && trans.outmsg_cnt > 0) {
        auto next_emulated_accounts = emulated_accounts_;
        next_emulated_accounts[account_addr_] = std::make_shared<block::Account>(std::move(emulation_success.account));
        vm::Dictionary dict{trans.r1.out_msgs, 15};
        for (int ind = 0; ind < trans.outmsg_cnt; ind++) {
            auto out_msg = dict.lookup_ref(td::BitArray<15>{ind});
            if (out_msg.is_null()) {
                set_error(td::Status::Error("Failed to lookup out_msg in emulated transaction"));
                return;
            }

            auto message_cs = vm::load_cell_slice(out_msg);
            if (block::gen::t_CommonMsgInfo.get_tag(message_cs) == block::gen::CommonMsgInfo::ext_out_msg_info) {
                continue;
            }
            
            auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), child_ind = pending_](td::Result<Trace *> R) {
                if (R.is_error()) {
                    td::actor::send_closure(SelfId, &TraceEmulator::set_error, R.move_as_error());
                    return;
                }
                td::actor::send_closure(SelfId, &TraceEmulator::trace_emulated, R.move_as_ok(), child_ind);
            });
            td::actor::create_actor<TraceEmulator>("TraceEmulator", emulator_, shard_states_, std::move(next_emulated_accounts), std::move(out_msg), depth_ - 1, std::move(P)).release();

            pending_++;
        }
        result_->children.resize(pending_);
    } 
    if (pending_ == 0) {
        promise_.set_value(std::move(result_));
        stop();
    }
}

void TraceEmulator::trace_emulated(Trace *trace, size_t ind) {
    result_->children[ind] = trace;
    if (--pending_ == 0) {
        promise_.set_value(std::move(result_));
        stop();
    }
}


