#include "FetchAccountFromShard.h"

#include "DataParser.h"


void FetchAccountFromShardV2::start_up()
{
    for (auto& root : shard_states_)
    {
        block::gen::ShardStateUnsplit::Record sstate;
        if (!tlb::unpack_cell(root, sstate))
        {
            promise_.set_error(td::Status::Error("Failed to unpack ShardStateUnsplit"));
            stop();
            return;
        }
        if (!ton::shard_contains(ton::ShardIdFull(block::ShardId(sstate.shard_id)),
                                 ton::extract_addr_prefix(address_.workchain, address_.addr)))
        {
            continue;
        }

        vm::AugmentedDictionary accounts_dict{
            vm::load_cell_slice_ref(sstate.accounts), 256, block::tlb::aug_ShardAccounts
        };

        auto shard_account_csr = accounts_dict.lookup(address_.addr);
        if (shard_account_csr.is_null())
        {
            promise_.set_error(td::Status::Error("Account not found in accounts_dict"));
            stop();
            return;
        }

        block::gen::ShardAccount::Record acc_info;
        if (!tlb::csr_unpack(std::move(shard_account_csr), acc_info))
        {
            LOG(ERROR) << "Failed to unpack ShardAccount " << address_.addr;
            stop();
            return;
        }
        int account_tag = block::gen::t_Account.get_tag(vm::load_cell_slice(acc_info.account));
        switch (account_tag)
        {
        case block::gen::Account::account_none:
            promise_.set_error(td::Status::Error("Account is empty"));
            stop();
            return;
        case block::gen::Account::account:
            {
                auto account_r = ParseQuery::parse_account(acc_info.account, sstate.gen_utime, acc_info.last_trans_hash,
                                                           acc_info.last_trans_lt);
                if (account_r.is_error())
                {
                    promise_.set_error(account_r.move_as_error());
                    stop();
                    return;
                }
                promise_.set_value(account_r.move_as_ok());
                stop();
                return;
            }
        default:
            promise_.set_error(td::Status::Error("Unknown account tag"));
            stop();
            return;
        }
    }
    promise_.set_error(td::Status::Error("Account not found in shards"));
    stop();
}
