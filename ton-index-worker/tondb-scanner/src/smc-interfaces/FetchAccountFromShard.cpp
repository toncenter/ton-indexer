#include "FetchAccountFromShard.h"

#include "DataParser.h"


td::Result<schema::AccountState> lookup_account(const AllShardStates& shard_states,
                                                const block::StdAddress& address)
{
    auto addr_prefix = ton::extract_addr_prefix(address.workchain, address.addr);
    for (const auto& root : shard_states)
    {
        block::gen::ShardStateUnsplit::Record sstate;
        if (!tlb::unpack_cell(root, sstate))
        {
            return td::Status::Error("Failed to unpack ShardStateUnsplit");
        }
        if (!ton::shard_contains(ton::ShardIdFull(block::ShardId(sstate.shard_id)), addr_prefix))
        {
            continue;
        }

        vm::AugmentedDictionary accounts_dict{
            vm::load_cell_slice_ref(sstate.accounts), 256, block::tlb::aug_ShardAccounts
        };

        auto shard_account_csr = accounts_dict.lookup(address.addr);
        if (shard_account_csr.is_null())
        {
            return td::Status::Error("Account not found in accounts_dict");
        }

        block::gen::ShardAccount::Record acc_info;
        if (!tlb::csr_unpack(std::move(shard_account_csr), acc_info))
        {
            // Return a named lookup error for an unknown shard.
            return td::Status::Error("Failed to unpack ShardAccount " + address.addr.to_hex());
        }
        int account_tag = block::gen::t_Account.get_tag(vm::load_cell_slice(acc_info.account));
        switch (account_tag)
        {
        case block::gen::Account::account_none:
            return td::Status::Error("Account is empty");
        case block::gen::Account::account:
            return ParseQuery::parse_account(acc_info.account, sstate.gen_utime, acc_info.last_trans_hash,
                                             acc_info.last_trans_lt);
        default:
            return td::Status::Error("Unknown account tag");
        }
    }
    return td::Status::Error("Account not found in shards");
}

void FetchAccountFromShardV2::start_up()
{
    promise_.set_result(lookup_account(shard_states_, address_));
    stop();
}
