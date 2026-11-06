#include "FetchAccountFromShard.h"

#include "DataParser.h"
#include "Statistics.h"
#include "safe-parse.h"

namespace {

td::Result<schema::AccountState> lookup_impl(const AllShardStates& shard_states,
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

td::Result<schema::AccountState> lookup_uncached(const AllShardStates& roots, const block::StdAddress& address) {
    const auto start = std::chrono::steady_clock::now();
    auto result = safe_parse::guard("Shard account lookup", [&] { return lookup_impl(roots, address); });
    g_statistics.record_time(LOOKUP_ACCOUNT,
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start).count());
    if (result.is_error()) g_statistics.record_count(LOOKUP_ACCOUNT_ERROR);
    return result;
}

size_t account_bytes(const schema::AccountState& state) {
    constexpr size_t integer_bytes = sizeof(td::BigInt256) + 64;
    return state.account_status.capacity() + 1 + integer_bytes + state.balance.extra_currencies.size() *
        (sizeof(std::pair<const uint32_t, td::RefInt256>) + 4 * sizeof(void*) + integer_bytes);
}

}  // namespace

struct AccountLookupContext::Impl {
    const AllShardStates roots;
    // Header/decoded-value budget plus an entry cap. Lazy code/data references
    // live only as long as this MC's already-retained shard-state snapshot.
    memo::Cache<block::StdAddress, schema::AccountState> cache{4 * 1024 * 1024, 1024};
    explicit Impl(AllShardStates value) : roots(std::move(value)) {}
};

AccountLookupContext::AccountLookupContext(AllShardStates roots) : impl_(std::make_unique<Impl>(std::move(roots))) {}
AccountLookupContext::~AccountLookupContext() {
    g_statistics.record_count(LOOKUP_ACCOUNT_EVICTION, impl_->cache.stats().evictions);
}

bool AccountLookupContext::matches(const AllShardStates& roots) const {
    if (roots.size() != impl_->roots.size()) return false;
    for (size_t i = 0; i < roots.size(); ++i) {
        if (roots[i].get() == impl_->roots[i].get()) continue;
        if (roots[i].is_null() || impl_->roots[i].is_null() || roots[i]->get_hash() != impl_->roots[i]->get_hash()) {
            return false;
        }
    }
    return true;
}

td::Result<schema::AccountState> AccountLookupContext::lookup(const block::StdAddress& address) {
    memo::Access access;
    auto result = impl_->cache.get(address, [&] { return lookup_uncached(impl_->roots, address); }, account_bytes, &access);
    g_statistics.record_count(access == memo::Access::hit ? LOOKUP_ACCOUNT_HIT :
        access == memo::Access::miss ? LOOKUP_ACCOUNT_MISS : LOOKUP_ACCOUNT_COALESCED);
    TRY_RESULT(value, std::move(result));
    return *value;
}

memo::Stats AccountLookupContext::stats() const { return impl_->cache.stats(); }

td::Result<schema::AccountState> lookup_account(const AllShardStates& roots, const block::StdAddress& address,
                                               AccountLookupContext* context) {
    if (context && context->matches(roots)) return context->lookup(address);
    // Even an accidentally supplied context from another MC cannot leak its
    // account state into this lookup. Existing direct callers also use this path.
    g_statistics.record_count(LOOKUP_ACCOUNT_BYPASS);
    return lookup_uncached(roots, address);
}

void FetchAccountFromShardV2::start_up()
{
    promise_.set_result(lookup_account(shard_states_, address_));
    stop();
}
