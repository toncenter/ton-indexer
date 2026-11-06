#include "BlockInterfacesDetector.h"
#include "Statistics.h"
#include "smc-interfaces/AccountLookupContext.h"
#include "../../tondb-scanner/test/smc-fixture.h"
#include "td/actor/TestScheduler.h"
#include "vm/cp0.h"

namespace {

struct Detection {
    std::vector<FullDetector::DetectedInterface> interfaces;
    TokenDetectionStatus status;
    std::string stats;
};

Detection detect(td::Ref<vm::Cell> code, bool filter) {
    Detection result;
    g_statistics.generate_report_and_reset();
    auto status = std::make_shared<TokenDetectionStatus>();
    bool finished = false;
    td::actor::TestScheduler scheduler;
    scheduler.run([&]() -> td::actor::Task<> {
        td::actor::create_actor<FullDetector>("filter_test", block::StdAddress(0, td::Bits256::zero()),
            code, vm::CellBuilder().finalize(), AllShardStates{}, test_smc_config(1700000000),
            td::PromiseCreator::lambda([&](td::Result<std::vector<FullDetector::DetectedInterface>> value) {
                result.interfaces = value.move_as_ok();
                result.status = *status;
                finished = true;
            }), status, DetectionOptions{filter, nullptr}).release();
        while (!finished) co_await scheduler.wait_sync_work();
        co_return td::Unit{};
    });
    result.stats = g_statistics.generate_report_and_reset();
    return result;
}

void count_is(const std::string& report, const std::string& name, unsigned count) {
    CHECK(report.find(name + " COUNT : " + std::to_string(count) + "\n") != std::string::npos);
}

void same(const Detection& a, const Detection& b) {
    CHECK(a.status.jetton_wallet == b.status.jetton_wallet && a.status.nft_item == b.status.nft_item);
    CHECK(a.interfaces.size() == b.interfaces.size());
    // These fixtures either detect nothing or one NFT, including the custom
    // selector whose get_nft_data is not in its dictionary.
    if (!a.interfaces.empty()) {
        const auto& x = std::get<NftItemDetectorR::Result>(a.interfaces.at(0));
        const auto& y = std::get<NftItemDetectorR::Result>(b.interfaces.at(0));
        CHECK(x.address == y.address && x.init == y.init && td::cmp(x.index, y.index) == 0);
        CHECK(x.collection_address == y.collection_address && x.owner_address == y.owner_address && x.content == y.content);
    }
}

void check_filter() {
    auto throw11 = vm::CellBuilder().store_long(0xf20b, 16).finalize();
    for (auto [method, expected_skips] : {std::pair{42U, 10U}, {97026U, 9U}, {72748U, 8U}, {123140U, 9U}}) {
        auto code = test_method_dispatcher(method, throw11);
        auto baseline = detect(code, false);
        auto filtered = detect(code, true);
        same(baseline, filtered);
        count_is(baseline.stats, "indexer.detector.started", 13);
        count_is(filtered.stats, "indexer.detector.started", 13 - expected_skips);
        count_is(filtered.stats, "indexer.detector.skipped", expected_skips);
        // A declared getter throwing 11 is unknown, not an absent interface.
        if (method == 97026) CHECK(!filtered.status.jetton_wallet.has_value());
    }
    auto nft = test_nft_code(false, {}, {});
    auto baseline = detect(nft, false);
    auto filtered = detect(nft, true);
    same(baseline, filtered);
    CHECK(filtered.interfaces.size() == 1 && filtered.status.nft_item == true);
    count_is(filtered.stats, "indexer.detector.skipped", 9);

    // A successfully parsed dictionary followed by a custom missing-key path:
    // it implements get_nft_data OUTSIDE the dictionary. Filtering would lose it.
    vm::CellSlice header(vm::NoVm{}, nft);
    header.advance(40);
    vm::Dictionary nft_methods(header.fetch_ref(), 19);
    auto nft_body = nft_methods.lookup(td::BitArray<19>(102351));
    vm::Dictionary dictionary(19);
    CHECK(dictionary.set(td::BitArray<19>(42), vm::load_cell_slice_ref(throw11)));
    auto custom = vm::CellBuilder().store_long(0xff00f4a413LL, 40).store_ref(dictionary.get_root_cell())
        .store_long(0xf4bc30, 24).append_cellslice(nft_body).finalize();
    CHECK(parse_contract_methods(custom).is_ok());
    CHECK(!get_contract_method_info(custom).move_as_ok()->exhaustive);
    baseline = detect(custom, false);
    filtered = detect(custom, true);
    same(baseline, filtered);
    CHECK(filtered.interfaces.size() == 1 && filtered.status.nft_item == true);
    count_is(filtered.stats, "indexer.detector.started", 13);
    count_is(filtered.stats, "indexer.detector.filter_fallback", 1);

    same(detect(throw11, false), detect(throw11, true));
    count_is(detect(throw11, true).stats, "indexer.detector.started", 13);
}

td::Ref<vm::Cell> wallet_code(const block::StdAddress& owner, const block::StdAddress& master) {
    auto result = vm::CellBuilder().store_long(0x802a, 16)  // 42 jettons
        .store_long(0x89, 8).store_ref(test_address_cell(owner))
        .store_long(0x89, 8).store_ref(test_address_cell(master))
        .store_long(0x88, 8).store_ref(vm::CellBuilder().finalize()).finalize();
    return test_method_dispatcher(97026, result);
}

void check_shared_block_lookup() {
    auto parsed = std::make_shared<ParsedBlock>();
    parsed->mc_block_.config_ = test_smc_config(1700000000);
    const auto master = block::StdAddress(0, td::Bits256::ones());
    // The synthetic master derives wallet = owner; two distinct wallets can
    // therefore verify against the same master with different owner arguments.
    auto master_code = test_method_dispatcher(103289, vm::CellBuilder().store_long(0, 8).finalize());
    schema::BlockDataState shard{};
    shard.block_state = test_shard_with_account(master, master_code);
    parsed->mc_block_.shard_blocks_.push_back(shard);
    for (int i = 1; i <= 2; ++i) {
        auto key = td::Bits256::zero();
        (key.bits() + 224).store_uint(i, 32);
        schema::AccountState account{};
        account.account = block::StdAddress(0, key);
        account.last_trans_lt = 100 + i;
        account.timestamp = 1700000000;
        account.code = wallet_code(account.account, master);
        account.data = vm::CellBuilder().finalize();
        account.code_hash = account.code->get_hash().bits();
        account.data_hash = account.data->get_hash().bits();
        parsed->account_states_.push_back(account);
    }
    g_statistics.generate_report_and_reset();
    bool finished = false;
    td::actor::TestScheduler scheduler;
    scheduler.run([&]() -> td::actor::Task<> {
        td::actor::create_actor<BlockInterfaceProcessor>("shared_master", parsed,
            td::PromiseCreator::lambda([&](td::Result<ParsedBlockPtr> result) {
                auto block = result.move_as_ok();
                CHECK(block->get_accounts_v2<schema::JettonWalletDataV2>().size() == 2);
                for (const auto& account : block->account_states_) {
                    CHECK(block->token_detection_.at(account.account).jetton_wallet == true);
                    const auto& value = std::get<schema::JettonWalletDataV2>(block->account_interfaces_.at(account.account).at(0));
                    CHECK(value.balance->to_long() == 42 && value.owner == account.account && value.jetton == master);
                }
                finished = true;
            })).release();
        while (!finished) co_await scheduler.wait_sync_work();
        co_return td::Unit{};
    });
    auto report = g_statistics.generate_report_and_reset();
    count_is(report, "indexer.lookup_account.cache.miss", 1);
    count_is(report, "indexer.lookup_account.cache.hit", 1);
    count_is(report, "indexer.lookup_account.error", 0);
}

}  // namespace

void check_detector_caches() {
    check_filter();
    check_shared_block_lookup();
}
