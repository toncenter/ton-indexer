#include "DataParser.h"
#include "crypto/block/block.h"
#include "vm/cells/PrunnedCell.h"

namespace {

td::Ref<vm::Cell> state_metrics(int wc, bool extra_present, std::optional<std::uint64_t> count,
                              bool nonempty, const std::string& amount = "100000000000000000001") {
    auto empty = vm::CellBuilder().finalize();
    // These referenced dictionary roots cannot be loaded. Parsing metadata must
    // succeed without enumerating queues, accounts, libraries or MC config.
    auto unreadable = vm::PrunnedCell<td::Unit>::create(empty, td::Unit{}).move_as_ok();
    vm::CellBuilder queue;
    queue.store_long(nonempty, 1);
    if (nonempty) queue.store_ref(unreadable);
    queue.store_long(0, 64).store_long(0, 1);  // queue augmentation; ProcessedInfo
    queue.store_long(extra_present, 1);
    if (extra_present) {
        queue.store_long(0, 4).store_long(nonempty, 1);
        if (nonempty) queue.store_ref(unreadable);
        queue.store_long(0, 64);  // old DispatchQueue augmentation (min_created_lt)
        queue.store_long(count.has_value(), 1);
        if (count) queue.store_long(*count, 48);
    }
    auto aux = vm::CellBuilder().store_zeroes(128 + 5 + 5 + 1 + 1).finalize();
    vm::CellBuilder root;
    root.store_long(0x9023afe2, 32).store_long(-3, 32)
        .store_long(0, 8).store_long(wc, 32).store_zeroes(64)
        .store_long(123, 32).store_long(0, 32).store_long(1700000000, 32).store_zeroes(64 + 32)
        .store_ref(queue.finalize()).store_long(0, 1).store_ref(unreadable).store_ref(aux)
        .store_long(wc == -1, 1);
    if (wc == -1) {
        auto mc_aux = vm::CellBuilder().store_zeroes(16 + 65 + 66 + 1 + 1).finalize();
        vm::CellBuilder mc;
        mc.store_long(0xcc26, 16).store_long(0, 1).store_zeroes(256).store_ref(unreadable).store_ref(mc_aux);
        CHECK(block::CurrencyCollection(td::dec_string_to_int256(amount)).store(mc));
        root.store_ref(mc.finalize());
    }
    return root.finalize();
}

}  // namespace

void check_stored_block_metrics() {
    schema::Block block{};
    block.seqno = 123;
    block.gen_utime = 1700000000;
    for (int wc : {-1, 0}) {
        block.workchain = wc;
        for (bool nonempty : {false, true}) {
            for (bool extra : {false, true}) {
                ParseQuery::parse_block_state_metrics(state_metrics(wc, extra, {}, nonempty), block).ensure();
                CHECK(!block.out_msg_queue_size);
                CHECK(wc == -1 ? block.global_balance->to_dec_string() == "100000000000000000001"
                               : block.global_balance.is_null());
            }
            for (auto count : {0ULL, 1234567ULL, (1ULL << 48) - 1}) {
                // Split/merge flags do not change the direct post-state read.
                block.after_split = true;
                block.after_merge = true;
                ParseQuery::parse_block_state_metrics(state_metrics(wc, true, count, nonempty), block).ensure();
                CHECK(block.out_msg_queue_size && *block.out_msg_queue_size == count);
            }
        }
    }
    block.workchain = -1;
    ParseQuery::parse_block_state_metrics(state_metrics(-1, true, 0, false, "0"), block).ensure();
    CHECK(block.global_balance.not_null() && block.global_balance->sgn() == 0);
    CHECK(ParseQuery::parse_block_state_metrics({}, block).is_error());
    CHECK(ParseQuery::parse_block_state_metrics(vm::CellBuilder().finalize(), block).is_error());
    CHECK(ParseQuery::parse_block_state_metrics(state_metrics(0, false, {}, false), block).is_error());
    ++block.seqno;
    CHECK(ParseQuery::parse_block_state_metrics(state_metrics(-1, true, 1, false), block).is_error());
}
