#include "DataParser.h"
#include "crypto/block/block.h"
#include "vm/cells/PrunnedCell.h"

namespace {

td::Ref<vm::Cell> state_metrics(int wc, bool extra_present, std::optional<std::uint64_t> count,
                              bool nonempty, const std::string& amount = "100000000000000000001",
                              bool unreadable_aux = false) {
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
    td::Ref<vm::Cell> aux = vm::CellBuilder().store_zeroes(128 + 5 + 5 + 1 + 1).finalize();
    if (unreadable_aux) aux = unreadable;
    vm::CellBuilder root;
    root.store_long(0x9023afe2, 32).store_long(-3, 32)
        .store_long(0, 8).store_long(wc, 32).store_zeroes(64)
        .store_long(123, 32).store_long(0, 32).store_long(1700000000, 32).store_zeroes(64 + 32)
        .store_ref(queue.finalize()).store_long(0, 1).store_ref(unreadable).store_ref(aux)
        .store_long(wc == -1, 1);
    if (wc == -1) {
        td::Ref<vm::Cell> mc_aux = vm::CellBuilder().store_zeroes(16 + 65 + 66 + 1 + 1).finalize();
        if (unreadable_aux) mc_aux = unreadable;
        vm::CellBuilder mc;
        mc.store_long(0xcc26, 16).store_long(nonempty, 1);
        if (nonempty) mc.store_ref(unreadable);  // shard hashes are not traversed
        mc.store_zeroes(256).store_ref(unreadable).store_ref(mc_aux);
        block::CurrencyCollection balance(td::dec_string_to_int256(amount));
        if (nonempty) balance.extra = unreadable;  // only the native grams value is read
        CHECK(balance.store(mc));
        root.store_ref(mc.finalize());
    }
    return root.finalize();
}

td::Ref<vm::Cell> with_ref(td::Ref<vm::Cell> root, unsigned index, td::Ref<vm::Cell> replacement) {
    auto cs = vm::load_cell_slice(std::move(root));
    vm::CellBuilder builder;
    builder.append_cellslice(*cs.prefetch_subslice(cs.size()));
    for (unsigned i = 0; i < cs.size_refs(); ++i) {
        builder.store_ref(i == index ? replacement : cs.prefetch_ref(i));
    }
    return builder.finalize();
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
    // Full generated records still provide the reference values when auxiliaries
    // are readable. Both auxiliaries are inaccessible in the shallow-read case.
    for (int wc : {-1, 0}) {
        block.workchain = wc;
        for (bool nonempty : {false, true}) {
            for (std::optional<std::uint64_t> count : {std::optional<std::uint64_t>{},
                    std::optional<std::uint64_t>{0}, std::optional<std::uint64_t>{(1ULL << 48) - 1}}) {
                auto original = state_metrics(wc, true, count, nonempty);
                block::gen::ShardStateUnsplit::Record full;
                CHECK(tlb::unpack_cell(original, full));
                block::CurrencyCollection reference;
                if (wc == -1) {
                    block::gen::McStateExtra::Record mc;
                    CHECK(tlb::unpack_cell(full.custom->prefetch_ref(), mc));
                    CHECK(reference.unpack(mc.global_balance));
                }
                auto shallow = state_metrics(wc, true, count, nonempty, "100000000000000000001", true);
                ParseQuery::parse_block_state_metrics(shallow, block).ensure();
                CHECK(block.out_msg_queue_size == count);
                CHECK(wc == -1 ? td::cmp(block.global_balance, reference.grams) == 0
                               : block.global_balance.is_null());
            }
            ParseQuery::parse_block_state_metrics(
                state_metrics(wc, false, {}, nonempty, "0", true), block).ensure();
            CHECK(!block.out_msg_queue_size);
            CHECK(wc == -1 ? block.global_balance->sgn() == 0 : block.global_balance.is_null());
        }
    }
    block.workchain = -1;
    ParseQuery::parse_block_state_metrics(state_metrics(-1, true, 0, false, "0"), block).ensure();
    CHECK(block.global_balance.not_null() && block.global_balance->sgn() == 0);
    CHECK(ParseQuery::parse_block_state_metrics({}, block).is_error());
    CHECK(ParseQuery::parse_block_state_metrics(vm::CellBuilder().finalize(), block).is_error());
    CHECK(ParseQuery::parse_block_state_metrics(state_metrics(0, false, {}, false), block).is_error());
    auto valid = state_metrics(-1, true, 0, false, "0", true);
    auto cs = vm::load_cell_slice(valid);
    for (unsigned bits : {0U, 31U, 167U, 199U, 263U, 359U, 360U, 361U}) {
        auto truncated = vm::CellBuilder().append_cellslice(*cs.prefetch_subslice(bits, cs.size_refs())).finalize();
        CHECK(ParseQuery::parse_block_state_metrics(truncated, block).is_error());
    }
    for (unsigned refs = 0; refs < cs.size_refs(); ++refs) {
        auto truncated = vm::CellBuilder().append_cellslice(*cs.prefetch_subslice(cs.size(), refs)).finalize();
        CHECK(ParseQuery::parse_block_state_metrics(truncated, block).is_error());
    }
    CHECK(ParseQuery::parse_block_state_metrics(
        vm::CellBuilder().append_cellslice(cs).store_long(0, 1).finalize(), block).is_error());
    auto empty = vm::CellBuilder().finalize();
    CHECK(ParseQuery::parse_block_state_metrics(with_ref(valid, 0, empty), block).is_error());
    CHECK(ParseQuery::parse_block_state_metrics(with_ref(valid, 3, empty), block).is_error());
    auto mc = vm::load_cell_slice(cs.prefetch_ref(3));
    CHECK(ParseQuery::parse_block_state_metrics(with_ref(valid, 3,
        vm::CellBuilder().append_cellslice(mc).store_long(0, 1).finalize()), block).is_error());
    ++block.gen_utime;
    CHECK(ParseQuery::parse_block_state_metrics(valid, block).is_error());
    --block.gen_utime;
    ++block.seqno;
    CHECK(ParseQuery::parse_block_state_metrics(state_metrics(-1, true, 1, false), block).is_error());
}
