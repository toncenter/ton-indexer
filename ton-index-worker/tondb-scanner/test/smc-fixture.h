#pragma once

#include "crypto/block/block.h"
#include "crypto/block/mc-config.h"

// A minimal MC zerostate with a real ConfigInfo timestamp. No network fixtures.
inline std::shared_ptr<block::ConfigInfo> test_smc_config(std::uint32_t now) {
    auto empty = vm::CellBuilder().finalize();
    vm::Dictionary config_dict{32};
    CHECK(config_dict.set_ref(td::BitArray<32>::zero(), empty));
    auto mc_aux = vm::CellBuilder().store_zeroes(16 + 65 + 66 + 1 + 1).finalize();
    auto mc_extra = vm::CellBuilder().store_long(0xcc26, 16).store_long(0, 1)
        .store_zeroes(256).store_ref(config_dict.get_root_cell()).store_ref(mc_aux)
        .store_long(0, 5).finalize();
    auto state_aux = vm::CellBuilder().store_zeroes(128 + 5 + 5 + 1 + 1).finalize();
    auto root = vm::CellBuilder().store_long(0x9023afe2, 32).store_long(-3, 32)
        .store_long(0, 8).store_long(-1, 32).store_zeroes(64)
        .store_zeroes(64).store_long(now, 32).store_zeroes(64 + 32)
        .store_ref(empty).store_long(0, 1).store_ref(empty).store_ref(state_aux)
        .store_long(1, 1).store_ref(mc_extra).finalize();
    ton::BlockIdExt id{ton::masterchainId, ton::shardIdAll, 0, td::Bits256::zero(), td::Bits256::zero()};
    return block::ConfigInfo::extract_config(root, id, 0).move_as_ok();
}

inline td::Ref<vm::Cell> test_method_dispatcher(unsigned method, td::Ref<vm::Cell> body) {
    vm::Dictionary methods{19};
    CHECK(methods.set(td::BitArray<19>(method), vm::load_cell_slice_ref(body)));
    // SETCP0; 19 DICTPUSHCONST; DICTIGETJMPZ; 11 THROW.
    return vm::CellBuilder().store_long(0xff00, 16).store_long(0xf4a413, 24)
        .store_ref(methods.get_root_cell()).store_long(0xf4bc, 16).store_long(0xf20b, 16).finalize();
}
