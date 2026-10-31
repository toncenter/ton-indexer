#pragma once

#include "crypto/block/block.h"
#include "crypto/block/mc-config.h"

// A minimal MC zerostate with a real ConfigInfo timestamp. No network fixtures.
inline std::shared_ptr<block::ConfigInfo> test_smc_config(
    std::uint32_t now, const std::vector<td::Ref<vm::Cell>>& libraries = {},
    const std::vector<std::pair<int32_t, td::Ref<vm::Cell>>>& params = {}) {
    auto empty = vm::CellBuilder().finalize();
    vm::Dictionary config_dict{32};
    CHECK(config_dict.set_ref(td::BitArray<32>::zero(), empty));
    if (!params.empty()) CHECK(config_dict.set_ref(td::BitArray<32>(31), vm::CellBuilder().store_long(0,1).finalize()));
    for (const auto& [id, cell] : params) CHECK(config_dict.set_ref(td::BitArray<32>(id), cell));
    auto mc_aux = vm::CellBuilder().store_zeroes(16 + 65 + 66 + 1 + 1).finalize();
    auto mc_extra = vm::CellBuilder().store_long(0xcc26, 16).store_long(0, 1)
        .store_zeroes(256).store_ref(config_dict.get_root_cell()).store_ref(mc_aux)
        .store_long(0, 5).finalize();
    vm::Dictionary library_dict{256};
    vm::Dictionary publishers{256};
    CHECK(publishers.set(td::Bits256::zero(), vm::load_cell_slice_ref(empty)));
    for (const auto& library : libraries) {
        auto descriptor = vm::CellBuilder().store_long(0, 2).store_ref(library)
            .append_cellslice(vm::load_cell_slice(publishers.get_root_cell())).finalize();
        CHECK(library_dict.set(library->get_hash().bits(), 256, vm::load_cell_slice_ref(descriptor)));
    }
    vm::CellBuilder aux;
    aux.store_zeroes(128 + 5 + 5);
    CHECK(aux.store_maybe_ref(library_dict.get_root_cell()));
    auto state_aux = aux.store_long(0, 1).finalize();
    auto root = vm::CellBuilder().store_long(0x9023afe2, 32).store_long(-3, 32)
        .store_long(0, 8).store_long(-1, 32).store_zeroes(64)
        .store_zeroes(64).store_long(now, 32).store_zeroes(64 + 32)
        .store_ref(empty).store_long(0, 1).store_ref(empty).store_ref(state_aux)
        .store_long(1, 1).store_ref(mc_extra).finalize();
    ton::BlockIdExt id{ton::masterchainId, ton::shardIdAll, 0, td::Bits256::zero(), td::Bits256::zero()};
    return block::ConfigInfo::extract_config(root, id,
        (libraries.empty() ? 0 : block::ConfigInfo::needLibraries) |
        (params.empty() ? 0 : block::ConfigInfo::needSpecialSmc)).move_as_ok();
}

inline td::Ref<vm::Cell> test_library_reference(const td::Ref<vm::Cell>& library) {
    return vm::CellBuilder().store_long(2, 8).store_bits(library->get_hash().bits(), 256).finalize(true);
}

inline td::Ref<vm::Cell> test_active_account(const block::StdAddress& address,
                                         const td::Ref<vm::Cell>& code, td::Ref<vm::Cell> data = {}, uint64_t last_trans_end = 0) {
    vm::CellBuilder account;
    account.store_long(1, 1).store_long(4, 3).store_long(address.workchain, 8).store_bits(address.addr.bits(), 256);
    account.store_zeroes(3 + 3 + 3 + 32 + 1);  // StorageInfo
    account.store_long(last_trans_end, 64).store_zeroes(5).store_long(1, 1);  // AccountStorage, active
    account.store_long(0, 2);  // StateInit split_depth/special
    CHECK(account.store_maybe_ref(code));
    CHECK(account.store_maybe_ref(data));
    return account.store_long(0, 1).finalize();  // no local libraries
}

inline td::Ref<vm::Cell> test_methods_dispatcher(
    const std::vector<std::pair<unsigned, td::Ref<vm::Cell>>>& bodies) {
    vm::Dictionary methods{19};
    for (const auto& [method, body] : bodies) {
        CHECK(methods.set(td::BitArray<19>(method), vm::load_cell_slice_ref(body)));
    }
    // SETCP0; 19 DICTPUSHCONST; DICTIGETJMPZ; 11 THROW.
    return vm::CellBuilder().store_long(0xff00, 16).store_long(0xf4a413, 24)
        .store_ref(methods.get_root_cell()).store_long(0xf4bc, 16).store_long(0xf20b, 16).finalize();
}

inline td::Ref<vm::Cell> test_method_dispatcher(unsigned method, td::Ref<vm::Cell> body) {
    return test_methods_dispatcher({{method, std::move(body)}});
}

inline td::Ref<vm::Cell> test_address_cell(std::optional<block::StdAddress> address = {}) {
    if (!address) {
        return vm::CellBuilder().store_long(0, 2).finalize();
    }
    return vm::CellBuilder().store_long(4, 3).store_long(address->workchain, 8)
        .store_bits(address->addr.bits(), 256).finalize();
}

inline td::Ref<vm::Cell> test_nft_code(bool init, td::Ref<vm::Cell> owner,
                                     td::Ref<vm::Cell> content, td::Ref<vm::Cell> collection = {}) {
    vm::CellBuilder body;
    // PUSHINT init; PUSHINT 0; PUSHREFSLICE collection; owner; content.
    body.store_long(init ? 0x7f : 0x70, 8).store_long(0x70, 8)
        .store_long(0x89, 8).store_ref(collection.not_null() ? collection : test_address_cell());
    if (owner.not_null()) {
        body.store_long(0x89, 8).store_ref(owner);
    } else {
        body.store_long(0x6d, 8);  // PUSHNULL
    }
    if (content.not_null()) {
        body.store_long(0x88, 8).store_ref(content);
    } else {
        body.store_long(0x6d, 8);
    }
    return test_method_dispatcher(102351, body.finalize());
}

inline td::Ref<vm::Cell> test_shard_with_account(const block::StdAddress& address,
                                               const td::Ref<vm::Cell>& code) {
    auto empty = vm::CellBuilder().finalize();
    auto account = test_active_account(address, code, empty);
    auto shard_account = vm::CellBuilder().store_ref(account).store_zeroes(256 + 64).finalize();
    vm::AugmentedDictionary accounts{256, block::tlb::aug_ShardAccounts};
    CHECK(accounts.set(address.addr, vm::load_cell_slice_ref(shard_account)));
    auto aux = vm::CellBuilder().store_zeroes(128 + 5 + 5 + 1 + 1).finalize();
    return vm::CellBuilder().store_long(0x9023afe2, 32).store_long(-3, 32)
        .store_long(0, 8).store_long(address.workchain, 32).store_zeroes(64 + 64)
        .store_long(1700000000, 32).store_zeroes(64 + 32)
        .store_ref(empty).store_long(0, 1).store_ref(accounts.get_wrapped_dict_root())
        .store_ref(aux).store_long(0, 1).finalize();
}
