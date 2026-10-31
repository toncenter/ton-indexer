#pragma once
#include "ElectorSnapshots.h"

inline vm::StackEntry elector_number(long long n) { return vm::StackEntry(td::make_refint(n)); }

inline std::vector<vm::StackEntry> test_elector_stack(bool flat = false, bool banned = false,
                                                     long long total = 95, bool corrupt = false) {
  vm::Dictionary frozen(256);
  vm::CellBuilder position;
  position.store_zeroes(256).store_long(9, 64);
  CHECK(block::tlb::t_Grams.store_integer_ref(position, td::make_refint(95)));
  if (!corrupt) position.store_long(banned ? -1 : 0, 1);
  CHECK(frozen.set(td::Bits256::zero(), vm::load_cell_slice_ref(position.finalize())));
  std::vector<vm::StackEntry> values{
      elector_number(200),elector_number(50),elector_number(123),
      vm::StackEntry(frozen.get_root_cell()),elector_number(total),elector_number(0),vm::StackEntry{}};
  vm::StackEntry election;
  if (flat) {
    values.insert(values.begin(),elector_number(100));
    election = vm::StackEntry(std::move(values));
  } else {
    election = vm::StackEntry(vm::make_tuple_ref(elector_number(100), vm::StackEntry(std::move(values))));
  }
  return {vm::StackEntry(vm::make_tuple_ref(election, vm::StackEntry{}))};
}

inline schema::ElectorSnapshot test_elector_snapshot(uint32_t mc = 100) {
  schema::ElectorSnapshot s{};
  s.mc_seqno=mc; s.gen_utime=1788220800; s.complete=true;
  s.address="-1:" + std::string(64,'0');
  s.elections=staking_history::parse_elections(test_elector_stack()).move_as_ok();
  return s;
}
