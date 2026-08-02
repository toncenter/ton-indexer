#include "AbiLeavesDict.h"

#include "vm/cells/CellSlice.h"
#include "vm/dict.h"

namespace ton_abi {

td::Status load_dict(vm::CellSlice &cs, int key_bits,
                     const std::function<td::Status(vm::CellSlice &, vm::CellSlice &)> &on_entry) {
  if (key_bits <= 0 || key_bits > 1023) {
    return td::Status::Error(PSLICE() << "map: key width " << key_bits << " out of (0,1023]");
  }
  // Reads the HashmapE maybe-bit (+ ref if present) from cs and advances it --
  // the loadDict equivalent (@ton/core).
  vm::Dictionary dict{vm::DictAdvance{}, cs, key_bits};
  if (!dict.is_valid()) {
    return td::Status::Error("map: malformed dictionary");
  }

  td::Status err = td::Status::OK();
  bool ok = dict.check_for_each([&](td::Ref<vm::CellSlice> value, td::ConstBitPtr key, int key_len) -> bool {
    vm::CellBuilder kb;
    if (!kb.store_bits_bool(key, key_len)) {
      err = td::Status::Error("map: cannot materialize key bits");
      return false;
    }
    vm::CellSlice key_cs = vm::load_cell_slice(kb.finalize());
    vm::CellSlice val_cs = value.is_null() ? vm::CellSlice{} : *value;
    td::Status st = on_entry(key_cs, val_cs);
    if (st.is_error()) {
      err = std::move(st);
      return false;
    }
    return true;
  });
  if (err.is_error()) {
    return err;
  }
  if (!ok) {
    return td::Status::Error("map: dictionary iteration failed");
  }
  return td::Status::OK();
}

td::Status store_dict(vm::CellBuilder &cb, int key_bits, std::size_t count,
                      const std::function<td::Status(std::size_t, vm::CellBuilder &, vm::CellBuilder &)> &emit) {
  if (key_bits <= 0 || key_bits > 1023) {
    return td::Status::Error(PSLICE() << "map: key width " << key_bits << " out of (0,1023]");
  }
  vm::Dictionary dict{key_bits};
  for (std::size_t i = 0; i < count; ++i) {
    vm::CellBuilder key_b;
    vm::CellBuilder val_b;
    TRY_STATUS(emit(i, key_b, val_b));
    if (key_b.size() != static_cast<unsigned>(key_bits)) {
      return td::Status::Error(PSLICE() << "map: entry " << i << " key is " << key_b.size() << " bits, expected "
                                         << key_bits);
    }
    vm::CellSlice key_cs = vm::load_cell_slice(key_b.finalize());
    td::Ref<vm::CellSlice> val = td::make_ref<vm::CellSlice>(vm::load_cell_slice(val_b.finalize()));
    if (!dict.set(key_cs.data_bits(), key_bits, std::move(val), vm::Dictionary::SetMode::Add)) {
      return td::Status::Error(PSLICE() << "map: cannot set entry " << i << " (duplicate key?)");
    }
  }
  if (!dict.append_dict_to_bool(cb)) {
    return td::Status::Error("map: cannot serialize dictionary");
  }
  return td::Status::OK();
}

}  // namespace ton_abi
