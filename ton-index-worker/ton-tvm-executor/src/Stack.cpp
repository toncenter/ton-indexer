// TVM stack values to and from ton.tvm.v1.StackEntry.
#include <unordered_map>
#include <unordered_set>
#include "crypto/vm/boc.h"
#include "crypto/vm/cells/CellBuilder.h"
#include "crypto/vm/cells/CellSlice.h"
#include "Calls.h"

namespace tvm_executor {
namespace {

namespace pbtvm = ton::tvm::v1;
using Cell = td::Ref<vm::Cell>;

// 1 to 33 bytes of two's complement big-endian without a redundant sign byte:
// exactly the range -2^256 to 2^256-1. The router checks it too.
bool minimal_int257(const std::string& b) {
  if (b.empty() || b.size() > 33) {
    return false;
  }
  auto byte = [&b](std::size_t i) { return static_cast<unsigned char>(b[i]); };
  if (b.size() == 33 && byte(0) != 0x00 && byte(0) != 0xff) {
    return false;
  }
  return b.size() == 1 || !((byte(0) == 0x00 && !(byte(1) & 0x80)) || (byte(0) == 0xff && (byte(1) & 0x80)));
}

td::Result<vm::StackEntry> from_proto(const pbtvm::StackEntry& entry, const std::string& where, int depth);

td::Result<std::vector<vm::StackEntry>> items_from_proto(const google::protobuf::RepeatedPtrField<pbtvm::StackEntry>& items,
                                                         const std::string& where, int depth) {
  if (depth == kMaxStackDepth) {
    return td::Status::Error(PSLICE() << where << " is nested deeper than " << kMaxStackDepth);
  }
  std::vector<vm::StackEntry> out;
  out.reserve(items.size());
  for (int i = 0; i < items.size(); i++) {
    TRY_RESULT(item, from_proto(items[i], PSTRING() << where << "[" << i << "]", depth + 1));
    out.push_back(std::move(item));
  }
  return out;
}

td::Result<vm::StackEntry> from_proto(const pbtvm::StackEntry& entry, const std::string& where, int depth) {
  switch (entry.value_case()) {
    case pbtvm::StackEntry::kNull:
      return vm::StackEntry{};
    case pbtvm::StackEntry::kNan:
      return vm::StackEntry{td::nan_refint()};
    case pbtvm::StackEntry::kInteger: {
      const auto& bytes = entry.integer();
      td::RefInt256 x{true};
      if (!minimal_int257(bytes) ||
          !x.unique_write().import_bytes(reinterpret_cast<const unsigned char*>(bytes.data()), bytes.size(), true) ||
          !x->signed_fits_bits(257)) {
        return td::Status::Error(PSLICE() << where << " is not a minimal int257");
      }
      return vm::StackEntry{std::move(x)};
    }
    case pbtvm::StackEntry::kCell: {
      TRY_RESULT(cell, request_boc(entry.cell(), where));
      return vm::StackEntry{std::move(cell)};
    }
    case pbtvm::StackEntry::kBuilder: {
      TRY_RESULT(cell, request_boc(entry.builder(), where));
      vm::CellSlice cs{vm::NoVmOrd(), cell};
      td::Ref<vm::CellBuilder> builder{true};
      if (!cs.is_valid() || !builder.write().append_cellslice_bool(cs)) {
        return td::Status::Error(PSLICE() << where << " is a builder of a special cell");
      }
      return vm::StackEntry{std::move(builder)};
    }
    case pbtvm::StackEntry::kSlice: {
      const auto& slice = entry.slice();
      TRY_RESULT(cell, request_boc(slice.cell_boc(), where));
      vm::CellSlice cs{vm::NoVmOrd(), cell};
      if (!cs.is_valid()) {
        return td::Status::Error(PSLICE() << where << " is a slice of a special cell");
      }
      unsigned bits = cs.size(), refs = cs.size_refs();
      if (slice.start_bit() > slice.end_bit() || slice.end_bit() > bits || slice.start_ref() > slice.end_ref() ||
          slice.end_ref() > refs || !cs.skip_first(slice.start_bit(), slice.start_ref()) ||
          !cs.only_first(slice.end_bit() - slice.start_bit(), slice.end_ref() - slice.start_ref())) {
        return td::Status::Error(PSLICE() << where << " has bounds out of its cell of " << bits << " bits and " << refs
                                          << " refs");
      }
      return vm::StackEntry{td::Ref<vm::CellSlice>{true, std::move(cs)}};
    }
    case pbtvm::StackEntry::kTuple: {
      if (static_cast<std::size_t>(entry.tuple().items_size()) > kMaxTupleSize) {
        return td::Status::Error(PSLICE() << where << " is a tuple of " << entry.tuple().items_size()
                                          << " items, over " << kMaxTupleSize);
      }
      TRY_RESULT(items, items_from_proto(entry.tuple().items(), where, depth));
      return vm::StackEntry{td::make_cnt_ref<std::vector<vm::StackEntry>>(std::move(items))};
    }
    case pbtvm::StackEntry::kList: {
      TRY_RESULT(items, items_from_proto(entry.list().items(), where, depth));
      return vm::StackEntry::make_list(std::move(items));
    }
    case pbtvm::StackEntry::kUnsupported:
      return td::Status::Error(PSLICE() << where << " is unsupported, which is for results only");
    case pbtvm::StackEntry::VALUE_NOT_SET:
      break;
  }
  return td::Status::Error(PSLICE() << where << " is empty");
}

// Whether the BOC of a cell can be within limit bytes: false only when a lower
// bound of its size is over it, found out before every cell of a large tree
// is read. Each distinct cell takes at least its two descriptor bytes, its
// data and a byte per reference.
td::Result<bool> boc_within(const Cell& root, std::size_t limit) {
  std::unordered_set<vm::CellHash> seen;
  std::vector<Cell> pending{root};
  std::size_t bytes = 0;
  while (!pending.empty()) {
    auto cell = std::move(pending.back());
    pending.pop_back();
    if (!seen.insert(cell->get_hash()).second) {
      continue;
    }
    auto loaded = cell->load_cell();
    if (loaded.is_error()) {
      return unreadable(loaded.move_as_error());
    }
    const auto& data = loaded.ok().data_cell;
    bytes += 2 + (data->get_bits() + 7) / 8 + data->get_refs_cnt();
    if (bytes > limit) {
      return false;
    }
    for (unsigned i = 0; i < data->get_refs_cnt(); i++) {
      pending.push_back(data->get_ref(i));
    }
  }
  return true;
}

// The bytes an entry takes in a result are counted as it is filled: shared
// values of TVM, such as a tuple of the same tuple twice, nested 32 times, are
// copies here, which must not take time or memory past the limit.
class ResultStack {
 public:
  explicit ResultStack(std::size_t& budget) : budget_(budget) {}

  // false once over the budget.
  td::Result<bool> fill(const vm::StackEntry& entry, pbtvm::StackEntry* out, int depth) {
    // A tag and a length at least.
    if (!take(2)) {
      return false;
    }
    switch (entry.type()) {
      case vm::StackEntry::t_null:
        out->mutable_null();
        return true;
      case vm::StackEntry::t_int:
        return fill_int(entry.as_int(), out);
      case vm::StackEntry::t_cell: {
        TRY_RESULT(boc, cell_boc(entry.as_cell()));
        if (!boc) {
          return false;
        }
        out->set_cell(std::move(*boc));
        return true;
      }
      case vm::StackEntry::t_builder: {
        TRY_RESULT(boc, cell_boc(entry.as_builder()->finalize_copy()));
        if (!boc) {
          return false;
        }
        out->set_builder(std::move(*boc));
        return true;
      }
      case vm::StackEntry::t_slice: {
        auto cs = entry.as_slice();
        TRY_RESULT(boc, cell_boc(cs->get_base_cell()));
        if (!boc || !take(4 * 6)) {
          return false;
        }
        auto* slice = out->mutable_slice();
        slice->set_cell_boc(std::move(*boc));
        slice->set_start_bit(cs->cur_pos());
        slice->set_end_bit(cs->cur_pos() + cs->size());
        slice->set_start_ref(cs->cur_ref());
        slice->set_end_ref(cs->cur_ref() + cs->size_refs());
        return true;
      }
      case vm::StackEntry::t_tuple:
        return fill_tuple(entry, out, depth);
      case vm::StackEntry::t_vmcont:
        return unsupported(out, "continuation");
      case vm::StackEntry::t_stack:
        return unsupported(out, "stack");
      case vm::StackEntry::t_string:
        return unsupported(out, "string");
      case vm::StackEntry::t_bytes:
        return unsupported(out, "bytes");
      case vm::StackEntry::t_bitstring:
        return unsupported(out, "bitstring");
      case vm::StackEntry::t_box:
        return unsupported(out, "box");
      case vm::StackEntry::t_atom:
        return unsupported(out, "atom");
      case vm::StackEntry::t_object:
        return unsupported(out, "object");
    }
    return unsupported(out, "unknown");
  }

 private:
  std::size_t& budget_;
  // The stack keeps every tuple alive and unchanged during conversion.
  std::unordered_map<const vm::Tuple*, bool> list_types_;

  bool is_list(const vm::StackEntry& entry) {
    std::vector<const vm::Tuple*> path;
    const auto* tail = &entry;
    bool list = true;
    while (!tail->empty()) {
      auto tuple = tail->as_tuple_range(2, 2);
      if (tuple.is_null()) {
        list = false;
        break;
      }
      if (auto it = list_types_.find(tuple.get()); it != list_types_.end()) {
        list = it->second;
        break;
      }
      path.push_back(tuple.get());
      tail = &tuple->at(1);
    }
    // Shared tails, including values at the depth limit, are scanned once.
    // Keep the existing list/tuple labels in unsupported results.
    for (auto* tuple : path) {
      list_types_.emplace(tuple, list);
    }
    return list;
  }

  bool take(std::size_t bytes) {
    if (bytes > budget_) {
      return false;
    }
    budget_ -= bytes;
    return true;
  }

  bool unsupported(pbtvm::StackEntry* out, std::string name) {
    if (!take(name.size())) {
      return false;
    }
    out->set_unsupported(std::move(name));
    return true;
  }

  bool fill_int(const td::RefInt256& x, pbtvm::StackEntry* out) {
    if (!x->is_valid()) {
      out->mutable_nan();
      return true;
    }
    unsigned char bytes[33];
    for (std::size_t n = 1; n <= sizeof(bytes); n++) {
      if (x->signed_fits_bits(static_cast<int>(8 * n)) && x->export_bytes(bytes, n, true)) {
        if (!take(n)) {
          return false;
        }
        out->set_integer(std::string(reinterpret_cast<const char*>(bytes), n));
        return true;
      }
    }
    // An integer of TVM is within 257 bits.
    return unsupported(out, "integer");
  }

  // none when over the budget.
  td::Result<std::optional<std::string>> cell_boc(const Cell& cell) {
    if (cell.is_null()) {
      return malformed("a null cell in a stack");
    }
    TRY_RESULT(within, boc_within(cell, budget_));
    if (!within) {
      return std::nullopt;
    }
    auto boc = vm::std_boc_serialize(cell);
    if (boc.is_error()) {
      return malformed(PSLICE() << "cannot serialize a cell of a stack: " << boc.error().message());
    }
    if (!take(boc.ok().size())) {
      return std::nullopt;
    }
    return boc.ok().as_slice().str();
  }

  td::Result<bool> fill_tuple(const vm::StackEntry& entry, pbtvm::StackEntry* out, int depth) {
    bool list = is_list(entry);
    if (depth == kMaxStackDepth) {
      return unsupported(out, PSTRING() << (list ? "list" : "tuple") << " nested deeper than " << kMaxStackDepth);
    }
    if (!list) {
      auto* tuple = out->mutable_tuple();
      for (const auto& item : *entry.as_tuple()) {
        TRY_RESULT(ok, fill(item, tuple->add_items(), depth + 1));
        if (!ok) {
          return false;
        }
      }
      return true;
    }
    // The pairs [x1, [x2, … [xn, null]]].
    auto* items = out->mutable_list();
    td::Ref<vm::Tuple> tuple;
    for (const vm::StackEntry* pair = &entry; !pair->empty(); pair = &tuple->at(1)) {
      tuple = pair->as_tuple();
      TRY_RESULT(ok, fill(tuple->at(0), items->add_items(), depth + 1));
      if (!ok) {
        return false;
      }
    }
    return true;
  }
};

}  // namespace

td::Result<std::vector<vm::StackEntry>> stack_from_proto(
    const google::protobuf::RepeatedPtrField<pbtvm::StackEntry>& entries) {
  std::vector<vm::StackEntry> out;
  out.reserve(entries.size() + 1);
  for (int i = 0; i < entries.size(); i++) {
    TRY_RESULT(entry, from_proto(entries[i], PSTRING() << "stack[" << i << "]", 0));
    out.push_back(std::move(entry));
  }
  return out;
}

td::Result<bool> stack_to_proto(const vm::Stack& stack, google::protobuf::RepeatedPtrField<pbtvm::StackEntry>* out,
                                std::size_t& budget) {
  ResultStack result{budget};
  // Bottom to top.
  for (const auto& entry : stack.as_span()) {
    TRY_RESULT(ok, result.fill(entry, out->Add(), 0));
    if (!ok) {
      return false;
    }
  }
  return true;
}

}  // namespace tvm_executor
