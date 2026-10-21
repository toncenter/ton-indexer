#include "Calls.h"
#include "crypto/vm/boc.h"
#include "crypto/vm/cells/CellBuilder.h"
#include "td/utils/tests.h"

namespace {

namespace pbtvm = ton::tvm::v1;
using namespace tvm_executor;
using Cell = td::Ref<vm::Cell>;

std::string boc(const Cell& cell) {
  return vm::std_boc_serialize(cell).move_as_ok().as_slice().str();
}

Cell library_ref() {
  return vm::CellBuilder{}.store_long(2, 8).store_zeroes(256).finalize(true);
}

std::string shard_account(const Cell& account) {
  return boc(vm::CellBuilder{}.store_ref(account).store_zeroes(320).finalize());
}

Cell active_account(const Cell& code, const Cell& extra_currency = {}) {
  vm::CellBuilder cb;
  cb.store_long(1, 1);                     // account$1
  cb.store_long(4, 3).store_zeroes(264);    // addr_std, no anycast, workchain 0, account id 0
  cb.store_zeroes(6 + 3 + 32 + 1);         // StorageInfo: no usage, extra info or due payment
  cb.store_zeroes(64 + 4);                 // last_trans_lt, zero grams
  cb.store_long(extra_currency.not_null(), 1);
  if (extra_currency.not_null()) {
    cb.store_ref(extra_currency);
  }
  cb.store_long(1, 1).store_long(4, 5);     // active, StateInit with code only
  cb.store_ref(code);
  return cb.finalize();
}

TEST(TvmCalls, ordinary_slice_preserves_bounds_and_base_cell) {
  auto first = vm::CellBuilder{}.store_long(1, 8).finalize();
  auto second = vm::CellBuilder{}.store_long(2, 8).finalize();
  auto cell = vm::CellBuilder{}.store_long(0xabcd, 16).store_ref(first).store_ref(second).finalize();
  google::protobuf::RepeatedPtrField<pbtvm::StackEntry> input;
  auto* slice = input.Add()->mutable_slice();
  slice->set_cell_boc(boc(cell));
  slice->set_start_bit(4);
  slice->set_end_bit(12);
  slice->set_start_ref(1);
  slice->set_end_ref(2);
  auto entries = stack_from_proto(input).move_as_ok();
  auto cs = entries[0].as_slice();
  ASSERT_EQ(8u, cs->size());
  ASSERT_EQ(1u, cs->size_refs());
  ASSERT_EQ(0xbc, cs->prefetch_ulong(8));
  ASSERT_EQ(second->get_hash(), cs->prefetch_ref()->get_hash());
  ASSERT_EQ(cell->get_hash(), cs->get_base_cell()->get_hash());

  vm::Stack stack{std::move(entries)};
  google::protobuf::RepeatedPtrField<pbtvm::StackEntry> output;
  auto budget = kMaxCallResultSize;
  ASSERT_TRUE(stack_to_proto(stack, &output, budget).move_as_ok());
  ASSERT_EQ(input[0].SerializeAsString(), output[0].SerializeAsString());

  slice->set_end_bit(17);
  ASSERT_TRUE(stack_from_proto(input).is_error());
  slice->set_end_bit(12);
  slice->set_end_ref(3);
  ASSERT_TRUE(stack_from_proto(input).is_error());
}

TEST(TvmCalls, ordinary_builder_preserves_bits_and_refs) {
  for (auto cell : {vm::CellBuilder{}.finalize(),
                    vm::CellBuilder{}.store_long(42, 8).store_ref(library_ref()).finalize()}) {
    google::protobuf::RepeatedPtrField<pbtvm::StackEntry> input;
    input.Add()->set_builder(boc(cell));
    auto entries = stack_from_proto(input).move_as_ok();
    ASSERT_EQ(cell->get_hash(), entries[0].as_builder()->finalize_copy()->get_hash());
  }
}

TEST(TvmCalls, special_slice_and_builder_are_invalid_requests) {
  auto none = vm::CellBuilder{}.store_long(0, 1).finalize();
  for (bool builder : {false, true}) {
    pbtvm::GetMethod call;
    if (builder) {
      call.add_stack()->set_builder(boc(library_ref()));
    } else {
      // Even empty bounds must not turn an invalid slice into a valid one.
      call.add_stack()->mutable_slice()->set_cell_boc(boc(library_ref()));
    }
    auto result = run_get_method(call, none, true, nullptr).move_as_ok();
    ASSERT_EQ(pbtvm::ERROR_CODE_INVALID_REQUEST, result.error().code());
  }
}

TEST(TvmCalls, malformed_account_is_rejected_before_address_lookup) {
  for (auto account : std::vector<Cell>{library_ref(), vm::CellBuilder{}.finalize(),
                                      vm::CellBuilder{}.store_long(1, 1).finalize(),
                                      vm::CellBuilder{}.store_zeroes(2).finalize(),
                                      active_account(vm::CellBuilder{}.finalize(), library_ref())}) {
    ASSERT_TRUE(request_account(shard_account(account)).is_error());
  }
}

TEST(TvmCalls, account_none_and_library_code_are_valid_inputs) {
  auto none = request_account(shard_account(vm::CellBuilder{}.store_long(0, 1).finalize())).move_as_ok();
  ASSERT_TRUE(!account_address(none));
  auto result = run_get_method(pbtvm::GetMethod{}, none, true, nullptr).move_as_ok();
  ASSERT_EQ(pbtvm::ERROR_CODE_ACCOUNT_NOT_ACTIVE, result.error().code());

  // The library reference is legal in code, but not in place of Account.
  auto account = active_account(library_ref());
  auto parsed = request_account(shard_account(account)).move_as_ok();
  ASSERT_EQ(account->get_hash(), parsed->get_hash());
  ASSERT_TRUE(account_address(parsed).has_value());
}

TEST(TvmCalls, malformed_config_overrides_are_invalid_requests) {
  auto none = vm::CellBuilder{}.store_long(0, 1).finalize();
  // A valid BOC whose root cannot be used as a config slice.
  for (int param : {8, 18, 19, 20, 21, 24, 25, 43, 45}) {
    pbtvm::GetMethod call;
    (*call.mutable_context()->mutable_config_params())[param] = boc(library_ref());
    auto result = run_get_method(call, none, true, nullptr).move_as_ok();
    ASSERT_EQ(pbtvm::ERROR_CODE_INVALID_REQUEST, result.error().code());
  }

  // An ordinary dictionary root with invalid children must also be caught,
  // including precompiled entries not looked up for a particular code hash.
  auto bad_dict = vm::CellBuilder{}.store_zeroes(2).store_ref(library_ref()).store_ref(library_ref()).finalize();
  for (int param : {18, 45}) {
    pbtvm::GetMethod call;
    auto cell = param == 18 ? bad_dict
                           : vm::CellBuilder{}.store_long(0xc0, 8).store_long(1, 1).store_ref(bad_dict).finalize();
    (*call.mutable_context()->mutable_config_params())[param] = boc(cell);
    auto result = run_get_method(call, none, true, nullptr).move_as_ok();
    ASSERT_EQ(pbtvm::ERROR_CODE_INVALID_REQUEST, result.error().code());
  }
}

TEST(TvmCalls, partial_and_custom_config_overrides_remain_valid) {
  pbtvm::GetMethod call;
  auto& params = *call.mutable_context()->mutable_config_params();
  params[8] = boc(vm::CellBuilder{}.store_long(0xc4, 8).store_long(11, 32).store_zeroes(64).finalize());
  params[19] = boc(vm::CellBuilder{}.store_long(-239, 32).finalize());
  params[45] = boc(vm::CellBuilder{}.store_long(0xc0, 8).store_long(0, 1).finalize());
  params[-1] = boc(library_ref());
  auto none = vm::CellBuilder{}.store_long(0, 1).finalize();
  auto result = run_get_method(call, none, true, nullptr).move_as_ok();
  ASSERT_EQ(pbtvm::ERROR_CODE_ACCOUNT_NOT_ACTIVE, result.error().code());
}

TEST(TvmCalls, shared_lists_at_depth_limit_keep_their_type) {
  auto entry = vm::StackEntry::make_list(std::vector<vm::StackEntry>(100000));
  for (int i = 0; i < kMaxStackDepth - 8; ++i) {
    entry = vm::StackEntry{vm::make_tuple_ref(entry)};
  }
  for (int i = 0; i < 8; ++i) {
    entry = vm::StackEntry{vm::make_tuple_ref(entry, entry)};
  }
  vm::Stack stack{std::vector<vm::StackEntry>{entry}};
  google::protobuf::RepeatedPtrField<pbtvm::StackEntry> output;
  auto budget = kMaxCallResultSize;
  ASSERT_TRUE(stack_to_proto(stack, &output, budget).move_as_ok());
  const auto* item = &output[0];
  for (int i = 0; i < kMaxStackDepth; ++i) {
    ASSERT_TRUE(item->has_tuple());
    if (i < 8) {
      ASSERT_EQ(item->tuple().items(0).SerializeAsString(), item->tuple().items(1).SerializeAsString());
    }
    item = &item->tuple().items(0);
  }
  ASSERT_EQ("list nested deeper than 32", item->unsupported());
}

TEST(TvmCalls, shared_proper_and_improper_list_tails) {
  auto list = vm::StackEntry::make_list(std::vector<vm::StackEntry>{vm::StackEntry{td::make_refint(1)}});
  auto improper = vm::StackEntry{vm::make_tuple_ref(vm::StackEntry{}, td::make_refint(2))};
  vm::Stack stack{std::vector<vm::StackEntry>{
      vm::StackEntry{vm::make_tuple_ref(td::make_refint(0), list)}, list,
      vm::StackEntry{vm::make_tuple_ref(td::make_refint(0), improper)}, improper}};
  google::protobuf::RepeatedPtrField<pbtvm::StackEntry> output;
  auto budget = kMaxCallResultSize;
  ASSERT_TRUE(stack_to_proto(stack, &output, budget).move_as_ok());
  ASSERT_EQ(2, output[0].list().items_size());
  ASSERT_EQ(1, output[1].list().items_size());
  ASSERT_EQ(output[0].list().items(1).SerializeAsString(), output[1].list().items(0).SerializeAsString());
  ASSERT_TRUE(output[2].has_tuple());
  ASSERT_TRUE(output[3].has_tuple());
  ASSERT_EQ(output[2].tuple().items(1).SerializeAsString(), output[3].SerializeAsString());
}

}  // namespace

int main() {
  td::TestsRunner::get_default().run_all();
  return 0;
}
