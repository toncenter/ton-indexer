#include "Calls.h"
#include "Replay.h"
#include "crypto/block/block-auto.h"
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

Cell active_account(const Cell& code, const Cell& extra_currency = {}, td::uint64 last_trans_end_lt = 0) {
  vm::CellBuilder cb;
  cb.store_long(1, 1);                     // account$1
  cb.store_long(4, 3).store_zeroes(264);    // addr_std, no anycast, workchain 0, account id 0
  cb.store_zeroes(6 + 3 + 32 + 1);         // StorageInfo: no usage, extra info or due payment
  cb.store_long(last_trans_end_lt, 64).store_zeroes(4);  // last_trans_lt, zero grams
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
  ASSERT_TRUE(!account_address(none.account));
  auto result = run_get_method(pbtvm::GetMethod{}, none.account, true, nullptr).move_as_ok();
  ASSERT_EQ(pbtvm::ERROR_CODE_ACCOUNT_NOT_ACTIVE, result.error().code());

  // The library reference is legal in code, but not in place of Account.
  auto account = active_account(library_ref());
  auto parsed = request_account(shard_account(account)).move_as_ok();
  ASSERT_EQ(account->get_hash(), parsed.account->get_hash());
  ASSERT_TRUE(account_address(parsed.account).has_value());
}

TEST(TvmCalls, shard_account_roundtrip_preserves_transaction_metadata) {
  auto account = active_account(vm::CellBuilder{}.finalize(), {}, 7000003);
  td::Bits256 hash;
  hash.as_slice().copy_from(std::string(32, '\x5a'));
  auto input = boc(vm::CellBuilder{}.store_ref(account).store_bits(hash.cbits(), 256)
                       .store_long(7000000, 64).finalize());
  auto state = request_account(input).move_as_ok();
  ASSERT_EQ(account->get_hash(), state.account->get_hash());
  ASSERT_EQ(hash, state.last_trans_hash);
  ASSERT_EQ(7000000u, state.last_trans_lt);
  block::gen::Account::Record_account record;
  block::gen::AccountStorage::Record storage;
  ASSERT_TRUE(tlb::unpack_cell(state.account, record));
  ASSERT_TRUE(tlb::csr_unpack(record.storage, storage));
  ASSERT_EQ(7000003u, storage.last_trans_lt);
  auto output = shard_account_boc(state).move_as_ok();
  ASSERT_TRUE(output.has_value());
  ASSERT_EQ(input, *output);
}

TEST(TvmCalls, block_seed_overrides_follow_native_seed_derivation) {
  td::Bits256 block_seed, account_id;
  for (int i = 0; i < 32; ++i) {
    block_seed.as_slice()[i] = static_cast<char>(i);
    account_id.as_slice()[i] = static_cast<char>(i + 32);
  }
  block::StdAddress address{0, account_id};
  // Config is not needed for seed/time resolution without config overrides.
  VmContext chain{nullptr, 1700000000, 7000123, block_seed};
  auto defaults = resolve_context(chain, parse_overrides({}).move_as_ok()).move_as_ok();
  // SHA256 of bytes 0..63, independent of the implementation under test.
  ASSERT_EQ("FDEAB9ACF3710362BD2658CDC9A29E8F9C757FCF9811603A8C447CD1D9151108",
            defaults.vm_seed(address, 8).to_hex());
  ASSERT_EQ("21D13A671973B84E62A0803334271DD4687EC1C0EDC06F262F426FDF9BCFB456",
            defaults.vm_seed(address, 7).to_hex());
  ASSERT_EQ(chain.lt, defaults.chain.lt);

  pbtvm::ContextOverrides overrides;
  overrides.set_now(1700000010);
  overrides.set_lt(8000000);
  overrides.set_block_rand_seed(std::string(32, '\xab'));
  auto custom = resolve_context(chain, parse_overrides(overrides).move_as_ok()).move_as_ok();
  ASSERT_EQ("F6A5406D6AAFACAA61F3F91BDBB9F7ED8FA142F1A612CBE709B7EF9F6C152D4B",
            custom.vm_seed(address, 8).to_hex());
  ASSERT_EQ("C287199E6D3C147C3DC7ECBAA42E94AF0DB956EA9CB13A21D3DA474E32FC280B",
            custom.vm_seed(address, 7).to_hex());
  ASSERT_EQ("8D3587B0CE80B84F9A3E499BF86677297960673A1056964204E2746DE0C4356E",
            custom.vm_seed(block::StdAddress{0, td::Bits256::zero()}, 8).to_hex());
  ASSERT_EQ(overrides.block_rand_seed(), custom.chain.block_rand_seed.as_slice().str());
  ASSERT_EQ(overrides.now(), custom.chain.now);
  ASSERT_EQ(overrides.lt(), custom.chain.lt);
  ASSERT_EQ(1700000000u, chain.now);
  ASSERT_EQ(7000123u, chain.lt);
  ASSERT_EQ(block_seed, chain.block_rand_seed);
}

TEST(TvmCalls, explicit_zero_context_values_do_not_become_defaults) {
  pbtvm::ContextOverrides overrides;
  overrides.set_now(0);
  ASSERT_TRUE(parse_overrides(overrides).is_error());
  overrides.clear_now();
  overrides.set_lt(0);
  ASSERT_TRUE(parse_overrides(overrides).is_error());
  overrides.clear_lt();
  for (const auto& seed : {std::string(), std::string(31, 'x'), std::string(32, '\0')}) {
    overrides.set_block_rand_seed(seed);
    ASSERT_TRUE(parse_overrides(overrides).is_error());
  }
}

TEST(TvmCalls, emulation_accepts_only_inbound_messages_with_a_destination) {
  auto external = vm::CellBuilder{}.store_long(2, 2).store_zeroes(2)
      .store_long(4, 3).store_long(-1, 8).store_zeroes(256 + 4 + 1 + 1).finalize();
  auto parsed = request_message(boc(external)).move_as_ok();
  ASSERT_EQ(-1, parsed.destination.workchain);
  ASSERT_EQ(td::Bits256::zero(), parsed.destination.addr);
  auto outgoing = vm::CellBuilder{}.store_long(3, 2).store_long(4, 3).store_zeroes(8 + 256)
      .store_zeroes(2 + 64 + 32 + 1 + 1).finalize();
  ASSERT_TRUE(request_message(boc(outgoing)).is_error());
  ASSERT_TRUE(request_message(boc(library_ref())).is_error());
  ASSERT_TRUE(request_message(boc(vm::CellBuilder{}.finalize())).is_error());
}

TEST(TvmCalls, transaction_config_validation_does_not_restrict_get_methods) {
  for (int param : {5, 9, 12, 14, 31, 44}) {
    pbtvm::ContextOverrides overrides;
    (*overrides.mutable_config_params())[param] = boc(library_ref());
    ASSERT_TRUE(parse_overrides(overrides).is_ok());
    ASSERT_TRUE(parse_overrides(overrides, true).is_error());
  }
}

TEST(TvmCalls, replay_follows_the_account_across_shard_splits_and_merges) {
  const auto hash = td::Bits256::zero();
  ton::BlockIdExt parent{0, ton::shardIdAll, 10, hash, hash};
  ton::BlockIdExt left{0, ton::shard_child(ton::shardIdAll, true), 11, hash, hash};
  ton::BlockIdExt right{0, ton::shard_child(ton::shardIdAll, false), 12, hash, hash};
  block::StdAddress first{0, hash};
  auto high = hash;
  high.as_slice()[0] = static_cast<char>(0x80);
  block::StdAddress second{0, high};
  ShardBlockData data{};
  data.prev = {parent};
  ASSERT_EQ(parent, account_predecessor(data, first).move_as_ok());
  ASSERT_EQ(parent, account_predecessor(data, second).move_as_ok());
  data.prev = {left, right};
  ASSERT_EQ(left, account_predecessor(data, first).move_as_ok());
  ASSERT_EQ(right, account_predecessor(data, second).move_as_ok());
  ASSERT_TRUE(account_predecessor(data, block::StdAddress{-1, hash}).is_error());
}

TEST(TvmCalls, vm_log_tail_is_bounded_and_safe_for_protobuf) {
  std::string precompiled = "Running precompiled smart contract test: exit_code=0 accepted=1 success=1 gas_used=100";
  ASSERT_EQ(precompiled, vm_log_tail(precompiled + " time=0.0001s"));
  ASSERT_EQ(precompiled, vm_log_tail(precompiled + " time=0.1234s"));
  std::string text = "discarded\n" + std::string(kMaxVmLogSize, 'x') + "\nlast\tline\xff\n";
  ASSERT_EQ("last\tline?\n", vm_log_tail(text));
  TailLog streamed;
  for (int i = 0; i < 4; ++i) streamed.append(text);
  auto tail = streamed.tail();
  ASSERT_TRUE(tail.size() <= kMaxVmLogSize);
  ASSERT_TRUE(tail.ends_with("last\tline?\n"));
  ASSERT_TRUE(tail.find('\xff') == std::string::npos);
}

TEST(TvmCalls, malformed_config_overrides_are_invalid_requests) {
  auto none = vm::CellBuilder{}.store_long(0, 1).finalize();
  // A valid BOC whose root cannot be used as a config slice.
  for (int param : {0, 8, 18, 19, 20, 21, 24, 25, 43, 45}) {
    pbtvm::GetMethod call;
    (*call.mutable_context()->mutable_config_params())[param] = boc(library_ref());
    auto result = run_get_method(call, none, true, nullptr).move_as_ok();
    ASSERT_EQ(pbtvm::ERROR_CODE_INVALID_REQUEST, result.error().code());
    pbtvm::Emulate emulate;
    *emulate.mutable_context() = call.context();
    result = run_emulate(emulate, {}, {}, true, {}).move_as_ok();
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
  params[0] = boc(vm::CellBuilder{}.store_zeroes(256).finalize());
  params[8] = boc(vm::CellBuilder{}.store_long(0xc4, 8).store_long(11, 32).store_zeroes(64).finalize());
  params[19] = boc(vm::CellBuilder{}.store_long(-239, 32).finalize());
  params[45] = boc(vm::CellBuilder{}.store_long(0xc0, 8).store_long(0, 1).finalize());
  params[-1] = boc(library_ref());
  ASSERT_TRUE(parse_overrides(call.context(), true).is_ok());
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

TEST(TvmCalls, nonexistent_accounts_have_no_last_transaction) {
  // As TON stores them, and as get_account gives them.
  block::Account account{0, td::Bits256::zero().cbits()};
  account.total_state = vm::CellBuilder{}.store_long(0, 1).finalize();
  account.last_trans_hash_.as_slice().fill('\x5a');
  account.last_trans_lt_ = 7000000;
  account.status = block::Account::acc_nonexist;
  auto state = account_state(account);
  ASSERT_EQ(td::Bits256::zero(), state.last_trans_hash);
  ASSERT_EQ(0u, state.last_trans_lt);
  account.status = block::Account::acc_uninit;
  ASSERT_EQ(7000000u, account_state(account).last_trans_lt);
}

}  // namespace

int main() {
  td::TestsRunner::get_default().run_all();
  return 0;
}
