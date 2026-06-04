#include "td/utils/port/signals.h"
#include "td/utils/OptionParser.h"
#include "td/utils/format.h"
#include "td/utils/logging.h"
#include "td/utils/check.h"

#include "crypto/vm/cp0.h"
#include "crypto/vm/boc.h"

#include "td/utils/tests.h"
#include "td/actor/actor.h"
#include "td/utils/base64.h"
#include "crypto/block/block.h"
#include "vm/cells/Cell.h"
#include "convert-utils.h"
#include "ActionDetector.h"
#include "IndexData.h"


TEST(convert, to_raw_address) {
    auto address = vm::std_boc_deserialize(td::base64_decode(td::Slice("te6ccgEBAQEAJAAAQ4ARhy+Ifz/haSyza6FGBWNSde+ZjHy+uBieS1O4PxaPVnA=")).move_as_ok()).move_as_ok();
    auto raw_address_serialized = convert::to_raw_address(vm::load_cell_slice_ref(address));
    CHECK(raw_address_serialized.is_ok());
    ASSERT_EQ("0:8C397C43F9FF0B49659B5D0A302B1A93AF7CCC63E5F5C0C4F25A9DC1F8B47AB3", raw_address_serialized.move_as_ok());
}

TEST(convert, to_raw_address_with_anycast) {
    // tx hash 692263ed0c02006a42c2570c1526dc0968e9ef36849086e7888599f5f7745f3b
    auto address = vm::std_boc_deserialize(td::base64_decode(td::Slice("te6ccgEBAQEAKAAAS74kmhnMAhgIba/WWH4XFusx+cERuqOcvI+CfNEkWIYKL2ECy/pq")).move_as_ok()).move_as_ok();
    auto raw_address_serialized = convert::to_raw_address(vm::load_cell_slice_ref(address));
    CHECK(raw_address_serialized.is_ok());
    ASSERT_EQ("0:249A19CFF5961F85C5BACC7E70446EA8E72F23E09F34491621828BD840B2FE9A", raw_address_serialized.move_as_ok());
}

TEST(convert, to_std_address) {
    auto address = vm::std_boc_deserialize(td::base64_decode(td::Slice("te6ccgEBAQEAJAAAQ4ARhy+Ifz/haSyza6FGBWNSde+ZjHy+uBieS1O4PxaPVnA=")).move_as_ok()).move_as_ok();
    auto std_address_serialized = convert::to_std_address(vm::load_cell_slice_ref(address));
    CHECK(std_address_serialized.is_ok());
    td::Bits256 addr;
    addr.from_hex(td::Slice("8C397C43F9FF0B49659B5D0A302B1A93AF7CCC63E5F5C0C4F25A9DC1F8B47AB3"));
    ASSERT_EQ(block::StdAddress(0, addr), std_address_serialized.move_as_ok());
}

TEST(convert, to_std_address_with_anycast) {
    auto address = vm::std_boc_deserialize(td::base64_decode(td::Slice("te6ccgEBAQEAKAAAS74kmhnMAhgIba/WWH4XFusx+cERuqOcvI+CfNEkWIYKL2ECy/pq")).move_as_ok()).move_as_ok();
    auto std_address_serialized = convert::to_std_address(vm::load_cell_slice_ref(address));
    CHECK(std_address_serialized.is_ok());
    td::Bits256 addr;
    addr.from_hex(td::Slice("249A19CFF5961F85C5BACC7E70446EA8E72F23E09F34491621828BD840B2FE9A"));
    ASSERT_EQ(block::StdAddress(0, addr), std_address_serialized.move_as_ok());
}

TEST(TonDbScanner, ActionDetector_parse_jetton_burn) {
  // message payload for tx xiOZW3mVbHkCtgLxQqXVAg4DIgxrTE3j9lHw6H3P/Yg=, correct layout
  auto message_payload = vm::load_cell_slice_ref(vm::std_boc_deserialize(td::base64_decode(
    td::Slice("te6cckEBAgEAOQABZllfB7xUbeTvz/ieq1AezMgZqAEPdvSWQKq0LY5UE5PZzQsh8ADqxh1H03XLREV/xoLz4QEAAaAxtO6I")).move_as_ok()).move_as_ok());

  auto transaction = schema::Transaction();
  transaction.account = block::StdAddress(std::string("EQCk6s76oduqQH_3Y3O7fxjWVoUXtD3Ev6-NbHjjDmfG1drE")); // jetton wallet
  transaction.in_msg = std::make_optional(schema::Message());
  transaction.in_msg->source = "0:87BB7A4B20555A16C72A09C9ECE68590F80075630EA3E9BAE5A222BFE34179F0"; // owner
  auto* descr = std::get_if<schema::TransactionDescr_ord>(&transaction.description);
  CHECK(descr != nullptr);
  descr->aborted = false;

  const std::string jetton_master_raw = "0:BDF3FA8098D129B54B4F73B5BAC5D1E1FD91EB054169C3916DFC8CCD536D1000";
  auto jetton_master_address = block::StdAddress::parse(jetton_master_raw);
  CHECK(jetton_master_address.is_ok());

  schema::JettonWalletDataV2 jetton_wallet;
  jetton_wallet.jetton = jetton_master_address.move_as_ok();

  ActionDetector detector(nullptr, td::Promise<ParsedBlockPtr>());
  auto result = detector.parse_jetton_burn(jetton_wallet, transaction, message_payload);
  CHECK(result.is_ok());
  auto burn = result.move_as_ok();
  ASSERT_EQ(transaction.in_msg->source.value(), burn.owner);
  ASSERT_EQ(convert::to_raw_address(transaction.account), burn.jetton_wallet);
  ASSERT_EQ(jetton_master_raw, burn.jetton_master);
  ASSERT_EQ(6083770390284902059, burn.query_id);
  ASSERT_TRUE(burn.custom_payload.not_null());
  ASSERT_EQ("A3FBB2B6DF19CEAA089D2794A26845822A2284D59B277469D167FFC19D00E44B", burn.custom_payload->get_hash().to_hex());
  CHECK(td::BigIntG<257>(8267792794) == **burn.amount.get());
}



int main(int argc, char **argv) {
  td::set_default_failure_signal_handler().ensure();
    auto &runner = td::TestsRunner::get_default();
  runner.run_all();
  return 0;
}
