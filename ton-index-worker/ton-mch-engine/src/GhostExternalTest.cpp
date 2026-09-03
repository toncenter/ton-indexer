// --ghost-external-test: exercises the production synthesis entry point over
// synthetic Telegram-wallet bodies. No fixture corpus, database, or node is
// needed.
#include "GhostExternalTest.h"

#include "GhostExternal.h"

#include "td/utils/base64.h"
#include "vm/boc.h"
#include "vm/cells/CellBuilder.h"

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace mch {

namespace {

constexpr std::uint32_t kSendOneMessageExternal = 0x63896E75;
constexpr std::uint32_t kSendBulkMessagesExternal = 0x73896E75;
constexpr std::uint32_t kChangePublicKeyExternal = 0xFBBA99C8;
constexpr std::uint8_t kSendMode = 3;

int g_fail = 0;

void check(const std::string &name, bool ok) {
  std::printf("%s %s\n", ok ? "PASS" : "FAIL", name.c_str());
  if (!ok) {
    g_fail++;
  }
}

td::Ref<vm::Cell> payload(std::uint32_t opcode) {
  vm::CellBuilder body;
  body.store_long(opcode, 32);

  // A minimal MessageRelaxed accepted by PayloadMessage:
  // int_msg_info, addr_none source/destination, zero CurrencyCollection and
  // fees, no StateInit, body in a ref.
  vm::CellBuilder msg;
  msg.store_zeroes(1)        // int_msg_info$0
      .store_ones(1)         // ihr_disabled
      .store_zeroes(2)       // bounce, bounced
      .store_zeroes(2)       // src:addr_none
      .store_zeroes(2)       // dest:addr_none
      .store_zeroes(4)       // grams:VarUInteger 16 = 0
      .store_zeroes(1)       // extra currencies absent
      .store_zeroes(4)       // ihr_fee = 0
      .store_zeroes(4)       // fwd_fee = 0
      .store_zeroes(64 + 32) // created_lt, created_at
      .store_zeroes(1)       // StateInit absent
      .store_ones(1)         // body is in a ref
      .store_ref(body.finalize());
  return msg.finalize();
}

void store_signed_request_header(vm::CellBuilder &body, std::uint32_t opcode) {
  body.store_zeroes(512) // signature
      .store_long(opcode, 32)
      .store_long(0x11223344, 32)  // subwallet_id
      .store_long(0x55667788, 32)  // valid_until
      .store_long(0x99AABBCC, 32); // seqno
}

td::Ref<vm::Cell> send_one_body(const td::Ref<vm::Cell> &message) {
  vm::CellBuilder body;
  store_signed_request_header(body, kSendOneMessageExternal);
  body.store_long(kSendMode, 8).store_ref(message);
  return body.finalize();
}

td::Ref<vm::Cell> five_message_bulk_body(
    const std::vector<td::Ref<vm::Cell>> &messages, std::uint8_t declared_length) {
  // The terminal chunk can use all four refs. The head spends its first ref on
  // the terminal chunk and carries the remaining first item. Traversal must
  // still produce messages in their original 0..4 order.
  vm::CellBuilder tail;
  tail.store_zeroes(1); // no next chunk
  for (std::size_t i = 1; i < 5; ++i) {
    tail.store_long(kSendMode, 8).store_ref(messages[i]);
  }

  vm::CellBuilder head;
  head.store_ones(1).store_ref(tail.finalize());
  head.store_long(kSendMode, 8).store_ref(messages[0]);

  vm::CellBuilder body;
  store_signed_request_header(body, kSendBulkMessagesExternal);
  body.store_long(declared_length, 8).store_ones(1).store_ref(head.finalize());
  return body.finalize();
}

td::Ref<vm::Cell> header_only_body(std::uint32_t opcode) {
  vm::CellBuilder body;
  store_signed_request_header(body, opcode);
  return body.finalize();
}

std::string boc_base64(const td::Ref<vm::Cell> &cell) {
  auto serialized = vm::std_boc_serialize(cell);
  if (serialized.is_error()) {
    return {};
  }
  return td::base64_encode(td::Slice(serialized.ok()));
}

struct SynthesisCase {
  Transaction tx;
  Message external;
  EventTree tree;

  explicit SynthesisCase(const td::Ref<vm::Cell> &body) {
    tx.hash = "tx-hash";
    tx.lt = 100;
    tx.now = 200;
    tx.account = "0:" + std::string(64, 'A');

    external.msg_hash = "external-hash";
    external.tx_hash = tx.hash;
    external.tx_lt = tx.lt;
    external.direction = "in";
    external.content = MsgContent{"body-hash", boc_base64(body)};
    external.tx = &tx;

    auto root = std::make_unique<EventNode>();
    root->msg = &external;
    root->tx = &tx;
    tree.root = root.get();
    tree.nodes.push_back(std::move(root));
  }

  std::size_t synthesize() {
    return synthesize_ghost_children(tree, tree.root);
  }
};

std::vector<std::uint32_t> child_opcodes(const SynthesisCase &c) {
  std::vector<std::uint32_t> out;
  for (const EventNode *child : c.tree.root->children) {
    if (child == nullptr || child->msg == nullptr || !child->msg->opcode32()) {
      continue;
    }
    out.push_back(*child->msg->opcode32());
  }
  return out;
}

void test_send_one() {
  SynthesisCase c(send_one_body(payload(0x01020304)));
  check("tg_external/send_one/count", c.synthesize() == 1);
  check("tg_external/send_one/opcode",
        child_opcodes(c) == std::vector<std::uint32_t>{0x01020304});
  check("tg_external/send_one/root_failed", c.tree.root->forced_failed);
  check("tg_external/send_one/ghost_child",
        c.tree.root->children.size() == 1 && c.tree.root->children[0]->ghost &&
            c.tree.root->children[0]->forced_failed);
}

void test_bulk_chunk_order_and_ignored_count() {
  const std::vector<std::uint32_t> expected = {
      0x10000000, 0x10000001, 0x10000002, 0x10000003, 0x10000004,
  };
  std::vector<td::Ref<vm::Cell>> messages;
  for (std::uint32_t opcode : expected) {
    messages.push_back(payload(opcode));
  }

  // Deliberately disagree with the five encoded items. The Python reference
  // consumes this byte but leaves validation to the wallet contract.
  SynthesisCase c(five_message_bulk_body(messages, /*declared_length=*/1));
  check("tg_external/bulk/count", c.synthesize() == expected.size());
  check("tg_external/bulk/chunk_order", child_opcodes(c) == expected);
}

void test_no_payload_requests_and_malformed_bodies() {
  {
    SynthesisCase c(header_only_body(kChangePublicKeyExternal));
    check("tg_external/change_key/no_ghosts",
          c.synthesize() == 0 && c.tree.root->children.empty());
  }
  {
    SynthesisCase c(header_only_body(kSendOneMessageExternal));
    check("tg_external/send_one_truncated/no_ghosts",
          c.synthesize() == 0 && c.tree.root->children.empty());
  }
  {
    vm::CellBuilder short_body;
    short_body.store_zeroes(127);
    SynthesisCase c(short_body.finalize());
    check("tg_external/truncated_header/no_ghosts",
          c.synthesize() == 0 && c.tree.root->children.empty());
  }
  {
    SynthesisCase c(header_only_body(0xDEADBEEF));
    check("tg_external/unknown_request/no_ghosts",
          c.synthesize() == 0 && c.tree.root->children.empty());
  }
}

}  // namespace

int run_ghost_external_test() {
  g_fail = 0;
  test_send_one();
  test_bulk_chunk_order_and_ignored_count();
  test_no_payload_requests_and_malformed_bodies();
  std::printf("GHOST-EXTERNAL-TEST %s\n",
              g_fail == 0 ? "ALL PASS" : "FAILURES");
  return g_fail == 0 ? 0 : 1;
}

}  // namespace mch
