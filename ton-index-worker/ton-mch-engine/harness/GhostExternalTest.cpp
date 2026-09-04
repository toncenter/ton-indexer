// --ghost-external-test: exercises wallet-request opcode recovery, gasless
// marker construction and ghost synthesis over synthetic wallet bodies. No
// fixture corpus, database or node is needed.
#include "GhostExternalTest.h"

#include "ActionBuild.h"
#include "BlockTree.h"
#include "ClassifyCore.h"
#include "GenMatchers.h"
#include "GhostExternal.h"
#include "TraceLoader.h"
#include "WalletRequest.h"
#include "fixtures/FixtureLookupSource.h"

#include "td/utils/base64.h"
#include "vm/boc.h"
#include "vm/cells/CellBuilder.h"

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace mch {
namespace {

constexpr std::uint8_t kSendMode = 3;

int g_fail = 0;

void check(const std::string &name, bool ok) {
  std::printf("%s %s\n", ok ? "PASS" : "FAIL", name.c_str());
  if (!ok) g_fail++;
}

// A minimal MessageRelaxed accepted by parse_payload: int_msg_info, addr_none
// source/destination, zero CurrencyCollection and fees, no StateInit, body
// (just the opcode) in a ref.
td::Ref<vm::Cell> payload(std::uint32_t opcode) {
  vm::CellBuilder body;
  body.store_long(opcode, 32);
  vm::CellBuilder msg;
  msg.store_zeroes(1)         // int_msg_info$0
      .store_ones(1)          // ihr_disabled
      .store_zeroes(2)        // bounce, bounced
      .store_zeroes(2)        // src:addr_none
      .store_zeroes(2)        // dest:addr_none
      .store_zeroes(4)        // grams:VarUInteger 16 = 0
      .store_zeroes(1)        // extra currencies absent
      .store_zeroes(4)        // ihr_fee = 0
      .store_zeroes(4)        // fwd_fee = 0
      .store_zeroes(64 + 32)  // created_lt, created_at
      .store_zeroes(1)        // StateInit absent
      .store_ones(1)          // body is in a ref
      .store_ref(body.finalize());
  return msg.finalize();
}

void store_signed_request_header(vm::CellBuilder &body, std::uint32_t opcode) {
  body.store_zeroes(512)  // signature
      .store_long(opcode, 32)
      .store_long(0x11223344, 32)   // subwallet_id
      .store_long(0x55667788, 32)   // valid_until
      .store_long(0x99AABBCC, 32);  // seqno
}

td::Ref<vm::Cell> send_one_body(const td::Ref<vm::Cell> &message) {
  vm::CellBuilder body;
  store_signed_request_header(body, kTgWalletSendOneMessageExternal);
  body.store_long(kSendMode, 8).store_ref(message);
  return body.finalize();
}

// Five messages over two chunks: the head spends one ref on the terminal chunk
// and carries item 0; the terminal chunk carries items 1..4. Traversal must
// still yield messages in their original order.
td::Ref<vm::Cell> five_message_bulk_body(const std::vector<td::Ref<vm::Cell>> &messages,
                                         std::uint8_t declared_length) {
  vm::CellBuilder tail;
  tail.store_zeroes(1);  // no next chunk
  for (std::size_t i = 1; i < 5; ++i) {
    tail.store_long(kSendMode, 8).store_ref(messages[i]);
  }
  vm::CellBuilder head;
  head.store_ones(1).store_ref(tail.finalize());
  head.store_long(kSendMode, 8).store_ref(messages[0]);
  vm::CellBuilder body;
  store_signed_request_header(body, kTgWalletSendBulkMessagesExternal);
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
  if (serialized.is_error()) return {};
  return td::base64_encode(td::Slice(serialized.ok()));
}

// A lone external-in message on one transaction, as the ghost path sees it.
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

  std::size_t synthesize() { return synthesize_ghost_children(tree, tree.root); }
};

std::vector<std::uint32_t> child_opcodes(const SynthesisCase &c) {
  std::vector<std::uint32_t> out;
  for (const EventNode *child : c.tree.root->children) {
    if (child == nullptr || child->msg == nullptr || !child->msg->opcode32()) continue;
    out.push_back(*child->msg->opcode32());
  }
  return out;
}

Block *child_with_type(Block *block, const std::string &btype) {
  for (Block *child : block->children_blocks) {
    if (child->btype == btype) return child;
  }
  return nullptr;
}

void test_send_one() {
  SynthesisCase c(send_one_body(payload(0x01020304)));
  check("tg_external/send_one/count", c.synthesize() == 1);
  check("tg_external/send_one/opcode", child_opcodes(c) == std::vector<std::uint32_t>{0x01020304});
  check("tg_external/send_one/root_failed", c.tree.root->forced_failed);
  check("tg_external/send_one/ghost_child",
        c.tree.root->children.size() == 1 && c.tree.root->children[0]->ghost &&
            c.tree.root->children[0]->forced_failed);
}

void test_bulk_chunk_order_and_ignored_count() {
  const std::vector<std::uint32_t> expected = {0x10000000, 0x10000001, 0x10000002, 0x10000003,
                                               0x10000004};
  std::vector<td::Ref<vm::Cell>> messages;
  for (std::uint32_t opcode : expected) messages.push_back(payload(opcode));
  // The declared length deliberately disagrees with the five encoded items:
  // the byte is consumed, validation is the wallet contract's job.
  SynthesisCase c(five_message_bulk_body(messages, /*declared_length=*/1));
  check("tg_external/bulk/count", c.synthesize() == expected.size());
  check("tg_external/bulk/chunk_order", child_opcodes(c) == expected);
}

void test_no_payload_requests_and_malformed_bodies() {
  {
    SynthesisCase c(header_only_body(kTgWalletChangePublicKeyExternal));
    check("tg_external/change_key/no_ghosts", c.synthesize() == 0 && c.tree.root->children.empty());
  }
  {
    SynthesisCase c(header_only_body(kTgWalletSendOneMessageExternal));
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

void test_opcode_recovery_and_gasless_markers() {
  const std::string source = "0:" + std::string(64, 'A');
  const std::string destination = "0:" + std::string(64, 'B');
  {
    SynthesisCase c(header_only_body(kTgWalletSendOneMessageInternal));
    c.external.opcode = 0x01020304;  // signature fragment stored in the DB row
    c.external.source = source;
    c.external.destination = destination;
    c.external.value = 5;
    check("wallet_request/tg_internal/recovered",
          get_tg_wallet_request_opcode(&c.external) ==
              std::optional<std::uint32_t>(kTgWalletSendOneMessageInternal));
    BlockArena arena;
    Block *request = init_block(arena, c.tree.root);
    Block *marker = child_with_type(request, "gasless_request");
    check("wallet_request/tg_internal/call_opcode",
          request->btype == "call_contract" && request->opcode == kTgWalletSendOneMessageInternal);
    check("wallet_request/tg_internal/marker", marker != nullptr);
    check("wallet_request/tg_internal/marker_has_no_opcode",
          marker != nullptr && marker->data.field("opcode") == nullptr);
  }
  {
    SynthesisCase c(header_only_body(kTgWalletSendOneMessageExternal));
    c.external.opcode = 0x05060708;
    c.external.destination = destination;
    BlockArena arena;
    Block *request = init_block(arena, c.tree.root);
    check("wallet_request/tg_external/call_opcode",
          request->btype == "call_contract" && request->opcode == kTgWalletSendOneMessageExternal);
    check("wallet_request/tg_external/no_marker", child_with_type(request, "gasless_request") == nullptr);
  }
  {
    vm::CellBuilder body;
    body.store_long(kWalletV5SignedRequestInternal, 32);
    SynthesisCase c(body.finalize());
    c.external.opcode = kWalletV5SignedRequestInternal;
    c.external.source = source;
    c.external.destination = destination;
    check("wallet_request/v5/not_tg", get_tg_wallet_request_opcode(&c.external) == std::nullopt);
    BlockArena arena;
    Block *request = init_block(arena, c.tree.root);
    check("wallet_request/v5/marker", child_with_type(request, "gasless_request") != nullptr);
  }
}

// A failed ChangePublicKey external classifies through the real matcher table
// into a failed change_wallet_key action (no ghosts: the request carries no
// messages).
void test_failed_external_change_key() {
  TraceContext ctx;
  ctx.trace.trace_id = "failed-change-key-trace";
  auto tx = std::make_unique<Transaction>();
  tx->hash = "failed-change-key-tx";
  tx->lt = 100;
  tx->now = 200;
  tx->mc_block_seqno = 300;
  tx->account = "0:" + std::string(64, 'B');
  tx->descr = "ord";
  tx->orig_status = "active";
  tx->end_status = "active";
  tx->aborted = true;
  auto request = std::make_unique<Message>();
  request->msg_hash = "failed-change-key-message";
  request->tx_hash = tx->hash;
  request->tx_lt = tx->lt;
  request->direction = "in";
  request->destination = tx->account;
  request->opcode = 0x01020304;  // signature fragment stored in the DB row
  request->content =
      MsgContent{"body-hash", boc_base64(header_only_body(kTgWalletChangePublicKeyExternal))};
  request->tx = tx.get();
  tx->messages.push_back(std::move(request));
  ctx.trace.transactions.push_back(std::move(tx));
  ctx.tree = to_tree(ctx.trace);
  ctx.root = init_block(ctx.arena, ctx.tree.root);
  check("wallet_request/change_key/aborted_leaf_is_normalized", ctx.root != nullptr && !ctx.root->failed);

  const std::vector<CompiledMatcher> &matchers = gen_matchers_ir();
  ClassifySetup setup = prepare_classify(matchers);
  FixtureLookupSource source(&ctx.trace.interfaces);
  ClassifyResult result = classify_trace(ctx, matchers, setup, source);
  bool found = false;
  bool success = true;
  for (const ActionRow &row : result.action_rows) {
    if (row.block == nullptr || row.block->btype != "change_wallet_key") continue;
    Action action;
    if (build_action(row, action)) {
      found = true;
      success = action.success;
    }
  }
  check("wallet_request/change_key/aborted_action_found", found);
  check("wallet_request/change_key/aborted_action_failed", found && !success);
}

}  // namespace

int run_ghost_external_test() {
  g_fail = 0;
  test_send_one();
  test_bulk_chunk_order_and_ignored_count();
  test_no_payload_requests_and_malformed_bodies();
  test_opcode_recovery_and_gasless_markers();
  test_failed_external_change_key();
  std::printf("GHOST-EXTERNAL-TEST %s\n", g_fail == 0 ? "ALL PASS" : "FAILURES");
  return g_fail == 0 ? 0 : 1;
}

}  // namespace mch
