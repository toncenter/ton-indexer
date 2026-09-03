#include "BlockTree.h"

#include "MsgParse.h"
#include "WalletRequest.h"

#include "td/utils/base64.h"
#include "vm/boc.h"
#include "vm/cellslice.h"

#include <algorithm>
#include <deque>
#include <limits>
#include <unordered_map>
#include <unordered_set>

namespace mch {

namespace {

// TonTransferMessage.encrypted_opcode — an encrypted-comment plain transfer.
constexpr std::uint32_t kEncryptedTonOpcode = 0x2167da4b;

// TonTransferBlock.comment/encrypted/comment_encoded (basic_blocks.py): parse
// the leaf's message body. `comment` is decoded (base64 when encrypted, else
// utf-8 backslashreplace with U+0000 stripped) exactly as ton_transfer_comment
// does; `encrypted` is the op == TonTransferMessage.encrypted_opcode flag;
// `comment_encoded` is set only when a comment is present AND encrypted. Any BOC
// failure == Python's inner try swallowing -> comment None, encrypted false.
struct LeafCommentInfo {
  std::optional<std::string> comment;
  bool encrypted{false};
  bool comment_encoded{false};
};

LeafCommentInfo leaf_comment_info(const EventNode *node) {
  LeafCommentInfo info;
  const Message *msg = node != nullptr ? node->msg : nullptr;
  if (msg == nullptr || !msg->content) {
    return info;
  }
  auto r_raw = td::base64_decode(td::Slice(msg->content->body));
  if (r_raw.is_error()) {
    return info;
  }
  auto r_cell = vm::std_boc_deserialize(r_raw.move_as_ok());
  if (r_cell.is_error()) {
    return info;
  }
  td::Ref<vm::Cell> cell = r_cell.move_as_ok();
  info.comment = ton_transfer_comment(cell);
  // TonTransferMessage.encrypted: op == 0x2167da4b (peek the first 32 bits).
  bool special = false;
  try {
    vm::CellSlice cs = vm::load_cell_slice_special(cell, special);
    if (cs.size() >= 32) {
      info.encrypted = static_cast<std::uint32_t>(cs.fetch_ulong(32)) == kEncryptedTonOpcode;
    }
  } catch (...) {
  }
  info.comment_encoded = info.comment.has_value() && info.encrypted;
  return info;
}

}  // namespace

// EventNode tree

EventTree to_tree(const Trace &trace) {
  EventTree tree;
  if (trace.transactions.empty()) {
    return tree;
  }

  std::vector<const Transaction *> sorted;
  for (const auto &tx : trace.transactions) {
    sorted.push_back(tx.get());
  }
  std::stable_sort(sorted.begin(), sorted.end(),
                   [](const Transaction *a, const Transaction *b) { return a->lt > b->lt; });

  std::unordered_map<std::string, std::string> msg_tx;   // in-msg hash -> owning tx hash
  std::unordered_map<std::string, EventNode *> tx_nodes;  // tx hash -> node

  auto make_node = [&tree](const Message *msg, const Transaction *tx, bool tick_tock) {
    tree.nodes.push_back(std::make_unique<EventNode>());
    EventNode *n = tree.nodes.back().get();
    n->msg = msg;
    n->tx = tx;
    n->is_tick_tock = tick_tock;
    return n;
  };

  for (const auto *tx : sorted) {
    if (tx_nodes.count(tx->hash) != 0) {
      continue;
    }
    const Message *in_msg = nullptr;
    for (const auto &m : tx->messages) {
      if (m->direction == "in") {
        in_msg = m.get();
        break;
      }
    }
    if (in_msg != nullptr) {
      msg_tx[in_msg->msg_hash] = tx->hash;
    }
    EventNode *node = make_node(in_msg, tx, tx->descr == "tick_tock");
    tx_nodes[tx->hash] = node;

    for (const auto &m : tx->messages) {
      if (m->direction != "out") {
        continue;
      }
      if (!m->destination) {
        // Notification / log leaf.
        EventNode *leaf = make_node(m.get(), tx, false);
        leaf->parent = node;
        node->children.push_back(leaf);
      } else {
        auto it = msg_tx.find(m->msg_hash);
        if (it != msg_tx.end()) {
          EventNode *child = tx_nodes[it->second];
          child->parent = node;
          node->children.push_back(child);
        }
        // Out-message with destination but no matching child tx: tolerated
        // (partial traces are legal input); counted via `unlinked` below.
      }
    }
  }

  EventNode *root = tx_nodes[sorted.back()->hash];
  while (root->parent != nullptr) {
    root = root->parent;
  }
  tree.root = root;

  // Python asserts a single root; we tolerate orphan subtrees but surface
  // them (silent loss is the worst kind of parity bug).
  std::size_t reachable = 0;
  std::deque<const EventNode *> queue{root};
  while (!queue.empty()) {
    const EventNode *cur = queue.front();
    queue.pop_front();
    reachable++;
    for (const EventNode *c : cur->children) {
      queue.push_back(c);
    }
  }
  tree.unlinked = tree.nodes.size() - reachable;
  return tree;
}

// Block

int Block::direction() const {
  if (event_nodes.size() == 1) {
    const auto *n = event_nodes.front();
    return (n->msg != nullptr && n->msg->direction == "out") ? 1 : 0;
  }
  return 2;
}

void Block::sort_next_blocks() {
  std::stable_sort(next_blocks.begin(), next_blocks.end(),
                   [](const Block *a, const Block *b) { return a->min_lt < b->min_lt; });
}

void Block::compact_connections() {
  std::unordered_set<const Block *> child_set(children_blocks.begin(), children_blocks.end());
  std::unordered_set<const Block *> seen;
  std::vector<Block *> out;
  for (Block *n : next_blocks) {
    if (child_set.count(n) != 0 || n == this) {
      continue;
    }
    Block *tp = n->topmost_parent();
    if (seen.insert(tp).second) {
      out.push_back(tp);
    }
  }
  next_blocks = std::move(out);
  sort_next_blocks();
}

void Block::insert_between(const std::vector<Block *> &targets, Block *new_block) {
  // Rewire grandchild edges: any child of this block whose next_blocks holds a
  // target has that edge redirected to new_block. Python removes the first
  // occurrence and appends new_block per (child, target) hit — mirror exactly
  // (a child can end up with new_block appended more than once).
  for (Block *child : children_blocks) {
    for (Block *t : targets) {
      auto &nb = child->next_blocks;
      auto it = std::find(nb.begin(), nb.end(), t);
      if (it != nb.end()) {
        nb.erase(it);
        nb.push_back(new_block);
      }
    }
  }
  // Drop every target from this block's next_blocks.
  std::unordered_set<const Block *> target_set(targets.begin(), targets.end());
  std::vector<Block *> kept;
  kept.reserve(next_blocks.size());
  for (Block *n : next_blocks) {
    if (target_set.count(n) == 0) {
      kept.push_back(n);
    }
  }
  next_blocks = std::move(kept);
  // Reparent each target's children that pointed back at the target.
  for (Block *t : targets) {
    for (Block *child : t->children_blocks) {
      if (child->previous_block == t) {
        child->previous_block = new_block;
      }
    }
  }
  connect(new_block);
  for (Block *t : targets) {
    new_block->connect(t);
  }
}

std::vector<Block *> Block::find_frontier(const std::vector<Block *> &in_set) const {
  std::unordered_set<const Block *> set(in_set.begin(), in_set.end());
  std::deque<Block *> queue(next_blocks.begin(), next_blocks.end());
  std::unordered_set<const Block *> seen;
  std::vector<Block *> out;
  while (!queue.empty()) {
    Block *cur = queue.front();
    queue.pop_front();
    if (set.count(cur) != 0) {
      for (Block *c : cur->next_blocks) {
        queue.push_back(c);
      }
    } else if (seen.insert(cur).second) {
      out.push_back(cur);
    }
  }
  return out;
}

void Block::calculate_min_max_lt() {
  std::int64_t lo = std::numeric_limits<std::int64_t>::max();
  for (const auto *n : event_nodes) {
    lo = std::min(lo, n->lt());
  }
  if (event_nodes.empty()) {
    lo = 0;
  }
  min_lt = lo;
  min_lt_without_initiating_tx = lo;
}

Block *ensure_earliest_common_block(const std::vector<Block *> &blocks) {
  std::vector<Block *> sorted = blocks;
  std::stable_sort(sorted.begin(), sorted.end(), [](const Block *a, const Block *b) {
    if (a->min_lt_without_initiating_tx != b->min_lt_without_initiating_tx) {
      return a->min_lt_without_initiating_tx < b->min_lt_without_initiating_tx;
    }
    return a->direction() < b->direction();
  });
  if (sorted.empty()) {
    return nullptr;
  }
  Block *earliest = sorted.front();
  std::unordered_set<const Block *> connected{earliest};
  for (Block *block : sorted) {
    if (connected.count(block) != 0) {
      continue;
    }
    if (block->previous_block != nullptr) {
      if (connected.count(block->previous_block) != 0) {
        connected.insert(block);
        continue;
      }
      return nullptr;
    }
  }
  return earliest;
}

bool Block::merge_blocks(const std::vector<Block *> &blocks) {
  std::unordered_set<const Block *> set;
  std::vector<Block *> to_merge;
  for (Block *b : blocks) {
    if (b == nullptr || set.count(b) != 0) {
      continue;
    }
    set.insert(b);
    to_merge.push_back(b);
  }
  Block *earliest = ensure_earliest_common_block(to_merge);
  if (earliest == nullptr) {
    return false;
  }
  for (Block *b : to_merge) {
    b->parent = this;
    event_nodes.insert(event_nodes.end(), b->event_nodes.begin(), b->event_nodes.end());
    children_blocks.push_back(b);
  }
  for (Block *fb : earliest->find_frontier(to_merge)) {
    next_blocks.push_back(fb);
    fb->previous_block = this;
  }
  previous_block = earliest->previous_block;
  // core.py merge_blocks: the merged block inherits initiating_event_node from
  // the earliest merged block (Step 2a) and ORs is_ghost_block over the nodes
  // it absorbed.
  initiating_event_node = earliest->initiating_event_node;
  for (const EventNode *n : event_nodes) {
    if (n->ghost) {
      is_ghost_block = true;
      break;
    }
  }
  calculate_min_max_lt();
  sort_next_blocks();
  if (earliest->previous_block != nullptr) {
    earliest->previous_block->compact_connections();
  }
  return true;
}

namespace {

// EventNode.failed in Python: message.transaction.aborted (for a tick_tock
// node: the tick-tock transaction's aborted flag). Our node->tx is exactly
// that transaction in both cases.
bool node_failed(const EventNode *node) {
  return node->forced_failed || (node->tx != nullptr && node->tx->aborted);
}

void init_leaf(Block *b, const EventNode *node) {
  b->event_nodes.push_back(node);
  b->is_ghost_block = node->ghost;  // core.py: len(nodes) == 1 and nodes[0].ghost_node
  b->min_lt = node->lt();
  b->min_lt_without_initiating_tx = node->tx != nullptr ? node->tx->lt : node->lt();
  // core.py Block.__init__: a single-node block's initiating_event_node is that
  // node's tree parent (nullptr at the root). (Step 2a.)
  b->initiating_event_node = node->parent;
}

Value account_or_null(const std::optional<std::string> &addr) {
  if (!addr) {
    return Value::null();
  }
  auto norm = normalize_raw_address(*addr);
  return Value::make_account_raw(norm ? *norm : *addr);
}

// Leaf-block data dicts mirror the basic_blocks.py constructors so a where_expr
// dotfield reads the same values as Python. extra_currencies remains omitted
// because its map is not decoded. For ton_transfer, leaf_comment_info populates
// comment, encrypted, and encoded state (`comment_encoded`).
Value leaf_data(const std::string &btype, const EventNode *node,
                std::optional<std::uint32_t> op) {
  const Message *msg = node->msg;
  if (btype == "tick_tock") {
    Value::Fields fs;
    if (node->tx != nullptr) {
      auto norm = normalize_raw_address(node->tx->account);
      fs.emplace_back("account",
                      Value::make_account_raw(norm ? *norm : node->tx->account));
    } else {
      fs.emplace_back("account", Value::null());
    }
    return Value::make_dict(std::move(fs));
  }
  Value::Fields fs;
  if (btype == "call_contract" || btype == "contract_deploy") {
    // CallContractBlock uses its recovered request opcode; ContractDeploy uses
    // EventNode.get_opcode() (the raw stored opcode), matching basic_blocks.py.
    fs.emplace_back("opcode", op ? Value::make_int64(static_cast<std::int64_t>(*op))
                                 : Value::null());
  }
  fs.emplace_back("source", account_or_null(msg != nullptr ? msg->source : std::nullopt));
  fs.emplace_back("destination",
                  account_or_null(msg != nullptr ? msg->destination : std::nullopt));
  fs.emplace_back("value", (msg != nullptr && msg->value)
                               ? Value::make_amount(td::make_refint(*msg->value))
                               : Value::make_amount_none());
  if (btype == "ton_transfer") {
    // TonTransferBlock comment/encrypted/comment_encoded: needed by the getgems
    // purchase predicate + fn and the nft_transfer_parent_absorb shaper.
    LeafCommentInfo ci = leaf_comment_info(node);
    fs.emplace_back("comment", ci.comment ? Value::make_str(*ci.comment) : Value::null());
    fs.emplace_back("encrypted", Value::make_bool(ci.encrypted));
    fs.emplace_back("comment_encoded", Value::make_bool(ci.comment_encoded));
  }
  return Value::make_dict(std::move(fs));
}

Block *make_leaf_block(BlockArena &arena, const EventNode *node) {
  const auto raw_op = node->opcode();
  auto op = raw_op;
  bool is_ton = !raw_op || *raw_op == 0 || *raw_op == kEncryptedTonOpcode;
  const Message *msg = node->msg;
  const Transaction *tx = node->tx;

  Block *b;
  if (node->is_tick_tock) {
    b = arena.make("tick_tock");
    b->failed = node_failed(node);
  } else if (is_ton && msg != nullptr && msg->destination && msg->source) {
    b = arena.make("ton_transfer");
    // TonTransferBlock failed logic (basic_blocks.py).
    if (node_failed(node)) {
      if (msg->bounce && *msg->bounce) {
        b->failed = true;
      } else if (tx != nullptr && tx->end_status == "uninit") {
        b->failed = false;
      } else if (msg->source && tx != nullptr && tx->skipped_reason &&
                 *tx->skipped_reason == "no_gas") {
        b->failed = false;  // ignore no-gas errors on incoming ton transfers
      } else {
        b->failed = true;
      }
    }
  } else {
    // A Telegram-wallet request begins with its signature. The opcode persisted
    // on the message is therefore meaningless; recover the request opcode from
    // the body only after the raw-opcode leaf-kind decision, exactly like
    // basic_blocks.get_call_contract_opcode.
    if (auto tg_op = get_tg_wallet_request_opcode(msg)) {
      op = tg_op;
    }
    b = arena.make("call_contract");
    b->opcode = op;
    b->failed = node_failed(node);
    // CallContractBlock exception: a failed valueless call with a non-zero
    // opcode and no extra currencies is not marked failed (log/event leaves).
    bool has_value = msg != nullptr && msg->value && *msg->value != 0;
    bool opcode_truthy = op && *op != 0;
    bool has_extra = msg != nullptr && msg->has_extra_currencies;
    if (b->failed && !has_value && opcode_truthy && !has_extra) {
      b->failed = false;
    }
  }
  b->data = leaf_data(b->btype, node, op);
  init_leaf(b, node);

  // GaslessRequestBlock is a marker child of the signed internal request. It
  // deliberately has no Block::parent until/unless a later merge claims it;
  // unwind_gasless_requests promotes it to the serialized spine.
  if (b->btype == "call_contract" && msg != nullptr && msg->source && op &&
      is_gasless_request_opcode(*op)) {
    Block *gasless = arena.make("gasless_request");
    gasless->failed = node_failed(node);
    gasless->data = leaf_data("gasless_request", node, std::nullopt);
    init_leaf(gasless, node);
    b->children_blocks.push_back(gasless);
  }

  // ContractDeploy side-effect blocks live in children_blocks after the
  // gasless marker, matching CallContractBlock's append order.
  if (!node->is_tick_tock && tx != nullptr && tx->end_status == "active" &&
      tx->orig_status != "active" && tx->orig_status != "frozen") {
    Block *deploy = arena.make("contract_deploy");
    deploy->opcode = raw_op;
    deploy->failed = node_failed(node);
    deploy->data = leaf_data("contract_deploy", node, raw_op);
    init_leaf(deploy, node);
    // NB: Python does not set .parent here — only merge_blocks does.
    b->children_blocks.push_back(deploy);
  }
  return b;
}

}  // namespace

Block *init_block(BlockArena &arena, const EventNode *node) {
  // Iterative DFS: trace depth is unbounded, recursion is not.
  Block *root = make_leaf_block(arena, node);
  std::vector<std::pair<const EventNode *, Block *>> stack{{node, root}};
  while (!stack.empty()) {
    auto [cur, block] = stack.back();
    stack.pop_back();
    for (const EventNode *c : cur->children) {
      Block *child = make_leaf_block(arena, c);
      block->connect(child);
      stack.emplace_back(c, child);
    }
    block->sort_next_blocks();
  }
  return root;
}

}  // namespace mch
