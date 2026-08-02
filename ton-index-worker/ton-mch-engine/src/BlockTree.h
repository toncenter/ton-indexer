// EventNode tree and Block substrate. Outgoing edges remain in stable min_lt
// order, frontier discovery deduplicates blocks, and connection compaction keeps
// first-seen order.
#pragma once

#include "TraceLoader.h"
#include "Value.h"

#include "td/utils/Status.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace mch {

// EventNode

// A node in the message tree. For a normal transaction the node's msg is the
// incoming message; for a notification/log leaf it is the destination-less
// outgoing message.
struct EventNode {
  const Message *msg{nullptr};
  const Transaction *tx{nullptr};
  bool is_tick_tock{false};
  EventNode *parent{nullptr};
  std::vector<EventNode *> children;

  // Ghost-external synthesis
  // EventNode.ghost_node is an intended but unsent message decoded from a
  // failed wallet external. The destination is not a participating account,
  // because no transaction ran there.
  bool ghost{false};
  // EventNode.failed, when init_from_external FORCES it True on the external's
  // own node and every ghost it hangs off it. Everywhere else `failed` is
  // tx->aborted, which is why node_failed() ORs rather than reads a field.
  bool forced_failed{false};

  std::optional<std::uint32_t> opcode() const {
    return msg != nullptr ? msg->opcode32() : std::nullopt;
  }

  // Mirrors EventNode.get_lt.
  std::int64_t lt() const {
    if (is_tick_tock && tx != nullptr) {
      return tx->lt;
    }
    if (msg != nullptr && msg->created_lt) {
      return *msg->created_lt;
    }
    if (msg != nullptr) {
      return msg->tx_lt;
    }
    return 0;
  }

  std::string tx_hash() const {
    if (tx != nullptr) {
      return tx->hash;
    }
    if (msg != nullptr) {
      return msg->tx_hash;
    }
    return {};
  }
};

// Owns every EventNode; nodes reference each other by raw pointer.
struct EventTree {
  std::vector<std::unique_ptr<EventNode>> nodes;
  // Messages no Transaction in the trace ever carried: the wallet-external
  // payloads ghost nodes point at (GhostExternal.h). Owned here so a ghost
  // node's `msg` has the same lifetime as the node itself.
  std::vector<std::unique_ptr<Message>> synthetic_msgs;
  EventNode *root{nullptr};
  // Nodes not reachable from root (Python asserts a single root; we tolerate
  // and count, so parity harnesses can flag silent loss).
  std::size_t unlinked{0};
};

// Ports tree_utils.to_tree: builds the EventNode tree from the flat
// transaction list using msg_hash linkage. The Trace must outlive the tree.
EventTree to_tree(const Trace &trace);

// Block

struct Block {
  std::string btype;
  std::optional<std::uint32_t> opcode;  // set for call_contract blocks

  // Python Block.data. Leaf blocks carry the basic_blocks.py constructor dicts
  // (subset: see init_block); a produced (composed) block will carry its build
  // output. Null == Python data=None. Read by where_exprs via rt_dotfield.
  Value data;

  std::vector<const EventNode *> event_nodes;
  std::vector<Block *> next_blocks;  // canonical order: (min_lt asc, stable)
  std::vector<Block *> children_blocks;
  Block *previous_block{nullptr};
  Block *parent{nullptr};

  // core.py Block.initiating_event_node: a leaf block's single node's tree
  // parent; a merged block inherits it from the earliest merged block. Drives
  // the serializer's account assembly and tx_hashes extension.
  const EventNode *initiating_event_node{nullptr};
  // core.py is_ghost_block: true iff a node behind this block is a ghost (an
  // intended-but-not-started op). Only the ghost-external fallback produces
  // them; on the main spine it stays false. Gate for the destination accounts.
  bool is_ghost_block{false};

  bool failed{false};
  bool broken{false};

  std::int64_t min_lt{0};
  std::int64_t min_lt_without_initiating_tx{0};

  // Ports Block.connect.
  void connect(Block *other) {
    next_blocks.push_back(other);
    other->previous_block = this;
  }

  Block *topmost_parent() {
    Block *b = this;
    while (b->parent != nullptr) {
      b = b->parent;
    }
    return b;
  }

  // Re-establishes the canonical next_blocks order (min_lt asc, stable).
  void sort_next_blocks();

  // Ports Block.compact_connections (deterministic: preserves first-seen
  // order instead of Python's set()).
  void compact_connections();

  // Ports Block.insert_between (core.py:244): splices new_block between this
  // and each block in `targets` (each of which must currently be in
  // next_blocks). Grandchild edges from this block's children that reached a
  // target are redirected to new_block; each target's children that pointed
  // back at the target are reparented to new_block. Used by shapers
  // (host/HostGetgems.cpp getgems_proxy_insert). No sort: mirrors Python's
  // append-only connect() exactly so topology parity holds.
  void insert_between(const std::vector<Block *> &targets, Block *new_block);

  // Ports Block.merge_blocks. Returns false when the merged set has no
  // earliest common block (Python raises -> clean build rejection). Must be
  // called only AFTER a build accepts: merges are never rolled back.
  bool merge_blocks(const std::vector<Block *> &blocks);

  std::string anchor_tx_hash() const {
    return event_nodes.empty() ? std::string{} : event_nodes.front()->tx_hash();
  }

  // Recompute min/max lt from the current event_nodes. Post-processing calls this
  // after absorbing a paired leg's nodes). Public: post-process is host-side.
  void calculate_min_max_lt();

 private:
  int direction() const;
  std::vector<Block *> find_frontier(const std::vector<Block *> &in_set) const;

  friend Block *ensure_earliest_common_block(const std::vector<Block *> &blocks);
};

// Owns every Block; blocks reference each other by raw pointer.
struct BlockArena {
  std::vector<std::unique_ptr<Block>> blocks;

  Block *make(std::string btype) {
    blocks.push_back(std::make_unique<Block>());
    blocks.back()->btype = std::move(btype);
    return blocks.back().get();
  }
};

// Ports event_processing.init_block: classify each event node into a leaf
// block (tick_tock / ton_transfer / call_contract) and connect the subtree.
// Iterative (trace depth is unbounded); leaves next_blocks canonical.
Block *init_block(BlockArena &arena, const EventNode *node);

// TraceContext

// Bundles the chained-lifetime objects (Trace -> EventTree -> BlockArena) so
// they cannot be torn apart. Everything in the arena points into the tree,
// which points into the trace.
struct TraceContext {
  Trace trace;
  EventTree tree;
  BlockArena arena;
  Block *root{nullptr};
};

}  // namespace mch
