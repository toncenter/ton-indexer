#include "ClassifyCore.h"

#include "BuildDriver.h"
#include "ExprRuntime.h"
#include "GenBuilds.h"
#include "GenMatchers.h"
#include "GenWheres.h"
#include "GhostExternal.h"
#include "HostRegistry.h"
#include "Walker.h"

#include <algorithm>
#include <cstdint>
#include <deque>
#include <exception>
#include <limits>
#include <map>
#include <optional>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>

namespace mch {

namespace {

constexpr std::uint32_t kExcessOpcode = 0xD53276DB;
constexpr std::uint32_t kBounceOpcode = 0xFFFFFFFF;

// The flat spine list in canonical BFS order: `root` plus everything reachable
// through next_blocks. Every consumer of "the spine" (prefilter inventory,
// post-process input, basic-actions fallback) wants exactly this.
//
// This is not usable for the matcher walk, which must read next_blocks after
// try_build ran on the node, because a merge mutates the tree mid-walk (Python
// generator resume order).
std::vector<Block *> collect_spine(Block *root) {
  std::vector<Block *> out;
  std::deque<Block *> queue{root};
  while (!queue.empty()) {
    Block *cur = queue.front();
    queue.pop_front();
    out.push_back(cur);
    for (Block *n : cur->next_blocks) {
      queue.push_back(n);
    }
  }
  return out;
}

bool try_build(const CompiledMatcher &m, BuildOutcome (*fn)(BuildEnv &), Block *block,
               BlockArena &arena, const std::set<std::string> &kinds,
               const LookupSource &src, const WhereTable &wheres,
               std::vector<CoreAction> &out) {
  auto res = matcher_match(m, block, wheres);  // root absorb=false
  if (!res) {
    return false;
  }
  std::vector<Value> slots = slots_from_captures(res->captures);
  std::vector<Value> consumed_vals;
  for (Block *b : res->consumed) {
    consumed_vals.push_back(Value::make_block(b));
  }
  bool needs_lookups = !(m.ref_lookups.empty() && m.ref_fns.empty());
  RejectCtx rctx = reject_ctx();
  rctx.matcher = &m.name;
  RejectScope reject_scope(rctx);
  BuildOutcome o = run_two_phase(fn, block, slots,
                                 Value::make_list(std::move(consumed_vals)), kinds, src,
                                 needs_lookups);
  if (o.is_rejected) {
    return false;
  }

  Block *produced = arena.make(o.btype.empty() ? m.produces.front() : o.btype);
  produced->data = o.data;
  produced->failed = o.failed;
  produced->broken = o.broken;
  if (!produced->merge_blocks(res->consumed)) {
    return false;  // Python: merge raises -> try_build catches -> clean rejection
  }

  if (!m.ref_shapers.empty()) {
    const std::string &shaper_name = *m.ref_shapers.begin();
    auto sit = host_shapers().find(shaper_name);
    if (sit != host_shapers().end()) {
      ShaperMatch sm;
      sm.arena = &arena;
      sm.consumed = res->consumed;
      for (const Capture &c : res->captures) {
        sm.captures[c.name] = c.vals.empty() ? nullptr : c.vals.front();
      }
      sit->second(produced, sm);
    }
  }

  // Root-level excess/bounce auto-append (engine.py try_build): from the
  // ANCHOR's next_blocks (unchanged by the merge), canonical order.
  std::vector<Block *> aux;
  {
    std::vector<Block *> nb = block->next_blocks;
    std::stable_sort(nb.begin(), nb.end(),
                     [](const Block *a, const Block *b) { return a->min_lt < b->min_lt; });
    for (Block *n : nb) {
      if (n->btype != "call_contract" || !n->opcode) {
        continue;
      }
      if ((*n->opcode == kExcessOpcode && m.include_excess) ||
          (*n->opcode == kBounceOpcode && m.include_bounces)) {
        aux.push_back(n);
      }
    }
  }

  out.push_back(CoreAction{m.name, block, produced, std::move(aux)});
  return true;
}

// Host post-processing passes
// Value flows and contract-deployment collection are outside this pipeline.
// All surgery is throw-safe (no aborts).

Value pp_field(const Value &d, const std::string &k) {
  const Value *v = d.field(k);
  return v != nullptr ? *v : Value::null();
}
bool pp_eq(const Value &a, const Value &b) {
  EvalResult r = rt_eq(a, b);
  return !r.faulted && r.value.t == VType::Bool && r.value.boolean;
}

// blocks/liquidity.py combine_deposits: merge the partial (first) + final
// deposit records. Throws on the consistency checks Python raises on (caller
// catches -> the pair is left unmerged, matching a Python raise's net effect).
Value combine_deposits(const Value &first, const Value &final_d) {
  Value tf1 = pp_field(first, "target_asset_1"), tf2 = pp_field(first, "target_asset_2");
  Value tg1 = pp_field(final_d, "target_asset_1"), tg2 = pp_field(final_d, "target_asset_2");
  bool same = (pp_eq(tf1, tg1) && pp_eq(tf2, tg2)) || (pp_eq(tf1, tg2) && pp_eq(tf2, tg1));
  if (!same) throw std::runtime_error("combine_deposits: target assets mismatch");

  struct Tup { Value asset, amount, wallet; };
  std::vector<Tup> tuples;
  auto scan = [&](const Value &d, bool merge) {
    for (int i = 1; i <= 2; i++) {
      std::string si = std::to_string(i);
      Value asset = pp_field(d, "asset_" + si);
      if (asset.is_null()) continue;
      Value amount = pp_field(d, "amount_" + si);
      Value wallet = pp_field(d, "user_jetton_wallet_" + si);
      if (merge) {
        int idx = -1;
        for (std::size_t j = 0; j < tuples.size(); j++) {
          if (pp_eq(tuples[j].asset, asset)) { idx = static_cast<int>(j); break; }
        }
        if (idx >= 0) {
          if (!pp_eq(tuples[idx].wallet, wallet)) {
            throw std::runtime_error("combine_deposits: user wallet mismatch");
          }
          EvalResult s = rt_add(tuples[idx].amount, amount);
          if (s.faulted) throw std::runtime_error("combine_deposits: amount add faulted");
          tuples[idx].amount = s.value;
          if (!wallet.is_null()) tuples[idx].wallet = wallet;
          continue;
        }
      }
      tuples.push_back({asset, amount, wallet});
    }
  };
  scan(first, false);
  scan(final_d, true);

  if (tuples.size() != 2) throw std::runtime_error("combine_deposits: expected 2 unique assets");
  bool ut = (pp_eq(tuples[0].asset, tf1) && pp_eq(tuples[1].asset, tf2)) ||
            (pp_eq(tuples[0].asset, tf2) && pp_eq(tuples[1].asset, tf1));
  if (!ut) throw std::runtime_error("combine_deposits: assets don't match target");

  Value::Fields out;
  if (final_d.fields) out = *final_d.fields;
  auto set_field = [&](const std::string &k, Value v) {
    for (auto &kv : out) {
      if (kv.first == k) { kv.second = std::move(v); return; }
    }
    out.emplace_back(k, std::move(v));
  };
  for (int i = 0; i < 2; i++) {
    std::string si = std::to_string(i + 1);
    set_field("asset_" + si, tuples[i].asset);
    set_field("amount_" + si, tuples[i].amount);
    if (!tuples[i].wallet.is_null()) set_field("user_jetton_wallet_" + si, tuples[i].wallet);
  }
  set_field("target_asset_1", tuples[0].asset);
  set_field("target_asset_2", tuples[1].asset);
  return Value::make_dict(std::move(out));
}

void post_process_dedust_liquidity(std::vector<Block *> &blocks, std::set<Block *> &removed) {
  std::vector<Block *> firsts, finals;
  std::map<std::string, int> used;
  auto contract_key = [](const Block *b) {
    Value c = pp_field(b->data, "deposit_contract");
    return c.t == VType::Account ? (c.addr_none ? std::string("addr_none") : c.str) : c.describe();
  };
  for (Block *b : blocks) {
    if (b->btype == "dedust_deposit_liquidity_partial") {
      firsts.push_back(b);
      used[contract_key(b)]++;
    } else if (b->btype == "dedust_deposit_liquidity") {
      finals.push_back(b);
      used[contract_key(b)]++;
    }
  }
  for (const auto &kv : used) {
    if (kv.second > 2) return;  // logger.warning + skip merging
  }
  for (Block *first : firsts) {
    Block *final_b = nullptr;
    for (Block *b : finals) {
      if (contract_key(b) == contract_key(first)) { final_b = b; break; }
    }
    if (final_b == nullptr) continue;
    Value merged;
    try {
      merged = combine_deposits(first->data, final_b->data);
    } catch (const std::exception &) {
      continue;  // Python raise -> pair unmerged (unreached over corpus)
    }
    blocks.erase(std::remove(blocks.begin(), blocks.end(), first), blocks.end());
    removed.insert(first);
    for (const EventNode *n : first->event_nodes) final_b->event_nodes.push_back(n);
    final_b->data = std::move(merged);
    for (Block *cb : first->children_blocks) final_b->children_blocks.push_back(cb);
    // Python does `list(set(children_blocks))` here, i.e. an order nothing
    // downstream can rely on, but the C++ substrate has to pick SOME order,
    // and sorting raw Block* sorted by ARENA ADDRESS: run-to-run stable within
    // one process, arbitrary across processes and across arena layouts. The
    // order reaches action output (unwind_deployments, the fallback rows), so
    // it must be a property of the trace, not of the allocator. Key: earliest
    // event lt, then btype, then the anchor tx hash; the pointer stays as the
    // final tiebreak so the ordering is total and the std::unique below still
    // collapses duplicates of the SAME block (identical key -> adjacent).
    std::sort(final_b->children_blocks.begin(), final_b->children_blocks.end(),
              [](const Block *a, const Block *b) {
                if (a->min_lt != b->min_lt) return a->min_lt < b->min_lt;
                if (a->btype != b->btype) return a->btype < b->btype;
                std::string ha = a->anchor_tx_hash(), hb = b->anchor_tx_hash();
                if (ha != hb) return ha < hb;
                return a < b;
              });
    final_b->children_blocks.erase(
        std::unique(final_b->children_blocks.begin(), final_b->children_blocks.end()),
        final_b->children_blocks.end());
    final_b->calculate_min_max_lt();
  }
}

// Pair DeDust V2 liquidity blocks by deposit escrow address. Only one asset leg
// reaches the completing block; the other remains a partial and is folded in.
// This remains separate from the V1 arithmetic merge because it fills null slots
// and scans final blocks first, which determines the winner among duplicates.
void post_process_dedust_v2_liquidity(std::vector<Block *> &blocks,
                                      std::set<Block *> &removed) {
  std::vector<Block *> partials, finals;
  std::map<std::string, int> used;
  auto contract_key = [](const Block *b) {
    Value c = pp_field(b->data, "deposit_contract");
    return c.t == VType::Account ? (c.addr_none ? std::string("addr_none") : c.str) : c.describe();
  };
  for (Block *b : blocks) {
    if (b->btype == "dedust_v2_deposit_liquidity_partial") {
      partials.push_back(b);
      used[contract_key(b)]++;
    } else if (b->btype == "dedust_v2_deposit_liquidity") {
      finals.push_back(b);
      used[contract_key(b)]++;
    }
  }
  if (partials.empty()) return;
  for (const auto &kv : used) {
    if (kv.second > 2) return;  // logger.warning + skip merging
  }
  for (Block *final_b : finals) {
    Block *partial = nullptr;
    for (Block *p : partials) {
      // Python's `p in blocks`: a partial already folded into an earlier final
      // is gone from the spine and must not be paired a second time.
      if (std::find(blocks.begin(), blocks.end(), p) == blocks.end()) continue;
      if (contract_key(p) == contract_key(final_b)) { partial = p; break; }
    }
    if (partial == nullptr) continue;

    Value::Fields out;
    if (final_b->data.fields) out = *final_b->data.fields;
    auto set_field = [&](const std::string &k, Value v) {
      for (auto &kv : out) {
        if (kv.first == k) { kv.second = std::move(v); return; }
      }
      out.emplace_back(k, std::move(v));
    };
    for (int i = 1; i <= 2; i++) {
      const std::string si = std::to_string(i);
      if (!pp_field(final_b->data, "asset_" + si).is_null()) continue;
      Value partial_asset = pp_field(partial->data, "asset_" + si);
      if (partial_asset.is_null()) continue;
      set_field("asset_" + si, std::move(partial_asset));
      set_field("sender_wallet_" + si, pp_field(partial->data, "sender_wallet_" + si));
      if (pp_field(final_b->data, "amount_" + si).is_null()) {
        set_field("amount_" + si, pp_field(partial->data, "amount_" + si));
      }
    }
    final_b->data = Value::make_dict(std::move(out));

    blocks.erase(std::remove(blocks.begin(), blocks.end(), partial), blocks.end());
    removed.insert(partial);
    // Extend event_nodes without appending initiating_event_node or deduplicating.
    // Keep this aligned with the V1 pass because both feed action account and
    // transaction-hash derivation.
    for (const EventNode *n : partial->event_nodes) final_b->event_nodes.push_back(n);
    for (Block *cb : partial->children_blocks) final_b->children_blocks.push_back(cb);
    // Sort by trace properties rather than arena addresses. The pointer is only
    // a final tiebreaker, keeping the order total so std::unique removes duplicate
    // references to the same block.
    std::sort(final_b->children_blocks.begin(), final_b->children_blocks.end(),
              [](const Block *a, const Block *b) {
                if (a->min_lt != b->min_lt) return a->min_lt < b->min_lt;
                if (a->btype != b->btype) return a->btype < b->btype;
                std::string ha = a->anchor_tx_hash(), hb = b->anchor_tx_hash();
                if (ha != hb) return ha < hb;
                return a < b;
              });
    final_b->children_blocks.erase(
        std::unique(final_b->children_blocks.begin(), final_b->children_blocks.end()),
        final_b->children_blocks.end());
    final_b->calculate_min_max_lt();
  }
}

// blocks/liquidity.py post_process_stonfi_v2_liquidity pairing pass. ZERO
// corpus coverage, the `< 2 legs` guard returns immediately for every trace.
// KEEP (do not delete as dead): it is the port of a live Python production pass;
// gating it needs a 2-leg stonfi_v2 deposit fixture the corpus does not have yet.
bool is_stonfi_v2_leg(const Block *b) {
  if (b->btype != "dex_deposit_liquidity") return false;
  Value dex = pp_field(b->data, "dex");
  return dex.t == VType::Str && dex.str == "stonfi_v2";
}
std::pair<int, std::int64_t> stonfi_v2_fire_order(const Block *b) {
  const Block *anchor = nullptr;
  for (Block *c : b->children_blocks) {
    if (c->opcode && *c->opcode == 0x37c096dfu) { anchor = c; break; }
  }
  int depth = 0;
  for (const Block *cur = anchor; cur != nullptr && cur->previous_block != nullptr;
       cur = cur->previous_block) {
    depth++;
  }
  return {depth, b->min_lt};
}
Block *stonfi_v2_leg_head(Block *block, Block *proxied) {
  for (Block *c : block->children_blocks) {
    if (c->previous_block == proxied) return c;
  }
  return nullptr;
}
void stonfi_v2_data_merge(Block *final_b, Block *partial) {
  auto set = [&](const std::string &k, Value v) {
    if (!final_b->data.fields) return;
    for (auto &kv : *final_b->data.fields) {
      if (kv.first == k) { kv.second = std::move(v); return; }
    }
    final_b->data.fields->emplace_back(k, std::move(v));
  };
  set("amount_2", pp_field(partial->data, "amount_1"));
  set("asset_2", pp_field(partial->data, "asset_1"));
  set("sender_wallet_2", pp_field(partial->data, "sender_wallet_1"));
  if (pp_field(final_b->data, "lp_tokens_minted").is_null()) {
    set("lp_tokens_minted", pp_field(partial->data, "lp_tokens_minted"));
  }
}
void post_process_stonfi_v2_liquidity(std::vector<Block *> &blocks, BlockArena &arena,
                                      std::set<Block *> &removed) {
  std::vector<Block *> deposits;
  for (Block *b : blocks) {
    if (is_stonfi_v2_leg(b)) deposits.push_back(b);
  }
  if (deposits.size() < 2) return;  // corpus always returns here
  std::stable_sort(deposits.begin(), deposits.end(), [](const Block *a, const Block *b) {
    return stonfi_v2_fire_order(a) < stonfi_v2_fire_order(b);
  });
  std::vector<Block *> alive;
  for (Block *b : deposits) {
    Block *a = nullptr;
    for (Block *cand : alive) {
      if (cand->previous_block != nullptr && cand->previous_block == b->previous_block) a = cand;
    }
    if (a != nullptr) {
      Value as = pp_field(a->data, "sender"), bs = pp_field(b->data, "sender");
      Value ap = pp_field(a->data, "pool"), bp = pp_field(b->data, "pool");
      Value al = pp_field(a->data, "lp_tokens_minted"), bl = pp_field(b->data, "lp_tokens_minted");
      Block *proxied = a->previous_block;
      if (pp_eq(as, bs) && pp_eq(ap, bp) && !pp_eq(al, bl)) {
        Block *in_transfer = stonfi_v2_leg_head(b, proxied);
        Block *proxy = arena.make("empty");
        if (proxied != nullptr && in_transfer != nullptr) {
          proxied->insert_between({a, in_transfer}, proxy);
        }
        b->children_blocks.push_back(a);
        b->children_blocks.push_back(proxy);
        a->parent = b;
        proxy->parent = b;
        for (const EventNode *n : a->event_nodes) b->event_nodes.push_back(n);
        b->calculate_min_max_lt();
        stonfi_v2_data_merge(b, a);
      } else {
        b->children_blocks.push_back(a);
        if (proxied != nullptr) b->children_blocks.push_back(proxied);
        a->parent = b;
        if (proxied != nullptr) proxied->parent = b;
        for (const EventNode *n : a->event_nodes) b->event_nodes.push_back(n);
        b->calculate_min_max_lt();
      }
      blocks.erase(std::remove(blocks.begin(), blocks.end(), a), blocks.end());
      alive.erase(std::remove(alive.begin(), alive.end(), a), alive.end());
      removed.insert(a);
    }
    alive.push_back(b);
  }
}

// event_processing.py unwind_deployments (3rd production post-processor, Step
// 2b): promote ContractDeploy blocks nested in children up to the top-level
// spine list. The Python `for block in blocks` loop SEES appends (it re-scans
// the promoted deploys), so the C++ index loop must too. NOTE: a no-op for the
// classify/actions DUMP, the dump emits matcher-produced blocks (`pending`),
// and this only appends to the spine, which the dump never walks; ported to
// keep the post-processor chain aligned with production.
void unwind_deployments(std::vector<Block *> &blocks) {
  std::set<Block *> visited;
  for (std::size_t i = 0; i < blocks.size(); i++) {  // index loop: sees appends
    std::deque<Block *> queue(blocks[i]->children_blocks.begin(),
                              blocks[i]->children_blocks.end());
    while (!queue.empty()) {
      Block *child = queue.front();
      queue.pop_front();
      if (child->btype == "contract_deploy" && visited.count(child) == 0) {
        blocks.push_back(child);
      } else {
        for (Block *c : child->children_blocks) queue.push_back(c);
      }
      visited.insert(child);
    }
  }
}

// event_processing.py unwind_gasless_requests: the same promotion walk as
// deployments, but for the marker child attached to every relayed signed
// wallet request. A fresh visited set is intentional: these are two distinct
// post-processors in the reference chain.
void unwind_gasless_requests(std::vector<Block *> &blocks) {
  std::set<Block *> visited;
  for (std::size_t i = 0; i < blocks.size(); i++) {  // index loop: sees appends
    std::deque<Block *> queue(blocks[i]->children_blocks.begin(),
                              blocks[i]->children_blocks.end());
    while (!queue.empty()) {
      Block *child = queue.front();
      queue.pop_front();
      if (child->btype == "gasless_request" && visited.count(child) == 0) {
        blocks.push_back(child);
      } else {
        for (Block *c : child->children_blocks) queue.push_back(c);
      }
      visited.insert(child);
    }
  }
}

// serialize_blocks' row filter (block_tree_serializer.py:1649-1655): a spine
// block becomes a row UNLESS it is the root wrapper, an `empty` proxy, or a
// call_contract whose message has a null source (the external-in leaf) or a null
// destination (the log / notification leaves).
bool serializes_to_row(const Block *b) {
  if (b->btype == "root" || b->btype == "empty") return false;
  if (b->btype != "call_contract") return true;
  return !pp_field(b->data, "source").is_null() && !pp_field(b->data, "destination").is_null();
}

// v1_ops (block_tree_serializer.py:1605-1643): the btypes whose merged children
// stay merged. Every OTHER btype gets its children expanded into rows of their
// own, the v1 API predates the composite actions, so a consumer that only knows
// the v1 set still sees the legs a teleitem_start_auction / jvault_stake / …
// absorbed. Keep in sync with the Python list; a btype missing here silently
// starts emitting child rows.
bool is_v1_op(const std::string &t) {
  static const std::set<std::string> ops{
      "call_contract",
      "contract_deploy",
      "jetton_burn",
      "tick_tock",
      "jetton_transfer",
      "nft_transfer",
      "nft_mint",
      "jetton_mint",
      "ton_transfer",
      "stake_deposit",
      "stake_withdrawal",
      "stake_withdrawal_request",
      "dex_deposit_liquidity",
      "jetton_swap",
      "change_dns",
      "delete_dns",
      "renew_dns",
      "subscribe",
      "dex_withdraw_liquidity",
      "unsubscribe",
      "election_deposit",
      "election_recover",
      "auction_bid",
      "nominator_pool_deposit",
      "nominator_pool_withdraw_request",
      "dedust_deposit_liquidity",
      "dedust_deposit_liquidity_partial",
      "dedust_v2_deposit_liquidity",
      "dedust_v2_deposit_liquidity_partial",
      "tonstakers_deposit",
      "tonstakers_withdraw_request",
      "tonstakers_withdraw",
      "ethena_withdrawal_request",
      "ethena_deposit",
      "tonco_deposit_liquidity",
      "tonco_withdraw_liquidity",
      "coffee_deposit_liquidity",
      "change_wallet_key",
      "gasless_request",
  };
  return ops.count(t) != 0;
}

const EventNode *root_event_node(const Block *b) {
  const EventNode *root = nullptr;
  for (const EventNode *n : b->event_nodes) {
    if (root == nullptr || n->lt() < root->lt()) root = n;
  }
  return root;
}

using GaslessRequestIds = std::unordered_map<const EventNode *, std::string>;

std::optional<std::string> parent_gasless_action(const Block *b,
                                                 const GaslessRequestIds &ids) {
  if (ids.empty()) return std::nullopt;
  const EventNode *node = root_event_node(b);
  if (node == nullptr) return std::nullopt;

  if (b->btype != "gasless_request" && b->btype != "call_contract") {
    auto own = ids.find(node);
    if (own != ids.end()) return own->second;
  }
  if (node->parent != nullptr) {
    auto parent = ids.find(node->parent);
    if (parent != ids.end()) return parent->second;
  }
  return std::nullopt;
}

// Link the already-flattened row set in one pass. Keeping this out of recursive
// row selection prevents gasless semantics from leaking into the generic
// parent_action_id/ancestor_type traversal.
void link_gasless_actions(std::vector<ActionRow> &rows) {
  GaslessRequestIds ids;
  for (const ActionRow &row : rows) {
    if (row.block->btype == "gasless_request") {
      if (const EventNode *node = root_event_node(row.block)) {
        ids[node] = calc_action_id(row.block);
      }
    }
  }
  for (ActionRow &row : rows) {
    row.parent_gasless_action = parent_gasless_action(row.block, ids);
  }
}

// Filter and recursively serialize action rows. Gasless causality is enriched
// only after this generic structural traversal has produced the complete set.
std::vector<ActionRow> serialize_rows_impl(const std::vector<Block *> &blocks,
                                           const std::string &parent_id) {
  std::vector<ActionRow> out;
  std::set<std::string> ids;
  for (Block *b : blocks) {
    if (!serializes_to_row(b)) continue;
    std::string id = calc_action_id(b);
    if (!parent_id.empty() && id == parent_id) continue;
    out.push_back(ActionRow{b, parent_id, {}});
    ids.insert(id);
    if (is_v1_op(b->btype)) continue;
    for (ActionRow &c : serialize_rows_impl(b->children_blocks, id)) {
      if (c.block->btype == "contract_deploy") continue;
      c.ancestor_type.push_back(b->btype);
      std::sort(c.ancestor_type.begin(), c.ancestor_type.end());
      c.ancestor_type.erase(std::unique(c.ancestor_type.begin(), c.ancestor_type.end()),
                            c.ancestor_type.end());
      // Python raises here (:1682) rather than dropping the row: two rows with
      // one action_id would collide on the actions PK, so the trace falls back.
      if (!ids.insert(calc_action_id(c.block)).second) {
        throw std::runtime_error("duplicate action id in child recursion");
      }
      out.push_back(std::move(c));
    }
  }
  return out;
}

std::vector<ActionRow> serialize_rows(const std::vector<Block *> &blocks) {
  std::vector<ActionRow> rows = serialize_rows_impl(blocks, "");
  link_gasless_actions(rows);
  return rows;
}

// Fresh leaf-only classification with no matchers, followed by post-processing
// and serialization. Any exception produces an empty fallback.
std::vector<ActionRow> basic_classify_fallback(TraceContext &ctx) {
  std::vector<ActionRow> out;
  try {
    Block *root = init_block(ctx.arena, ctx.tree.root);
    Block *wrap = ctx.arena.make("root");
    wrap->connect(root);
    std::vector<Block *> spine = collect_spine(wrap);
    std::set<Block *> removed;
    post_process_dedust_liquidity(spine, removed);
    post_process_dedust_v2_liquidity(spine, removed);
    post_process_stonfi_v2_liquidity(spine, ctx.arena, removed);
    unwind_deployments(spine);
    unwind_gasless_requests(spine);
    // Use the recursive serialization path. Basic btypes are all v1_ops, so
    // recursion normally has no additional rows to visit.
    out = serialize_rows(spine);
  } catch (...) {
    out.clear();  // Python: fallback returns [] on any exception
  }
  return out;
}

// If normal serialization is empty, synthesize rows for the unsent messages of a
// lone failed wallet external. This uses a fresh block tree and falls back to the
// unknown row on any exception.
std::vector<ActionRow> ghost_external_rows(TraceContext &ctx, const LookupSource &src) {
  std::vector<ActionRow> out;
  try {
    EventNode *root_node = ctx.tree.root;
    // "Only external in allowed": a childless root whose message is an ext-in.
    if (root_node == nullptr || !root_node->children.empty() || root_node->msg == nullptr ||
        root_node->msg->source) {
      return out;
    }
    if (synthesize_ghost_children(ctx.tree, root_node) == 0) {
      return out;
    }
    Block *b = init_block(ctx.arena, root_node);
    Block *wrapper = ctx.arena.make("root");
    wrapper->connect(b);
    // matchers_for_failed_externals over the same BFS the main loop uses
    // (next_blocks read AFTER the build, parent-is-None guard).
    std::deque<Block *> queue{wrapper};
    while (!queue.empty()) {
      Block *cur = queue.front();
      queue.pop_front();
      if (cur->parent == nullptr) {
        fallback_jetton_transfer(cur, ctx.arena, src);
      }
      for (Block *n : cur->next_blocks) {
        queue.push_back(n);
      }
    }
    std::vector<Block *> spine = collect_spine(wrapper);
    std::set<Block *> removed;
    post_process_dedust_liquidity(spine, removed);
    post_process_dedust_v2_liquidity(spine, removed);
    post_process_stonfi_v2_liquidity(spine, ctx.arena, removed);
    unwind_deployments(spine);
    unwind_gasless_requests(spine);
    // FLAT, unlike the other two paths: try_classify_unknown_trace open-codes the
    // row filter (:370-375) instead of calling serialize_blocks, so a ghost row
    // never recurses into children and never carries parent_action_id.
    for (Block *sb : spine) {
      if (serializes_to_row(sb)) out.push_back(ActionRow{sb, "", {}});
    }
  } catch (...) {
    out.clear();
  }
  return out;
}

}  // namespace

ClassifySetup prepare_classify(const std::vector<CompiledMatcher> &matchers) {
  ClassifySetup s;
  // The matcher / build / where tables are three views of ONE document, wired up
  // by the same CMake step (MCH_CLASSIFY_ARTIFACT). Three shas that disagree mean
  // the build regenerated some of them and not others, the tables would then run
  // matchers against another document's build programs, which is silent
  // wrongness, so it is a hard setup error.
  const std::string sha = gen_matchers_ir_source_sha();
  if (sha != gen_builds_ir_source_sha() || sha != gen_wheres_ir_source_sha()) {
    s.table_missing = true;
    s.error = "generated tables disagree on their source document (matchers " + sha + ", builds " +
              gen_builds_ir_source_sha() + ", wheres " + gen_wheres_ir_source_sha() +
              ") — reconfigure and rebuild ton-mch-engine";
    return s;
  }
  for (const GenWhere &g : gen_wheres_ir()) {
    s.where_fns.emplace(g.node_id, g.fn);
  }
  for (int i = 0; i < static_cast<int>(matchers.size()); i++) {
    std::string reason = match_skip_reason(matchers[i]);
    if (reason.empty()) {
      reason = build_skip_reason(matchers[i]);
    }
    if (!reason.empty()) {
      s.skips.emplace_back(matchers[i].name, reason);
      continue;
    }
    s.kinds.insert(matchers[i].ref_lookups.begin(), matchers[i].ref_lookups.end());
    s.included.push_back(i);
  }
  std::sort(s.skips.begin(), s.skips.end());
  for (const GenBuild &g : gen_builds_ir()) {
    s.build_fns.emplace(g.id, g.fn);
  }
  for (int idx : s.included) {
    const CompiledMatcher &m = matchers[idx];
    if (s.build_fns.find(m.artifact_index) == s.build_fns.end()) {
      s.fn_missing = true;
      s.error = "generated build table has no fn for matcher '" + m.name + "' (artifact index " +
                std::to_string(m.artifact_index) + ") — rebuild ton-mch-engine";
      return s;
    }
    // Where coverage: every where_expr node of a matcher the loop will run must
    // have a compiled fn, or eval_where would silently fail that node's head
    // test (a wrong match, not a loud error).
    for (const CompiledNode &n : m.nodes) {
      if (n.has_where_expr && s.where_fns.find(n.global_id) == s.where_fns.end()) {
        s.fn_missing = true;
        s.error = "generated where table has no fn for node " + std::to_string(n.global_id) +
                  " (matcher '" + m.name + "') — rebuild ton-mch-engine";
        return s;
      }
    }
  }
  return s;
}

ClassifyResult classify_trace(TraceContext &ctx, const std::vector<CompiledMatcher> &matchers,
                              const ClassifySetup &setup, const LookupSource &src) {
  ClassifyResult result;

  // Name the trace on every [mch-reject] line this classify emits (BuildRuntime.h).
  RejectCtx rctx;
  rctx.trace_id = &ctx.trace.trace_id;
  RejectScope reject_scope(rctx);

  // Synthetic root wrapper (process_event_async: Block('root').connect(...)).
  Block *wrapper = ctx.arena.make("root");
  wrapper->connect(ctx.root);

  // B1 anchor prefilter inventory: the opcodes + btypes present on the spine
  // (blocks reachable via next_blocks, the outer BFS's anchor candidates). A
  // matcher whose opcode/btype anchor intersects neither can match nothing, so
  // its whole traversal is skipped. Opcodes are STATIC (produced composite blocks
  // carry no opcode); btypes GROW as matchers fire (a later matcher may anchor on
  // a produced btype), so produced btypes are folded in after each matcher. The
  // inventory is a conservative superset, consumed blocks are never removed, so
  // a skip only ever fires when the traversal provably matches nothing.
  std::unordered_set<std::uint32_t> inv_opcodes;
  std::unordered_set<std::string> inv_btypes;
  for (Block *b : collect_spine(wrapper)) {
    inv_btypes.insert(b->btype);
    if (b->opcode) inv_opcodes.insert(*b->opcode);
  }
  // Only opcode/btype anchors are prefilterable (the two anchor kinds
  // matcher_test_self can decide from the block's own op/btype); pred and
  // never-runnable mixed anchors stay full-scan.
  auto anchor_can_match = [&](const CompiledMatcher &m) -> bool {
    if (m.anchor_kind == AnchorKind::OpcodeSet) {
      for (std::uint32_t op : m.anchor_opcodes) {
        if (inv_opcodes.count(op)) return true;
      }
      return false;
    }
    if (m.anchor_kind == AnchorKind::BType) {
      for (const std::string &bt : m.anchor_btypes) {
        if (inv_btypes.count(bt)) return true;
      }
      return false;
    }
    return true;
  };

  std::vector<CoreAction> pending;
  try {
    for (int idx : setup.included) {
      const CompiledMatcher &m = matchers[idx];
      if (!anchor_can_match(m)) {
        continue;  // No spine op/btype can satisfy this anchor; skip traversal.
      }
      auto fn = setup.build_fns.at(m.artifact_index);
      std::size_t produced_from = pending.size();
      // Python bfs_iter port: FIFO; cur's next_blocks are read AFTER try_build
      // ran on cur (generator resume order), merges mutate the tree mid-walk.
      std::deque<Block *> queue{wrapper};
      while (!queue.empty()) {
        Block *cur = queue.front();
        queue.pop_front();
        if (cur->parent == nullptr) {
          try_build(m, fn, cur, ctx.arena, setup.kinds, src, setup.where_fns, pending);
        }
        for (Block *n : cur->next_blocks) {
          queue.push_back(n);
        }
      }
      // Fold this matcher's produced btypes into the inventory so a later
      // btype-anchored matcher composing on them is not wrongly skipped.
      for (std::size_t j = produced_from; j < pending.size(); j++) {
        if (pending[j].produced != nullptr) inv_btypes.insert(pending[j].produced->btype);
      }
    }

    // Post-process the flat spine list ([root] + BFS).
    std::vector<Block *> spine = collect_spine(wrapper);
    std::set<Block *> removed;
    post_process_dedust_liquidity(spine, removed);
    post_process_dedust_v2_liquidity(spine, removed);
    post_process_stonfi_v2_liquidity(spine, ctx.arena, removed);
    unwind_deployments(spine);  // promotes nested ContractDeploy onto the spine
    unwind_gasless_requests(spine);  // promotes relayed-request marker blocks
    for (CoreAction &a : pending) {
      if (!removed.count(a.produced)) result.actions.push_back(std::move(a));
    }
    // serialize_blocks: the ROW set is the spine, not the fire list. A fire whose
    // product a later matcher consumed is off the spine and emits no row of its
    // own, unless its consumer's btype is outside v1_ops, in which case the
    // recursion brings it back as a CHILD row; a leaf no matcher consumed emits
    // a basic row.
    result.action_rows = serialize_rows(spine);
  } catch (const std::exception &e) {
    result.failure = true;
    result.failure_reason = e.what();
    // The two-phase driver throws this exact message on non-convergence, an
    // infra fault, distinct from a spec/parse engine fault. Everything else is
    // a generic engine_fault (Python's per-trace fallback boundary).
    result.failure_category =
        result.failure_reason.find("two-phase lookup did not converge") != std::string::npos
            ? FailureCategory::lookup_infra_fail
            : FailureCategory::engine_fault;
  } catch (...) {
    result.failure = true;
    result.failure_reason = "unknown exception";
    result.failure_category = FailureCategory::engine_fault;
  }
  if (result.failure) {
    result.actions.clear();  // event_processing.py discards the trace's actions
    result.action_rows.clear();
    result.fallback_rows = basic_classify_fallback(ctx);
  } else if (result.action_rows.empty() && !ctx.trace.transactions.empty()) {
    // trace_processor.py:44-46, classification succeeded and serialized nothing.
    // try_classify_unknown_trace tries the GHOST path first and only mints the
    // `unknown` row when that yields nothing too (event_processing.py:367-388).
    result.action_rows = ghost_external_rows(ctx, src);
    result.unknown_trace = result.action_rows.empty();
  }
  return result;
}

}  // namespace mch
