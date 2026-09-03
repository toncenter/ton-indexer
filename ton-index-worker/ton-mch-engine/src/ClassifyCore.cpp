#include "ClassifyCore.h"

#include "BuildDriver.h"
#include "EnginePrep.h"
#include "ExprRuntime.h"
#include "GenBuilds.h"
#include "GenMatchers.h"
#include "GenWheres.h"
#include "GhostExternal.h"
#include "HostRegistry.h"
#include "ParsedBlockLookupSource.h"
#include "SchemaTraceLoader.h"
#include "Walker.h"
#include "btypes_gen.h"

#include "vm/excno.hpp"

#include <algorithm>
#include <cstdint>
#include <deque>
#include <exception>
#include <limits>
#include <map>
#include <stdexcept>
#include <unordered_set>

namespace mch {

std::vector<Block *> gather_blocks(Block *root) {
  std::vector<Block *> out;
  std::unordered_set<Block *> seen;
  std::deque<Block *> queue{root};
  while (!queue.empty()) {
    Block *cur = queue.front();
    queue.pop_front();
    if (!seen.insert(cur).second) {
      continue;
    }
    out.push_back(cur);
    std::vector<Block *> next = cur->next_blocks;
    std::stable_sort(next.begin(), next.end(),
                     [](const Block *a, const Block *b) { return a->min_lt < b->min_lt; });
    for (Block *block : next) {
      queue.push_back(block);
    }
  }
  return out;
}

namespace {

Block *wrap_root(BlockArena &arena, Block *root) {
  Block *wrapper = arena.make(mch::btype::kRoot);
  wrapper->connect(root);
  return wrapper;
}

// The flat spine list in canonical BFS order: `root` plus everything reachable
// through next_blocks. Every consumer of "the spine" (prefilter inventory,
// post-process input, basic-actions fallback) wants exactly this.
//
// Not usable for the matcher walk, which must read next_blocks after try_build
// ran on the node, because a merge mutates the tree mid-walk.
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

// Sort children by trace-derived keys, using the pointer only as a total-order
// tiebreaker, then remove duplicate block references.
void sort_unique_children(std::vector<Block *> &children_blocks) {
  std::sort(children_blocks.begin(), children_blocks.end(),
            [](const Block *a, const Block *b) {
              if (a->min_lt != b->min_lt) return a->min_lt < b->min_lt;
              if (a->btype != b->btype) return a->btype < b->btype;
              std::string ha = a->anchor_tx_hash(), hb = b->anchor_tx_hash();
              if (ha != hb) return ha < hb;
              return a < b;
            });
  children_blocks.erase(std::unique(children_blocks.begin(), children_blocks.end()),
                        children_blocks.end());
}

// Scrub non-owning Block values before the TraceContext arena dies. Shared
// container payloads make this a repair, so the count tracks every replacement.
std::size_t scrub_value(Value &v) {
  if (v.t == VType::Block) {
    v = Value::null();
    return 1;
  }
  std::size_t n = 0;
  if (v.items) {
    for (Value &item : *v.items) {
      n += scrub_value(item);
    }
  }
  if (v.fields) {
    for (auto &[key, field] : *v.fields) {
      n += scrub_value(field);
    }
  }
  return n;
}

std::size_t scrub_arena_refs(Action &a) {
  std::size_t n = 0;
  for (Value *v : {
#define MCH_ACTION_REF(name) &a.name,
           MCH_ACTION_VALUE_FIELDS(MCH_ACTION_REF)
#undef MCH_ACTION_REF
       }) {
    n += scrub_value(*v);
  }
  return n;
}

bool try_build(const CompiledMatcher &m, BuildOutcome (*fn)(BuildEnv &), Block *block,
               BlockArena &arena, const std::set<std::string> &kinds,
               const LookupSource &src, const WhereTable &wheres,
               std::vector<CoreAction> &out) {
  auto res = matcher_match(m, block, wheres);
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
    return false;  // merge failure is a clean rejection
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

  // Root-level excess/bounce auto-append: from the ANCHOR's next_blocks
  // (unchanged by the merge), canonical order.
  std::vector<Block *> aux;
  {
    std::vector<Block *> nb = block->next_blocks;
    std::stable_sort(nb.begin(), nb.end(),
                     [](const Block *a, const Block *b) { return a->min_lt < b->min_lt; });
    for (Block *n : nb) {
      if (n->btype != mch::btype::kCallContract || !n->opcode) {
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

// Merge the partial (first) + final deposit records. Consistency failures
// leave this pair unmerged without affecting other actions (do not fail the
// whole trace).
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

std::string deposit_contract_key(const Block *b) {
  Value c = pp_field(b->data, "deposit_contract");
  return c.t == VType::Account ? (c.addr_none ? std::string("addr_none") : c.str) : c.describe();
}

void post_process_dedust_liquidity(std::vector<Block *> &blocks, std::set<Block *> &removed) {
  std::vector<Block *> firsts;
  std::map<std::string, int> used;
  std::map<std::string, Block *> final_by_key;
  for (Block *b : blocks) {
    if (b->btype == mch::btype::kDedustDepositLiquidityPartial) {
      firsts.push_back(b);
      used[deposit_contract_key(b)]++;
    } else if (b->btype == mch::btype::kDedustDepositLiquidity) {
      final_by_key.emplace(deposit_contract_key(b), b);
      used[deposit_contract_key(b)]++;
    }
  }
  for (const auto &kv : used) {
    if (kv.second > 2) return;  // skip merging
  }
  for (Block *first : firsts) {
    auto it = final_by_key.find(deposit_contract_key(first));
    Block *final_b = (it != final_by_key.end()) ? it->second : nullptr;
    if (final_b == nullptr) continue;
    Value merged;
    try {
      merged = combine_deposits(first->data, final_b->data);
    } catch (const std::exception &) {
      continue;  // Leave only this pair unmerged; other actions remain.
    }
    blocks.erase(std::remove(blocks.begin(), blocks.end(), first), blocks.end());
    removed.insert(first);
    for (const EventNode *n : first->event_nodes) final_b->event_nodes.push_back(n);
    final_b->data = std::move(merged);
    for (Block *cb : first->children_blocks) final_b->children_blocks.push_back(cb);
    sort_unique_children(final_b->children_blocks);
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
  std::map<std::string, Block *> partial_by_key;
  for (Block *b : blocks) {
    if (b->btype == mch::btype::kDedustV2DepositLiquidityPartial) {
      partials.push_back(b);
      partial_by_key.emplace(deposit_contract_key(b), b);
      used[deposit_contract_key(b)]++;
    } else if (b->btype == mch::btype::kDedustV2DepositLiquidity) {
      finals.push_back(b);
      used[deposit_contract_key(b)]++;
    }
  }
  if (partials.empty()) return;
  for (const auto &kv : used) {
    if (kv.second > 2) return;  // skip merging
  }
  for (Block *final_b : finals) {
    auto it = partial_by_key.find(deposit_contract_key(final_b));
    if (it == partial_by_key.end()) continue;
    Block *partial = it->second;
    partial_by_key.erase(it);

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
    // Extend event_nodes without appending initiating_event_node or
    // deduplicating. Keep aligned with the V1 pass: both feed action account
    // and transaction-hash derivation.
    for (const EventNode *n : partial->event_nodes) final_b->event_nodes.push_back(n);
    for (Block *cb : partial->children_blocks) final_b->children_blocks.push_back(cb);
    sort_unique_children(final_b->children_blocks);
    final_b->calculate_min_max_lt();
  }
}

// Stonfi v2 liquidity pairing. ZERO corpus coverage: the `< 2 legs` guard
// returns immediately for every trace. KEEP; exercising it needs a 2-leg
// fixture the corpus does not have yet.
bool is_stonfi_v2_leg(const Block *b) {
  if (b->btype != mch::btype::kDexDepositLiquidity) return false;
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
        Block *proxy = arena.make(mch::btype::kEmpty);
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

// Promote marker blocks of one btype (contract_deploy, gasless_request) nested
// in children to the top-level spine. The index loop must see appends (it
// re-scans the promoted markers). A no-op for the classify/actions dump, which
// never walks the spine.
void unwind_markers(std::vector<Block *> &blocks, const char *btype) {
  std::set<Block *> visited;
  for (std::size_t i = 0; i < blocks.size(); i++) {  // index loop: sees appends
    std::deque<Block *> queue(blocks[i]->children_blocks.begin(),
                              blocks[i]->children_blocks.end());
    while (!queue.empty()) {
      Block *child = queue.front();
      queue.pop_front();
      if (child->btype == btype && visited.count(child) == 0) {
        blocks.push_back(child);
      } else {
        for (Block *c : child->children_blocks) queue.push_back(c);
      }
      visited.insert(child);
    }
  }
}

// Order is load-bearing: V1 merge before V2 pairing, markers unwound last.
void run_post_processors(std::vector<Block *> &spine, BlockArena &arena,
                         std::set<Block *> &removed) {
  post_process_dedust_liquidity(spine, removed);
  post_process_dedust_v2_liquidity(spine, removed);
  post_process_stonfi_v2_liquidity(spine, arena, removed);
  unwind_markers(spine, mch::btype::kContractDeploy);
  unwind_markers(spine, mch::btype::kGaslessRequest);
}

// A spine block becomes a row unless it is the root wrapper, an `empty`
// proxy, or a call_contract whose message has a null source (external-in)
// or a null destination (log / notification).
bool serializes_to_row(const Block *b) {
  if (b->btype == mch::btype::kRoot || b->btype == mch::btype::kEmpty) return false;
  if (b->btype != mch::btype::kCallContract) return true;
  return !pp_field(b->data, "source").is_null() && !pp_field(b->data, "destination").is_null();
}

// Btypes whose merged children stay merged. Every other btype expands
// children into their own rows so a v1 consumer still sees absorbed legs.
// A btype missing here silently starts emitting child rows.
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

// Links every row that is the immediate result of a relayed (gas-free) request
// to its gasless_request marker: the request message itself (when a matcher
// merged it into a composite) and its direct children. Anything deeper is the
// receiving wallet's own step. Nested relaying chains marker to marker.
void link_gasless_parents(const std::vector<Block *> &spine, std::vector<ActionRow> &rows) {
  std::map<const EventNode *, std::string> marker_ids;
  for (const Block *b : spine) {
    if (b->btype == mch::btype::kGaslessRequest) marker_ids[root_event_node(b)] = calc_action_id(b);
  }
  if (marker_ids.empty()) return;
  for (ActionRow &row : rows) {
    const EventNode *node = root_event_node(row.block);
    const std::string &t = row.block->btype;
    auto it = marker_ids.end();
    if (t != mch::btype::kGaslessRequest && t != mch::btype::kCallContract) {
      it = marker_ids.find(node);
    }
    if (it == marker_ids.end() && node != nullptr && node->parent != nullptr) {
      it = marker_ids.find(node->parent);
    }
    if (it != marker_ids.end()) row.parent_gasless_action = it->second;
  }
}

// Filter and recursively serialize action rows.
std::vector<ActionRow> serialize_rows(const std::vector<Block *> &blocks,
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
    for (ActionRow &c : serialize_rows(b->children_blocks, id)) {
      // Markers were unwound onto the spine; a second copy here would collide.
      if (c.block->btype == mch::btype::kContractDeploy ||
          c.block->btype == mch::btype::kGaslessRequest) {
        continue;
      }
      c.ancestor_type.push_back(b->btype);
      std::sort(c.ancestor_type.begin(), c.ancestor_type.end());
      c.ancestor_type.erase(std::unique(c.ancestor_type.begin(), c.ancestor_type.end()),
                            c.ancestor_type.end());
      // Raise rather than drop: two rows with one action_id collide on the
      // actions PK, so the trace falls back.
      if (!ids.insert(calc_action_id(c.block)).second) {
        throw std::runtime_error("duplicate action id in child recursion");
      }
      out.push_back(std::move(c));
    }
  }
  if (parent_id.empty()) link_gasless_parents(blocks, out);  // top-level call only
  return out;
}

// Fresh leaf-only classification with no matchers, followed by post-processing
// and serialization. Any exception produces an empty fallback.
std::vector<ActionRow> basic_classify_fallback(TraceContext &ctx) {
  std::vector<ActionRow> out;
  try {
    Block *root = init_block(ctx.arena, ctx.tree.root);
    Block *wrap = wrap_root(ctx.arena, root);
    std::vector<Block *> spine = collect_spine(wrap);
    std::set<Block *> removed;
    run_post_processors(spine, ctx.arena, removed);
    // Recursive serialization; basic btypes are all v1_ops, so recursion
    // normally has no additional rows.
    out = serialize_rows(spine, "");
  } catch (...) {
    out.clear();  // fallback returns empty on any exception
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
    Block *wrapper = wrap_root(ctx.arena, b);
    // Same BFS as the main loop (next_blocks read AFTER the build,
    // parent-is-None guard).
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
    run_post_processors(spine, ctx.arena, removed);
    // Flat: apply the row filter directly, so a ghost row never recurses
    // into children and never carries parent_action_id.
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
  // Matcher / build / where tables are three views of one document. Disagreeing
  // shas mean the tables would run matchers against another document's build
  // programs — a hard setup error.
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

  // Name the trace on every [mch-reject] line this classify emits.
  RejectCtx rctx;
  rctx.trace_id = &ctx.trace.trace_id;
  RejectScope reject_scope(rctx);

  Block *wrapper = wrap_root(ctx.arena, ctx.root);

  // Anchor prefilter: opcodes + btypes on the spine. A matcher whose
  // opcode/btype anchor intersects neither can match nothing, so its
  // traversal is skipped. Opcodes are static (produced composites carry
  // none); btypes grow as matchers fire, so produced btypes are folded in
  // after each matcher. Consumed blocks are never removed, so a skip only
  // fires when the traversal provably matches nothing.
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
      // FIFO; cur's next_blocks are read after try_build ran on cur. Merges
      // mutate the tree mid-walk.
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

    std::vector<Block *> spine = collect_spine(wrapper);
    std::set<Block *> removed;
    run_post_processors(spine, ctx.arena, removed);
    for (CoreAction &a : pending) {
      if (!removed.count(a.produced)) result.actions.push_back(std::move(a));
    }
    // Row set is the spine, not the fire list. A fire whose product a later
    // matcher consumed is off the spine unless its consumer's btype is
    // outside v1_ops, in which case recursion brings it back as a child row;
    // a leaf no matcher consumed emits a basic row.
    result.action_rows = serialize_rows(spine, "");
  } catch (const vm::VmError &e) {
    result.failure = true;
    result.failure_reason = std::string("vm exception: ") + e.get_msg();
    result.failure_category = FailureCategory::engine_fault;
  } catch (const vm::VmNoGas &e) {
    result.failure = true;
    result.failure_reason = std::string("vm exception: ") + e.get_msg();
    result.failure_category = FailureCategory::engine_fault;
  } catch (const vm::VmVirtError &e) {
    result.failure = true;
    result.failure_reason = std::string("vm exception: ") + e.get_msg();
    result.failure_category = FailureCategory::engine_fault;
  } catch (const std::exception &e) {
    result.failure = true;
    result.failure_reason = e.what();
    // The two-phase driver throws this exact message on non-convergence, an
    // infra fault, distinct from a spec/parse engine fault.
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
    result.actions.clear();
    result.action_rows.clear();
    result.fallback_rows = basic_classify_fallback(ctx);
  } else if (result.action_rows.empty() && !ctx.trace.transactions.empty()) {
    // Fall back to ghost external rows before minting the unknown action.
    result.action_rows = ghost_external_rows(ctx, src);
    result.unknown_trace = result.action_rows.empty();
  }
  return result;
}

SchemaClassifyResult classify_schema_trace(
    const MchEnginePrep &prep, const std::string &trace_id,
    const std::vector<schema::Transaction> &txs, const ParsedBlockLookupSource &src,
    std::vector<Action> &rows, std::vector<std::string> &matcher_names,
    std::size_t &scrubbed, bool &unknown_row) {
  SchemaClassifyResult result;
  try {
    auto r_trace = schema_to_trace(trace_id, txs);
    if (r_trace.is_error()) {
      result.failure = true;
      result.failure_reason = r_trace.move_as_error().message().str();
      result.failure_category = FailureCategory::malformed_trace;
      return result;
    }
    TraceContext ctx;
    ctx.trace = r_trace.move_as_ok();
    ctx.tree = to_tree(ctx.trace);
    if (ctx.tree.root == nullptr) {
      result.failure = true;
      result.failure_reason = "empty event tree";
      result.failure_category = FailureCategory::malformed_trace;
      return result;
    }
    ctx.root = init_block(ctx.arena, ctx.tree.root);

    ClassifyResult classified = classify_trace(ctx, *prep.matchers, prep.setup, src);
    result.failure = classified.failure;
    result.failure_reason = classified.failure_reason;
    result.failure_category = classified.failure_category;
    result.used_fallback = classified.failure;

    const std::vector<ActionRow> &core =
        classified.failure ? classified.fallback_rows : classified.action_rows;
    for (const ActionRow &row : core) {
      Action action;
      if (!build_action(row, action)) {
        result.unported_btypes++;
        continue;
      }
      scrubbed += scrub_arena_refs(action);
      rows.push_back(std::move(action));
    }

    // Collect all fired matchers, including absorbed matches.
    std::set<std::string> names;
    for (const CoreAction &action : classified.actions) {
      names.insert(action.matcher_name);
    }
    matcher_names.assign(names.begin(), names.end());
    if (classified.unknown_trace) {
      rows.push_back(create_unknown_action(ctx.trace));
      unknown_row = true;
    }
  } catch (const vm::VmError &e) {
    result.failure = true;
    result.failure_reason = std::string("vm exception: ") + e.get_msg();
    result.failure_category = FailureCategory::engine_fault;
  } catch (const vm::VmNoGas &e) {
    result.failure = true;
    result.failure_reason = std::string("vm exception: ") + e.get_msg();
    result.failure_category = FailureCategory::engine_fault;
  } catch (const vm::VmVirtError &e) {
    result.failure = true;
    result.failure_reason = std::string("vm exception: ") + e.get_msg();
    result.failure_category = FailureCategory::engine_fault;
  } catch (const std::exception &e) {
    result.failure = true;
    result.failure_reason = std::string("exception: ") + e.what();
    result.failure_category = FailureCategory::engine_fault;
  } catch (...) {
    result.failure = true;
    result.failure_reason = "unknown exception";
    result.failure_category = FailureCategory::engine_fault;
  }
  return result;
}

}  // namespace mch
