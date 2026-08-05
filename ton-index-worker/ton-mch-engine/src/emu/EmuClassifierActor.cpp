#include "EmuClassifierActor.h"

#include "BlockTree.h"
#include "EmuActionSerialize.h"
#include "EmuTraceConvert.h"
#include "SchemaTraceLoader.h"

#include "vm/excno.hpp"  // vm::VmError is not a std::exception.

#include "td/utils/logging.h"

#include <algorithm>
#include <exception>
#include <set>
#include <string>
#include <utility>

namespace mch {

namespace {

constexpr double kStatsIntervalSec = 10.0;
constexpr std::size_t kMaxSeenTraces = 65536;

const char *category_name(FailureCategory c) {
  switch (c) {
    case FailureCategory::none: return "none";
    case FailureCategory::lookup_infra_fail: return "lookup_infra_fail";
    case FailureCategory::engine_fault: return "engine_fault";
    case FailureCategory::malformed_trace: return "malformed_trace";
  }
  return "?";
}

// Scrub non-owning Block values before the TraceContext arena dies. Cells remain
// valid through the view's anchor. Shared container payloads make this a repair,
// not a pure observation, so arena_refs_scrubbed counts each replacement.
std::size_t scrub_value(Value &v) {
  if (v.t == VType::Block) {
    v = Value::null();
    return 1;
  }
  std::size_t n = 0;
  // Composite action fields require a recursive container walk.
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

// Keep this field set and order synchronized with optional_fields().
std::size_t scrub_arena_refs(Action &a) {
  std::size_t n = 0;
  for (Value *v : {&a.source, &a.source_secondary, &a.destination, &a.destination_secondary,
                   &a.asset, &a.asset_secondary, &a.asset2, &a.asset2_secondary, &a.amount,
                   &a.value, &a.opcode, &a.ton_transfer_data, &a.jetton_transfer_data,
                   &a.jetton_swap_data, &a.nft_transfer_data, &a.nft_listing_data,
                   &a.nft_mint_data,
                   &a.dex_deposit_liquidity_data, &a.dex_withdraw_liquidity_data, &a.staking_data,
                   &a.evaa_supply_data, &a.evaa_withdraw_data, &a.evaa_liquidate_data,
                   &a.vesting_send_message_data,
                   &a.vesting_add_whitelist_data, &a.tonco_deploy_pool_data,
                   &a.multisig_create_order_data, &a.multisig_approve_data,
                   &a.multisig_execute_data,
                   &a.cocoon_worker_payout_data, &a.cocoon_proxy_payout_data,
                   &a.cocoon_proxy_charge_data, &a.cocoon_client_top_up_data,
                   &a.cocoon_register_proxy_data, &a.cocoon_unregister_proxy_data,
                   &a.cocoon_client_register_data,
                   &a.cocoon_client_change_secret_hash_data,
                   &a.cocoon_client_request_refund_data, &a.cocoon_grant_refund_data,
                   &a.cocoon_client_increase_stake_data, &a.cocoon_client_withdraw_data,
                   &a.layerzero_packet_data, &a.layerzero_send_data,
                   &a.layerzero_dvn_verify_data,
                   &a.jvault_stake_data, &a.jvault_claim_data, &a.change_dns_record_data,
                   &a.coffee_create_pool_data, &a.coffee_staking_deposit_data,
                   &a.coffee_staking_withdraw_data,
                   &a.extra}) {
    n += scrub_value(*v);
  }
  return n;
}

// Builds and classifies the trace, then materializes actions while the arena is
// alive. All failures are mapped onto the result; this function never throws.
void classify_txs(const EmuClassifierConfig &cfg, const EmuTraceView &view,
                  const std::vector<schema::Transaction> &txs, EmuClassifyResult &res,
                  std::vector<Action> &rows, std::vector<std::string> &matcher_names,
                  std::size_t &scrubbed, bool &unknown_row) {
  const MchEnginePrep &prep = *cfg.prep;
  const ParsedBlockLookupSource::InterfaceMap &ifaces = view.interfaces;
  try {
    auto r_trace = schema_to_trace(res.trace_id, txs);
    if (r_trace.is_error()) {
      res.failure = true;
      res.failure_reason = r_trace.move_as_error().message().str();
      res.failure_category = FailureCategory::malformed_trace;
      return;
    }
    TraceContext ctx;
    ctx.trace = r_trace.move_as_ok();
    ctx.tree = to_tree(ctx.trace);
    if (ctx.tree.root == nullptr) {
      res.failure = true;
      res.failure_reason = "empty event tree";
      res.failure_category = FailureCategory::malformed_trace;
      return;
    }
    ctx.root = init_block(ctx.arena, ctx.tree.root);

    // Tier-2 memoization is scoped to this classification.
    EmuCelldbTier2 tier2(&view.shard_states, view.config);
    ParsedBlockLookupSource::Tier2Hook hook;
    if (cfg.tier2) {
      hook = [&tier2](const std::string &kind, const std::vector<Value> &args) {
        return tier2.fetch(kind, args);
      };
    }
    ParsedBlockLookupSource src(&ifaces, std::move(hook));
    ClassifyResult cres = classify_trace(ctx, *prep.matchers, prep.setup, src);
    res.failure = cres.failure;
    res.failure_reason = cres.failure_reason;
    res.failure_category = cres.failure_category;
    res.lookup_stats = src.stats();
    res.tier2_stats = tier2.stats();

    // Classification failure replaces regular actions with basic fallback rows.
    res.used_fallback = cres.failure;
    const std::vector<ActionRow> &core = cres.failure ? cres.fallback_rows : cres.action_rows;
    for (const ActionRow &r : core) {
      Action act;
      if (!build_action(r, act)) {  // btype outside the ported fill set
        res.unported_btypes++;
        continue;
      }
      scrubbed += scrub_arena_refs(act);
      rows.push_back(std::move(act));
    }
    // Record every fired matcher, including matches absorbed by later rules.
    std::set<std::string> names;
    for (const CoreAction &a : cres.actions) {
      names.insert(a.matcher_name);
    }
    matcher_names.assign(names.begin(), names.end());
    // A clean unmatched trace receives one synthetic unknown action.
    if (cres.unknown_trace) {
      rows.push_back(create_unknown_action(ctx.trace));
      unknown_row = true;
    }
  } catch (const std::exception &e) {
    res.failure = true;
    res.failure_reason = std::string("exception: ") + e.what();
    res.failure_category = FailureCategory::engine_fault;
  } catch (...) {
    res.failure = true;
    res.failure_reason = "unknown exception";
    res.failure_category = FailureCategory::engine_fault;
  }
}

// Build the Redis writeback payload while the view remains alive.
EmuActionPayload build_payload(const EmuTraceView &view, const std::vector<Action> &rows,
                               std::int64_t aai_score, ActionSerializeStats &st) {
  EmuActionPayload payload;
  payload.action_count = rows.size();
  payload.aai_score = aai_score;
  payload.actions_blob = serialize_actions(rows, view, &st);

  for (const Action &a : rows) {
    payload.routes.push_back(EmuActionRoute{
        .type = a.type,
        .accounts = a.accounts,
    });
    // Preserve the action ID byte-for-byte in `<trace_key>:<action_id>` members.
    const std::string member = view.trace_id + ":" + a.action_id;
    for (const std::string &account : a.accounts) {
      payload.aai.emplace_back(account, member);
    }
  }
  return payload;
}

}  // namespace

void EmuClassifierActor::start_up() {
  alarm_timestamp() = td::Timestamp::in(kStatsIntervalSec);
}

bool EmuClassifierActor::remember_seen(const std::string &trace_id) {
  if (!seen_traces_.insert(trace_id).second) {
    return false;
  }
  seen_order_.push_back(trace_id);
  while (seen_order_.size() > kMaxSeenTraces) {
    seen_traces_.erase(seen_order_.front());
    seen_order_.pop_front();
  }
  return true;
}

void EmuClassifierActor::classify(EmuTraceView view, std::int64_t enqueued_us,
                                  td::Promise<EmuClassifyResult> promise) {
  EmuClassifyResult res;
  res.trace_id = view.trace_id;
  // Derive payload and guard finality from the same view.
  res.payload.finality = static_cast<std::uint8_t>(view_finality(view));
  res.payload.update_seq = view.update_seq;

  const std::int64_t started_us = emu_now_us();
  res.queue_us = started_us - enqueued_us;
  if (res.queue_us > 0 && static_cast<std::size_t>(res.queue_us) > stats_.queue_us_max) {
    stats_.queue_us_max = static_cast<std::size_t>(res.queue_us);
  }

  stats_.emissions++;
  if (!remember_seen(view.trace_id)) {
    stats_.reemissions++;
  }
  if (!view.nodes.empty()) {
    stats_.by_finality[static_cast<std::size_t>(view.nodes.front().finality)]++;
  }
  if (view.tx_limit_exceeded) {
    stats_.tx_limit_exceeded++;
  }

  // Serialize rows before the view and its cell anchor leave scope.
  std::vector<Action> rows;
  bool unknown_row = false;

  // Catch vm::VmError explicitly because it does not derive from std::exception.
  // Every path reaches the single response below.
  try {
    auto r_txs = emu_to_schema_txs(view, cfg_.global_version);
    if (r_txs.is_error()) {
      stats_.convert_failed++;
      res.outcome = EmuClassifyOutcome::convert_failed;
      res.failure = true;
      // Conversion failures use their own counter and a non-null failure category.
      res.failure_category = FailureCategory::malformed_trace;
      res.failure_reason = r_txs.move_as_error().message().str();
      res.payload.state = "convert_failed";
      LOG(WARNING) << "[mch-emu-w" << worker_index_ << "] trace=" << view.trace_id
                   << " CONVERT FAILED: " << res.failure_reason;
    } else {
      auto txs = r_txs.move_as_ok();
      std::vector<std::string> matcher_names;
      std::size_t scrubbed = 0;
      classify_txs(cfg_, view, txs, res, rows, matcher_names, scrubbed, unknown_row);
      // AAI uses the minimum transaction LT as the trace-start score. Redis
      // sorted-set scores lose integer precision above 2^53.
      std::int64_t aai_score = 0;
      bool first_tx = true;
      for (const auto &tx : txs) {
        const auto lt = static_cast<std::int64_t>(tx.lt);
        aai_score = first_tx ? lt : std::min(aai_score, lt);
        first_tx = false;
      }
      // Serialize within the anchor lifetime and measure it separately.
      const std::int64_t ser_started_us = emu_now_us();
      ActionSerializeStats ser;
      const std::uint8_t finality = res.payload.finality;
      const std::uint64_t update_seq = res.payload.update_seq;
      res.payload = build_payload(view, rows, aai_score, ser);
      res.payload.finality = finality;
      res.payload.update_seq = update_seq;
      res.serialize_us = emu_now_us() - ser_started_us;
      if (res.serialize_us > 0) {
        auto us = static_cast<std::size_t>(res.serialize_us);
        stats_.serialize_us_total += us;
        stats_.serialize_us_max = std::max(stats_.serialize_us_max, us);
      }
      stats_.actions_blob_bytes += res.payload.actions_blob.size();
      stats_.ser_float_values += ser.float_values;
      stats_.ser_cell_values += ser.cell_values;
      stats_.ser_unrenderable += ser.unrenderable;
      if (ser.float_values != 0 || ser.cell_values != 0 || ser.unrenderable != 0) {
        LOG(WARNING) << "[mch-emu-w" << worker_index_ << "] trace=" << res.trace_id
                     << " serializer saw float=" << ser.float_values
                     << " cell=" << ser.cell_values << " unrenderable=" << ser.unrenderable;
      }
      stats_.actions += rows.size();
      if (res.used_fallback) {
        stats_.fallback_actions += rows.size();
      }
      stats_.unported_btypes += res.unported_btypes;
      stats_.lookups.tier1_hits += res.lookup_stats.tier1_hits;
      stats_.lookups.tier2_hits += res.lookup_stats.tier2_hits;
      stats_.lookups.misses += res.lookup_stats.misses;
      for (const auto &[kind, n] : res.lookup_stats.misses_by_kind) {
        stats_.lookups.misses_by_kind[kind] += n;
      }
      stats_.tier2.fetched += res.tier2_stats.fetched;
      stats_.tier2.memo_hits += res.tier2_stats.memo_hits;
      if (scrubbed > 0) {
        stats_.arena_refs_scrubbed += scrubbed;
        LOG(WARNING) << "[mch-emu-w" << worker_index_ << "] trace=" << view.trace_id
                     << " scrubbed " << scrubbed
                     << " arena block reference(s) out of the action rows";
      }
      if (res.failure) {
        stats_.failed++;
        stats_.by_category[res.failure_category]++;
        res.outcome = EmuClassifyOutcome::classify_failed;
        // Classification failures write the basic-action fallback payload.
        res.payload.state = "fallback";
        LOG(WARNING) << "[mch-emu-w" << worker_index_ << "] trace=" << view.trace_id
                     << " FAILED ("
                     << category_name(res.failure_category) << "): " << res.failure_reason;
      } else {
        stats_.classified++;
        res.outcome = EmuClassifyOutcome::classified;
        // The state distinguishes a synthetic unknown row from protocol actions.
        res.payload.state = unknown_row ? "unknown" : "ok";
        std::string names;
        for (const std::string &n : matcher_names) {
          if (!names.empty()) names += ",";
          names += n;
        }
        LOG(DEBUG) << "[mch-emu-w" << worker_index_ << "] trace=" << view.trace_id
                   << " txs=" << txs.size()
                   << " actions=" << res.payload.action_count
                   << " serialize_us=" << res.serialize_us << " matchers=[" << names << "]";
      }
    }
  } catch (const vm::VmError &e) {
    stats_.convert_failed++;
    res.outcome = EmuClassifyOutcome::convert_failed;
    res.failure = true;
    res.failure_category = FailureCategory::malformed_trace;
    res.failure_reason = std::string("exception (vm): ") + e.get_msg();
    res.payload.state = "convert_failed";
    LOG(WARNING) << "[mch-emu-w" << worker_index_ << "] trace=" << view.trace_id
                 << " EXCEPTION (vm): " << e.get_msg();
  } catch (...) {
    stats_.convert_failed++;
    res.outcome = EmuClassifyOutcome::convert_failed;
    res.failure = true;
    res.failure_category = FailureCategory::malformed_trace;
    res.failure_reason = "unknown exception";
    res.payload.state = "convert_failed";
    LOG(WARNING) << "[mch-emu-w" << worker_index_ << "] trace=" << view.trace_id
                 << " EXCEPTION";
  }

  res.classify_us = emu_now_us() - started_us;
  if (res.classify_us > 0) {
    auto us = static_cast<std::size_t>(res.classify_us);
    stats_.classify_us_total += us;
    if (us > stats_.classify_us_max) {
      stats_.classify_us_max = us;
    }
  }
  stats_.latency_samples++;

  // The single answer: after this the sink's continuation runs the insert.
  promise.set_value(std::move(res));
}

void EmuClassifierActor::alarm() {
  std::string cats;
  for (const auto &[cat, n] : stats_.by_category) {
    if (!cats.empty()) cats += ",";
    cats += std::string(category_name(cat)) + "=" + std::to_string(n);
  }
  // Per-kind miss attribution: the total alone cannot separate a kind with no
  // source (an expected gap) from a real resolution failure.
  std::string miss_kinds;
  for (const auto &[kind, n] : stats_.lookups.misses_by_kind) {
    if (!miss_kinds.empty()) miss_kinds += ",";
    miss_kinds += kind + "=" + std::to_string(n);
  }
  std::size_t resolved = stats_.lookups.tier1_hits + stats_.lookups.tier2_hits;
  LOG(INFO) << "[mch-emu-w" << worker_index_ << "] emissions=" << stats_.emissions << " (+"
            << (stats_.emissions - prev_.emissions) << ")"
            << " reemissions=" << stats_.reemissions
            << " classified=" << stats_.classified << " (+"
            << (stats_.classified - prev_.classified) << ")"
            << " failed=" << stats_.failed << " convert_failed=" << stats_.convert_failed
            << " tx_limit_exceeded=" << stats_.tx_limit_exceeded
            << " actions=" << stats_.actions << " (+" << (stats_.actions - prev_.actions) << ")"
            << " fallback_actions=" << stats_.fallback_actions
            << " unported_btypes=" << stats_.unported_btypes
            << " arena_refs_scrubbed=" << stats_.arena_refs_scrubbed
            << " finality{emulated=" << stats_.by_finality[0]
            << " confirmed=" << stats_.by_finality[1]
            << " finalized=" << stats_.by_finality[2] << "}"
            << " [" << cats << "]"
            << " lookups{tier1=" << stats_.lookups.tier1_hits
            << " tier2=" << stats_.lookups.tier2_hits << " miss=" << stats_.lookups.misses
            << " miss_by_kind={" << miss_kinds << "}"
            << " tier2_rate=" << (resolved ? (100 * stats_.lookups.tier2_hits / resolved) : 0)
            << "%}"
            << " tier2{fetched=" << stats_.tier2.fetched
            << " memo_hits=" << stats_.tier2.memo_hits << "}"
            << " queue_us{max=" << stats_.queue_us_max << "}"
            << " classify_us{avg="
            << (stats_.latency_samples ? stats_.classify_us_total / stats_.latency_samples : 0)
            << " max=" << stats_.classify_us_max << "}"
            << " serialize_us{avg="
            << (stats_.latency_samples ? stats_.serialize_us_total / stats_.latency_samples : 0)
            << " max=" << stats_.serialize_us_max << "}"
            << " blob_bytes=" << stats_.actions_blob_bytes
            << " ser{float=" << stats_.ser_float_values << " cell=" << stats_.ser_cell_values
            << " unrenderable=" << stats_.ser_unrenderable << "}";
  prev_ = stats_;
  alarm_timestamp() = td::Timestamp::in(kStatsIntervalSec);
}

}  // namespace mch
