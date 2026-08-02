#include "Classify.h"

#include "ActionBuild.h"
#include "BlockTree.h"
#include "BuildDriver.h"
#include "ClassifyCore.h"
#include "BuildRuntime.h"
#include "fixtures/FixtureLoader.h"
#include "fixtures/FixtureLookupSource.h"
#include "ExprRuntime.h"
#include "fixtures/Render.h"
#include "GenMatchers.h"
#include "MsgParse.h"

#include "td/utils/base64.h"
#include "td/utils/crypto.h"

#include "common/refint.h"

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <deque>
#include <exception>
#include <filesystem>
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace mch {

namespace {

// Render + print a collected CoreAction from its FINAL (post-pass) state. The
// aux/flows strings (fire-time) are rendered HERE from the core's raw block/
// flow lists. The core stays rendering-agnostic.
void emit_action(const CoreAction &a) {
  std::vector<std::string> ck;
  for (Block *cb : a.produced->children_blocks) ck.push_back(block_key(cb));
  std::sort(ck.begin(), ck.end());
  std::string consumed_s = "[";
  for (std::size_t i = 0; i < ck.size(); i++) {
    if (i) consumed_s += ",";
    consumed_s += ck[i];
  }
  consumed_s += "]";

  std::string aux_s = "[";
  for (std::size_t i = 0; i < a.aux.size(); i++) {
    if (i) aux_s += ",";
    aux_s += block_key(a.aux[i]);
  }
  aux_s += "]";

  std::printf("ACTION %s anchor=%s btype=%s failed=%d broken=%d data=%s "
              "consumed=%s auto=%s\n",
              a.matcher_name.c_str(), block_key(a.anchor).c_str(), a.produced->btype.c_str(),
              a.produced->failed ? 1 : 0, a.produced->broken ? 1 : 0,
              render_value(a.produced->data).c_str(), consumed_s.c_str(),
              aux_s.c_str());
}

}  // namespace

// Dump-side render of an Action row (built lib-side by ActionBuild). Fixed
// column set; unset fields are Null, rendered "null".
std::string render_action(const Action &a) {
  Value::Fields f;
  f.emplace_back("id", Value::make_str(a.action_id));
  f.emplace_back("type", Value::make_str(a.type));
  f.emplace_back("success", Value::make_bool(a.success));
  f.emplace_back("start_lt", Value::make_int64(a.start_lt));
  f.emplace_back("end_lt", Value::make_int64(a.end_lt));
  f.emplace_back("start_utime", Value::make_int64(a.start_utime));
  f.emplace_back("end_utime", Value::make_int64(a.end_utime));
  f.emplace_back("mc_seqno_end", Value::make_int64(a.mc_seqno_end));
  std::vector<std::string> th = a.tx_hashes;
  std::sort(th.begin(), th.end());
  std::vector<Value> thv;
  for (const std::string &s : th) thv.push_back(Value::make_str(s));
  f.emplace_back("tx_hashes", Value::make_list(std::move(thv)));
  std::vector<std::string> ac = a.accounts;
  std::sort(ac.begin(), ac.end());
  std::vector<Value> acv;
  for (const std::string &s : ac) acv.push_back(Value::make_str(s));
  f.emplace_back("accounts", Value::make_list(std::move(acv)));
  f.emplace_back("source", a.source);
  f.emplace_back("source_secondary", a.source_secondary);
  f.emplace_back("destination", a.destination);
  f.emplace_back("destination_secondary", a.destination_secondary);
  f.emplace_back("asset", a.asset);
  f.emplace_back("asset_secondary", a.asset_secondary);
  f.emplace_back("asset2", a.asset2);
  f.emplace_back("asset2_secondary", a.asset2_secondary);
  f.emplace_back("amount", a.amount);
  f.emplace_back("value", a.value);
  f.emplace_back("opcode", a.opcode);
  f.emplace_back("ton_transfer_data", a.ton_transfer_data);
  f.emplace_back("jetton_transfer_data", a.jetton_transfer_data);
  f.emplace_back("jetton_swap_data", a.jetton_swap_data);
  f.emplace_back("nft_transfer_data", a.nft_transfer_data);
  f.emplace_back("nft_listing_data", a.nft_listing_data);
  f.emplace_back("nft_mint_data", a.nft_mint_data);
  f.emplace_back("dex_deposit_liquidity_data", a.dex_deposit_liquidity_data);
  f.emplace_back("dex_withdraw_liquidity_data", a.dex_withdraw_liquidity_data);
  f.emplace_back("staking_data", a.staking_data);
  f.emplace_back("evaa_supply_data", a.evaa_supply_data);
  f.emplace_back("evaa_withdraw_data", a.evaa_withdraw_data);
  f.emplace_back("evaa_liquidate_data", a.evaa_liquidate_data);
  f.emplace_back("vesting_send_message_data", a.vesting_send_message_data);
  f.emplace_back("vesting_add_whitelist_data", a.vesting_add_whitelist_data);
  f.emplace_back("tonco_deploy_pool_data", a.tonco_deploy_pool_data);
  f.emplace_back("multisig_create_order_data", a.multisig_create_order_data);
  f.emplace_back("multisig_approve_data", a.multisig_approve_data);
  f.emplace_back("multisig_execute_data", a.multisig_execute_data);
  f.emplace_back("cocoon_worker_payout_data", a.cocoon_worker_payout_data);
  f.emplace_back("cocoon_proxy_payout_data", a.cocoon_proxy_payout_data);
  f.emplace_back("cocoon_proxy_charge_data", a.cocoon_proxy_charge_data);
  f.emplace_back("cocoon_client_top_up_data", a.cocoon_client_top_up_data);
  f.emplace_back("cocoon_register_proxy_data", a.cocoon_register_proxy_data);
  f.emplace_back("cocoon_unregister_proxy_data", a.cocoon_unregister_proxy_data);
  f.emplace_back("cocoon_client_register_data", a.cocoon_client_register_data);
  f.emplace_back("cocoon_client_change_secret_hash_data", a.cocoon_client_change_secret_hash_data);
  f.emplace_back("cocoon_client_request_refund_data", a.cocoon_client_request_refund_data);
  f.emplace_back("cocoon_grant_refund_data", a.cocoon_grant_refund_data);
  f.emplace_back("cocoon_client_increase_stake_data", a.cocoon_client_increase_stake_data);
  f.emplace_back("cocoon_client_withdraw_data", a.cocoon_client_withdraw_data);
  f.emplace_back("layerzero_packet_data", a.layerzero_packet_data);
  f.emplace_back("layerzero_send_data", a.layerzero_send_data);
  f.emplace_back("layerzero_dvn_verify_data", a.layerzero_dvn_verify_data);
  f.emplace_back("jvault_stake_data", a.jvault_stake_data);
  f.emplace_back("jvault_claim_data", a.jvault_claim_data);
  f.emplace_back("change_dns_record_data", a.change_dns_record_data);
  f.emplace_back("coffee_create_pool_data", a.coffee_create_pool_data);
  f.emplace_back("coffee_staking_deposit_data", a.coffee_staking_deposit_data);
  f.emplace_back("coffee_staking_withdraw_data", a.coffee_staking_withdraw_data);
  f.emplace_back("extra", a.extra);
  // Child-action recursion columns: null / empty list on a top-level row.
  f.emplace_back("parent_action_id", a.parent_action_id.empty()
                                         ? Value::null()
                                         : Value::make_str(a.parent_action_id));
  std::vector<Value> anc;
  for (const std::string &s : a.ancestor_type) anc.push_back(Value::make_str(s));
  f.emplace_back("ancestor_type", Value::make_list(std::move(anc)));
  return render_value(Value::make_dict(std::move(f)));
}

// Shared CLI dump adapter over ClassifyCore. The core computes a buffered
// per-trace ClassifyResult; this reproduces the
// stdout byte-for-byte (SKIP header, `=== fixture`, ERROR/TRACE_FAIL lines, then
// per surviving action either the classify line (emit_action) or the serialized
// Action row (build_action + render_action)). --classify and --actions differ
// ONLY in the per-action render, so they share this.
int run_dump(const std::vector<std::string> &paths,
             bool actions_mode, const TraceContextLoader &load_ctx,
             const LookupSourceFactory &lookup_factory) {
  const std::vector<CompiledMatcher> &matchers = gen_matchers_ir();
  warn_missing_artifact_parsers(matchers);

  ClassifySetup setup = prepare_classify(matchers);
  if (setup.table_missing || setup.fn_missing) {
    std::fprintf(stderr, "%s\n", setup.error.c_str());
    return 2;
  }
  for (const auto &s : setup.skips) {
    std::printf("SKIP %s reason=%s\n", s.first.c_str(), s.second.c_str());
  }

  namespace fs = std::filesystem;
  std::vector<std::string> fixtures;
  for (const std::string &p : paths) {
    std::error_code ec;
    if (fs::is_directory(p, ec)) {
      std::vector<std::string> names;
      for (const auto &e : fs::directory_iterator(p)) {
        if (e.path().extension() == ".lz4") {
          names.push_back(e.path().string());
        }
      }
      std::sort(names.begin(), names.end());
      fixtures.insert(fixtures.end(), names.begin(), names.end());
    } else {
      fixtures.push_back(p);
    }
  }

  for (const std::string &f : fixtures) {
    std::printf("=== %s\n", fs::path(f).filename().string().c_str());
    auto r_ctx = load_ctx(f);
    if (r_ctx.is_error()) {
      std::printf("ERROR: %s\n", r_ctx.error().message().c_str());
      continue;
    }
    auto ctx = r_ctx.move_as_ok();
    // Orphan warning: stderr only (stdout is the A/B channel; never fires on corpus).
    if (ctx.tree.unlinked != 0) {
      std::fprintf(stderr, "WARN %s: %zu orphan nodes unreachable from root\n",
                   fs::path(f).filename().string().c_str(), ctx.tree.unlinked);
    }
    std::shared_ptr<LookupSource> src =
        lookup_factory ? lookup_factory(ctx)
                       : std::make_shared<FixtureLookupSource>(&ctx.trace.interfaces);

    ClassifyResult res = classify_trace(ctx, matchers, setup, *src);
    if (res.failure) {
      // Per-trace exception boundary (event_processing.py): buffered, the
      // regular actions are discarded, a single TRACE_FAIL on stdout, the reason
      // on stderr (A/B channel stays byte-comparable). In --actions mode the
      // Basic-action fallback rows follow behind a FALLBACK tag.
      // No corpus trace triggers this, the fallback path is ungated.
      std::printf("TRACE_FAIL\n");
      std::fprintf(stderr, "TRACE_FAIL %s: %s\n", fs::path(f).filename().string().c_str(),
                   res.failure_reason.c_str());
      if (actions_mode) {
        for (const ActionRow &r : res.fallback_rows) {
          Action act;
          if (!build_action(r, act)) continue;
          std::printf("FALLBACK anchor=%s %s\n", block_key(r.block).c_str(),
                      render_action(act).c_str());
        }
      }
      continue;
    }
    // --actions renders the serialize_blocks ROW set (spine); --classify renders
    // the matcher FIRE list. The two sets differ (ClassifyCore.h action_rows).
    if (actions_mode) {
      // SORTED: spine order is not comparable against Python (its
      // compact_connections rebuilds next_blocks through a `set()`), and no
      // consumer of the rows depends on their order.
      std::vector<std::string> lines;
      for (const ActionRow &r : res.action_rows) {
        Action act;
        if (!build_action(r, act)) continue;  // btype outside ported set
        lines.push_back("ACTION anchor=" + block_key(r.block) + " " + render_action(act));
      }
      std::sort(lines.begin(), lines.end());
      for (const std::string &line : lines) {
        std::printf("%s\n", line.c_str());
      }
    } else {
      for (const CoreAction &a : res.actions) {
        emit_action(a);
      }
    }
    // The trace classified cleanly and matched nothing. The synthetic `unknown`
    // row has no Block, so it prints the literal anchor `unknown` (the twin
    // prints the same) instead of a block_key.
    if (actions_mode && res.unknown_trace) {
      Action act = create_unknown_action(ctx.trace);
      std::printf("ACTION anchor=unknown %s\n", render_action(act).c_str());
    }
  }
  return 0;
}

int run_classify(const std::vector<std::string> &paths) {
  return run_dump(paths, /*actions_mode=*/false, load_trace_context);
}

int run_actions(const std::vector<std::string> &paths) {
  return run_dump(paths, /*actions_mode=*/true, load_trace_context);
}

}  // namespace mch
