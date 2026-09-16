#include "Classify.h"

#include "ActionBuild.h"
#include "BlockTree.h"
#include "BuildDriver.h"
#include "ClassifyCore.h"
#include "BuildRuntime.h"
#include "fixtures/FixtureLoader.h"
#include "fixtures/FixtureLookupSource.h"
#include "ExprRuntime.h"
#include "fixtures/IrJson.h"
#include "fixtures/Render.h"
#include "GenMatchers.h"
#include "MsgParse.h"

#include "td/utils/base64.h"
#include "td/utils/JsonBuilder.h"
#include "td/utils/crypto.h"

#include "common/refint.h"

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <deque>
#include <exception>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
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

// One action row in YAML block style, as a list item at two-space indent.
// Directional/scalar columns are always present (null explicit); composites
// only when set, with their nested nulls explicit; child rows add their
// parent id and ancestor types.
std::string render_action(const Action &a, const std::string &matcher, const std::string &anchor) {
  std::string out;
  auto scalar = [&](const char *k, const std::string &text) {
    out += std::string("    ") + k + ": " + text + "\n";
  };
  auto strings = [&](const char *k, std::vector<std::string> v) {
    std::sort(v.begin(), v.end());
    if (v.empty()) {
      scalar(k, "[]");
      return;
    }
    out += std::string("    ") + k + ":\n";
    for (const std::string &x : v) out += "      - " + yaml_str(x) + "\n";
  };
  out += "  - type: " + yaml_str(a.type) + "\n";
  if (!matcher.empty() && matcher != "-") scalar("matcher", yaml_str(matcher));  // engine-made rows have none
  if (!anchor.empty()) scalar("anchor", yaml_str(anchor));
  scalar("id", yaml_str(a.action_id));
  scalar("success", a.success ? "true" : "false");
  static const char *const kAlways[] = {"source", "source_secondary", "destination",
                                        "destination_secondary", "asset", "asset_secondary",
                                        "asset2", "asset2_secondary", "amount", "value"};
  std::map<std::string, const Value *> fields;
#define MCH_COLLECT_FIELD(name) fields.emplace(#name, &a.name);
  MCH_ACTION_VALUE_FIELDS(MCH_COLLECT_FIELD)
#undef MCH_COLLECT_FIELD
  for (const char *k : kAlways) {
    render_yaml_field(k, *fields.at(k), 4, out);
    fields.erase(k);
  }
  const Value &op = *fields.at("opcode");
  if (op.t == VType::Int && !op.num.is_null()) {
    char buf[16];
    std::snprintf(buf, sizeof(buf), "0x%08llx",
                  static_cast<unsigned long long>(op.num->to_long() & 0xFFFFFFFFll));
    scalar("opcode", buf);
  } else {
    scalar("opcode", "null");
  }
  fields.erase("opcode");
  for (const auto &kv : fields) {  // composites + extra, sorted, present only
    if (!kv.second->is_null()) render_yaml_field(kv.first, *kv.second, 4, out);
  }
  strings("tx_hashes", a.tx_hashes);
  strings("accounts", a.accounts);
  scalar("start_lt", std::to_string(a.start_lt));
  scalar("end_lt", std::to_string(a.end_lt));
  scalar("start_utime", std::to_string(a.start_utime));
  scalar("end_utime", std::to_string(a.end_utime));
  scalar("mc_seqno_end", std::to_string(a.mc_seqno_end));
  if (!a.parent_action_id.empty()) {
    scalar("parent_action_id", yaml_str(a.parent_action_id));
    strings("ancestor_type", a.ancestor_type);
  }
  return out;
}

namespace {
// goldens/fixtures.json: names and layout for the per-fixture output.
struct FixtureMeta {
  std::string group, name, slug, note;
};

std::map<std::string, FixtureMeta> load_fixture_manifest(const std::string &path) {
  std::map<std::string, FixtureMeta> out;
  if (path.empty()) return out;
  std::ifstream in(path);
  if (!in) throw std::runtime_error("cannot read fixture manifest " + path);
  std::stringstream ss;
  ss << in.rdbuf();
  std::string buf = ss.str();
  auto r_root = td::json_decode(td::MutableSlice(buf));
  if (r_root.is_error()) throw std::runtime_error("fixture manifest: " + r_root.error().message().str());
  td::JsonValue root = r_root.move_as_ok();
  const td::JsonValue *fixtures = jfield(root, "fixtures");
  if (fixtures == nullptr || fixtures->type() != td::JsonValue::Type::Object) {
    throw std::runtime_error("fixture manifest: no `fixtures` object");
  }
  for (const auto &kv : fixtures->get_object().field_values_) {
    const td::JsonValue &e = kv.second;
    out[kv.first.str()] = FixtureMeta{jstr(e, "group"), jstr(e, "name"), jstr(e, "slug"), jstr(e, "note")};
  }
  return out;
}

// `<group>/<slug>.<first 8 id chars>.yaml`; an unlisted fixture lands in misc/.
std::string fixture_relpath(const std::string &trace_id, const FixtureMeta *meta) {
  if (meta == nullptr) return "misc/" + trace_id + ".yaml";
  return meta->group + "/" + meta->slug + "." + trace_id.substr(0, 8) + ".yaml";
}

std::string fixture_header(const std::string &trace_id, const FixtureMeta *meta) {
  std::string out;
  if (meta != nullptr) {
    out += "# " + meta->group + " / " + meta->name + "\n";
    if (!meta->note.empty()) out += "# note: " + meta->note + "\n";
  }
  return out + "# trace: " + trace_id + "\n";
}
}  // namespace

// Shared CLI dump adapter over ClassifyCore. Without an output directory it
// preserves the fixture stream; per-fixture output omits only each `===` line.
int run_dump(const std::vector<std::string> &paths,
             bool actions_mode, const TraceContextLoader &load_ctx,
             const LookupSourceFactory &lookup_factory, const std::string &output_dir,
             const std::string &fixtures_manifest) {
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
  std::map<std::string, FixtureMeta> manifest;
  try {
    manifest = load_fixture_manifest(fixtures_manifest);
  } catch (const std::exception &e) {
    std::fprintf(stderr, "%s\n", e.what());
    return 2;
  }
  const bool per_fixture_output = actions_mode && !output_dir.empty();
  std::set<std::string> written;
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
    const std::string trace_id = fs::path(f).stem().string();
    using FilePtr = std::unique_ptr<std::FILE, int (*)(std::FILE *)>;
    FilePtr fixture_output(nullptr, &std::fclose);
    std::FILE *out = stdout;
    auto meta_it = manifest.find(trace_id);
    const FixtureMeta *meta = meta_it != manifest.end() ? &meta_it->second : nullptr;
    if (per_fixture_output) {
      fs::path output_path = fs::path(output_dir) / fixture_relpath(trace_id, meta);
      if (!written.insert(output_path.string()).second) {
        std::fprintf(stderr, "fixture manifest: two fixtures map to %s\n",
                     output_path.string().c_str());
        return 2;
      }
      std::error_code ec;
      fs::create_directories(output_path.parent_path(), ec);
      fixture_output.reset(ec ? nullptr : std::fopen(output_path.string().c_str(), "wb"));
      if (!fixture_output) {
        std::fprintf(stderr, "failed to open actions output file %s\n",
                     output_path.string().c_str());
        return 2;
      }
      out = fixture_output.get();
    } else {
      std::fprintf(out, "=== %s\n", fs::path(f).filename().string().c_str());
    }
    if (actions_mode) std::fprintf(out, "%s", fixture_header(trace_id, meta).c_str());
    auto r_ctx = load_ctx(f);
    if (r_ctx.is_error()) {
      if (actions_mode) {
        std::fprintf(out, "error: %s\n", yaml_str(r_ctx.error().message().str()).c_str());
      } else {
        std::fprintf(out, "ERROR: %s\n", r_ctx.error().message().c_str());
      }
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
      // Per-trace exception boundary: regular actions discarded, the failure
      // on the dump channel, reason on stderr. In --actions mode the
      // basic-action fallback rows follow. No corpus trace triggers this.
      std::fprintf(stderr, "TRACE_FAIL %s: %s\n", fs::path(f).filename().string().c_str(),
                   res.failure_reason.c_str());
      if (!actions_mode) {
        std::fprintf(out, "TRACE_FAIL\n");
        continue;
      }
      std::fprintf(out, "failure: %s\nactions:%s\n", yaml_str(res.failure_reason).c_str(),
                   res.fallback_rows.empty() ? " []" : "");
      for (const ActionRow &r : res.fallback_rows) {
        Action act;
        if (!build_action(r, act)) continue;
        std::fprintf(out, "%s", render_action(act, "-", block_key(r.block)).c_str());
      }
      continue;
    }
    // --actions renders the spine row set; --classify renders the matcher
    // fire list. The two sets differ.
    if (actions_mode) {
      // Sorted by anchor: no consumer of the rows depends on their order.
      std::vector<std::pair<std::string, std::string>> rows;
      std::unordered_map<const Block *, const std::string *> matcher_names;
      for (const CoreAction &a : res.actions) {
        matcher_names.emplace(a.produced, &a.matcher_name);
      }
      for (const ActionRow &r : res.action_rows) {
        Action act;
        if (!build_action(r, act)) continue;  // btype outside ported set
        auto matcher = matcher_names.find(r.block);
        const std::string matcher_name =
            matcher != matcher_names.end() ? *matcher->second : std::string("-");
        const std::string anchor = block_key(r.block);
        rows.emplace_back(anchor + " " + act.action_id, render_action(act, matcher_name, anchor));
      }
      // The trace classified cleanly and matched nothing. The synthetic
      // `unknown` row has no Block, so it prints the literal anchor `unknown`.
      if (res.unknown_trace) {
        Action act = create_unknown_action(ctx.trace);
        rows.emplace_back("unknown", render_action(act, "-", "unknown"));
      }
      std::sort(rows.begin(), rows.end());
      std::fprintf(out, "actions:%s\n", rows.empty() ? " []" : "");
      for (const auto &row : rows) std::fprintf(out, "%s", row.second.c_str());
    } else {
      for (const CoreAction &a : res.actions) {
        emit_action(a);
      }
    }
  }
  return 0;
}

int run_classify(const std::vector<std::string> &paths) {
  return run_dump(paths, /*actions_mode=*/false, load_trace_context);
}

int run_actions(const std::vector<std::string> &paths, const std::string &output_dir,
                const std::string &fixtures_manifest) {
  return run_dump(paths, /*actions_mode=*/true, load_trace_context, {}, output_dir,
                  fixtures_manifest);
}

}  // namespace mch
