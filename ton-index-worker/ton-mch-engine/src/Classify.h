// End-to-end classifier and serialized-action dump adapters.
// Generated build tables are source-hash checked against the matcher artifact.
#pragma once

#include "BlockTree.h"  // TraceContext (loader callback return type)

#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace mch {

struct CoreAction;  // ClassifyCore.h
struct Action;  // ActionBuild.h
class LookupSource;  // BuildRuntime.h

// Supplies a TraceContext for a fixture path. The default path uses the MsgPack
// loader; schema-based harnesses can inject a SchemaTraceLoader callback.
using TraceContextLoader = std::function<td::Result<TraceContext>(const std::string &)>;

// Parameterizes the LookupSource used by run_dump. Empty (default) uses the
// fixture-backed FixtureLookupSource; callers may inject a different source.
using LookupSourceFactory = std::function<std::shared_ptr<LookupSource>(TraceContext &)>;

// Dump-side fixed-column Action renderer, exposed for hermetic action-surface
// tests that must not enter the matcher pipeline.
std::string render_action(const Action &action);

// Renders the intermediate matcher-fire view rather than serialized action rows.
int run_classify(const std::vector<std::string> &paths);

// Shared classify/actions dump over an injectable context loader (the render +
// engine loop live here; only ctx acquisition varies A vs B). Exported so the
// schema A/B gate reuses the exact rendering, keeping path B byte-comparable.
int run_dump(const std::vector<std::string> &paths,
             bool actions_mode, const TraceContextLoader &load_ctx,
             const LookupSourceFactory &lookup_factory = {});

// `--actions` serialized-action-row dump. Uses the same collect-to-final-pass
// pipeline as run_classify; each surviving post-pass produced block is rendered
// as an Action row, with one block_tree_serializer.py _fill_* port per supported
// serialized btype. Python twin:
// twins/py_actions.py. Dump line:
//   ACTION anchor=<key> {id=..,type=..,<directional>,<composite maps>..}
// Value rendering per fixtures/Render.h render_value (sorted keys). Rows are built
// lib-side by ActionBuild.cpp. See its header block for what is and is not
// rendered (roles are not; accounts and the initiating-tx extension ARE).
int run_actions(const std::vector<std::string> &paths);

}  // namespace mch
