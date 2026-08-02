// Build-time and test-only IR artifact loader. It copies JSON data into immutable
// matcher tables and remains independent of host registries. Production consumes
// generated tables instead.
#pragma once

#include "IrTables.h"

#include "td/utils/Status.h"

#include <string>
#include <vector>

namespace mch {

struct LoadedIr {
  std::vector<CompiledMatcher> matchers;  // priority order (stable by priority)

  // SHA-256 (hex) of the artifact file bytes. This is the identity the generated
  // table records (gen_matchers_ir_source_sha), so the equivalence gate can
  // prove it compared the document the build compiled in.
  std::string source_sha256;
};

// Load + compile an artifact from a JSON file path.
td::Result<LoadedIr> load_ir(const std::string &path);

}  // namespace mch
