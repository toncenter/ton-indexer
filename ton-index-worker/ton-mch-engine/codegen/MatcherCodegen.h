// Deterministic MCH matcher-table-to-C++ generator.
// It prints the loader's compiled table instead of parsing the artifact again.
#pragma once

#include "fixtures/IrLoader.h"

#include <string>

namespace mch_codegen {

// Unordered containers are sorted so regenerated matcher tables are byte-identical.
std::string generate_matchers_file(const mch::LoadedIr &ir, const std::string &header,
                                   const std::string &suffix);

}  // namespace mch_codegen
