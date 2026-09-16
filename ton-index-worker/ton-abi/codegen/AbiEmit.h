#pragma once

// Generates committed C++ source pairs from a loaded ContractABI and AbiKernel.
// Resolution/dispatch are baked into typed from_slice/store/to_abi_value.
// Non-standard map keys fail generation; unsupported fields become compiling stubs.

#include "AbiKernel.h"
#include "AbiModel.h"

#include "td/utils/Status.h"

#include <set>
#include <string>

namespace ton_abi {

struct GeneratedFiles {
  std::string contract_snake;  // sanitized snake name -> file/namespace base
  std::string header;          // <contract_snake>_gen.h contents
  std::string source;          // <contract_snake>_gen.cpp contents
};

// ABI-facing "StructName.field" names whose decoded wire values must equal
// their declared defaults.
using ValidateManifest = std::set<std::string>;

// Emit C++ files for abi, failing on resolution or unsupported map keys. out_name
// overrides the generated basename and namespace; validate_manifest selects
// decoded fields whose declared defaults are enforced.
td::Result<GeneratedFiles> emit_abi(const ContractABI &abi, const AbiKernel &kernel,
                                    const std::string &out_name = {},
                                    const ValidateManifest &validate_manifest = {});

}  // namespace ton_abi
