#pragma once

// Generates committed C++ source pairs from a loaded ContractABI and AbiKernel.
// Resolution and dispatch are baked into typed from_slice, store, and
// to_abi_value bodies. Non-standard map keys fail generation; unsupported
// per-direction fields produce compiling runtime-error stubs. Generated code
// has no dependency on the loader or kernel.

#include "AbiKernel.h"
#include "AbiModel.h"

#include "td/utils/Status.h"

#include <string>

namespace ton_abi {

struct GeneratedFiles {
  std::string contract_snake;  // sanitized snake name -> file/namespace base
  std::string header;          // <contract_snake>_gen.h contents
  std::string source;          // <contract_snake>_gen.cpp contents
};

// Emit the generated pair for `abi` (kernel resolves it). Fail-closed on
// non-standard map keys and any resolution error. `out_name` (a fixture stem)
// determines the output file basename + namespace -- NOT contract_name, since
// distinct fixtures can share a contract_name (e.g. both err-cont-on-stack-*
// are "Err"). When empty, falls back to the contract_name's snake form.
td::Result<GeneratedFiles> emit_abi(const ContractABI &abi, const AbiKernel &kernel,
                                    const std::string &out_name = {});

}  // namespace ton_abi
