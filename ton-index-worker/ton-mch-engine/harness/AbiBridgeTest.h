// --abi-bridge-test mode: self-contained checks for the ton-abi bridge
// (AbiBridge.{h,cpp}). Drives the trampoline (from_slice -> to_abi_value ->
// adapter) over committed generated fixture structs, plus direct AbiValue->Value
// adapter units for the kinds no fixture reaches, plus the registry dup policy.
// Prints one PASS/FAIL line per check and returns 0 iff all checks pass.
#pragma once

namespace mch {

int run_abi_bridge_test();

}  // namespace mch
