// Bridge for the emulator's ::Trace type. Its separate target keeps mch-emu
// linkable without emulator_static.
#pragma once

#include "ParsedBlockLookupSource.h"

struct Trace;  // Emulator ::Trace; mch::Trace also exists.

struct DetectedAccounts;

namespace mch {

// Adapts detector results without retaining transaction or block cells.
ParsedBlockLookupSource::InterfaceMap make_interface_map(const ::Trace &trace);
ParsedBlockLookupSource::InterfaceMap make_interface_map(const ::DetectedAccounts &accounts);

}  // namespace mch
