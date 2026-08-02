// Bridge for the emulator's ::Trace type. Its separate target keeps mch-emu
// linkable without emulator_static.
#pragma once

#include "EmuClassifierActor.h"
#include "EmuTypes.h"

struct Trace;  // Emulator ::Trace; mch::Trace also exists.

namespace mch {

// Copies references, PODs, and the cell anchor without loading cells.
EmuTraceView make_view(const ::Trace &trace);

}  // namespace mch
