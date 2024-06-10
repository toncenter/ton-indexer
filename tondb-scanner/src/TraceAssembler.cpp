#include "TraceAssembler.h"


void TraceAssembler::assemble(int mc_seqno, ParsedBlockPtr mc_block_, td::Promise<ParsedBlockPtr> promise) {
    // LOG(INFO) << "Assebled trace for seqno " << mc_seqno;

    promise.set_result(std::move(mc_block_));
}
