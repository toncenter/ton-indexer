#pragma once
#include "IndexData.h"


class TraceAssembler: public td::actor::Actor {
public:
    TraceAssembler() {}
    
    void assemble(int mc_seqno, ParsedBlockPtr mc_block_, td::Promise<ParsedBlockPtr> promise);
};
