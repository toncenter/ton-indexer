#pragma once
#include "IndexData.h"


class ParseManager: public td::actor::Actor {
public:
    ParseManager();
    void start_up() override;

    void parse(int, MasterchainBlockDataState, td::Promise<ParsedBlock>);
};
