#pragma once
#include "td/actor/actor.h"

class DbReader: public td::actor::Actor {
    std::string db_root_;
public:
    DbReader(std::string db_root): db_root_(db_root) {}

    void start_up() override;
    void alarm() override;
};
