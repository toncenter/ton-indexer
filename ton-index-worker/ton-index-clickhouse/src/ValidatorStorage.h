#pragma once

#include "InsertManagerBase.h"
#include "clickhouse/client.h"

namespace validator_storage {

const std::vector<std::string>& schema_queries();

struct TableBlock {
    std::string table;
    clickhouse::Block block;
};

// Builds and validates the entire batch before its first insert.
std::vector<TableBlock> prepare_blocks(const std::vector<InsertTaskStruct>& tasks);
void insert(clickhouse::Client& client, const std::vector<InsertTaskStruct>& tasks);

}  // namespace validator_storage
