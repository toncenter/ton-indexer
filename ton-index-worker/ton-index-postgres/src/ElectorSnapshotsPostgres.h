#pragma once
#include "InsertManagerBase.h"
#include <pqxx/pqxx>
namespace staking_history::postgres {
void insert_states(pqxx::work& txn, const std::vector<InsertTaskStruct>& tasks);
}
