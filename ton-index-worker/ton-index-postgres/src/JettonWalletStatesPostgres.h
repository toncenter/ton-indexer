#pragma once

#include "InsertManagerBase.h"
#include <pqxx/pqxx>

namespace jetton_history::postgres {

void insert_states(pqxx::work& txn, const std::vector<InsertTaskStruct>& tasks);

}  // namespace jetton_history::postgres
