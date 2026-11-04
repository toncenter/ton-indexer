#pragma once

#include "InsertManagerBase.h"
#include <pqxx/pqxx>

namespace elector::postgres {
void insert_round_results(pqxx::work& txn, const std::vector<InsertTaskStruct>& tasks);
}
