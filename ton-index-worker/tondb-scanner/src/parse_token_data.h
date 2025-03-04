#pragma once
#include <map>
#include "td/utils/Status.h"
#include "vm/cells/Cell.h"
#include "common/refcnt.hpp"


td::Result<std::map<std::string, std::string>> parse_token_data(td::Ref<vm::Cell> cell);
