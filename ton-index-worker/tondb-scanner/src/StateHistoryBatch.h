#pragma once

#include "InsertManagerBase.h"
#include <stdexcept>

namespace account_history {

template<class Row, class Consumer>
void for_each_row(const std::vector<InsertTaskStruct>& tasks,
                  std::optional<std::vector<Row>> ParsedBlock::* member, Consumer&& consume) {
    for (const auto& task : tasks) {
        const auto& rows = task.parsed_block_.get()->*member;
        if (!rows) {
            throw std::runtime_error("Missing token history in insert batch");
        }
        for (const auto& row : *rows) {
            if (row.mc_seqno != task.mc_seqno_ || !row.valid()) {
                throw std::runtime_error("Invalid token history in insert batch");
            }
            consume(row);
        }
    }
}

}  // namespace account_history
