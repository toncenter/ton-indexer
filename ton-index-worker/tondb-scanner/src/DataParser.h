#pragma once
#include "IndexData.h"

td::Result<td::Bits256> ext_in_msg_get_normalized_hash(td::Ref<vm::Cell> ext_in_msg_cell);
td::Result<schema::AccountState> parse_account_state(td::Ref<vm::Cell> account_root, uint32_t gen_utime, td::Bits256 last_trans_hash, uint64_t last_trans_lt);

class ParseManager: public td::actor::Actor {
public:
    ParseManager() = default;

    void parse(int mc_seqno, DataContainerPtr parsed_block, std::shared_ptr<vm::CellDbReader> cell_db_reader, td::Promise<DataContainerPtr> promise);
};