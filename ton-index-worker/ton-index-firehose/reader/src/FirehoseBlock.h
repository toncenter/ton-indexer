#pragma once
#include <cstddef>
#include <cstdint>
#include <string>
#include "td/utils/Status.h"
#include "crypto/vm/cells/Cell.h"
#include "ton/ton-types.h"
#include "IndexData.h"
#include "sf/ton/type/v1/type.pb.h"

inline constexpr const char kFirehoseBlockType[] = "sf.ton.type.v1.Block";

// A masterchain round as a Firehose block: the serialized sf.ton.type.v1.Block
// and the metadata Firehose links blocks with. Masterchain blocks are final as
// soon as they are committed, so every block is its own last irreversible block.
struct FirehoseBlock {
  std::uint64_t number;  // masterchain seqno
  std::string id;        // masterchain root hash, lowercase hex
  std::uint64_t parent_num;
  std::string parent_id;
  std::uint32_t gen_utime;
  std::string payload;
};

td::Result<FirehoseBlock> make_firehose_block(const ParsedBlock& parsed_block);

// Sets the size in bytes of the cache of converted accounts across blocks; 0 disables it.
void set_account_cache_size(std::size_t bytes);

// Counts of where converted accounts came from, for the log.
std::string firehose_account_stats();

// The payload as JSON with proto field names, to compare against API v3.
td::Result<std::string> firehose_payload_to_json(const std::string& payload);

// The account with this root cell, as in blocks but with code, data and
// library given by their hashes only. Loads the cells it reads.
td::Result<sf::ton::type::v1::Account> account_to_proto(const td::Ref<vm::Cell>& root);

// A standalone transaction, with messages and phases, without trace metadata.
td::Result<sf::ton::type::v1::Transaction> transaction_to_proto(const td::Ref<vm::Cell>& root);

void block_id_to_proto(sf::ton::type::v1::BlockIdExt* out, const ton::BlockIdExt& id);
