#pragma once
#include <cstddef>
#include <cstdint>
#include <string>
#include "td/utils/Status.h"
#include "IndexData.h"

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
