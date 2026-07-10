#include "KvrocksState.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <iterator>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <tuple>
#include <unordered_map>
#include <unordered_set>

#include "convert-utils.h"
#include "td/utils/JsonBuilder.h"
#include "td/utils/base64.h"
#include "td/utils/crypto.h"
#include "td/utils/logging.h"

namespace kvrocks_state {
namespace {

constexpr std::size_t KVROCKS_PIPELINE_FLUSH_SIZE = 1000;
constexpr std::size_t KVROCKS_INDEX_SNAPSHOT_CHUNK_SIZE = 1000;
constexpr long long KVROCKS_INDEX_SNAPSHOT_CONFLICT = -1;
constexpr std::size_t KVROCKS_INDEX_SNAPSHOT_MAX_RETRIES = 5;

class KvrocksIndexSnapshotConflict final : public std::runtime_error {
public:
  KvrocksIndexSnapshotConflict() : KvrocksIndexSnapshotConflict("Kvrocks indexed write snapshot conflict") {
  }

  explicit KvrocksIndexSnapshotConflict(const std::string& message) : std::runtime_error(message) {
  }
};

std::string trim(std::string value) {
  auto is_space = [](unsigned char ch) {
    return std::isspace(ch) != 0;
  };
  value.erase(value.begin(), std::find_if(value.begin(), value.end(), [&](char ch) {
    return !is_space(static_cast<unsigned char>(ch));
  }));
  value.erase(std::find_if(value.rbegin(), value.rend(), [&](char ch) {
    return !is_space(static_cast<unsigned char>(ch));
  }).base(), value.end());
  return value;
}

std::vector<std::string> split(const std::string& value, char delimiter) {
  std::vector<std::string> result;
  std::stringstream ss(value);
  std::string item;
  while (std::getline(ss, item, delimiter)) {
    result.push_back(trim(item));
  }
  return result;
}

template <typename Map, typename KeyFn>
auto get_ordered_map_values(const Map& values, KeyFn key_fn) {
  using Value = typename Map::mapped_type;
  std::vector<std::pair<std::string, const Value*>> ordered;
  ordered.reserve(values.size());
  for (const auto& [key, value] : values) {
    ordered.emplace_back(key_fn(key, value), &value);
  }
  std::sort(ordered.begin(), ordered.end(), [](const auto& lhs, const auto& rhs) {
    return lhs.first < rhs.first;
  });
  return ordered;
}

template <typename Container, typename KeyFn>
auto get_ordered_values(const Container& values, KeyFn key_fn) {
  using Value = typename Container::value_type;
  std::vector<std::pair<std::string, const Value*>> ordered;
  ordered.reserve(values.size());
  for (const auto& value : values) {
    ordered.emplace_back(key_fn(value), &value);
  }
  std::sort(ordered.begin(), ordered.end(), [](const auto& lhs, const auto& rhs) {
    return lhs.first < rhs.first;
  });
  return ordered;
}

std::string to_bits256_base64(const td::Bits256& value) {
  return td::base64_encode(value.as_slice());
}

const std::string& kvrocks_set_once_script() {
  static const std::string script = R"(
local exists = redis.call('EXISTS', KEYS[1])
if exists == 0 then
  redis.call('HSET', KEYS[1], 'source_mc_seqno', ARGV[1])
  redis.call('SET', KEYS[2], ARGV[2])
  return 1
end
return 0
)";
  return script;
}

const std::string& kvrocks_set_current_script() {
  static const std::string script = R"(
local current = redis.call('HGET', KEYS[1], 'source_mc_seqno')
if (not current) or tonumber(current) <= tonumber(ARGV[1]) then
  redis.call('HSET', KEYS[1], 'source_mc_seqno', ARGV[1])
  redis.call('SET', KEYS[2], ARGV[2])
  return 1
end
return 0
)";
  return script;
}

const std::string& kvrocks_set_current_existing_script() {
  static const std::string script = R"(
local current = redis.call('HGET', KEYS[1], 'source_mc_seqno')
if (not current) or tonumber(current) > tonumber(ARGV[1]) then
  return 0
end

redis.call('HSET', KEYS[1], 'source_mc_seqno', ARGV[1])
redis.call('SET', KEYS[2], ARGV[2])
return 1
)";
  return script;
}

const std::string& kvrocks_set_indexed_current_script() {
  static const std::string script = R"(
local current = redis.call('HGET', KEYS[1], 'source_mc_seqno')
if current and tonumber(current) > tonumber(ARGV[1]) then
  return 0
end

local old_count = tonumber(ARGV[3])
local idx_count = tonumber(ARGV[4])
local actual_old_count = tonumber(redis.call('HGET', KEYS[1], 'idx_count') or '0')
if actual_old_count ~= old_count then
  return -1
end

for i = 1, old_count do
  local old_key = KEYS[2 + i]
  local old_member = ARGV[4 + i]
  local actual_old_key = redis.call('HGET', KEYS[1], 'idx_key_' .. i)
  local actual_old_member = redis.call('HGET', KEYS[1], 'idx_member_' .. i)
  if (not actual_old_key) or actual_old_key ~= old_key or (not actual_old_member) or actual_old_member ~= old_member then
    return -1
  end
end

for i = 1, old_count do
  local old_key = KEYS[2 + i]
  local old_member = ARGV[4 + i]
  redis.call('ZREM', old_key, old_member)
end

local first_seen = redis.call('HGET', KEYS[1], 'first_seen_seqno')
if not first_seen then
  first_seen = ARGV[1]
end

redis.call('HSET', KEYS[1],
  'source_mc_seqno', ARGV[1],
  'first_seen_seqno', first_seen)
redis.call('SET', KEYS[2], ARGV[2])

redis.call('HSET', KEYS[1], 'idx_count', idx_count)
local key_pos = 3 + old_count
local arg_pos = 5 + old_count
for i = 1, idx_count do
  local idx_key = KEYS[key_pos]
  local idx_member = ARGV[arg_pos]
  local idx_score = ARGV[arg_pos + 1]
  if idx_score == '__first_seen__' then
    idx_score = first_seen
  end
  redis.call('ZADD', idx_key, idx_score, idx_member)
  redis.call('HSET', KEYS[1],
    'idx_key_' .. i, idx_key,
    'idx_member_' .. i, idx_member)
  key_pos = key_pos + 1
  arg_pos = arg_pos + 2
end

for i = idx_count + 1, old_count do
  redis.call('HDEL', KEYS[1], 'idx_key_' .. i, 'idx_member_' .. i)
end

return 1
)";
  return script;
}

const std::string& kvrocks_set_indexed_current_existing_script() {
  static const std::string script = R"(
local current = redis.call('HGET', KEYS[1], 'source_mc_seqno')
if (not current) or tonumber(current) > tonumber(ARGV[1]) then
  return 0
end

local old_count = tonumber(ARGV[3])
local idx_count = tonumber(ARGV[4])
local actual_old_count = tonumber(redis.call('HGET', KEYS[1], 'idx_count') or '0')
if actual_old_count ~= old_count then
  return -1
end

for i = 1, old_count do
  local old_key = KEYS[2 + i]
  local old_member = ARGV[4 + i]
  local actual_old_key = redis.call('HGET', KEYS[1], 'idx_key_' .. i)
  local actual_old_member = redis.call('HGET', KEYS[1], 'idx_member_' .. i)
  if (not actual_old_key) or actual_old_key ~= old_key or (not actual_old_member) or actual_old_member ~= old_member then
    return -1
  end
end

for i = 1, old_count do
  local old_key = KEYS[2 + i]
  local old_member = ARGV[4 + i]
  redis.call('ZREM', old_key, old_member)
end

local first_seen = redis.call('HGET', KEYS[1], 'first_seen_seqno')
if not first_seen then
  first_seen = ARGV[1]
end

redis.call('HSET', KEYS[1],
  'source_mc_seqno', ARGV[1],
  'first_seen_seqno', first_seen)
redis.call('SET', KEYS[2], ARGV[2])

redis.call('HSET', KEYS[1], 'idx_count', idx_count)
local key_pos = 3 + old_count
local arg_pos = 5 + old_count
for i = 1, idx_count do
  local idx_key = KEYS[key_pos]
  local idx_member = ARGV[arg_pos]
  local idx_score = ARGV[arg_pos + 1]
  if idx_score == '__first_seen__' then
    idx_score = first_seen
  end
  redis.call('ZADD', idx_key, idx_score, idx_member)
  redis.call('HSET', KEYS[1],
    'idx_key_' .. i, idx_key,
    'idx_member_' .. i, idx_member)
  key_pos = key_pos + 1
  arg_pos = arg_pos + 2
end

for i = idx_count + 1, old_count do
  redis.call('HDEL', KEYS[1], 'idx_key_' .. i, 'idx_member_' .. i)
end

return 1
)";
  return script;
}

const std::string& kvrocks_set_indexed_once_script() {
  static const std::string script = R"(
local exists = redis.call('EXISTS', KEYS[1])
if exists ~= 0 then
  return 0
end

redis.call('HSET', KEYS[1],
  'source_mc_seqno', ARGV[1],
  'first_seen_seqno', ARGV[1])
redis.call('SET', KEYS[2], ARGV[2])

local idx_count = tonumber(ARGV[4])
redis.call('HSET', KEYS[1], 'idx_count', idx_count)
local key_pos = 3
local arg_pos = 5
for i = 1, idx_count do
  local idx_key = KEYS[key_pos]
  local idx_member = ARGV[arg_pos]
  local idx_score = ARGV[arg_pos + 1]
  if idx_score == '__first_seen__' then
    idx_score = ARGV[1]
  end
  redis.call('ZADD', idx_key, idx_score, idx_member)
  redis.call('HSET', KEYS[1],
    'idx_key_' .. i, idx_key,
    'idx_member_' .. i, idx_member)
  key_pos = key_pos + 1
  arg_pos = arg_pos + 2
end

return 1
)";
  return script;
}

std::string redis_script_sha1(const std::string& script) {
  std::array<unsigned char, 20> digest{};
  td::sha1(td::Slice(script.data(), script.size()), digest.data());
  return td::hex_encode(td::Slice(reinterpret_cast<const char*>(digest.data()), digest.size()));
}

const std::string& kvrocks_set_once_script_sha() {
  static const std::string sha = redis_script_sha1(kvrocks_set_once_script());
  return sha;
}

const std::string& kvrocks_set_current_script_sha() {
  static const std::string sha = redis_script_sha1(kvrocks_set_current_script());
  return sha;
}

const std::string& kvrocks_set_current_existing_script_sha() {
  static const std::string sha = redis_script_sha1(kvrocks_set_current_existing_script());
  return sha;
}

const std::string& kvrocks_set_indexed_current_script_sha() {
  static const std::string sha = redis_script_sha1(kvrocks_set_indexed_current_script());
  return sha;
}

const std::string& kvrocks_set_indexed_current_existing_script_sha() {
  static const std::string sha = redis_script_sha1(kvrocks_set_indexed_current_existing_script());
  return sha;
}

const std::string& kvrocks_set_indexed_once_script_sha() {
  static const std::string sha = redis_script_sha1(kvrocks_set_indexed_once_script());
  return sha;
}

std::string kvrocks_key(const std::string& table, const std::string& id) {
  return "ton-index:v1:" + table + ":" + id;
}

std::string kvrocks_payload_key(const std::string& table, const std::string& id) {
  return kvrocks_key(table, id) + ":payload";
}

std::string kvrocks_index_key(const std::string& table, const std::string& name) {
  return "ton-index:v1:idx:" + table + ":" + name;
}

std::size_t parse_kvrocks_index_count(const sw::redis::OptionalString& value, const std::string& key) {
  if (!value) {
    return 0;
  }

  std::size_t pos = 0;
  unsigned long long count = 0;
  try {
    count = std::stoull(*value, &pos);
  } catch (const std::exception&) {
    throw std::runtime_error("Invalid Kvrocks idx_count for " + key + ": " + *value);
  }
  if (pos != value->size() || count > std::numeric_limits<std::size_t>::max()) {
    throw std::runtime_error("Invalid Kvrocks idx_count for " + key + ": " + *value);
  }
  return static_cast<std::size_t>(count);
}

std::string address_key(const block::StdAddress& address) {
  return convert::to_raw_address(address);
}

std::string hash_key(const td::Bits256& hash) {
  return to_bits256_base64(hash);
}

std::string nft_real_owner_resolver_id(const block::StdAddress& owner_contract, const block::StdAddress& nft_address) {
  return address_key(owner_contract) + ":" + address_key(nft_address);
}

std::string nft_real_owner_resolver_payload(const std::optional<block::StdAddress>& real_owner) {
  return real_owner ? address_key(*real_owner) : std::string();
}

std::string pad_unsigned_decimal(std::string value, std::size_t width) {
  if (value.size() >= width) {
    return value;
  }
  return std::string(width - value.size(), '0') + value;
}

std::string int256_key(const td::RefInt256& value, std::size_t width = 80) {
  return pad_unsigned_decimal(value->to_dec_string(), width);
}

std::string u64_key(std::uint64_t value, std::size_t width = 20) {
  return pad_unsigned_decimal(std::to_string(value), width);
}

std::string size_key(std::size_t value, std::size_t width = 10) {
  return pad_unsigned_decimal(std::to_string(value), width);
}

std::string optional_address_key(const std::optional<block::StdAddress>& value) {
  return value ? address_key(*value) : "_";
}

void add_by_id_index(std::vector<KvrocksIndexEntry>& indexes, const std::string& table, const std::string& name,
                     const std::string& id) {
  indexes.push_back({kvrocks_index_key(table, name), id, "__first_seen__"});
}

void add_lex_index(std::vector<KvrocksIndexEntry>& indexes, const std::string& table, const std::string& name,
                   const std::string& member) {
  indexes.push_back({kvrocks_index_key(table, name), member, "0"});
}

void json_put_null(td::JsonObjectScope& obj, td::Slice field) {
  obj(field, td::JsonNull());
}

void json_put_bool(td::JsonObjectScope& obj, td::Slice field, bool value) {
  obj(field, td::JsonBool(value));
}

void json_put_i64(td::JsonObjectScope& obj, td::Slice field, std::int64_t value) {
  obj(field, td::JsonLong(value));
}

void json_put_u64_string(td::JsonObjectScope& obj, td::Slice field, std::uint64_t value) {
  obj(field, std::to_string(value));
}

void json_put_string(td::JsonObjectScope& obj, td::Slice field, const std::string& value) {
  obj(field, value);
}

void json_put_raw_json(td::JsonObjectScope& obj, td::Slice field, const std::string& value) {
  obj(field, td::JsonRaw(value));
}

void json_put_int256(td::JsonObjectScope& obj, td::Slice field, const td::RefInt256& value) {
  obj(field, value->to_dec_string());
}

void json_put_optional_int256(td::JsonObjectScope& obj, td::Slice field, const td::RefInt256& value) {
  if (value.not_null()) {
    json_put_int256(obj, field, value);
  } else {
    json_put_null(obj, field);
  }
}

void json_put_hash(td::JsonObjectScope& obj, td::Slice field, const td::Bits256& value) {
  obj(field, hash_key(value));
}

void json_put_optional_hash(td::JsonObjectScope& obj, td::Slice field, const std::optional<td::Bits256>& value) {
  if (value) {
    json_put_hash(obj, field, *value);
  } else {
    json_put_null(obj, field);
  }
}

void json_put_address(td::JsonObjectScope& obj, td::Slice field, const block::StdAddress& value) {
  obj(field, address_key(value));
}

void json_put_optional_address(td::JsonObjectScope& obj, td::Slice field,
                               const std::optional<block::StdAddress>& value) {
  if (value) {
    json_put_address(obj, field, *value);
  } else {
    json_put_null(obj, field);
  }
}

void json_put_optional_string(td::JsonObjectScope& obj, td::Slice field, const std::optional<std::string>& value) {
  if (value) {
    json_put_string(obj, field, *value);
  } else {
    json_put_null(obj, field);
  }
}

void json_put_optional_bool(td::JsonObjectScope& obj, td::Slice field, const std::optional<bool>& value) {
  if (value) {
    json_put_bool(obj, field, *value);
  } else {
    json_put_null(obj, field);
  }
}

void json_put_optional_u32(td::JsonObjectScope& obj, td::Slice field, const std::optional<std::uint32_t>& value) {
  if (value) {
    json_put_i64(obj, field, *value);
  } else {
    json_put_null(obj, field);
  }
}

void json_put_optional_u64_string(td::JsonObjectScope& obj, td::Slice field,
                                  const std::optional<std::uint64_t>& value) {
  if (value) {
    json_put_u64_string(obj, field, *value);
  } else {
    json_put_null(obj, field);
  }
}

void json_put_optional_raw_json(td::JsonObjectScope& obj, td::Slice field, const std::optional<std::string>& value) {
  if (value) {
    json_put_raw_json(obj, field, *value);
  } else {
    json_put_null(obj, field);
  }
}

std::string address_array_json(const std::vector<block::StdAddress>& addresses) {
  td::JsonBuilder jb;
  auto arr = jb.enter_array();
  for (const auto& address : addresses) {
    arr(address_key(address));
  }
  arr.leave();
  return jb.string_builder().as_cslice().str();
}

std::string nominators_json(const std::vector<schema::NominatorPoolNominator>& nominators) {
  td::JsonBuilder jb;
  auto arr = jb.enter_array();
  for (const auto& nominator : nominators) {
    auto value = arr.enter_value();
    auto obj = value.enter_object();
    json_put_address(obj, "address", nominator.address);
    json_put_int256(obj, "balance", nominator.balance);
    json_put_int256(obj, "pending_balance", nominator.pending_balance);
  }
  arr.leave();
  return jb.string_builder().as_cslice().str();
}

std::vector<std::string> nominator_address_keys(const std::vector<schema::NominatorPoolNominator>& nominators) {
  std::vector<std::string> result;
  result.reserve(nominators.size());
  for (const auto& nominator : nominators) {
    result.push_back(address_key(nominator.address));
  }
  std::sort(result.begin(), result.end());
  result.erase(std::unique(result.begin(), result.end()), result.end());
  return result;
}

void json_put_common(td::JsonObjectScope& obj, std::uint32_t source_mc_seqno) {
  json_put_i64(obj, "schema_version", 1);
  json_put_i64(obj, "source_mc_seqno", source_mc_seqno);
}

template <typename Fn>
std::string build_json_payload(Fn fn) {
  td::JsonBuilder jb;
  auto obj = jb.enter_object();
  fn(obj);
  obj.leave();
  return jb.string_builder().as_cslice().str();
}

std::string build_payload(const PreparedMessageContentRow& row) {
  return build_json_payload([&](td::JsonObjectScope& obj) {
    json_put_common(obj, row.source_mc_seqno);
    json_put_hash(obj, "hash", row.hash);
    json_put_string(obj, "body", row.body);
  });
}

std::string build_payload(const PreparedAccountStateRow& row) {
  return build_json_payload([&](td::JsonObjectScope& obj) {
    json_put_common(obj, row.source_mc_seqno);
    json_put_hash(obj, "hash", row.hash);
    json_put_address(obj, "account", row.account);
    json_put_int256(obj, "balance", row.balance);
    json_put_raw_json(obj, "balance_extra_currencies", row.balance_extra_currencies);
    json_put_string(obj, "account_status", row.account_status);
    json_put_optional_hash(obj, "frozen_hash", row.frozen_hash);
    json_put_optional_hash(obj, "code_hash", row.code_hash);
    json_put_optional_hash(obj, "data_hash", row.data_hash);
  });
}

std::string build_payload(const PreparedLatestAccountStateRow& row) {
  return build_json_payload([&](td::JsonObjectScope& obj) {
    json_put_common(obj, row.source_mc_seqno);
    json_put_address(obj, "account", row.account);
    json_put_hash(obj, "hash", row.hash);
    json_put_int256(obj, "balance", row.balance);
    json_put_raw_json(obj, "balance_extra_currencies", row.balance_extra_currencies);
    json_put_string(obj, "account_status", row.account_status);
    json_put_i64(obj, "timestamp", row.timestamp);
    json_put_hash(obj, "last_trans_hash", row.last_trans_hash);
    json_put_u64_string(obj, "last_trans_lt", row.last_trans_lt);
    json_put_optional_hash(obj, "frozen_hash", row.frozen_hash);
    json_put_optional_hash(obj, "data_hash", row.data_hash);
    json_put_optional_hash(obj, "code_hash", row.code_hash);
    json_put_optional_string(obj, "data_boc", row.data_boc);
    json_put_optional_string(obj, "code_boc", row.code_boc);
  });
}

std::string build_payload(const PreparedJettonMasterRow& row) {
  return build_json_payload([&](td::JsonObjectScope& obj) {
    json_put_common(obj, row.source_mc_seqno);
    json_put_address(obj, "address", row.address);
    json_put_int256(obj, "total_supply", row.total_supply);
    json_put_bool(obj, "mintable", row.mintable);
    json_put_optional_address(obj, "admin_address", row.admin_address);
    json_put_optional_raw_json(obj, "jetton_content", row.jetton_content);
    json_put_hash(obj, "jetton_wallet_code_hash", row.jetton_wallet_code_hash);
    json_put_u64_string(obj, "last_transaction_lt", row.last_transaction_lt);
    json_put_hash(obj, "code_hash", row.code_hash);
    json_put_hash(obj, "data_hash", row.data_hash);
    json_put_bool(obj, "destroyed", row.destroyed);
  });
}

std::string build_payload(const PreparedJettonWalletRow& row) {
  return build_json_payload([&](td::JsonObjectScope& obj) {
    json_put_common(obj, row.source_mc_seqno);
    json_put_int256(obj, "balance", row.balance);
    json_put_address(obj, "address", row.address);
    json_put_address(obj, "owner", row.owner);
    json_put_address(obj, "jetton", row.jetton);
    json_put_u64_string(obj, "last_transaction_lt", row.last_transaction_lt);
    json_put_hash(obj, "code_hash", row.code_hash);
    json_put_hash(obj, "data_hash", row.data_hash);
    json_put_optional_bool(obj, "mintless_is_claimed", row.mintless_is_claimed);
    json_put_bool(obj, "destroyed", row.destroyed);
  });
}

std::string build_payload(const PreparedMintlessMasterRow& row) {
  return build_json_payload([&](td::JsonObjectScope& obj) {
    json_put_common(obj, row.source_mc_seqno);
    json_put_address(obj, "address", row.address);
    json_put_bool(obj, "is_indexed", row.is_indexed);
    json_put_null(obj, "custom_payload_api_uri");
  });
}

std::string build_payload(const PreparedNftCollectionRow& row) {
  return build_json_payload([&](td::JsonObjectScope& obj) {
    json_put_common(obj, row.source_mc_seqno);
    json_put_address(obj, "address", row.address);
    json_put_int256(obj, "next_item_index", row.next_item_index);
    json_put_optional_address(obj, "owner_address", row.owner_address);
    json_put_optional_raw_json(obj, "collection_content", row.collection_content);
    json_put_u64_string(obj, "last_transaction_lt", row.last_transaction_lt);
    json_put_hash(obj, "code_hash", row.code_hash);
    json_put_hash(obj, "data_hash", row.data_hash);
    json_put_bool(obj, "destroyed", row.destroyed);
  });
}

std::string build_payload(const PreparedNftItemRow& row) {
  return build_json_payload([&](td::JsonObjectScope& obj) {
    json_put_common(obj, row.source_mc_seqno);
    json_put_address(obj, "address", row.address);
    json_put_bool(obj, "init", row.init);
    json_put_int256(obj, "index", row.index);
    json_put_optional_address(obj, "collection_address", row.collection_address);
    json_put_optional_address(obj, "owner_address", row.owner_address);
    json_put_optional_raw_json(obj, "content", row.content);
    json_put_u64_string(obj, "last_transaction_lt", row.last_transaction_lt);
    json_put_hash(obj, "code_hash", row.code_hash);
    json_put_hash(obj, "data_hash", row.data_hash);
    json_put_optional_address(obj, "real_owner", row.real_owner);
    json_put_bool(obj, "destroyed", row.destroyed);
  });
}

std::string build_payload(const PreparedDnsEntryRow& row) {
  return build_json_payload([&](td::JsonObjectScope& obj) {
    json_put_common(obj, row.source_mc_seqno);
    json_put_address(obj, "nft_item_address", row.nft_item_address);
    json_put_optional_address(obj, "nft_item_owner", row.nft_item_owner);
    json_put_string(obj, "domain", row.domain);
    json_put_optional_address(obj, "dns_next_resolver", row.dns_next_resolver);
    json_put_optional_address(obj, "dns_wallet", row.dns_wallet);
    json_put_optional_hash(obj, "dns_site_adnl", row.dns_site_adnl);
    json_put_optional_hash(obj, "dns_storage_bag_id", row.dns_storage_bag_id);
    json_put_optional_address(obj, "max_bid_address", row.max_bid_address);
    json_put_optional_int256(obj, "max_bid_amount", row.max_bid_amount);
    json_put_optional_u64_string(obj, "auction_end_time", row.auction_end_time);
    json_put_optional_u64_string(obj, "last_fill_up_time", row.last_fill_up_time);
    json_put_u64_string(obj, "last_transaction_lt", row.last_transaction_lt);
    json_put_bool(obj, "destroyed", row.destroyed);
  });
}

std::string build_payload(const PreparedGetgemsSaleRow& row) {
  return build_json_payload([&](td::JsonObjectScope& obj) {
    json_put_common(obj, row.source_mc_seqno);
    json_put_address(obj, "address", row.address);
    json_put_bool(obj, "is_complete", row.is_complete);
    json_put_i64(obj, "created_at", row.created_at);
    json_put_address(obj, "marketplace_address", row.marketplace_address);
    json_put_address(obj, "nft_address", row.nft_address);
    json_put_optional_address(obj, "nft_owner_address", row.nft_owner_address);
    json_put_int256(obj, "full_price", row.full_price);
    json_put_address(obj, "marketplace_fee_address", row.marketplace_fee_address);
    json_put_int256(obj, "marketplace_fee", row.marketplace_fee);
    json_put_address(obj, "royalty_address", row.royalty_address);
    json_put_int256(obj, "royalty_amount", row.royalty_amount);
    json_put_optional_u32(obj, "sold_at", row.sold_at);
    json_put_optional_u64_string(obj, "sold_query_id", row.sold_query_id);
    json_put_optional_raw_json(obj, "jetton_price_dict", row.jetton_price_dict);
    json_put_u64_string(obj, "last_transaction_lt", row.last_transaction_lt);
    json_put_hash(obj, "code_hash", row.code_hash);
    json_put_hash(obj, "data_hash", row.data_hash);
    json_put_bool(obj, "destroyed", row.destroyed);
  });
}

std::string build_payload(const PreparedGetgemsAuctionRow& row) {
  return build_json_payload([&](td::JsonObjectScope& obj) {
    json_put_common(obj, row.source_mc_seqno);
    json_put_address(obj, "address", row.address);
    json_put_bool(obj, "end_flag", row.end_flag);
    json_put_i64(obj, "end_time", row.end_time);
    json_put_address(obj, "mp_addr", row.mp_addr);
    json_put_address(obj, "nft_addr", row.nft_addr);
    json_put_optional_address(obj, "nft_owner", row.nft_owner);
    json_put_int256(obj, "last_bid", row.last_bid);
    json_put_optional_address(obj, "last_member", row.last_member);
    json_put_i64(obj, "min_step", row.min_step);
    json_put_address(obj, "mp_fee_addr", row.mp_fee_addr);
    json_put_i64(obj, "mp_fee_factor", row.mp_fee_factor);
    json_put_i64(obj, "mp_fee_base", row.mp_fee_base);
    json_put_address(obj, "royalty_fee_addr", row.royalty_fee_addr);
    json_put_i64(obj, "royalty_fee_factor", row.royalty_fee_factor);
    json_put_i64(obj, "royalty_fee_base", row.royalty_fee_base);
    json_put_int256(obj, "max_bid", row.max_bid);
    json_put_int256(obj, "min_bid", row.min_bid);
    json_put_i64(obj, "created_at", row.created_at);
    json_put_i64(obj, "last_bid_at", row.last_bid_at);
    json_put_bool(obj, "is_canceled", row.is_canceled);
    json_put_optional_bool(obj, "activated", row.activated);
    json_put_optional_u32(obj, "step_time", row.step_time);
    json_put_optional_u64_string(obj, "last_query_id", row.last_query_id);
    json_put_optional_address(obj, "jetton_wallet", row.jetton_wallet);
    json_put_optional_address(obj, "jetton_master", row.jetton_master);
    json_put_optional_bool(obj, "is_broken_state", row.is_broken_state);
    json_put_optional_string(obj, "public_key", row.public_key);
    json_put_u64_string(obj, "last_transaction_lt", row.last_transaction_lt);
    json_put_hash(obj, "code_hash", row.code_hash);
    json_put_hash(obj, "data_hash", row.data_hash);
    json_put_bool(obj, "destroyed", row.destroyed);
  });
}

std::string build_payload(const PreparedMultisigContractRow& row) {
  return build_json_payload([&](td::JsonObjectScope& obj) {
    json_put_common(obj, row.source_mc_seqno);
    json_put_address(obj, "address", row.address);
    json_put_int256(obj, "next_order_seqno", row.next_order_seqno);
    json_put_i64(obj, "threshold", row.threshold);
    json_put_raw_json(obj, "signers", address_array_json(row.signers));
    json_put_raw_json(obj, "proposers", address_array_json(row.proposers));
    json_put_u64_string(obj, "last_transaction_lt", row.last_transaction_lt);
    json_put_hash(obj, "code_hash", row.code_hash);
    json_put_hash(obj, "data_hash", row.data_hash);
    json_put_bool(obj, "destroyed", row.destroyed);
  });
}

std::string build_payload(const PreparedMultisigOrderRow& row) {
  return build_json_payload([&](td::JsonObjectScope& obj) {
    json_put_common(obj, row.source_mc_seqno);
    json_put_address(obj, "address", row.address);
    json_put_address(obj, "multisig_address", row.multisig_address);
    json_put_int256(obj, "order_seqno", row.order_seqno);
    json_put_i64(obj, "threshold", row.threshold);
    json_put_bool(obj, "sent_for_execution", row.sent_for_execution);
    json_put_int256(obj, "approvals_mask", row.approvals_mask);
    json_put_i64(obj, "approvals_num", row.approvals_num);
    json_put_int256(obj, "expiration_date", row.expiration_date);
    json_put_optional_string(obj, "order_boc", row.order_boc);
    json_put_raw_json(obj, "signers", address_array_json(row.signers));
    json_put_u64_string(obj, "last_transaction_lt", row.last_transaction_lt);
    json_put_hash(obj, "code_hash", row.code_hash);
    json_put_hash(obj, "data_hash", row.data_hash);
    json_put_bool(obj, "destroyed", row.destroyed);
  });
}

std::string build_payload(const PreparedDedustPoolRow& row) {
  return build_json_payload([&](td::JsonObjectScope& obj) {
    json_put_common(obj, row.source_mc_seqno);
    json_put_address(obj, "address", row.address);
    json_put_optional_address(obj, "asset_1", row.asset_1);
    json_put_optional_address(obj, "asset_2", row.asset_2);
    json_put_int256(obj, "reserve_1", row.reserve_1);
    json_put_int256(obj, "reserve_2", row.reserve_2);
    json_put_string(obj, "pool_type", row.pool_type);
    json_put_string(obj, "dex", row.dex);
    obj("fee", td::JsonFloat(row.fee));
    json_put_u64_string(obj, "last_transaction_lt", row.last_transaction_lt);
    json_put_hash(obj, "code_hash", row.code_hash);
    json_put_hash(obj, "data_hash", row.data_hash);
    json_put_bool(obj, "destroyed", row.destroyed);
  });
}

std::string build_payload(const PreparedVestingContractRow& row) {
  return build_json_payload([&](td::JsonObjectScope& obj) {
    json_put_common(obj, row.source_mc_seqno);
    json_put_address(obj, "address", row.address);
    json_put_i64(obj, "vesting_start_time", row.vesting_start_time);
    json_put_i64(obj, "vesting_total_duration", row.vesting_total_duration);
    json_put_i64(obj, "unlock_period", row.unlock_period);
    json_put_i64(obj, "cliff_duration", row.cliff_duration);
    json_put_int256(obj, "vesting_total_amount", row.vesting_total_amount);
    json_put_address(obj, "vesting_sender_address", row.vesting_sender_address);
    json_put_address(obj, "owner_address", row.owner_address);
    json_put_u64_string(obj, "last_transaction_lt", row.last_transaction_lt);
    json_put_hash(obj, "code_hash", row.code_hash);
    json_put_hash(obj, "data_hash", row.data_hash);
    json_put_bool(obj, "destroyed", row.destroyed);
  });
}

std::string build_payload(const PreparedVestingWhitelistRow& row) {
  return build_json_payload([&](td::JsonObjectScope& obj) {
    json_put_common(obj, row.source_mc_seqno);
    json_put_address(obj, "vesting_contract_address", row.vesting_contract_address);
    json_put_address(obj, "wallet_address", row.wallet_address);
  });
}

std::string build_payload(const PreparedNominatorPoolRow& row) {
  return build_json_payload([&](td::JsonObjectScope& obj) {
    json_put_common(obj, row.source_mc_seqno);
    json_put_address(obj, "address", row.address);
    json_put_i64(obj, "state", row.state);
    json_put_i64(obj, "nominators_count", row.nominators_count);
    json_put_int256(obj, "stake_amount_sent", row.stake_amount_sent);
    json_put_int256(obj, "validator_amount", row.validator_amount);
    json_put_address(obj, "validator_address", row.validator_address);
    json_put_i64(obj, "validator_reward_share", row.validator_reward_share);
    json_put_i64(obj, "max_nominators_count", row.max_nominators_count);
    json_put_int256(obj, "min_validator_stake", row.min_validator_stake);
    json_put_int256(obj, "min_nominator_stake", row.min_nominator_stake);
    json_put_raw_json(obj, "active_nominators", row.active_nominators);
    json_put_u64_string(obj, "last_transaction_lt", row.last_transaction_lt);
    json_put_hash(obj, "code_hash", row.code_hash);
    json_put_hash(obj, "data_hash", row.data_hash);
    json_put_bool(obj, "destroyed", row.destroyed);
  });
}

std::string build_payload(const PreparedTelemintRow& row) {
  return build_json_payload([&](td::JsonObjectScope& obj) {
    json_put_common(obj, row.source_mc_seqno);
    json_put_address(obj, "address", row.address);
    json_put_string(obj, "token_name", row.token_name);
    json_put_optional_address(obj, "bidder_address", row.bidder_address);
    json_put_int256(obj, "bid", row.bid);
    json_put_i64(obj, "bid_ts", row.bid_ts);
    json_put_int256(obj, "min_bid", row.min_bid);
    json_put_i64(obj, "end_time", row.end_time);
    json_put_optional_address(obj, "beneficiary_address", row.beneficiary_address);
    json_put_int256(obj, "initial_min_bid", row.initial_min_bid);
    json_put_int256(obj, "max_bid", row.max_bid);
    json_put_int256(obj, "min_bid_step", row.min_bid_step);
    json_put_i64(obj, "min_extend_time", row.min_extend_time);
    json_put_i64(obj, "duration", row.duration);
    json_put_i64(obj, "royalty_numerator", row.royalty_numerator);
    json_put_i64(obj, "royalty_denominator", row.royalty_denominator);
    json_put_address(obj, "royalty_destination", row.royalty_destination);
    json_put_u64_string(obj, "last_transaction_lt", row.last_transaction_lt);
    json_put_hash(obj, "code_hash", row.code_hash);
    json_put_hash(obj, "data_hash", row.data_hash);
    json_put_bool(obj, "destroyed", row.destroyed);
  });
}

std::string build_payload(const PreparedContractMethodsRow& row) {
  return build_json_payload([&](td::JsonObjectScope& obj) {
    json_put_common(obj, row.source_mc_seqno);
    json_put_hash(obj, "code_hash", row.code_hash);
    json_put_string(obj, "methods", row.methods);
  });
}

std::vector<KvrocksIndexEntry> build_indexes(const PreparedLatestAccountStateRow& row) {
  std::vector<KvrocksIndexEntry> indexes;
  const auto id = address_key(row.account);
  add_by_id_index(indexes, "latest_account_states", "all:by_account", id);
  if (row.code_hash) {
    add_by_id_index(indexes, "latest_account_states", "code_hash:" + hash_key(*row.code_hash) + ":by_account", id);
  }
  add_lex_index(indexes, "latest_account_states", "balance", int256_key(row.balance) + ":" + id);
  return indexes;
}

std::vector<KvrocksIndexEntry> build_indexes(const PreparedJettonMasterRow& row) {
  std::vector<KvrocksIndexEntry> indexes;
  if (row.destroyed) {
    return indexes;
  }
  const auto id = address_key(row.address);
  add_by_id_index(indexes, "jetton_masters", "all:by_id", id);
  if (row.admin_address) {
    add_by_id_index(indexes, "jetton_masters", "admin:" + address_key(*row.admin_address) + ":by_id", id);
  }
  return indexes;
}

std::vector<KvrocksIndexEntry> build_indexes(const PreparedJettonWalletRow& row) {
  std::vector<KvrocksIndexEntry> indexes;
  if (row.destroyed) {
    return indexes;
  }
  const auto id = address_key(row.address);
  const auto owner = address_key(row.owner);
  const auto jetton = address_key(row.jetton);
  const auto balance_str = row.balance->to_dec_string();
  const auto balance_member = pad_unsigned_decimal(balance_str, 80) + ":" + id;
  const auto nonzero = balance_str != "0" || (row.mintless_is_claimed && !*row.mintless_is_claimed);
  add_by_id_index(indexes, "jetton_wallets", "all:by_id", id);
  add_by_id_index(indexes, "jetton_wallets", "owner:" + owner + ":by_id", id);
  add_by_id_index(indexes, "jetton_wallets", "jetton:" + jetton + ":by_id", id);
  add_by_id_index(indexes, "jetton_wallets", "owner:" + owner + ":jetton:" + jetton + ":by_id", id);
  add_lex_index(indexes, "jetton_wallets", "all:by_balance", balance_member);
  add_lex_index(indexes, "jetton_wallets", "owner:" + owner + ":by_balance", balance_member);
  add_lex_index(indexes, "jetton_wallets", "jetton:" + jetton + ":by_balance", balance_member);
  add_lex_index(indexes, "jetton_wallets", "owner:" + owner + ":jetton:" + jetton + ":by_balance", balance_member);
  if (nonzero) {
    add_by_id_index(indexes, "jetton_wallets", "all:nonzero:by_id", id);
    add_by_id_index(indexes, "jetton_wallets", "owner:" + owner + ":nonzero:by_id", id);
    add_by_id_index(indexes, "jetton_wallets", "jetton:" + jetton + ":nonzero:by_id", id);
    add_by_id_index(indexes, "jetton_wallets", "owner:" + owner + ":jetton:" + jetton + ":nonzero:by_id", id);
    add_lex_index(indexes, "jetton_wallets", "all:nonzero:by_balance", balance_member);
    add_lex_index(indexes, "jetton_wallets", "owner:" + owner + ":nonzero:by_balance", balance_member);
    add_lex_index(indexes, "jetton_wallets", "jetton:" + jetton + ":nonzero:by_balance", balance_member);
    add_lex_index(indexes, "jetton_wallets", "owner:" + owner + ":jetton:" + jetton + ":nonzero:by_balance", balance_member);
  }
  return indexes;
}

std::vector<KvrocksIndexEntry> build_indexes(const PreparedNftCollectionRow& row) {
  std::vector<KvrocksIndexEntry> indexes;
  if (row.destroyed) {
    return indexes;
  }
  const auto id = address_key(row.address);
  add_by_id_index(indexes, "nft_collections", "all:by_id", id);
  if (row.owner_address) {
    add_by_id_index(indexes, "nft_collections", "owner:" + address_key(*row.owner_address) + ":by_id", id);
  }
  return indexes;
}

std::vector<KvrocksIndexEntry> build_indexes(const PreparedNftItemRow& row) {
  std::vector<KvrocksIndexEntry> indexes;
  if (row.destroyed) {
    return indexes;
  }
  const auto id = address_key(row.address);
  const auto collection = optional_address_key(row.collection_address);
  const auto index_member = collection + ":" + int256_key(row.index) + ":" + id;
  const auto lt_member = u64_key(row.last_transaction_lt) + ":" + id;
  add_by_id_index(indexes, "nft_items", "all:by_id", id);
  add_lex_index(indexes, "nft_items", "all:by_last_transaction_lt", lt_member);
  if (row.collection_address) {
    add_lex_index(indexes, "nft_items", "collection:" + collection + ":by_index", int256_key(row.index) + ":" + id);
    add_by_id_index(indexes, "nft_items", "collection:" + collection + ":index:" + int256_key(row.index), id);
    add_lex_index(indexes, "nft_items", "collection:" + collection + ":by_last_transaction_lt", lt_member);
  }
  if (row.owner_address) {
    const auto owner = address_key(*row.owner_address);
    add_lex_index(indexes, "nft_items", "owner:" + owner + ":by_collection_index", index_member);
    add_lex_index(indexes, "nft_items", "owner:" + owner + ":by_last_transaction_lt", lt_member);
    if (row.collection_address) {
      add_lex_index(indexes, "nft_items", "owner:" + owner + ":collection:" + collection + ":by_index",
                    int256_key(row.index) + ":" + id);
      add_lex_index(indexes, "nft_items", "owner:" + owner + ":collection:" + collection + ":by_last_transaction_lt",
                    lt_member);
    }
  }
  if (row.real_owner) {
    const auto owner = address_key(*row.real_owner);
    add_lex_index(indexes, "nft_items", "real_owner:" + owner + ":by_collection_index", index_member);
    add_lex_index(indexes, "nft_items", "real_owner:" + owner + ":by_last_transaction_lt", lt_member);
    if (row.collection_address) {
      add_lex_index(indexes, "nft_items", "real_owner:" + owner + ":collection:" + collection + ":by_index",
                    int256_key(row.index) + ":" + id);
      add_lex_index(indexes, "nft_items", "real_owner:" + owner + ":collection:" + collection + ":by_last_transaction_lt",
                    lt_member);
    }
  }
  return indexes;
}

std::vector<KvrocksIndexEntry> build_indexes(const PreparedDnsEntryRow& row) {
  std::vector<KvrocksIndexEntry> indexes;
  if (row.destroyed) {
    return indexes;
  }
  const auto id = address_key(row.nft_item_address);
  add_by_id_index(indexes, "dns_entries", "domain:" + row.domain + ":by_id", id);
  if (row.dns_wallet) {
    const auto domain_member = size_key(row.domain.size()) + ":" + row.domain + ":" + id;
    add_lex_index(indexes, "dns_entries", "wallet:" + address_key(*row.dns_wallet) + ":by_domain",
                  domain_member);
    if (row.nft_item_owner && address_key(*row.nft_item_owner) == address_key(*row.dns_wallet)) {
      add_lex_index(indexes, "dns_entries", "owner_wallet:" + address_key(*row.dns_wallet) + ":by_domain",
                    domain_member);
    }
  }
  if (row.max_bid_address && row.auction_end_time) {
    // Index active auctions by bidder and end time. Lua cleanup removes the old
    // member when the auction changes or is claimed.
    add_lex_index(indexes, "dns_entries", "bidder:" + address_key(*row.max_bid_address) + ":by_auction_end_time",
                  u64_key(*row.auction_end_time) + ":" + id);
  }
  return indexes;
}

std::vector<KvrocksIndexEntry> build_indexes(const PreparedMultisigContractRow& row) {
  std::vector<KvrocksIndexEntry> indexes;
  if (row.destroyed) {
    return indexes;
  }
  const auto id = address_key(row.address);
  add_by_id_index(indexes, "multisig", "all:by_id", id);
  for (const auto& signer : row.signers) {
    add_by_id_index(indexes, "multisig", "wallet:" + address_key(signer) + ":by_id", id);
  }
  for (const auto& proposer : row.proposers) {
    add_by_id_index(indexes, "multisig", "wallet:" + address_key(proposer) + ":by_id", id);
  }
  return indexes;
}

std::vector<KvrocksIndexEntry> build_indexes(const PreparedMultisigOrderRow& row) {
  std::vector<KvrocksIndexEntry> indexes;
  if (row.destroyed) {
    return indexes;
  }
  const auto id = address_key(row.address);
  add_by_id_index(indexes, "multisig_orders", "all:by_id", id);
  add_by_id_index(indexes, "multisig_orders", "multisig:" + address_key(row.multisig_address) + ":by_id", id);
  return indexes;
}

std::vector<KvrocksIndexEntry> build_indexes(const PreparedVestingContractRow& row) {
  std::vector<KvrocksIndexEntry> indexes;
  if (row.destroyed) {
    return indexes;
  }
  const auto id = address_key(row.address);
  add_by_id_index(indexes, "vesting_contracts", "all:by_id", id);
  add_by_id_index(indexes, "vesting_contracts", "wallet:" + address_key(row.owner_address) + ":by_id", id);
  add_by_id_index(indexes, "vesting_contracts", "wallet:" + address_key(row.vesting_sender_address) + ":by_id", id);
  return indexes;
}

std::vector<KvrocksIndexEntry> build_indexes(const PreparedVestingWhitelistRow& row) {
  std::vector<KvrocksIndexEntry> indexes;
  const auto contract = address_key(row.vesting_contract_address);
  const auto wallet = address_key(row.wallet_address);
  add_lex_index(indexes, "vesting_whitelist", "contract:" + contract + ":by_wallet", wallet);
  add_by_id_index(indexes, "vesting_contracts", "wallet_whitelist:" + wallet + ":by_id", contract);
  return indexes;
}

std::vector<KvrocksIndexEntry> build_indexes(const PreparedNominatorPoolRow& row) {
  std::vector<KvrocksIndexEntry> indexes;
  if (row.destroyed) {
    return indexes;
  }
  const auto pool = address_key(row.address);
  add_by_id_index(indexes, "nominator_pools", "all:by_id", pool);
  for (const auto& nominator : row.active_nominator_addresses) {
    add_lex_index(indexes, "nominator_pools", "nominator:" + nominator + ":by_pool", pool);
  }
  return indexes;
}

std::runtime_error make_parse_error(const std::string& context, td::Status status) {
  return std::runtime_error(context + ": " + status.to_string());
}

block::StdAddress parse_raw_address_or_throw(const std::string& value, const std::string& context) {
  auto parsed = block::StdAddress::parse(value);
  if (parsed.is_error()) {
    throw make_parse_error(context + " address " + value, parsed.move_as_error());
  }
  return parsed.move_as_ok();
}

std::optional<block::StdAddress> parse_optional_address_field(td::JsonObject& obj, td::Slice field) {
  auto field_r = obj.extract_optional_field(field, td::JsonValue::Type::Null);
  if (field_r.is_error()) {
    throw make_parse_error("Failed to parse nft_items payload field", field_r.move_as_error());
  }
  auto value = field_r.move_as_ok();
  if (value.type() == td::JsonValue::Type::Null) {
    return std::nullopt;
  }
  if (value.type() != td::JsonValue::Type::String) {
    throw std::runtime_error("nft_items payload field must be a string or null");
  }
  return parse_raw_address_or_throw(value.get_string().str(), "Invalid nft_items payload");
}

td::RefInt256 parse_int256_field(const td::JsonObject& obj, td::Slice field) {
  auto value_r = obj.get_required_string_field(field);
  if (value_r.is_error()) {
    throw make_parse_error("Failed to parse nft_items payload int field", value_r.move_as_error());
  }
  auto value = td::dec_string_to_int256(value_r.move_as_ok());
  if (value.is_null()) {
    throw std::runtime_error("Invalid nft_items payload int field");
  }
  return value;
}

std::uint32_t parse_u32_field(const td::JsonObject& obj, td::Slice field) {
  auto value_r = obj.get_required_long_field(field);
  if (value_r.is_error()) {
    throw make_parse_error("Failed to parse nft_items payload uint32 field", value_r.move_as_error());
  }
  auto value = value_r.move_as_ok();
  if (value < 0 || value > std::numeric_limits<std::uint32_t>::max()) {
    throw std::runtime_error("nft_items payload uint32 field is out of range");
  }
  return static_cast<std::uint32_t>(value);
}

std::uint64_t parse_u64_string_field(const td::JsonObject& obj, td::Slice field) {
  auto value_r = obj.get_required_string_field(field);
  if (value_r.is_error()) {
    throw make_parse_error("Failed to parse nft_items payload uint64 field", value_r.move_as_error());
  }
  return static_cast<std::uint64_t>(std::stoull(value_r.move_as_ok()));
}

td::Bits256 parse_bits256_base64_field(const td::JsonObject& obj, td::Slice field) {
  auto value_r = obj.get_required_string_field(field);
  if (value_r.is_error()) {
    throw make_parse_error("Failed to parse nft_items payload hash field", value_r.move_as_error());
  }
  auto decoded_r = td::base64_decode(value_r.move_as_ok());
  if (decoded_r.is_error()) {
    throw make_parse_error("Failed to decode nft_items payload hash field", decoded_r.move_as_error());
  }
  auto decoded = decoded_r.move_as_ok();
  if (decoded.size() != 32) {
    throw std::runtime_error("nft_items payload hash field has invalid length");
  }
  td::Bits256 value;
  value.as_slice().copy_from(td::Slice(decoded.data(), decoded.size()));
  return value;
}

std::optional<std::string> parse_optional_json_raw_field(td::JsonObject& obj, td::Slice field) {
  auto field_r = obj.extract_optional_field(field, td::JsonValue::Type::Null);
  if (field_r.is_error()) {
    throw make_parse_error("Failed to parse nft_items payload JSON field", field_r.move_as_error());
  }
  auto value = field_r.move_as_ok();
  if (value.type() == td::JsonValue::Type::Null) {
    return std::nullopt;
  }

  td::JsonBuilder jb;
  {
    auto scope = jb.enter_value();
    scope << value;
  }
  return jb.string_builder().as_cslice().str();
}

bool parse_optional_bool_field(td::JsonObject& obj, td::Slice field) {
  auto field_r = obj.extract_optional_field(field, td::JsonValue::Type::Null);
  if (field_r.is_error()) {
    throw make_parse_error("Failed to parse payload bool field", field_r.move_as_error());
  }
  auto value = field_r.move_as_ok();
  if (value.type() == td::JsonValue::Type::Null) {
    return false;
  }
  if (value.type() != td::JsonValue::Type::Boolean) {
    throw std::runtime_error("payload bool field has invalid type");
  }
  return value.get_boolean();
}

PreparedNftItemRow parse_nft_item_payload(std::string payload) {
  auto json_r = td::json_decode(td::MutableSlice(payload));
  if (json_r.is_error()) {
    throw make_parse_error("Failed to parse nft_items payload", json_r.move_as_error());
  }
  auto json = json_r.move_as_ok();
  if (json.type() != td::JsonValue::Type::Object) {
    throw std::runtime_error("nft_items payload is not a JSON object");
  }
  auto& obj = json.get_object();

  auto address_r = obj.get_required_string_field("address");
  if (address_r.is_error()) {
    throw make_parse_error("Failed to parse nft_items payload address", address_r.move_as_error());
  }
  auto init_r = obj.get_required_bool_field("init");
  if (init_r.is_error()) {
    throw make_parse_error("Failed to parse nft_items payload init", init_r.move_as_error());
  }

  return PreparedNftItemRow{
    .address = parse_raw_address_or_throw(address_r.move_as_ok(), "Invalid nft_items payload"),
    .init = init_r.move_as_ok(),
    .index = parse_int256_field(obj, "index"),
    .collection_address = parse_optional_address_field(obj, "collection_address"),
    .owner_address = parse_optional_address_field(obj, "owner_address"),
    .content = parse_optional_json_raw_field(obj, "content"),
    .last_transaction_lt = parse_u64_string_field(obj, "last_transaction_lt"),
    .code_hash = parse_bits256_base64_field(obj, "code_hash"),
    .data_hash = parse_bits256_base64_field(obj, "data_hash"),
    .real_owner = parse_optional_address_field(obj, "real_owner"),
    .destroyed = parse_optional_bool_field(obj, "destroyed"),
    .source_mc_seqno = parse_u32_field(obj, "source_mc_seqno"),
  };
}

struct NftRealOwnerCorrection {
  block::StdAddress owner_contract;
  block::StdAddress nft_address;
  block::StdAddress real_owner;
  std::uint32_t source_mc_seqno;
};

std::vector<NftRealOwnerCorrection> collect_nft_real_owner_corrections(const StateBatch& batch) {
  std::map<std::string, NftRealOwnerCorrection> corrections;
  for (const auto& row : batch.getgems_nft_sales) {
    if (row.destroyed || !row.nft_owner_address) {
      continue;
    }
    corrections[nft_real_owner_resolver_id(row.address, row.nft_address)] = {
      .owner_contract = row.address,
      .nft_address = row.nft_address,
      .real_owner = *row.nft_owner_address,
      .source_mc_seqno = row.source_mc_seqno,
    };
  }
  for (const auto& row : batch.getgems_nft_auctions) {
    if (row.destroyed || !row.nft_owner) {
      continue;
    }
    corrections[nft_real_owner_resolver_id(row.address, row.nft_addr)] = {
      .owner_contract = row.address,
      .nft_address = row.nft_addr,
      .real_owner = *row.nft_owner,
      .source_mc_seqno = row.source_mc_seqno,
    };
  }

  std::vector<NftRealOwnerCorrection> result;
  result.reserve(corrections.size());
  for (auto& [_, correction] : corrections) {
    result.push_back(std::move(correction));
  }
  return result;
}

void write_nft_real_owner_resolvers(sw::redis::Redis& redis, const StateBatch& batch) {
  auto pipeline = redis.pipeline(false);
  std::size_t pending = 0;

  auto flush = [&]() {
    if (pending == 0) {
      return;
    }
    auto replies = pipeline.exec();
    if (replies.size() != pending) {
      throw std::runtime_error("Kvrocks NFT real_owner resolver pipeline reply count mismatch");
    }
    for (std::size_t i = 0; i < replies.size(); ++i) {
      replies.get<long long>(i);
    }
    pending = 0;
  };

  auto queue_resolver = [&](const block::StdAddress& owner_contract, const block::StdAddress& nft_address,
                            const std::optional<block::StdAddress>& real_owner,
                            std::uint32_t source_mc_seqno) {
    const auto id = nft_real_owner_resolver_id(owner_contract, nft_address);
    const auto key = kvrocks_key("nft_real_owner_resolvers", id);
    const auto payload_key = kvrocks_payload_key("nft_real_owner_resolvers", id);
    pipeline.evalsha(kvrocks_set_current_script_sha(), {key, payload_key},
                     {std::to_string(source_mc_seqno), nft_real_owner_resolver_payload(real_owner)});
    ++pending;
    if (pending >= KVROCKS_PIPELINE_FLUSH_SIZE) {
      flush();
    }
  };

  for (const auto& row : batch.getgems_nft_sales) {
    if (row.destroyed) {
      continue;
    }
    queue_resolver(row.address, row.nft_address, row.nft_owner_address, row.source_mc_seqno);
  }
  for (const auto& row : batch.getgems_nft_auctions) {
    if (row.destroyed) {
      continue;
    }
    queue_resolver(row.address, row.nft_addr, row.nft_owner, row.source_mc_seqno);
  }

  flush();
}

void append_batch_nft_repair_rows_from_resolvers(sw::redis::Redis& redis, const StateBatch& batch, StateBatch& repair_batch) {
  std::vector<std::size_t> row_indexes;
  std::vector<std::string> resolver_keys;
  row_indexes.reserve(batch.nft_items.size());
  resolver_keys.reserve(batch.nft_items.size());

  for (std::size_t i = 0; i < batch.nft_items.size(); ++i) {
    const auto& row = batch.nft_items[i];
    if (row.destroyed || !row.owner_address) {
      continue;
    }
    row_indexes.push_back(i);
    resolver_keys.push_back(kvrocks_payload_key(
        "nft_real_owner_resolvers", nft_real_owner_resolver_id(*row.owner_address, row.address)));
  }
  if (resolver_keys.empty()) {
    return;
  }

  std::vector<sw::redis::OptionalString> resolver_values;
  resolver_values.reserve(resolver_keys.size());
  redis.mget(resolver_keys.begin(), resolver_keys.end(), std::back_inserter(resolver_values));
  if (resolver_values.size() != resolver_keys.size()) {
    throw std::runtime_error("Kvrocks resolver MGET reply count mismatch");
  }

  for (std::size_t i = 0; i < resolver_values.size(); ++i) {
    if (!resolver_values[i] || resolver_values[i]->empty()) {
      continue;
    }
    const auto& row = batch.nft_items[row_indexes[i]];
    auto real_owner = parse_raw_address_or_throw(*resolver_values[i], "Invalid NFT real_owner resolver");
    if (row.real_owner && address_key(*row.real_owner) == address_key(real_owner)) {
      continue;
    }
    auto repaired = row;
    repaired.real_owner = std::move(real_owner);
    repair_batch.nft_items.push_back(std::move(repaired));
  }
}

void append_existing_nft_repair_rows_from_sales(sw::redis::Redis& redis, const StateBatch& batch, StateBatch& repair_batch) {
  auto corrections = collect_nft_real_owner_corrections(batch);
  if (corrections.empty()) {
    return;
  }

  std::vector<std::string> nft_payload_keys;
  nft_payload_keys.reserve(corrections.size());
  for (const auto& correction : corrections) {
    nft_payload_keys.push_back(kvrocks_payload_key("nft_items", address_key(correction.nft_address)));
  }

  std::vector<sw::redis::OptionalString> nft_payloads;
  nft_payloads.reserve(nft_payload_keys.size());
  redis.mget(nft_payload_keys.begin(), nft_payload_keys.end(), std::back_inserter(nft_payloads));
  if (nft_payloads.size() != nft_payload_keys.size()) {
    throw std::runtime_error("Kvrocks nft_items MGET reply count mismatch");
  }

  for (std::size_t i = 0; i < nft_payloads.size(); ++i) {
    if (!nft_payloads[i]) {
      continue;
    }
    auto row = parse_nft_item_payload(*nft_payloads[i]);
    if (row.destroyed) {
      continue;
    }
    const auto& correction = corrections[i];
    if (!row.owner_address || address_key(*row.owner_address) != address_key(correction.owner_contract)) {
      continue;
    }
    if (row.real_owner && address_key(*row.real_owner) == address_key(correction.real_owner)) {
      continue;
    }
    row.real_owner = correction.real_owner;
    repair_batch.nft_items.push_back(std::move(row));
  }
}

void repair_nft_real_owners(sw::redis::Redis& redis, const StateBatch& batch) {
  write_nft_real_owner_resolvers(redis, batch);

  StateBatch repair_batch;
  append_batch_nft_repair_rows_from_resolvers(redis, batch, repair_batch);
  append_existing_nft_repair_rows_from_sales(redis, batch, repair_batch);
  if (repair_batch.nft_items.empty()) {
    return;
  }

  KvrocksBatchWriter writer(redis, repair_batch);
  writer.write();
  LOG(INFO) << "Repaired " << repair_batch.nft_items.size() << " NFT real_owner rows in Kvrocks";
}

}  // namespace

bool KvrocksConfig::use_sentinel() const {
  return !sentinel_nodes.empty();
}

std::string KvrocksConfig::describe() const {
  std::stringstream ss;
  if (use_sentinel()) {
    ss << "sentinel master=" << sentinel_master_name << " sentinels=";
    bool first = true;
    for (const auto& [host, port] : sentinel_nodes) {
      if (!first) {
        ss << ",";
      }
      ss << host << ":" << port;
      first = false;
    }
  } else {
    ss << "direct uri=" << (uri.empty() ? "tcp://127.0.0.1:6379" : uri);
  }
  ss << " pool_size=" << pool_size;
  ss << " connect_timeout_ms=" << connect_timeout.count();
  ss << " socket_timeout_ms=" << socket_timeout.count();
  ss << " wait_timeout_ms=" << wait_timeout.count();
  if (use_sentinel()) {
    ss << " sentinel_retry_interval_ms=" << sentinel_retry_interval.count();
    ss << " sentinel_max_retry=" << sentinel_max_retry;
  }
  return ss.str();
}

std::vector<std::pair<std::string, int>> parse_kvrocks_sentinel_nodes(const std::string& nodes) {
  std::vector<std::pair<std::string, int>> result;
  for (const auto& raw_node : split(nodes, ',')) {
    if (raw_node.empty()) {
      continue;
    }

    auto colon_pos = raw_node.rfind(':');
    if (colon_pos == std::string::npos || colon_pos == 0 || colon_pos + 1 >= raw_node.size()) {
      throw std::invalid_argument("sentinel node must be host:port: " + raw_node);
    }

    auto host = trim(raw_node.substr(0, colon_pos));
    auto port_str = trim(raw_node.substr(colon_pos + 1));
    int port = 0;
    try {
      port = std::stoi(port_str);
    } catch (...) {
      throw std::invalid_argument("sentinel node port is not a number: " + raw_node);
    }
    if (port <= 0 || port >= 65536) {
      throw std::invalid_argument("sentinel node port is out of range: " + raw_node);
    }
    result.emplace_back(std::move(host), port);
  }

  if (result.empty()) {
    throw std::invalid_argument("sentinel node list is empty");
  }
  return result;
}

KvrocksClient::KvrocksClient(KvrocksConfig config) : config_(std::move(config)) {
  if (!config_.enabled) {
    throw std::invalid_argument("Kvrocks client cannot be constructed with disabled config");
  }
  if (config_.use_sentinel() && config_.sentinel_master_name.empty()) {
    throw std::invalid_argument("Kvrocks sentinel master name is required in sentinel mode");
  }
}

const KvrocksConfig& KvrocksClient::config() const {
  return config_;
}

std::shared_ptr<sw::redis::Redis> KvrocksClient::make_redis() const {
  auto connection_opts = build_connection_options();
  auto pool_opts = build_pool_options();

  if (config_.use_sentinel()) {
    auto sentinel = std::make_shared<sw::redis::Sentinel>(build_sentinel_options());
    return std::make_shared<sw::redis::Redis>(
        sentinel, config_.sentinel_master_name, sw::redis::Role::MASTER, connection_opts, pool_opts);
  }

  return std::make_shared<sw::redis::Redis>(connection_opts, pool_opts);
}

void KvrocksClient::ping() const {
  auto redis = make_redis();
  auto response = redis->ping();
  if (response != "PONG") {
    throw std::runtime_error("unexpected Kvrocks PING response: " + response);
  }
}

sw::redis::ConnectionOptions KvrocksClient::build_connection_options() const {
  sw::redis::ConnectionOptions opts;
  if (!config_.use_sentinel() && !config_.uri.empty()) {
    opts = sw::redis::Uri(config_.uri).connection_options();
  } else {
    opts.host = "127.0.0.1";
    opts.port = 6379;
  }

  if (config_.user.has_value()) {
    opts.user = *config_.user;
  }
  if (config_.password.has_value()) {
    opts.password = *config_.password;
  }
  if (config_.db.has_value()) {
    opts.db = *config_.db;
  }
  opts.keep_alive = true;
  opts.connect_timeout = config_.connect_timeout;
  opts.socket_timeout = config_.socket_timeout;
  return opts;
}

sw::redis::ConnectionPoolOptions KvrocksClient::build_pool_options() const {
  sw::redis::ConnectionPoolOptions opts;
  opts.size = config_.pool_size;
  opts.wait_timeout = config_.wait_timeout;
  return opts;
}

sw::redis::SentinelOptions KvrocksClient::build_sentinel_options() const {
  sw::redis::SentinelOptions opts;
  opts.nodes = config_.sentinel_nodes;
  if (config_.sentinel_user.has_value()) {
    opts.user = *config_.sentinel_user;
  }
  if (config_.sentinel_password.has_value()) {
    opts.password = *config_.sentinel_password;
  }
  opts.connect_timeout = config_.connect_timeout;
  opts.socket_timeout = config_.socket_timeout;
  opts.retry_interval = config_.sentinel_retry_interval;
  opts.max_retry = config_.sentinel_max_retry;
  return opts;
}

std::string content_to_json_string(const std::map<std::string, std::string>& content) {
  td::JsonBuilder content_json;
  auto obj = content_json.enter_object();
  for (auto& attr : content) {
    auto value = attr.second;
    value.erase(std::remove(value.begin(), value.end(), '\0'), value.end());
    obj(attr.first, value);
  }
  obj.leave();
  return content_json.string_builder().as_cslice().str();
}

std::string extra_currencies_to_json_string(const std::map<std::uint32_t, td::RefInt256>& extra_currencies) {
  td::JsonBuilder extra_currencies_json;
  auto obj = extra_currencies_json.enter_object();
  for (auto& currency : extra_currencies) {
    obj(std::to_string(currency.first), currency.second->to_dec_string());
  }
  obj.leave();
  return extra_currencies_json.string_builder().as_cslice().str();
}

std::optional<std::string> serialize_cell_to_base64(td::Ref<vm::Cell> cell) {
  auto res = vm::std_boc_serialize(cell);
  if (res.is_error()) {
    return std::nullopt;
  }
  return td::base64_encode(res.move_as_ok());
}

PreparedLatestAccountStateRow prepare_latest_account_state_row(const LatestAccountStateSourceRow& source,
                                                               std::int32_t max_data_depth) {
  const auto& account_state = source.account_state;
  std::optional<std::string> code_str = std::nullopt;
  std::optional<std::string> data_str = std::nullopt;

  if (max_data_depth >= 0 && account_state.data.not_null() &&
      (max_data_depth == 0 || account_state.data->get_depth() <= max_data_depth)) {
    data_str = serialize_cell_to_base64(account_state.data);
  } else if (account_state.data.not_null()) {
    LOG(DEBUG) << "Large account data: " << account_state.account
               << " Depth: " << account_state.data->get_depth();
  }

  code_str = serialize_cell_to_base64(account_state.code);
  if (code_str && code_str->length() > 128000) {
    LOG(WARNING) << "Large account code: " << account_state.account;
  }

  return PreparedLatestAccountStateRow{
    .account = account_state.account,
    .hash = account_state.hash,
    .balance = account_state.balance.grams,
    .balance_extra_currencies = extra_currencies_to_json_string(account_state.balance.extra_currencies),
    .account_status = account_state.account_status,
    .timestamp = account_state.timestamp,
    .last_trans_hash = account_state.last_trans_hash,
    .last_trans_lt = account_state.last_trans_lt,
    .frozen_hash = account_state.frozen_hash,
    .data_hash = account_state.data_hash,
    .code_hash = account_state.code_hash,
    .data_boc = std::move(data_str),
    .code_boc = std::move(code_str),
    .source_mc_seqno = source.source_mc_seqno,
  };
}

struct DestroyedAccountState {
  block::StdAddress address;
  std::uint64_t last_transaction_lt;
  std::uint32_t source_mc_seqno;
};

td::RefInt256 zero_refint() {
  return td::make_refint(0);
}

td::Bits256 zero_bits256() {
  return {};
}

block::StdAddress zero_masterchain_address() {
  block::StdAddress address;
  address.workchain = -1;
  address.addr.set_zero();
  return address;
}

PreparedJettonMasterRow make_destroyed_jetton_master_row(const DestroyedAccountState& state) {
  return {
    .address = state.address,
    .total_supply = zero_refint(),
    .mintable = false,
    .admin_address = std::nullopt,
    .jetton_content = std::nullopt,
    .jetton_wallet_code_hash = zero_bits256(),
    .last_transaction_lt = state.last_transaction_lt,
    .code_hash = zero_bits256(),
    .data_hash = zero_bits256(),
    .destroyed = true,
    .source_mc_seqno = state.source_mc_seqno,
  };
}

PreparedJettonWalletRow make_destroyed_jetton_wallet_row(const DestroyedAccountState& state) {
  return {
    .balance = zero_refint(),
    .address = state.address,
    .owner = state.address,
    .jetton = state.address,
    .last_transaction_lt = state.last_transaction_lt,
    .code_hash = zero_bits256(),
    .data_hash = zero_bits256(),
    .mintless_is_claimed = std::nullopt,
    .destroyed = true,
    .source_mc_seqno = state.source_mc_seqno,
  };
}

PreparedNftCollectionRow make_destroyed_nft_collection_row(const DestroyedAccountState& state) {
  return {
    .address = state.address,
    .next_item_index = zero_refint(),
    .owner_address = std::nullopt,
    .collection_content = std::nullopt,
    .last_transaction_lt = state.last_transaction_lt,
    .code_hash = zero_bits256(),
    .data_hash = zero_bits256(),
    .destroyed = true,
    .source_mc_seqno = state.source_mc_seqno,
  };
}

PreparedNftItemRow make_destroyed_nft_item_row(const DestroyedAccountState& state) {
  return {
    .address = state.address,
    .init = false,
    .index = zero_refint(),
    .collection_address = std::nullopt,
    .owner_address = std::nullopt,
    .content = std::nullopt,
    .last_transaction_lt = state.last_transaction_lt,
    .code_hash = zero_bits256(),
    .data_hash = zero_bits256(),
    .real_owner = std::nullopt,
    .destroyed = true,
    .source_mc_seqno = state.source_mc_seqno,
  };
}

PreparedDnsEntryRow make_destroyed_dns_entry_row(const DestroyedAccountState& state) {
  return {
    .nft_item_address = state.address,
    .nft_item_owner = std::nullopt,
    .domain = "",
    .dns_next_resolver = std::nullopt,
    .dns_wallet = std::nullopt,
    .dns_site_adnl = std::nullopt,
    .dns_storage_bag_id = std::nullopt,
    .max_bid_address = std::nullopt,
    .max_bid_amount = {},
    .auction_end_time = std::nullopt,
    .last_fill_up_time = std::nullopt,
    .last_transaction_lt = state.last_transaction_lt,
    .destroyed = true,
    .source_mc_seqno = state.source_mc_seqno,
  };
}

PreparedGetgemsSaleRow make_destroyed_getgems_sale_row(const DestroyedAccountState& state) {
  return {
    .address = state.address,
    .is_complete = false,
    .created_at = 0,
    .marketplace_address = state.address,
    .nft_address = state.address,
    .nft_owner_address = std::nullopt,
    .full_price = zero_refint(),
    .marketplace_fee_address = state.address,
    .marketplace_fee = zero_refint(),
    .royalty_address = state.address,
    .royalty_amount = zero_refint(),
    .sold_at = std::nullopt,
    .sold_query_id = std::nullopt,
    .jetton_price_dict = std::nullopt,
    .last_transaction_lt = state.last_transaction_lt,
    .code_hash = zero_bits256(),
    .data_hash = zero_bits256(),
    .destroyed = true,
    .source_mc_seqno = state.source_mc_seqno,
  };
}

PreparedGetgemsAuctionRow make_destroyed_getgems_auction_row(const DestroyedAccountState& state) {
  return {
    .address = state.address,
    .end_flag = false,
    .end_time = 0,
    .mp_addr = state.address,
    .nft_addr = state.address,
    .nft_owner = std::nullopt,
    .last_bid = zero_refint(),
    .last_member = std::nullopt,
    .min_step = 0,
    .mp_fee_addr = state.address,
    .mp_fee_factor = 0,
    .mp_fee_base = 0,
    .royalty_fee_addr = state.address,
    .royalty_fee_factor = 0,
    .royalty_fee_base = 0,
    .max_bid = zero_refint(),
    .min_bid = zero_refint(),
    .created_at = 0,
    .last_bid_at = 0,
    .is_canceled = false,
    .activated = std::nullopt,
    .step_time = std::nullopt,
    .last_query_id = std::nullopt,
    .jetton_wallet = std::nullopt,
    .jetton_master = std::nullopt,
    .is_broken_state = std::nullopt,
    .public_key = std::nullopt,
    .last_transaction_lt = state.last_transaction_lt,
    .code_hash = zero_bits256(),
    .data_hash = zero_bits256(),
    .destroyed = true,
    .source_mc_seqno = state.source_mc_seqno,
  };
}

PreparedMultisigContractRow make_destroyed_multisig_contract_row(const DestroyedAccountState& state) {
  return {
    .address = state.address,
    .next_order_seqno = zero_refint(),
    .threshold = 0,
    .signers = {},
    .proposers = {},
    .last_transaction_lt = state.last_transaction_lt,
    .code_hash = zero_bits256(),
    .data_hash = zero_bits256(),
    .destroyed = true,
    .source_mc_seqno = state.source_mc_seqno,
  };
}

PreparedMultisigOrderRow make_destroyed_multisig_order_row(const DestroyedAccountState& state) {
  return {
    .address = state.address,
    .multisig_address = state.address,
    .order_seqno = zero_refint(),
    .threshold = 0,
    .sent_for_execution = false,
    .approvals_mask = zero_refint(),
    .approvals_num = 0,
    .expiration_date = zero_refint(),
    .order_boc = std::nullopt,
    .signers = {},
    .last_transaction_lt = state.last_transaction_lt,
    .code_hash = zero_bits256(),
    .data_hash = zero_bits256(),
    .destroyed = true,
    .source_mc_seqno = state.source_mc_seqno,
  };
}

PreparedDedustPoolRow make_destroyed_dedust_pool_row(const DestroyedAccountState& state) {
  return {
    .address = state.address,
    .asset_1 = std::nullopt,
    .asset_2 = std::nullopt,
    .reserve_1 = zero_refint(),
    .reserve_2 = zero_refint(),
    .pool_type = "volatile",
    .dex = "dedust",
    .fee = 0.0,
    .last_transaction_lt = state.last_transaction_lt,
    .code_hash = zero_bits256(),
    .data_hash = zero_bits256(),
    .destroyed = true,
    .source_mc_seqno = state.source_mc_seqno,
  };
}

PreparedVestingContractRow make_destroyed_vesting_contract_row(const DestroyedAccountState& state) {
  return {
    .address = state.address,
    .vesting_start_time = 0,
    .vesting_total_duration = 0,
    .unlock_period = 0,
    .cliff_duration = 0,
    .vesting_total_amount = zero_refint(),
    .vesting_sender_address = state.address,
    .owner_address = state.address,
    .last_transaction_lt = state.last_transaction_lt,
    .code_hash = zero_bits256(),
    .data_hash = zero_bits256(),
    .destroyed = true,
    .source_mc_seqno = state.source_mc_seqno,
  };
}

PreparedNominatorPoolRow make_destroyed_nominator_pool_row(const DestroyedAccountState& state) {
  return {
    .address = state.address,
    .state = 0,
    .nominators_count = 0,
    .stake_amount_sent = zero_refint(),
    .validator_amount = zero_refint(),
    .validator_address = zero_masterchain_address(),
    .validator_reward_share = 0,
    .max_nominators_count = 0,
    .min_validator_stake = zero_refint(),
    .min_nominator_stake = zero_refint(),
    .active_nominators = "[]",
    .active_nominator_addresses = {},
    .last_transaction_lt = state.last_transaction_lt,
    .code_hash = zero_bits256(),
    .data_hash = zero_bits256(),
    .destroyed = true,
    .source_mc_seqno = state.source_mc_seqno,
  };
}

PreparedTelemintRow make_destroyed_telemint_row(const DestroyedAccountState& state) {
  return {
    .address = state.address,
    .token_name = "",
    .bidder_address = std::nullopt,
    .bid = zero_refint(),
    .bid_ts = 0,
    .min_bid = zero_refint(),
    .end_time = 0,
    .beneficiary_address = std::nullopt,
    .initial_min_bid = zero_refint(),
    .max_bid = zero_refint(),
    .min_bid_step = zero_refint(),
    .min_extend_time = 0,
    .duration = 0,
    .royalty_numerator = 0,
    .royalty_denominator = 0,
    .royalty_destination = state.address,
    .last_transaction_lt = state.last_transaction_lt,
    .code_hash = zero_bits256(),
    .data_hash = zero_bits256(),
    .destroyed = true,
    .source_mc_seqno = state.source_mc_seqno,
  };
}

StateBatch prepare_point_state_batch(const std::vector<PointStateData>& data,
                                     std::uint32_t source_mc_seqno,
                                     std::int32_t max_data_depth) {
  StateBatch batch;

  std::unordered_map<block::StdAddress, DestroyedAccountState> destroyed_accounts;
  for (const auto& item : data) {
    if (!std::holds_alternative<schema::AccountState>(item)) {
      continue;
    }
    const auto& account_state = std::get<schema::AccountState>(item);
    if (account_state.account_status != "nonexist") {
      continue;
    }
    auto it = destroyed_accounts.find(account_state.account);
    if (it == destroyed_accounts.end() || it->second.last_transaction_lt < account_state.last_trans_lt) {
      destroyed_accounts[account_state.account] = DestroyedAccountState{
        .address = account_state.account,
        .last_transaction_lt = account_state.last_trans_lt,
        .source_mc_seqno = source_mc_seqno,
      };
    }
  }
  auto ordered_destroyed_accounts = get_ordered_map_values(destroyed_accounts, [](const auto& key, const auto&) {
    return convert::to_raw_address(key);
  });
  auto has_newer_destroyed_state = [&](const block::StdAddress& address, std::uint64_t last_transaction_lt) {
    auto it = destroyed_accounts.find(address);
    return it != destroyed_accounts.end() && last_transaction_lt < it->second.last_transaction_lt;
  };

  {
    struct AccountStateWithSource {
      schema::AccountState account_state;
      std::uint32_t source_mc_seqno;
    };
    std::unordered_map<std::string, AccountStateWithSource> latest_account_states;
    for (const auto& item : data) {
      if (!std::holds_alternative<schema::AccountState>(item)) {
        continue;
      }
      const auto& account_state = std::get<schema::AccountState>(item);
      auto account_addr = convert::to_raw_address(account_state.account);
      auto it = latest_account_states.find(account_addr);
      if (it == latest_account_states.end() || it->second.account_state.last_trans_lt < account_state.last_trans_lt) {
        latest_account_states[account_addr] = AccountStateWithSource{
          .account_state = account_state,
          .source_mc_seqno = source_mc_seqno,
        };
      }
    }
    auto ordered = get_ordered_map_values(latest_account_states, [](const auto& key, const auto&) {
      return key;
    });
    batch.latest_account_states.reserve(ordered.size());
    for (const auto& [raw_account, account_state_with_source_ptr] : ordered) {
      batch.latest_account_states.push_back(prepare_latest_account_state_row({
        .account_state = account_state_with_source_ptr->account_state,
        .raw_account = raw_account,
        .source_mc_seqno = account_state_with_source_ptr->source_mc_seqno,
      }, max_data_depth));
    }
  }

  {
    struct JettonMasterWithSource {
      schema::JettonMasterDataV2 value;
      std::uint32_t source_mc_seqno;
    };
    std::unordered_map<block::StdAddress, JettonMasterWithSource> values;
    for (const auto& item : data) {
      if (!std::holds_alternative<schema::JettonMasterDataV2>(item)) {
        continue;
      }
      const auto& value = std::get<schema::JettonMasterDataV2>(item);
      auto it = values.find(value.address);
      if (it == values.end() || it->second.value.last_transaction_lt < value.last_transaction_lt) {
        values[value.address] = {value, source_mc_seqno};
      }
    }
    auto ordered = get_ordered_map_values(values, [](const auto& key, const auto&) {
      return convert::to_raw_address(key);
    });
    batch.jetton_masters.reserve(ordered.size());
    for (const auto& [_, value_with_source_ptr] : ordered) {
      const auto& value = value_with_source_ptr->value;
      std::optional<std::string> content = std::nullopt;
      if (value.jetton_content) {
        content = content_to_json_string(value.jetton_content.value());
      }
      batch.jetton_masters.push_back({
        .address = value.address,
        .total_supply = value.total_supply,
        .mintable = value.mintable,
        .admin_address = value.admin_address,
        .jetton_content = std::move(content),
        .jetton_wallet_code_hash = value.jetton_wallet_code_hash,
        .last_transaction_lt = value.last_transaction_lt,
        .code_hash = value.code_hash,
        .data_hash = value.data_hash,
        .destroyed = false,
        .source_mc_seqno = value_with_source_ptr->source_mc_seqno,
      });
    }
    for (const auto& [_, destroyed_ptr] : ordered_destroyed_accounts) {
      auto it = values.find(destroyed_ptr->address);
      if (it == values.end() || it->second.value.last_transaction_lt < destroyed_ptr->last_transaction_lt) {
        batch.jetton_masters.push_back(make_destroyed_jetton_master_row(*destroyed_ptr));
      }
    }
  }

  {
    struct JettonWalletWithSource {
      schema::JettonWalletDataV2 value;
      std::uint32_t source_mc_seqno;
    };
    std::unordered_map<block::StdAddress, JettonWalletWithSource> values;
    std::unordered_map<block::StdAddress, std::uint32_t> known_mintless_masters;
    for (const auto& item : data) {
      if (!std::holds_alternative<schema::JettonWalletDataV2>(item)) {
        continue;
      }
      const auto& value = std::get<schema::JettonWalletDataV2>(item);
      auto it = values.find(value.address);
      if (it == values.end() || it->second.value.last_transaction_lt < value.last_transaction_lt) {
        values[value.address] = {value, source_mc_seqno};
      }
    }
    auto ordered = get_ordered_map_values(values, [](const auto& key, const auto&) {
      return convert::to_raw_address(key);
    });
    batch.jetton_wallets.reserve(ordered.size());
    for (const auto& [_, value_with_source_ptr] : ordered) {
      const auto& value = value_with_source_ptr->value;
      batch.jetton_wallets.push_back({
        .balance = value.balance,
        .address = value.address,
        .owner = value.owner,
        .jetton = value.jetton,
        .last_transaction_lt = value.last_transaction_lt,
        .code_hash = value.code_hash,
        .data_hash = value.data_hash,
        .mintless_is_claimed = value.mintless_is_claimed,
        .destroyed = false,
        .source_mc_seqno = value_with_source_ptr->source_mc_seqno,
      });
      if (value.mintless_is_claimed.has_value()) {
        known_mintless_masters.emplace(value.jetton, value_with_source_ptr->source_mc_seqno);
      }
    }
    std::vector<std::pair<std::string, const std::pair<const block::StdAddress, std::uint32_t>*>> ordered_mintless;
    ordered_mintless.reserve(known_mintless_masters.size());
    for (const auto& entry : known_mintless_masters) {
      ordered_mintless.emplace_back(convert::to_raw_address(entry.first), &entry);
    }
    std::sort(ordered_mintless.begin(), ordered_mintless.end(), [](const auto& lhs, const auto& rhs) {
      return lhs.first < rhs.first;
    });
    batch.mintless_jetton_masters.reserve(ordered_mintless.size());
    for (const auto& [_, entry_ptr] : ordered_mintless) {
      batch.mintless_jetton_masters.push_back({
        .address = entry_ptr->first,
        .is_indexed = false,
        .source_mc_seqno = entry_ptr->second,
      });
    }
    for (const auto& [_, destroyed_ptr] : ordered_destroyed_accounts) {
      auto it = values.find(destroyed_ptr->address);
      if (it == values.end() || it->second.value.last_transaction_lt < destroyed_ptr->last_transaction_lt) {
        batch.jetton_wallets.push_back(make_destroyed_jetton_wallet_row(*destroyed_ptr));
      }
    }
  }

  {
    struct NftCollectionWithSource {
      schema::NFTCollectionDataV2 value;
      std::uint32_t source_mc_seqno;
    };
    std::unordered_map<block::StdAddress, NftCollectionWithSource> values;
    for (const auto& item : data) {
      if (!std::holds_alternative<schema::NFTCollectionDataV2>(item)) {
        continue;
      }
      const auto& value = std::get<schema::NFTCollectionDataV2>(item);
      auto it = values.find(value.address);
      if (it == values.end() || it->second.value.last_transaction_lt < value.last_transaction_lt) {
        values[value.address] = {value, source_mc_seqno};
      }
    }
    auto ordered = get_ordered_map_values(values, [](const auto& key, const auto&) {
      return convert::to_raw_address(key);
    });
    batch.nft_collections.reserve(ordered.size());
    for (const auto& [_, value_with_source_ptr] : ordered) {
      const auto& value = value_with_source_ptr->value;
      std::optional<std::string> content = std::nullopt;
      if (value.collection_content) {
        content = content_to_json_string(value.collection_content.value());
      }
      batch.nft_collections.push_back({
        .address = value.address,
        .next_item_index = value.next_item_index,
        .owner_address = value.owner_address,
        .collection_content = std::move(content),
        .last_transaction_lt = value.last_transaction_lt,
        .code_hash = value.code_hash,
        .data_hash = value.data_hash,
        .destroyed = false,
        .source_mc_seqno = value_with_source_ptr->source_mc_seqno,
      });
    }
    for (const auto& [_, destroyed_ptr] : ordered_destroyed_accounts) {
      auto it = values.find(destroyed_ptr->address);
      if (it == values.end() || it->second.value.last_transaction_lt < destroyed_ptr->last_transaction_lt) {
        batch.nft_collections.push_back(make_destroyed_nft_collection_row(*destroyed_ptr));
      }
    }
  }

  {
    struct UnifiedSaleData {
      block::StdAddress address;
      bool is_complete;
      std::uint32_t created_at;
      block::StdAddress marketplace_address;
      block::StdAddress nft_address;
      std::optional<block::StdAddress> nft_owner_address;
      td::RefInt256 full_price;
      block::StdAddress marketplace_fee_address;
      td::RefInt256 marketplace_fee;
      block::StdAddress royalty_address;
      td::RefInt256 royalty_amount;
      std::optional<std::uint32_t> sold_at;
      std::optional<std::uint64_t> sold_query_id;
      std::map<std::string, std::string> jetton_price_dict;
      std::uint64_t last_transaction_lt;
      td::Bits256 code_hash;
      td::Bits256 data_hash;
      std::uint32_t source_mc_seqno;
    };

    std::unordered_map<block::StdAddress, UnifiedSaleData> values;
    for (const auto& item : data) {
      if (std::holds_alternative<schema::GetGemsNftFixPriceSaleData>(item)) {
        const auto& sale = std::get<schema::GetGemsNftFixPriceSaleData>(item);
        auto it = values.find(sale.address);
        if (it == values.end() || it->second.last_transaction_lt < sale.last_transaction_lt) {
          values[sale.address] = {
            .address = sale.address,
            .is_complete = sale.is_complete,
            .created_at = sale.created_at,
            .marketplace_address = sale.marketplace_address,
            .nft_address = sale.nft_address,
            .nft_owner_address = sale.nft_owner_address,
            .full_price = sale.full_price,
            .marketplace_fee_address = sale.marketplace_fee_address,
            .marketplace_fee = sale.marketplace_fee,
            .royalty_address = sale.royalty_address,
            .royalty_amount = sale.royalty_amount,
            .sold_at = std::nullopt,
            .sold_query_id = std::nullopt,
            .jetton_price_dict = {},
            .last_transaction_lt = sale.last_transaction_lt,
            .code_hash = sale.code_hash,
            .data_hash = sale.data_hash,
            .source_mc_seqno = source_mc_seqno,
          };
        }
      } else if (std::holds_alternative<schema::GetGemsNftFixPriceSaleV4Data>(item)) {
        const auto& sale = std::get<schema::GetGemsNftFixPriceSaleV4Data>(item);
        auto it = values.find(sale.address);
        if (it == values.end() || it->second.last_transaction_lt < sale.last_transaction_lt) {
          values[sale.address] = {
            .address = sale.address,
            .is_complete = sale.is_complete,
            .created_at = sale.created_at,
            .marketplace_address = sale.marketplace_address,
            .nft_address = sale.nft_address,
            .nft_owner_address = sale.nft_owner_address,
            .full_price = sale.full_price,
            .marketplace_fee_address = sale.marketplace_fee_address,
            .marketplace_fee = sale.marketplace_fee,
            .royalty_address = sale.royalty_address,
            .royalty_amount = sale.royalty_amount,
            .sold_at = sale.sold_at,
            .sold_query_id = sale.sold_query_id,
            .jetton_price_dict = sale.jetton_price_dict,
            .last_transaction_lt = sale.last_transaction_lt,
            .code_hash = sale.code_hash,
            .data_hash = sale.data_hash,
            .source_mc_seqno = source_mc_seqno,
          };
        }
      }
    }

    auto ordered = get_ordered_map_values(values, [](const auto& key, const auto&) {
      return convert::to_raw_address(key);
    });
    batch.getgems_nft_sales.reserve(ordered.size());
    for (const auto& [_, value_ptr] : ordered) {
      const auto& value = *value_ptr;
      std::optional<std::string> jetton_dict_json = std::nullopt;
      if (!value.jetton_price_dict.empty()) {
        td::JsonBuilder jb;
        auto obj = jb.enter_object();
        for (const auto& [key, dict_value] : value.jetton_price_dict) {
          obj(key, dict_value);
        }
        obj.leave();
        jetton_dict_json = jb.string_builder().as_cslice().str();
      }
      batch.getgems_nft_sales.push_back({
        .address = value.address,
        .is_complete = value.is_complete,
        .created_at = value.created_at,
        .marketplace_address = value.marketplace_address,
        .nft_address = value.nft_address,
        .nft_owner_address = value.nft_owner_address,
        .full_price = value.full_price,
        .marketplace_fee_address = value.marketplace_fee_address,
        .marketplace_fee = value.marketplace_fee,
        .royalty_address = value.royalty_address,
        .royalty_amount = value.royalty_amount,
        .sold_at = value.sold_at,
        .sold_query_id = value.sold_query_id,
        .jetton_price_dict = std::move(jetton_dict_json),
        .last_transaction_lt = value.last_transaction_lt,
        .code_hash = value.code_hash,
        .data_hash = value.data_hash,
        .destroyed = false,
        .source_mc_seqno = value.source_mc_seqno,
      });
    }
    for (const auto& [_, destroyed_ptr] : ordered_destroyed_accounts) {
      auto it = values.find(destroyed_ptr->address);
      if (it == values.end() || it->second.last_transaction_lt < destroyed_ptr->last_transaction_lt) {
        batch.getgems_nft_sales.push_back(make_destroyed_getgems_sale_row(*destroyed_ptr));
      }
    }
  }

  {
    struct NftAuctionWithSource {
      schema::GetGemsNftAuctionData value;
      std::uint32_t source_mc_seqno;
    };
    std::unordered_map<block::StdAddress, NftAuctionWithSource> values;
    for (const auto& item : data) {
      if (!std::holds_alternative<schema::GetGemsNftAuctionData>(item)) {
        continue;
      }
      const auto& value = std::get<schema::GetGemsNftAuctionData>(item);
      auto it = values.find(value.address);
      if (it == values.end() || it->second.value.last_transaction_lt < value.last_transaction_lt) {
        values[value.address] = {value, source_mc_seqno};
      }
    }
    auto ordered = get_ordered_map_values(values, [](const auto& key, const auto&) {
      return convert::to_raw_address(key);
    });
    batch.getgems_nft_auctions.reserve(ordered.size());
    for (const auto& [_, value_with_source_ptr] : ordered) {
      const auto& value = value_with_source_ptr->value;
      std::optional<std::string> public_key;
      if (value.public_key.has_value()) {
        public_key = value.public_key.value()->to_hex_string();
      }
      batch.getgems_nft_auctions.push_back({
        .address = value.address,
        .end_flag = value.end,
        .end_time = value.end_time,
        .mp_addr = value.mp_addr,
        .nft_addr = value.nft_addr,
        .nft_owner = value.nft_owner,
        .last_bid = value.last_bid,
        .last_member = value.last_member,
        .min_step = value.min_step,
        .mp_fee_addr = value.mp_fee_addr,
        .mp_fee_factor = value.mp_fee_factor,
        .mp_fee_base = value.mp_fee_base,
        .royalty_fee_addr = value.royalty_fee_addr,
        .royalty_fee_factor = value.royalty_fee_factor,
        .royalty_fee_base = value.royalty_fee_base,
        .max_bid = value.max_bid,
        .min_bid = value.min_bid,
        .created_at = value.created_at,
        .last_bid_at = value.last_bid_at,
        .is_canceled = value.is_canceled,
        .activated = value.activated,
        .step_time = value.step_time,
        .last_query_id = value.last_query_id,
        .jetton_wallet = value.jetton_wallet,
        .jetton_master = value.jetton_master,
        .is_broken_state = value.is_broken_state,
        .public_key = std::move(public_key),
        .last_transaction_lt = value.last_transaction_lt,
        .code_hash = value.code_hash,
        .data_hash = value.data_hash,
        .destroyed = false,
        .source_mc_seqno = value_with_source_ptr->source_mc_seqno,
      });
    }
    for (const auto& [_, destroyed_ptr] : ordered_destroyed_accounts) {
      auto it = values.find(destroyed_ptr->address);
      if (it == values.end() || it->second.value.last_transaction_lt < destroyed_ptr->last_transaction_lt) {
        batch.getgems_nft_auctions.push_back(make_destroyed_getgems_auction_row(*destroyed_ptr));
      }
    }
  }

  {
    std::map<std::pair<ton::StdSmcAddress, ton::StdSmcAddress>, block::StdAddress> sale_real_owners;
    std::map<std::pair<ton::StdSmcAddress, ton::StdSmcAddress>, block::StdAddress> auction_real_owners;
    for (const auto& item : data) {
      if (std::holds_alternative<schema::GetGemsNftFixPriceSaleData>(item)) {
        const auto& sale = std::get<schema::GetGemsNftFixPriceSaleData>(item);
        if (has_newer_destroyed_state(sale.address, sale.last_transaction_lt)) {
          continue;
        }
        if (sale.nft_owner_address) {
          sale_real_owners[{sale.address.addr, sale.nft_address.addr}] = sale.nft_owner_address.value();
        }
      } else if (std::holds_alternative<schema::GetGemsNftFixPriceSaleV4Data>(item)) {
        const auto& sale = std::get<schema::GetGemsNftFixPriceSaleV4Data>(item);
        if (has_newer_destroyed_state(sale.address, sale.last_transaction_lt)) {
          continue;
        }
        if (sale.nft_owner_address) {
          sale_real_owners[{sale.address.addr, sale.nft_address.addr}] = sale.nft_owner_address.value();
        }
      } else if (std::holds_alternative<schema::GetGemsNftAuctionData>(item)) {
        const auto& auction = std::get<schema::GetGemsNftAuctionData>(item);
        if (has_newer_destroyed_state(auction.address, auction.last_transaction_lt)) {
          continue;
        }
        if (auction.nft_owner) {
          auction_real_owners[{auction.address.addr, auction.nft_addr.addr}] = auction.nft_owner.value();
        }
      }
    }

    struct NftItemWithSource {
      schema::NFTItemDataV2 value;
      std::uint32_t source_mc_seqno;
    };
    std::unordered_map<block::StdAddress, NftItemWithSource> values;
    for (const auto& item : data) {
      if (!std::holds_alternative<schema::NFTItemDataV2>(item)) {
        continue;
      }
      const auto& value = std::get<schema::NFTItemDataV2>(item);
      auto it = values.find(value.address);
      if (it == values.end() || it->second.value.last_transaction_lt < value.last_transaction_lt) {
        values[value.address] = {value, source_mc_seqno};
      }
    }
    auto ordered = get_ordered_map_values(values, [](const auto& key, const auto&) {
      return convert::to_raw_address(key);
    });
    batch.nft_items.reserve(ordered.size());
    std::unordered_map<block::StdAddress, std::uint64_t> dns_entry_lts;
    for (const auto& [_, value_with_source_ptr] : ordered) {
      const auto& value = value_with_source_ptr->value;
      std::optional<std::string> content = std::nullopt;
      if (value.content) {
        content = content_to_json_string(value.content.value());
      }

      std::optional<block::StdAddress> real_owner = value.owner_address;
      if (value.owner_address) {
        auto sale_it = sale_real_owners.find({value.owner_address.value().addr, value.address.addr});
        if (sale_it != sale_real_owners.end()) {
          real_owner = sale_it->second;
        } else {
          auto auction_it = auction_real_owners.find({value.owner_address.value().addr, value.address.addr});
          if (auction_it != auction_real_owners.end()) {
            real_owner = auction_it->second;
          }
        }
      }

      batch.nft_items.push_back({
        .address = value.address,
        .init = value.init,
        .index = value.index,
        .collection_address = value.collection_address,
        .owner_address = value.owner_address,
        .content = std::move(content),
        .last_transaction_lt = value.last_transaction_lt,
        .code_hash = value.code_hash,
        .data_hash = value.data_hash,
        .real_owner = real_owner,
        .destroyed = false,
        .source_mc_seqno = value_with_source_ptr->source_mc_seqno,
      });

      if (value.dns_entry) {
        dns_entry_lts[value.address] = value.last_transaction_lt;
        batch.dns_entries.push_back({
          .nft_item_address = value.address,
          .nft_item_owner = value.owner_address,
          .domain = value.dns_entry->domain,
          .dns_next_resolver = value.dns_entry->next_resolver,
          .dns_wallet = value.dns_entry->wallet,
          .dns_site_adnl = value.dns_entry->site_adnl,
          .dns_storage_bag_id = value.dns_entry->storage_bag_id,
          .max_bid_address = value.dns_entry->max_bid_address,
          .max_bid_amount = value.dns_entry->max_bid_amount,
          .auction_end_time = value.dns_entry->auction_end_time,
          .last_fill_up_time = value.dns_entry->last_fill_up_time,
          .last_transaction_lt = value.last_transaction_lt,
          .destroyed = false,
          .source_mc_seqno = value_with_source_ptr->source_mc_seqno,
        });
      }
    }
    for (const auto& [_, destroyed_ptr] : ordered_destroyed_accounts) {
      auto it = values.find(destroyed_ptr->address);
      if (it == values.end() || it->second.value.last_transaction_lt < destroyed_ptr->last_transaction_lt) {
        batch.nft_items.push_back(make_destroyed_nft_item_row(*destroyed_ptr));
      }
      auto dns_it = dns_entry_lts.find(destroyed_ptr->address);
      if (dns_it == dns_entry_lts.end() || dns_it->second < destroyed_ptr->last_transaction_lt) {
        batch.dns_entries.push_back(make_destroyed_dns_entry_row(*destroyed_ptr));
      }
    }
  }

  {
    struct MultisigContractWithSource {
      schema::MultisigContractData value;
      std::uint32_t source_mc_seqno;
    };
    std::unordered_map<block::StdAddress, MultisigContractWithSource> values;
    for (const auto& item : data) {
      if (!std::holds_alternative<schema::MultisigContractData>(item)) {
        continue;
      }
      const auto& value = std::get<schema::MultisigContractData>(item);
      auto it = values.find(value.address);
      if (it == values.end() || it->second.value.last_transaction_lt < value.last_transaction_lt) {
        values[value.address] = {value, source_mc_seqno};
      }
    }
    auto ordered = get_ordered_map_values(values, [](const auto& key, const auto&) {
      return convert::to_raw_address(key);
    });
    batch.multisig_contracts.reserve(ordered.size());
    for (const auto& [_, value_with_source_ptr] : ordered) {
      const auto& value = value_with_source_ptr->value;
      batch.multisig_contracts.push_back({
        .address = value.address,
        .next_order_seqno = value.next_order_seqno,
        .threshold = value.threshold,
        .signers = value.signers,
        .proposers = value.proposers,
        .last_transaction_lt = value.last_transaction_lt,
        .code_hash = value.code_hash,
        .data_hash = value.data_hash,
        .destroyed = false,
        .source_mc_seqno = value_with_source_ptr->source_mc_seqno,
      });
    }
    for (const auto& [_, destroyed_ptr] : ordered_destroyed_accounts) {
      auto it = values.find(destroyed_ptr->address);
      if (it == values.end() || it->second.value.last_transaction_lt < destroyed_ptr->last_transaction_lt) {
        batch.multisig_contracts.push_back(make_destroyed_multisig_contract_row(*destroyed_ptr));
      }
    }
  }

  {
    struct MultisigOrderWithSource {
      schema::MultisigOrderData value;
      std::uint32_t source_mc_seqno;
    };
    std::unordered_map<block::StdAddress, MultisigOrderWithSource> values;
    for (const auto& item : data) {
      if (!std::holds_alternative<schema::MultisigOrderData>(item)) {
        continue;
      }
      const auto& value = std::get<schema::MultisigOrderData>(item);
      auto it = values.find(value.address);
      if (it == values.end() || it->second.value.last_transaction_lt < value.last_transaction_lt) {
        values[value.address] = {value, source_mc_seqno};
      }
    }
    auto ordered = get_ordered_map_values(values, [](const auto& key, const auto&) {
      return convert::to_raw_address(key);
    });
    batch.multisig_orders.reserve(ordered.size());
    for (const auto& [_, value_with_source_ptr] : ordered) {
      const auto& value = value_with_source_ptr->value;
      std::optional<std::string> order_boc = std::nullopt;
      if (value.order.not_null()) {
        order_boc = serialize_cell_to_base64(value.order);
      }
      batch.multisig_orders.push_back({
        .address = value.address,
        .multisig_address = value.multisig_address,
        .order_seqno = value.order_seqno,
        .threshold = value.threshold,
        .sent_for_execution = value.sent_for_execution,
        .approvals_mask = value.approvals_mask,
        .approvals_num = value.approvals_num,
        .expiration_date = value.expiration_date,
        .order_boc = std::move(order_boc),
        .signers = value.signers,
        .last_transaction_lt = value.last_transaction_lt,
        .code_hash = value.code_hash,
        .data_hash = value.data_hash,
        .destroyed = false,
        .source_mc_seqno = value_with_source_ptr->source_mc_seqno,
      });
    }
    for (const auto& [_, destroyed_ptr] : ordered_destroyed_accounts) {
      auto it = values.find(destroyed_ptr->address);
      if (it == values.end() || it->second.value.last_transaction_lt < destroyed_ptr->last_transaction_lt) {
        batch.multisig_orders.push_back(make_destroyed_multisig_order_row(*destroyed_ptr));
      }
    }
  }

  {
    struct DedustPoolWithSource {
      schema::DedustPoolData value;
      std::uint32_t source_mc_seqno;
    };
    std::unordered_map<block::StdAddress, DedustPoolWithSource> values;
    for (const auto& item : data) {
      if (!std::holds_alternative<schema::DedustPoolData>(item)) {
        continue;
      }
      const auto& value = std::get<schema::DedustPoolData>(item);
      auto it = values.find(value.address);
      if (it == values.end() || it->second.value.last_transaction_lt < value.last_transaction_lt) {
        values[value.address] = {value, source_mc_seqno};
      }
    }
    auto ordered = get_ordered_map_values(values, [](const auto& key, const auto&) {
      return convert::to_raw_address(key);
    });
    batch.dedust_pools.reserve(ordered.size());
    std::string dex = "dedust";
    for (const auto& [_, value_with_source_ptr] : ordered) {
      const auto& value = value_with_source_ptr->value;
      std::string pool_type = value.is_stable ? "stable" : "volatile";
      batch.dedust_pools.push_back({
        .address = value.address,
        .asset_1 = value.asset_1,
        .asset_2 = value.asset_2,
        .reserve_1 = value.reserve_1,
        .reserve_2 = value.reserve_2,
        .pool_type = std::move(pool_type),
        .dex = dex,
        .fee = value.fee,
        .last_transaction_lt = value.last_transaction_lt,
        .code_hash = value.code_hash,
        .data_hash = value.data_hash,
        .destroyed = false,
        .source_mc_seqno = value_with_source_ptr->source_mc_seqno,
      });
    }
    for (const auto& [_, destroyed_ptr] : ordered_destroyed_accounts) {
      auto it = values.find(destroyed_ptr->address);
      if (it == values.end() || it->second.value.last_transaction_lt < destroyed_ptr->last_transaction_lt) {
        batch.dedust_pools.push_back(make_destroyed_dedust_pool_row(*destroyed_ptr));
      }
    }
  }

  {
    struct VestingWithSource {
      schema::VestingData value;
      std::uint32_t source_mc_seqno;
    };
    std::unordered_map<block::StdAddress, VestingWithSource> values;
    for (const auto& item : data) {
      if (!std::holds_alternative<schema::VestingData>(item)) {
        continue;
      }
      const auto& value = std::get<schema::VestingData>(item);
      auto it = values.find(value.address);
      if (it == values.end() || it->second.value.last_transaction_lt < value.last_transaction_lt) {
        values[value.address] = {value, source_mc_seqno};
      }
    }
    auto ordered = get_ordered_map_values(values, [](const auto& key, const auto&) {
      return convert::to_raw_address(key);
    });
    batch.vesting_contracts.reserve(ordered.size());
    for (const auto& [_, value_with_source_ptr] : ordered) {
      const auto& value = value_with_source_ptr->value;
      batch.vesting_contracts.push_back({
        .address = value.address,
        .vesting_start_time = value.vesting_start_time,
        .vesting_total_duration = value.vesting_total_duration,
        .unlock_period = value.unlock_period,
        .cliff_duration = value.cliff_duration,
        .vesting_total_amount = value.vesting_total_amount,
        .vesting_sender_address = value.vesting_sender_address,
        .owner_address = value.owner_address,
        .last_transaction_lt = value.last_transaction_lt,
        .code_hash = value.code_hash,
        .data_hash = value.data_hash,
        .destroyed = false,
        .source_mc_seqno = value_with_source_ptr->source_mc_seqno,
      });
      auto ordered_wallets = get_ordered_values(value.whitelist, [](const auto& address) {
        return convert::to_raw_address(address);
      });
      for (const auto& [__, wallet_addr_ptr] : ordered_wallets) {
        batch.vesting_whitelist.push_back({
          .vesting_contract_address = value.address,
          .wallet_address = *wallet_addr_ptr,
          .source_mc_seqno = value_with_source_ptr->source_mc_seqno,
        });
      }
    }
    for (const auto& [_, destroyed_ptr] : ordered_destroyed_accounts) {
      auto it = values.find(destroyed_ptr->address);
      if (it == values.end() || it->second.value.last_transaction_lt < destroyed_ptr->last_transaction_lt) {
        batch.vesting_contracts.push_back(make_destroyed_vesting_contract_row(*destroyed_ptr));
      }
    }
  }

  {
    struct NominatorPoolWithSource {
      schema::NominatorPoolData value;
      std::uint32_t source_mc_seqno;
    };
    std::unordered_map<block::StdAddress, NominatorPoolWithSource> values;
    for (const auto& item : data) {
      if (!std::holds_alternative<schema::NominatorPoolData>(item)) {
        continue;
      }
      const auto& value = std::get<schema::NominatorPoolData>(item);
      auto it = values.find(value.address);
      if (it == values.end() || it->second.value.last_transaction_lt < value.last_transaction_lt) {
        values[value.address] = {value, source_mc_seqno};
      }
    }
    auto ordered = get_ordered_map_values(values, [](const auto& key, const auto&) {
      return convert::to_raw_address(key);
    });
    batch.nominator_pools.reserve(ordered.size());
    for (const auto& [_, value_with_source_ptr] : ordered) {
      const auto& value = value_with_source_ptr->value;
      batch.nominator_pools.push_back({
        .address = value.address,
        .state = value.state,
        .nominators_count = value.nominators_count,
        .stake_amount_sent = value.stake_amount_sent,
        .validator_amount = value.validator_amount,
        .validator_address = value.validator_address,
        .validator_reward_share = value.validator_reward_share,
        .max_nominators_count = value.max_nominators_count,
        .min_validator_stake = value.min_validator_stake,
        .min_nominator_stake = value.min_nominator_stake,
        .active_nominators = nominators_json(value.nominators),
        .active_nominator_addresses = nominator_address_keys(value.nominators),
        .last_transaction_lt = value.last_transaction_lt,
        .code_hash = value.code_hash,
        .data_hash = value.data_hash,
        .destroyed = false,
        .source_mc_seqno = value_with_source_ptr->source_mc_seqno,
      });
    }
    for (const auto& [_, destroyed_ptr] : ordered_destroyed_accounts) {
      auto it = values.find(destroyed_ptr->address);
      if (it == values.end() || it->second.value.last_transaction_lt < destroyed_ptr->last_transaction_lt) {
        batch.nominator_pools.push_back(make_destroyed_nominator_pool_row(*destroyed_ptr));
      }
    }
  }

  {
    struct TelemintWithSource {
      schema::TelemintData value;
      std::uint32_t source_mc_seqno;
    };
    std::unordered_map<block::StdAddress, TelemintWithSource> values;
    std::unordered_map<block::StdAddress, schema::NFTItemDataV2> nft_items;
    for (const auto& item : data) {
      if (std::holds_alternative<schema::NFTItemDataV2>(item)) {
        const auto& nft_item = std::get<schema::NFTItemDataV2>(item);
        auto it = nft_items.find(nft_item.address);
        if (it == nft_items.end() || it->second.last_transaction_lt < nft_item.last_transaction_lt) {
          nft_items[nft_item.address] = nft_item;
        }
      }
    }
    for (const auto& item : data) {
      if (!std::holds_alternative<schema::TelemintData>(item)) {
        continue;
      }
      const auto& value = std::get<schema::TelemintData>(item);
      if (nft_items.find(value.address) == nft_items.end()) {
        continue;
      }
      auto it = values.find(value.address);
      if (it == values.end() || it->second.value.last_transaction_lt < value.last_transaction_lt) {
        values[value.address] = {value, source_mc_seqno};
      }
    }
    auto ordered = get_ordered_map_values(values, [](const auto& key, const auto&) {
      return convert::to_raw_address(key);
    });
    batch.telemint_nft_items.reserve(ordered.size());
    for (const auto& [_, value_with_source_ptr] : ordered) {
      const auto& value = value_with_source_ptr->value;
      auto token_name = value.token_name;
      token_name.erase(std::remove(token_name.begin(), token_name.end(), '\0'), token_name.end());
      batch.telemint_nft_items.push_back({
        .address = value.address,
        .token_name = std::move(token_name),
        .bidder_address = value.bidder_address,
        .bid = value.bid,
        .bid_ts = value.bid_ts,
        .min_bid = value.min_bid,
        .end_time = value.end_time,
        .beneficiary_address = value.beneficiary_address,
        .initial_min_bid = value.initial_min_bid,
        .max_bid = value.max_bid,
        .min_bid_step = value.min_bid_step,
        .min_extend_time = value.min_extend_time,
        .duration = value.duration,
        .royalty_numerator = value.royalty_numerator,
        .royalty_denominator = value.royalty_denominator,
        .royalty_destination = value.royalty_destination,
        .last_transaction_lt = value.last_transaction_lt,
        .code_hash = value.code_hash,
        .data_hash = value.data_hash,
        .destroyed = false,
        .source_mc_seqno = value_with_source_ptr->source_mc_seqno,
      });
    }
    for (const auto& [_, destroyed_ptr] : ordered_destroyed_accounts) {
      auto it = values.find(destroyed_ptr->address);
      if (it == values.end() || it->second.value.last_transaction_lt < destroyed_ptr->last_transaction_lt) {
        batch.telemint_nft_items.push_back(make_destroyed_telemint_row(*destroyed_ptr));
      }
    }
  }

  return batch;
}

void load_kvrocks_scripts(sw::redis::Redis& redis) {
  auto set_once_sha = redis.script_load(kvrocks_set_once_script());
  auto set_current_sha = redis.script_load(kvrocks_set_current_script());
  auto set_current_existing_sha = redis.script_load(kvrocks_set_current_existing_script());
  auto set_indexed_current_sha = redis.script_load(kvrocks_set_indexed_current_script());
  auto set_indexed_current_existing_sha = redis.script_load(kvrocks_set_indexed_current_existing_script());
  auto set_indexed_once_sha = redis.script_load(kvrocks_set_indexed_once_script());
  if (set_once_sha != kvrocks_set_once_script_sha() || set_current_sha != kvrocks_set_current_script_sha() ||
      set_current_existing_sha != kvrocks_set_current_existing_script_sha() ||
      set_indexed_current_sha != kvrocks_set_indexed_current_script_sha() ||
      set_indexed_current_existing_sha != kvrocks_set_indexed_current_existing_script_sha() ||
      set_indexed_once_sha != kvrocks_set_indexed_once_script_sha()) {
    throw std::runtime_error("Kvrocks returned an unexpected script SHA");
  }
}

bool is_kvrocks_no_script_error(const std::exception& e) {
  const std::string message = e.what();
  return message.find("NOSCRIPT") != std::string::npos || message.find("No matching script") != std::string::npos;
}

KvrocksBatchWriter::KvrocksBatchWriter(sw::redis::Redis& redis, const StateBatch& batch)
    : redis_(redis), pipeline_(redis.pipeline(true)), batch_(batch) {
}

void KvrocksBatchWriter::write() {
  for (std::size_t attempt = 1; attempt <= KVROCKS_INDEX_SNAPSHOT_MAX_RETRIES; ++attempt) {
    reset_pipeline();
    try {
      write_once();
      return;
    } catch (const KvrocksIndexSnapshotConflict& e) {
      if (attempt == KVROCKS_INDEX_SNAPSHOT_MAX_RETRIES) {
        throw;
      }
      LOG(WARNING) << e.what() << ", retrying Kvrocks batch attempt " << (attempt + 1) << "/"
                   << KVROCKS_INDEX_SNAPSHOT_MAX_RETRIES;
    }
  }
}

void KvrocksBatchWriter::write_once() {
  for (const auto& row : batch_.message_contents) {
    queue_set_once("message_contents", hash_key(row.hash), row.source_mc_seqno, build_payload(row));
  }
  for (const auto& row : batch_.account_states) {
    queue_set_once("account_states", hash_key(row.hash), row.source_mc_seqno, build_payload(row));
  }
  for (const auto& row : batch_.contract_methods) {
    queue_set_once("contract_methods", hash_key(row.code_hash), row.source_mc_seqno, build_payload(row));
  }

  auto make_indexed_write = [](const std::string& script_sha, const std::string& table, const std::string& id,
                               std::uint32_t source_mc_seqno, std::string payload,
                               std::vector<KvrocksIndexEntry> indexes, bool read_old_indexes) {
    const auto key = kvrocks_key(table, id);
    return IndexedWrite{script_sha, key, kvrocks_payload_key(table, id), source_mc_seqno, std::move(payload),
                        std::move(indexes), read_old_indexes};
  };

  auto queue_indexed_current_rows = [&](const auto& rows, const std::string& table, auto id_fn) {
    std::vector<IndexedWrite> writes;
    writes.reserve(rows.size());
    for (const auto& row : rows) {
      writes.push_back(make_indexed_write(kvrocks_set_indexed_current_script_sha(), table, id_fn(row),
                                          row.source_mc_seqno, build_payload(row), build_indexes(row), true));
    }
    queue_indexed_writes(writes);
  };

  auto queue_indexed_interface_current_rows = [&](const auto& rows, const std::string& table, auto id_fn) {
    std::vector<IndexedWrite> writes;
    writes.reserve(rows.size());
    for (const auto& row : rows) {
      const auto& script_sha = row.destroyed ? kvrocks_set_indexed_current_existing_script_sha()
                                             : kvrocks_set_indexed_current_script_sha();
      writes.push_back(make_indexed_write(script_sha, table, id_fn(row), row.source_mc_seqno, build_payload(row),
                                          build_indexes(row), true));
    }
    queue_indexed_writes(writes);
  };

  queue_indexed_current_rows(batch_.latest_account_states, "latest_account_states", [](const auto& row) {
    return address_key(row.account);
  });

  auto queue_interface_current = [&](const std::string& table, const std::string& id, const auto& row) {
    auto payload = build_payload(row);
    if (row.destroyed) {
      queue_set_current_existing(table, id, row.source_mc_seqno, payload);
    } else {
      queue_set_current(table, id, row.source_mc_seqno, payload);
    }
  };
  queue_indexed_interface_current_rows(batch_.jetton_masters, "jetton_masters", [](const auto& row) {
    return address_key(row.address);
  });
  queue_indexed_interface_current_rows(batch_.jetton_wallets, "jetton_wallets", [](const auto& row) {
    return address_key(row.address);
  });
  for (const auto& row : batch_.mintless_jetton_masters) {
    queue_set_once("mintless_jetton_masters", address_key(row.address), row.source_mc_seqno, build_payload(row));
  }
  queue_indexed_interface_current_rows(batch_.nft_collections, "nft_collections", [](const auto& row) {
    return address_key(row.address);
  });
  queue_indexed_interface_current_rows(batch_.nft_items, "nft_items", [](const auto& row) {
    return address_key(row.address);
  });
  queue_indexed_interface_current_rows(batch_.dns_entries, "dns_entries", [](const auto& row) {
    return address_key(row.nft_item_address);
  });
  for (const auto& row : batch_.getgems_nft_sales) {
    queue_interface_current("getgems_nft_sales", address_key(row.address), row);
  }
  for (const auto& row : batch_.getgems_nft_auctions) {
    queue_interface_current("getgems_nft_auctions", address_key(row.address), row);
  }
  queue_indexed_interface_current_rows(batch_.multisig_contracts, "multisig", [](const auto& row) {
    return address_key(row.address);
  });
  queue_indexed_interface_current_rows(batch_.multisig_orders, "multisig_orders", [](const auto& row) {
    return address_key(row.address);
  });
  queue_indexed_interface_current_rows(batch_.vesting_contracts, "vesting_contracts", [](const auto& row) {
    return address_key(row.address);
  });
  for (const auto& row : batch_.vesting_whitelist) {
    queue_set_indexed_once("vesting_whitelist", address_key(row.vesting_contract_address) + ":" + address_key(row.wallet_address),
                           row.source_mc_seqno, build_payload(row), build_indexes(row));
  }
  queue_indexed_interface_current_rows(batch_.nominator_pools, "nominator_pools", [](const auto& row) {
    return address_key(row.address);
  });
  for (const auto& row : batch_.telemint_nft_items) {
    queue_interface_current("telemint_nft_items", address_key(row.address), row);
  }
  for (const auto& row : batch_.dedust_pools) {
    queue_interface_current("dex_pools", address_key(row.address), row);
  }

  flush();
  if (queued_ > 0) {
    LOG(INFO) << "Inserted " << queued_ << " point-state rows into Kvrocks";
  }
}

double KvrocksBatchWriter::exec_elapsed_millis() const {
  return exec_elapsed_millis_;
}

void KvrocksBatchWriter::queue_set_once(const std::string& table, const std::string& id, std::uint32_t source_mc_seqno,
                                        const std::string& payload) {
  const auto key = kvrocks_key(table, id);
  const auto payload_key = kvrocks_payload_key(table, id);
  const auto source = std::to_string(source_mc_seqno);
  pipeline_.evalsha(kvrocks_set_once_script_sha(), {key, payload_key}, {source, payload});
  record_pending_command("set_once", key, source_mc_seqno);
  flush_if_needed();
}

void KvrocksBatchWriter::queue_set_current(const std::string& table, const std::string& id,
                                           std::uint32_t source_mc_seqno, const std::string& payload) {
  const auto key = kvrocks_key(table, id);
  const auto payload_key = kvrocks_payload_key(table, id);
  const auto source = std::to_string(source_mc_seqno);
  pipeline_.evalsha(kvrocks_set_current_script_sha(), {key, payload_key}, {source, payload});
  record_pending_command("set_current", key, source_mc_seqno);
  flush_if_needed();
}

void KvrocksBatchWriter::queue_set_current_existing(const std::string& table, const std::string& id,
                                                    std::uint32_t source_mc_seqno, const std::string& payload) {
  const auto key = kvrocks_key(table, id);
  const auto payload_key = kvrocks_payload_key(table, id);
  const auto source = std::to_string(source_mc_seqno);
  pipeline_.evalsha(kvrocks_set_current_existing_script_sha(), {key, payload_key}, {source, payload});
  record_pending_command("set_current_existing", key, source_mc_seqno);
  flush_if_needed();
}

void KvrocksBatchWriter::queue_set_indexed_once(const std::string& table, const std::string& id,
                                                std::uint32_t source_mc_seqno, const std::string& payload,
                                                const std::vector<KvrocksIndexEntry>& indexes) {
  queue_set_indexed(kvrocks_set_indexed_once_script_sha(), table, id, source_mc_seqno, payload, indexes, false);
}

void KvrocksBatchWriter::queue_set_indexed(const std::string& script_sha, const std::string& table,
                                           const std::string& id, std::uint32_t source_mc_seqno,
                                           const std::string& payload,
                                           const std::vector<KvrocksIndexEntry>& indexes, bool read_old_indexes) {
  const auto key = kvrocks_key(table, id);
  const auto payload_key = kvrocks_payload_key(table, id);
  IndexedWrite write{script_sha, key, payload_key, source_mc_seqno, payload, indexes, read_old_indexes};
  if (read_old_indexes) {
    std::vector<IndexedWrite> writes;
    writes.push_back(std::move(write));
    queue_indexed_writes(writes);
    return;
  }

  queue_indexed_write(write, {});
}

void KvrocksBatchWriter::queue_indexed_writes(const std::vector<IndexedWrite>& writes) {
  std::size_t begin = 0;
  while (begin < writes.size()) {
    std::unordered_set<std::string> chunk_keys;
    std::size_t end = begin;
    for (; end < writes.size() && end - begin < KVROCKS_INDEX_SNAPSHOT_CHUNK_SIZE; ++end) {
      if (!chunk_keys.insert(writes[end].key).second) {
        break;
      }
    }
    if (end == begin) {
      end = begin + 1;
    }

    bool pending_conflicts_with_snapshot = false;
    for (std::size_t i = begin; i < end; ++i) {
      if (writes[i].read_old_indexes && pending_row_keys_.count(writes[i].key) != 0) {
        pending_conflicts_with_snapshot = true;
        break;
      }
    }
    if (pending_conflicts_with_snapshot) {
      flush();
    }

    const auto old_snapshots = read_old_index_snapshots(writes, begin, end);
    for (std::size_t i = begin; i < end; ++i) {
      queue_indexed_write(writes[i], old_snapshots[i - begin]);
    }
    flush();
    begin = end;
  }
}

void KvrocksBatchWriter::queue_indexed_write(const IndexedWrite& write,
                                             const std::vector<KvrocksIndexSnapshotEntry>& old_indexes) {
  std::vector<std::string> keys;
  keys.reserve(2 + old_indexes.size() + write.indexes.size());
  keys.push_back(write.key);
  keys.push_back(write.payload_key);
  for (const auto& old_index : old_indexes) {
    keys.push_back(old_index.key);
  }
  for (const auto& index : write.indexes) {
    keys.push_back(index.key);
  }

  std::vector<std::string> args;
  args.reserve(4 + old_indexes.size() + write.indexes.size() * 2);
  args.push_back(std::to_string(write.source_mc_seqno));
  args.push_back(write.payload);
  args.push_back(std::to_string(old_indexes.size()));
  args.push_back(std::to_string(write.indexes.size()));
  for (const auto& old_index : old_indexes) {
    args.push_back(old_index.member);
  }
  for (const auto& index : write.indexes) {
    args.push_back(index.member);
    args.push_back(index.score);
  }
  pipeline_.evalsha(write.script_sha, keys.begin(), keys.end(), args.begin(), args.end());
  record_pending_command("set_indexed old_indexes=" + std::to_string(old_indexes.size()) +
                             " new_indexes=" + std::to_string(write.indexes.size()),
                         write.key, write.source_mc_seqno);
  flush_if_needed();
}

std::vector<std::vector<KvrocksIndexSnapshotEntry>> KvrocksBatchWriter::read_old_index_snapshots(
    const std::vector<IndexedWrite>& writes, std::size_t begin, std::size_t end) {
  std::vector<std::vector<KvrocksIndexSnapshotEntry>> snapshots(end - begin);
  std::vector<std::size_t> count_offsets;
  count_offsets.reserve(end - begin);

  td::Timer count_timer;
  auto count_pipeline = redis_.pipeline(true);
  for (std::size_t i = begin; i < end; ++i) {
    if (!writes[i].read_old_indexes) {
      continue;
    }
    count_pipeline.hget(writes[i].key, "idx_count");
    count_offsets.push_back(i - begin);
  }
  if (count_offsets.empty()) {
    count_timer.pause();
    return snapshots;
  }

  auto count_replies = count_pipeline.exec();
  count_timer.pause();
  exec_elapsed_millis_ += count_timer.elapsed() * 1e3;
  if (count_replies.size() != count_offsets.size()) {
    throw std::runtime_error("Kvrocks HGET old index snapshot reply count mismatch");
  }

  std::vector<std::size_t> old_counts(snapshots.size(), 0);
  for (std::size_t i = 0; i < count_replies.size(); ++i) {
    const auto offset = count_offsets[i];
    old_counts[offset] = parse_kvrocks_index_count(count_replies.get<sw::redis::OptionalString>(i),
                                                   writes[begin + offset].key);
  }

  std::vector<std::size_t> value_offsets;
  value_offsets.reserve(count_offsets.size());
  std::vector<std::vector<std::string>> fields_by_offset(snapshots.size());
  td::Timer values_timer;
  auto values_pipeline = redis_.pipeline(true);
  for (std::size_t offset = 0; offset < snapshots.size(); ++offset) {
    const auto old_count = old_counts[offset];
    if (old_count == 0) {
      continue;
    }

    auto& fields = fields_by_offset[offset];
    fields.reserve(old_count * 2);
    for (std::size_t i = 1; i <= old_count; ++i) {
      fields.push_back("idx_key_" + std::to_string(i));
      fields.push_back("idx_member_" + std::to_string(i));
    }

    values_pipeline.hmget(writes[begin + offset].key, fields.begin(), fields.end());
    value_offsets.push_back(offset);
  }
  if (value_offsets.empty()) {
    values_timer.pause();
    exec_elapsed_millis_ += values_timer.elapsed() * 1e3;
    return snapshots;
  }

  auto value_replies = values_pipeline.exec();
  values_timer.pause();
  exec_elapsed_millis_ += values_timer.elapsed() * 1e3;
  if (value_replies.size() != value_offsets.size()) {
    throw std::runtime_error("Kvrocks HMGET old index snapshot reply count mismatch");
  }

  for (std::size_t reply_i = 0; reply_i < value_replies.size(); ++reply_i) {
    const auto offset = value_offsets[reply_i];
    const auto& fields = fields_by_offset[offset];
    std::vector<sw::redis::OptionalString> values;
    values.reserve(fields.size());
    value_replies.get(reply_i, std::back_inserter(values));
    if (values.size() != fields.size()) {
      throw std::runtime_error("Kvrocks HMGET old index snapshot reply count mismatch for " +
                               writes[begin + offset].key);
    }

    auto& snapshot = snapshots[offset];
    snapshot.reserve(old_counts[offset]);
    for (std::size_t i = 0; i < old_counts[offset]; ++i) {
      const auto& index_key = values[i * 2];
      const auto& index_member = values[i * 2 + 1];
      if (!index_key || !index_member) {
        throw KvrocksIndexSnapshotConflict(
            "Kvrocks indexed write snapshot conflict while reading old index snapshot: key=" +
            writes[begin + offset].key + " source_mc_seqno=" +
            std::to_string(writes[begin + offset].source_mc_seqno) + " idx_count=" +
            std::to_string(old_counts[offset]) + " missing idx_" + std::to_string(i + 1) +
            " idx_key_missing=" + (index_key ? "false" : "true") +
            " idx_member_missing=" + (index_member ? "false" : "true"));
      }
      snapshot.push_back({*index_key, *index_member});
    }
  }
  return snapshots;
}

void KvrocksBatchWriter::record_pending_command(const std::string& op, const std::string& key,
                                                std::uint32_t source_mc_seqno) {
  ++pending_;
  ++queued_;
  pending_row_keys_.insert(key);
  pending_debug_.push_back(op + " key=" + key + " source_mc_seqno=" + std::to_string(source_mc_seqno));
}

void KvrocksBatchWriter::reset_pipeline() {
  pipeline_ = redis_.pipeline(true);
  pending_ = 0;
  queued_ = 0;
  pending_row_keys_.clear();
  pending_debug_.clear();
}

void KvrocksBatchWriter::flush_if_needed() {
  if (pending_ >= KVROCKS_PIPELINE_FLUSH_SIZE) {
    flush();
  }
}

void KvrocksBatchWriter::flush() {
  if (pending_ == 0) {
    return;
  }

  td::Timer exec_timer;
  auto replies = pipeline_.exec();
  if (replies.size() != pending_) {
    throw std::runtime_error("Kvrocks pipeline reply count mismatch");
  }
  bool snapshot_conflict = false;
  std::string snapshot_conflict_detail;
  for (std::size_t i = 0; i < replies.size(); ++i) {
    if (replies.get<long long>(i) == KVROCKS_INDEX_SNAPSHOT_CONFLICT) {
      if (!snapshot_conflict) {
        snapshot_conflict = true;
        snapshot_conflict_detail = i < pending_debug_.size() ? pending_debug_[i] : "unknown pending command";
      }
    }
  }
  exec_timer.pause();
  exec_elapsed_millis_ += exec_timer.elapsed() * 1e3;
  pending_ = 0;
  pending_row_keys_.clear();
  pending_debug_.clear();
  if (snapshot_conflict) {
    throw KvrocksIndexSnapshotConflict("Kvrocks indexed write snapshot conflict: " + snapshot_conflict_detail);
  }
}

void write_batch_with_script_reload(sw::redis::Redis& redis, const StateBatch& batch) {
  try {
    KvrocksBatchWriter writer(redis, batch);
    writer.write();
  } catch (const sw::redis::Error& e) {
    if (!is_kvrocks_no_script_error(e)) {
      throw;
    }

    LOG(WARNING) << "Kvrocks script cache is missing, reloading scripts and retrying batch: " << e.what();
    load_kvrocks_scripts(redis);
    KvrocksBatchWriter writer(redis, batch);
    writer.write();
  }
}

void repair_nft_real_owners_with_script_reload(sw::redis::Redis& redis, const StateBatch& batch) {
  try {
    repair_nft_real_owners(redis, batch);
  } catch (const sw::redis::Error& e) {
    if (!is_kvrocks_no_script_error(e)) {
      throw;
    }

    LOG(WARNING) << "Kvrocks script cache is missing, reloading scripts and retrying NFT real_owner repair: "
                 << e.what();
    load_kvrocks_scripts(redis);
    repair_nft_real_owners(redis, batch);
  }
}

}  // namespace kvrocks_state
