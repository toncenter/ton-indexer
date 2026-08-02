#include "FixtureLoader.h"

#include "td/utils/filesystem.h"
#include "td/utils/format.h"

#include <lz4frame.h>
#include <msgpack.hpp>

namespace mch {

namespace {

td::Result<std::string> lz4_frame_decompress(td::Slice input) {
  LZ4F_dctx *dctx = nullptr;
  if (LZ4F_isError(LZ4F_createDecompressionContext(&dctx, LZ4F_VERSION))) {
    return td::Status::Error("lz4: failed to create decompression context");
  }
  std::string out;
  std::vector<char> chunk(1 << 16);
  const char *src = input.data();
  size_t src_left = input.size();
  while (src_left > 0) {
    size_t dst_size = chunk.size();
    size_t src_size = src_left;
    size_t hint = LZ4F_decompress(dctx, chunk.data(), &dst_size, src, &src_size, nullptr);
    if (LZ4F_isError(hint)) {
      LZ4F_freeDecompressionContext(dctx);
      return td::Status::Error(PSTRING() << "lz4: decompress failed: " << LZ4F_getErrorName(hint));
    }
    out.append(chunk.data(), dst_size);
    src += src_size;
    src_left -= src_size;
    if (hint == 0 && src_left == 0) {
      break;
    }
    if (dst_size == 0 && src_size == 0) {
      LZ4F_freeDecompressionContext(dctx);
      return td::Status::Error("lz4: no progress (truncated frame?)");
    }
  }
  LZ4F_freeDecompressionContext(dctx);
  return out;
}

const msgpack::object *map_get(const msgpack::object &m, const char *key) {
  if (m.type != msgpack::type::MAP) {
    return nullptr;
  }
  for (std::uint32_t i = 0; i < m.via.map.size; i++) {
    const auto &kv = m.via.map.ptr[i];
    if (kv.key.type == msgpack::type::STR &&
        td::Slice(kv.key.via.str.ptr, kv.key.via.str.size) == td::Slice(key)) {
      return &kv.val;
    }
  }
  return nullptr;
}

bool is_nil(const msgpack::object *o) {
  return o == nullptr || o->type == msgpack::type::NIL;
}

std::string as_str(const msgpack::object &o) {
  if (o.type == msgpack::type::STR) {
    return std::string(o.via.str.ptr, o.via.str.size);
  }
  if (o.type == msgpack::type::BIN) {
    return std::string(o.via.bin.ptr, o.via.bin.size);
  }
  return {};
}

std::string get_str(const msgpack::object &m, const char *key) {
  const auto *o = map_get(m, key);
  return is_nil(o) ? std::string{} : as_str(*o);
}

std::optional<std::string> get_opt_str(const msgpack::object &m, const char *key) {
  const auto *o = map_get(m, key);
  if (is_nil(o)) {
    return std::nullopt;
  }
  return as_str(*o);
}

std::optional<std::int64_t> get_opt_i64(const msgpack::object &m, const char *key) {
  const auto *o = map_get(m, key);
  if (is_nil(o)) {
    return std::nullopt;
  }
  if (o->type == msgpack::type::POSITIVE_INTEGER) {
    return static_cast<std::int64_t>(o->via.u64);
  }
  if (o->type == msgpack::type::NEGATIVE_INTEGER) {
    return o->via.i64;
  }
  return std::nullopt;
}

std::int64_t get_i64(const msgpack::object &m, const char *key) {
  return get_opt_i64(m, key).value_or(0);
}

bool get_bool(const msgpack::object &m, const char *key) {
  const auto *o = map_get(m, key);
  return !is_nil(o) && o->type == msgpack::type::BOOLEAN && o->via.boolean;
}

std::optional<bool> get_opt_bool(const msgpack::object &m, const char *key) {
  const auto *o = map_get(m, key);
  if (is_nil(o) || o->type != msgpack::type::BOOLEAN) {
    return std::nullopt;
  }
  return o->via.boolean;
}

std::unique_ptr<Message> parse_message(const msgpack::object &m) {
  auto msg = std::make_unique<Message>();
  msg->msg_hash = get_str(m, "msg_hash");
  msg->tx_hash = get_str(m, "tx_hash");
  msg->tx_lt = get_i64(m, "tx_lt");
  msg->direction = get_str(m, "direction");
  msg->source = get_opt_str(m, "source");
  msg->destination = get_opt_str(m, "destination");
  msg->opcode = get_opt_i64(m, "opcode");
  msg->value = get_opt_i64(m, "value");
  msg->created_lt = get_opt_i64(m, "created_lt");
  msg->created_at = get_opt_i64(m, "created_at");
  msg->bounce = get_opt_bool(m, "bounce");
  msg->bounced = get_bool(m, "bounced");
  const auto *extra = map_get(m, "value_extra_currencies");
  msg->has_extra_currencies =
      !is_nil(extra) && extra->type == msgpack::type::MAP && extra->via.map.size > 0;
  const auto *content = map_get(m, "message_content");
  if (!is_nil(content)) {
    MsgContent c;
    c.hash = get_str(*content, "hash");
    c.body = get_str(*content, "body");
    msg->content = std::move(c);
  }
  // ton-fixture-save writes the StateInit under its own inline key, same
  // {hash, body} shape as message_content (main.cpp).
  const auto *init = map_get(m, "init_state");
  if (!is_nil(init)) {
    MsgContent c;
    c.hash = get_str(*init, "hash");
    c.body = get_str(*init, "body");
    msg->init_state = std::move(c);
  }
  return msg;
}

// Generic msgpack -> Value for the `interfaces` section (schema note in
// TraceLoader.h). u64 values above int64 range widen through RefInt256.
Value msgpack_to_value(const msgpack::object &o) {
  switch (o.type) {
    case msgpack::type::NIL:
      return Value::null();
    case msgpack::type::BOOLEAN:
      return Value::make_bool(o.via.boolean);
    case msgpack::type::POSITIVE_INTEGER: {
      unsigned long long v = o.via.u64;
      if (v <= 0x7FFFFFFFFFFFFFFFULL) {
        return Value::make_int64(static_cast<std::int64_t>(v));
      }
      auto hi = td::make_refint(static_cast<long long>(v >> 32));
      return Value::make_int((hi << 32) +
                             td::make_refint(static_cast<long long>(v & 0xFFFFFFFFULL)));
    }
    case msgpack::type::NEGATIVE_INTEGER:
      return Value::make_int64(o.via.i64);
    case msgpack::type::STR:
      return Value::make_str(std::string(o.via.str.ptr, o.via.str.size));
    case msgpack::type::BIN:
      return Value::make_bytes(std::string(o.via.bin.ptr, o.via.bin.size));
    case msgpack::type::ARRAY: {
      std::vector<Value> xs;
      for (std::uint32_t i = 0; i < o.via.array.size; i++) {
        xs.push_back(msgpack_to_value(o.via.array.ptr[i]));
      }
      return Value::make_list(std::move(xs));
    }
    case msgpack::type::MAP: {
      Value::Fields fs;
      for (std::uint32_t i = 0; i < o.via.map.size; i++) {
        const auto &kv = o.via.map.ptr[i];
        std::string key = kv.key.type == msgpack::type::STR
                              ? std::string(kv.key.via.str.ptr, kv.key.via.str.size)
                              : std::string{};
        fs.emplace_back(std::move(key), msgpack_to_value(kv.val));
      }
      return Value::make_dict(std::move(fs));
    }
    case msgpack::type::FLOAT32:
    case msgpack::type::FLOAT64:
      // Fixture interface data (e.g. a huge NftItem.index or NftSale.full_price)
      // decodes to a Python float; kept as VType::Float so a bare one renders
      // unrenderable exactly as the Python twin's render() raises.
      return Value::make_float(o.via.f64);
    default:
      return Value::null();
  }
}

std::unique_ptr<Transaction> parse_transaction(const msgpack::object &t) {
  auto tx = std::make_unique<Transaction>();
  tx->hash = get_str(t, "hash");
  tx->lt = get_i64(t, "lt");
  tx->now = get_i64(t, "now");
  tx->mc_block_seqno = get_i64(t, "mc_block_seqno");
  tx->account = get_str(t, "account");
  tx->descr = get_str(t, "descr");
  tx->orig_status = get_str(t, "orig_status");
  tx->end_status = get_str(t, "end_status");
  tx->skipped_reason = get_opt_str(t, "skipped_reason");
  tx->compute_exit_code = get_opt_i64(t, "compute_exit_code");
  tx->aborted = get_bool(t, "aborted");
  const auto *messages = map_get(t, "messages");
  if (!is_nil(messages) && messages->type == msgpack::type::ARRAY) {
    for (std::uint32_t i = 0; i < messages->via.array.size; i++) {
      tx->messages.push_back(parse_message(messages->via.array.ptr[i]));
    }
  }
  return tx;
}

}  // namespace

td::Result<Trace> load_trace(const std::string &path) {
  TRY_RESULT(raw, td::read_file_str(path));
  TRY_RESULT(unpacked, lz4_frame_decompress(raw));

  msgpack::object_handle handle;
  try {
    handle = msgpack::unpack(unpacked.data(), unpacked.size());
  } catch (const std::exception &e) {
    return td::Status::Error(PSTRING() << "msgpack: unpack failed: " << e.what());
  }
  const msgpack::object &root = handle.get();
  if (root.type != msgpack::type::MAP) {
    return td::Status::Error("msgpack: root is not a map");
  }

  Trace trace;
  const auto *trace_obj = map_get(root, "trace");
  if (!is_nil(trace_obj)) {
    trace.trace_id = get_str(*trace_obj, "trace_id");
    // The header is authoritative here, exactly as Python's Trace ROW is
    // (deserialize_trace, tests/utils/trace_deserializer.py).
    trace.start_lt = get_i64(*trace_obj, "start_lt");
    trace.end_lt = get_i64(*trace_obj, "end_lt");
    trace.start_utime = get_i64(*trace_obj, "start_utime");
    trace.end_utime = get_i64(*trace_obj, "end_utime");
    trace.mc_seqno_end = get_i64(*trace_obj, "mc_seqno_end");
  }
  const auto *txs = map_get(root, "transactions");
  if (is_nil(txs) || txs->type != msgpack::type::ARRAY) {
    return td::Status::Error("msgpack: no transactions array");
  }
  for (std::uint32_t i = 0; i < txs->via.array.size; i++) {
    trace.transactions.push_back(parse_transaction(txs->via.array.ptr[i]));
  }
  for (auto &tx : trace.transactions) {
    for (auto &m : tx->messages) {
      m->tx = tx.get();
    }
  }
  if (const auto *ifs = map_get(root, "interfaces");
      !is_nil(ifs) && ifs->type == msgpack::type::MAP) {
    for (std::uint32_t i = 0; i < ifs->via.map.size; i++) {
      const auto &kv = ifs->via.map.ptr[i];
      if (kv.key.type != msgpack::type::STR) {
        continue;
      }
      trace.interfaces.emplace(std::string(kv.key.via.str.ptr, kv.key.via.str.size),
                               msgpack_to_value(kv.val));
    }
  }
  return trace;
}

td::Result<TraceContext> load_trace_context(const std::string &path) {
  TraceContext ctx;
  TRY_RESULT_ASSIGN(ctx.trace, load_trace(path));
  ctx.tree = to_tree(ctx.trace);
  if (ctx.tree.root == nullptr) {
    return td::Status::Error("empty event tree");
  }
  ctx.root = init_block(ctx.arena, ctx.tree.root);
  return ctx;
}

}  // namespace mch
