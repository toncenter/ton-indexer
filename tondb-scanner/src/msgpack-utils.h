#include <msgpack.hpp>
#include "crypto/block/block-auto.h"
#include "crypto/block/block-parse.h"

namespace msgpack {
  MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS) {
    namespace adaptor {

    template<>
    struct pack<block::StdAddress> {
      template <typename Stream>
      msgpack::packer<Stream>& operator()(msgpack::packer<Stream>& o, const block::StdAddress& v) const {
        std::string addr = std::to_string(v.workchain) + ":" + v.addr.to_hex();
        o.pack(addr);
        return o;
      }
    };

    template <>
    struct convert<block::StdAddress> {
      msgpack::object const& operator()(msgpack::object const& o, block::StdAddress& v) const {
        if (o.type != msgpack::type::STR) throw msgpack::type_error();
        std::string addr = o.as<std::string>();
        if (!v.parse_addr(addr)) throw std::runtime_error("Failed to deserialize block::StdAddress");
        return o;
      }
    };

    template<>
    struct pack<td::Bits256> {
      template <typename Stream>
      msgpack::packer<Stream>& operator()(msgpack::packer<Stream>& o, const td::Bits256& v) const {
        o.pack_bin(32);
        o.pack_bin_body((const char*)v.data(), 32);
        return o;
      }
    };

    template <>
    struct convert<td::Bits256> {
      msgpack::object const& operator()(msgpack::object const& o, td::Bits256& v) const {
        if (o.type == msgpack::type::BIN || o.via.bin.size == 32) {
          std::memcpy(v.data(), o.via.bin.ptr, 32);
          return o;
        }
        // Previously td::Bits256 was serialized as a hex string. Keep this for compatibility.
        if (o.type == msgpack::type::STR) {
          std::string hex = o.as<std::string>();
          if (v.from_hex(hex) < 0) throw std::runtime_error("Failed to deserialize td::Bits256");
          return o;
        }
        throw msgpack::type_error();
      }
    };

    template<>
    struct pack<td::RefInt256> {
      template <typename Stream>
      msgpack::packer<Stream>& operator()(msgpack::packer<Stream>& o, const td::RefInt256& v) const {
        o.pack(v->to_dec_string());
        return o;
      }
    };

    template <>
    struct convert<td::RefInt256> {
      msgpack::object const& operator()(msgpack::object const& o, td::RefInt256& v) const {
        throw std::runtime_error("Deserializion of td::RefInt256 is not implemented");
        return o;
      }
    };

    } // namespace adaptor
  } // MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS)
} // namespace msgpack
