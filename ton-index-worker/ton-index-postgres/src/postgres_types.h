#pragma once
#include <pqxx/pqxx>

namespace pqxx {
  std::optional<pqxx::bytes> to_pq_bytes(const std::optional<std::string> &value) {
    if (value.has_value()) {
      return pqxx::binary_cast(value.value()).data();
    }
    return std::nullopt;
  }

  template<>
  struct nullness<schema::BlockReference> : pqxx::no_null<schema::BlockReference> {
  };

  template<>
  struct string_traits<schema::BlockReference> {
    static constexpr bool converts_to_string{true};
    static constexpr bool converts_from_string{false};

    static zview to_buf(char *begin, char *end, schema::BlockReference const &value) {
      return zview{
        begin,
        static_cast<std::size_t>(into_buf(begin, end, value) - begin - 1)
      };
    }

    static char *into_buf(char *begin, char *end, schema::BlockReference const &value) {
      std::ostringstream stream;
      stream << "(" << value.workchain << ", " << value.shard << ", " << value.seqno << ")";
      auto text = stream.str();
      if (pqxx::internal::cmp_greater_equal(std::size(text), end - begin))
        throw conversion_overrun{"Not enough buffer for schema::BlockReference."};
      std::memcpy(begin, text.c_str(), std::size(text) + 1);
      return begin + std::size(text) + 1;
    }

    static std::size_t size_buffer(schema::BlockReference const &value) noexcept {
      return 64;
    }
  };

  template<>
  struct nullness<td::RefInt256> {
    static constexpr bool has_null{true};

    static constexpr bool always_null{false};

    static bool is_null(td::RefInt256 const &value) {
      return value.is_null();
    }
  };

  template<>
  struct string_traits<td::RefInt256> {
    static constexpr bool converts_to_string{true};
    static constexpr bool converts_from_string{false};

    static zview to_buf(char *begin, char *end, td::RefInt256 const &value) {
      return zview{begin, static_cast<std::size_t>(into_buf(begin, end, value) - begin - 1)};
    }

    static char *into_buf(char *begin, char *end, td::RefInt256 const &value) {
      auto text = value->to_dec_string();
      if (pqxx::internal::cmp_greater_equal(std::size(text), end - begin))
        throw conversion_overrun{"Not enough buffer for td::RefInt256."};
      std::memcpy(begin, text.c_str(), std::size(text) + 1);
      return begin + std::size(text) + 1;
    }

    static std::size_t size_buffer(td::RefInt256 const &value) noexcept {
      return 128;
    }
  };

  template<>
  struct nullness<td::Bits256> : pqxx::no_null<td::Bits256> {
  };

  template<>
  struct string_traits<td::Bits256> {
    static constexpr bool converts_to_string{true};
    static constexpr bool converts_from_string{false};

    static zview to_buf(char *begin, char *end, td::Bits256 const &value) {
      return zview{begin, static_cast<std::size_t>(into_buf(begin, end, value) - begin - 1)};
    }

    static char *into_buf(char *begin, char *end, td::Bits256 const &value) {
      return string_traits<bytes_view>::into_buf(begin, end, pqxx::binary_cast(value.as_slice().str()));
    }

    static std::size_t size_buffer(td::Bits256 const &value) noexcept {
      return string_traits<bytes_view>::size_buffer(pqxx::binary_cast(value.as_slice().str()));;
    }
  };

  template<>
  struct nullness<vm::CellHash> : pqxx::no_null<vm::CellHash> {
  };

  template<>
  struct string_traits<vm::CellHash> {
    static constexpr bool converts_to_string{true};
    static constexpr bool converts_from_string{false};

    static zview to_buf(char *begin, char *end, vm::CellHash const &value) {
      return zview{begin, static_cast<std::size_t>(into_buf(begin, end, value) - begin - 1)};
    }

    static char *into_buf(char *begin, char *end, vm::CellHash const &value) {
      return string_traits<bytes_view>::into_buf(begin, end, pqxx::binary_cast(value.as_slice().str()));
    }

    static std::size_t size_buffer(td::Bits256 const &value) noexcept {
      return string_traits<bytes_view>::size_buffer(pqxx::binary_cast(value.as_slice().str()));;
    }
  };

  template<>
  struct nullness<block::StdAddress> : pqxx::no_null<block::StdAddress> {
  };

  template<>
  struct string_traits<block::StdAddress> {
    static constexpr bool converts_to_string{true};
    static constexpr bool converts_from_string{false};

    static zview to_buf(char *begin, char *end, block::StdAddress const &value) {
      return zview{begin, static_cast<std::size_t>(into_buf(begin, end, value) - begin - 1)};
    }


    static char *into_buf(char *begin, char *end, block::StdAddress const &value) {
      bytes data{36, std::byte{0}};
      std::memcpy(&data[0], &value.workchain, 4);
      std::memcpy(&data[0] + 4, value.addr.as_slice().str().c_str(), 32);
      return string_traits<bytes>::into_buf(begin, end, data);
    }

    static std::size_t size_buffer(block::StdAddress const &value) noexcept {
      bytes data{36, std::byte{0}};
      std::memcpy(&data[0], &value.workchain, 4);
      std::memcpy(&data[0] + 4, value.addr.as_slice().str().c_str(), 32);
      return string_traits<bytes>::size_buffer(data);
    }
  };

  template<>
  struct nullness<schema::AddressNone> {
    static constexpr bool has_null{true};
    static constexpr bool always_null{true};

    static bool is_null(const schema::AddressNone& value) {
      return true;
    }
  };

  // TODO: implement schema::AddressVar
  template<>
  struct nullness<schema::AddressVar> {
    static constexpr bool has_null{true};
    static constexpr bool always_null{true};

    static bool is_null(const schema::AddressVar& value) {
      return true;
    }
  };

  template<>
  struct nullness<schema::AddressStd> : pqxx::no_null<schema::AddressStd> {
  };

  template<>
  struct string_traits<schema::AddressStd> {
    static constexpr bool converts_to_string{true};
    static constexpr bool converts_from_string{false};

    static zview to_buf(char *begin, char *end, schema::AddressStd const &value) {
      return zview{begin, static_cast<std::size_t>(into_buf(begin, end, value) - begin - 1)};
    }

    static bytes to_bytes(const schema::AddressStd& value) {
      bytes data{36, std::byte{0}};
      std::memcpy(&data[0], &value.workchain, 4);
      std::memcpy(&data[0] + 4, value.addr.as_slice().str().c_str(), 32);
      return data;
    }

    static char *into_buf(char *begin, char *end, schema::AddressStd const &value) {
      auto data = to_bytes(value);
      return string_traits<bytes>::into_buf(begin, end, data);
    }

    static std::size_t size_buffer(schema::AddressStd const &value) noexcept {
      auto data = to_bytes(value);
      return string_traits<bytes>::size_buffer(data);
    }
  };

  template<>
  struct string_traits<schema::AddressExtern> {
    static constexpr bool converts_to_string{true};
    static constexpr bool converts_from_string{false};

    static zview to_buf(char *begin, char *end, schema::AddressExtern const &value) {
      return zview{begin, static_cast<std::size_t>(into_buf(begin, end, value) - begin - 1)};
    }

    static bytes to_bytes(const schema::AddressExtern& value) {
      size_t data_size = 8 + ((value.len + 7) >> 3);
      bytes data{data_size, std::byte{0}};
      data[0] = std::byte{128};
      std::memcpy(&data[0] + 4, &value.len, sizeof(std::int32_t));
      if (value.len && value.external_address.not_null()) {
        std::memcpy(&data[0] + 8, value.external_address->bits().get_byte_ptr(), value.external_address->byte_size());
      }
      return data;
    }

    static char *into_buf(char *begin, char *end, schema::AddressExtern const &value) {
      auto data = to_bytes(value);
      return string_traits<bytes>::into_buf(begin, end, data);
    }

    static std::size_t size_buffer(schema::AddressExtern const &value) noexcept {
      auto data = to_bytes(value);
      return string_traits<bytes>::size_buffer(data);
    }
  };

  template<>
  struct nullness<schema::AccountAddress> {
    static constexpr bool has_null{true};
    static constexpr bool always_null{false};

    static bool is_null(const schema::AccountAddress& value) {
      return std::holds_alternative<schema::AddressNone>(value) || std::holds_alternative<schema::AddressVar>(value);
    }
  };

  template<>
  struct string_traits<schema::AccountAddress> {
    static constexpr bool converts_to_string{true};
    static constexpr bool converts_from_string{false};

    static zview to_buf(char *begin, char *end, schema::AccountAddress const &value) {
      return zview{begin, static_cast<std::size_t>(into_buf(begin, end, value) - begin - 1)};
    }

    static bytes to_bytes(const schema::AccountAddress& value) {
      if (std::holds_alternative<schema::AddressStd>(value)) {
        return string_traits<schema::AddressStd>::to_bytes(std::get<schema::AddressStd>(value));
      }
      if (std::holds_alternative<schema::AddressExtern>(value)) {
        return string_traits<schema::AddressExtern>::to_bytes(std::get<schema::AddressExtern>(value));
      }
      if (std::holds_alternative<schema::AddressVar>(value)) {
        throw std::runtime_error("unreachable if branch");
      }

      if (std::holds_alternative<schema::AddressNone>(value)) {
        throw std::runtime_error("unreachable if branch");
      }
      return bytes{};
    }

    static char *into_buf(char *begin, char *end, schema::AccountAddress const &value) {
      auto data = to_bytes(value);
      return string_traits<bytes>::into_buf(begin, end, data);
    }

    static std::size_t size_buffer(schema::AccountAddress const &value) noexcept {
      auto data = to_bytes(value);
      return string_traits<bytes>::size_buffer(data);
    }
  };
}
