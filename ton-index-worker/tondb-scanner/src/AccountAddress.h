#pragma once
#include <variant>
#include <cstdlib>
#include "crypto/block/block-parse.h"

namespace schema {
  struct AddressNone {
    friend bool operator==(const AddressNone &a, const AddressNone &b)  {
      return std::is_eq(a <=> b);
    }

    friend std::strong_ordering operator<=>(const AddressNone &, const AddressNone &);
  };

  struct AddressExtern {
    std::int32_t len;
    td::Ref<td::BitString> external_address;

    friend bool operator==(const AddressExtern &a, const AddressExtern &b) {
      return std::is_eq(a <=> b);
    }

    friend std::strong_ordering operator<=>(const AddressExtern &lhs, const AddressExtern &rhs);
  };

  struct AddressStd : block::StdAddress {
    explicit AddressStd() = default;

    explicit AddressStd(const StdAddress &addr) : StdAddress(addr) {
    }

    explicit AddressStd(StdAddress &&addr) : StdAddress(addr) {
    }

    explicit AddressStd(std::int32_t workchain, const td::Bits256 &addr, bool bounceable = true,
                        bool testnet = false) : StdAddress(workchain, addr, bounceable, testnet) {
    }

    friend bool operator==(const AddressStd &a, const AddressStd &b) {
      return std::is_eq(a <=> b);
    }

    friend std::strong_ordering operator<=>(const AddressStd &lhs, const AddressStd &rhs);
  };

  struct AddressVar {
    std::int32_t addr_len;
    std::int32_t workchain;
    td::Ref<td::BitString> external_address;
    friend bool operator==(const AddressVar &a, const AddressVar &b) {
      return std::is_eq(a <=> b);
    }

    friend std::strong_ordering operator<=>(const AddressVar &, const AddressVar &);
  };

  using AccountAddress = std::variant<AddressNone, AddressVar, AddressExtern, AddressStd>;

  std::strong_ordering operator<=>(const schema::AddressExtern &lhs, const schema::AddressExtern &rhs);

  std::strong_ordering operator<=>(const schema::AddressNone &, const schema::AddressNone &);

  std::strong_ordering operator<=>(const schema::AddressStd &lhs, const schema::AddressStd &rhs);

  std::strong_ordering operator<=>(const schema::AddressVar &, const schema::AddressVar &);
}

namespace std {
  template <>
  struct hash<schema::AddressNone> {
    size_t operator()(const schema::AddressNone& a) const noexcept {
      return 0x9e3779b9;
    }
  };
  template <>
  struct hash<schema::AddressVar> {
    size_t operator()(const schema::AddressVar& a) const noexcept {
      return 0x9e3779b9;
    }
  };
  template <>
  struct hash<schema::AddressStd> {
    size_t operator()(const schema::AddressStd& a) const noexcept {
      std::size_t seed = 0;
      seed ^= std::hash<std::int32_t>{}(a.workchain) + 0x9e3779b9;
      seed ^= std::hash<bool>{}(a.bounceable) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
      seed ^= std::hash<bool>{}(a.testnet) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
      for(const auto& el : a.addr.as_array()) {
        seed ^= std::hash<td::uint8>{}(el) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
      }
      return seed;
    }
  };
  template <>
  struct hash<schema::AddressExtern> {
    size_t operator()(const schema::AddressExtern& a) const noexcept {
      std::size_t seed = 0;
      seed ^= std::hash<std::int32_t>{}(a.len) + 0x9e3779b9;
      if (a.external_address.not_null()) {
        const auto& bits = a.external_address->bits();
        for (size_t idx = 0; idx < a.external_address->byte_size(); ++idx) {
          seed ^= std::hash<td::uint8>{}(bits.ptr[idx]) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        }
      }
      return seed;
    }
  };
}


inline auto operator<<(td::StringBuilder &sb, const schema::AddressStd &addr) -> td::StringBuilder & {
  sb << addr.workchain << ":" << addr.addr.to_hex();
  return sb;
}

inline auto operator<<(td::StringBuilder &sb, const schema::AddressNone &addr) -> td::StringBuilder & {
  sb << "addr_none";
  return sb;
}

inline auto operator<<(td::StringBuilder &sb, const schema::AddressExtern &addr) -> td::StringBuilder & {
  sb << "addr_extern$";
  if (addr.external_address.not_null()) {
    sb << addr.len << ":" << addr.external_address->to_hex();
  } else {
    sb << addr.len << ":null";
  }
  return sb;
}

inline auto operator<<(td::StringBuilder &sb, const schema::AddressVar &addr) -> td::StringBuilder & {
  sb << "addr_var";
  return sb;
}

inline td::StringBuilder& operator<<(td::StringBuilder& sb, const schema::AccountAddress& addr) {
  std::visit([&sb](const auto& a) { sb << a; }, addr);
  return sb;
}
