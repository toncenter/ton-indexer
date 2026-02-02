#pragma once
#include <variant>
#include <cstdlib>
#include "crypto/block/block-parse.h"

namespace schema {
  struct AddressNone {
    friend bool operator==(const AddressNone &, const AddressNone &) = default;
    friend std::strong_ordering operator<=>(const AddressNone &, const AddressNone &);
  };

  struct AddressExtern {
    std::int32_t len;
    td::Ref<td::BitString> external_address;

    friend bool operator==(const AddressExtern &, const AddressExtern &) = default;
    friend std::strong_ordering operator<=>(const AddressExtern &lhs, const AddressExtern &rhs);
  };

  using AddressStd = block::StdAddress;

  struct AddressVar {
    friend bool operator==(const AddressVar &, const AddressVar &) = default;
    friend std::strong_ordering operator<=>(const AddressVar &, const AddressVar &);
  };

  using AccountAddress = std::variant<AddressNone, AddressVar, AddressExtern, AddressStd>;
}

std::strong_ordering operator<=>(const schema::AddressExtern &lhs, const schema::AddressExtern &rhs);
std::strong_ordering operator<=>(const schema::AddressNone &, const schema::AddressNone &);
std::strong_ordering operator<=>(const block::StdAddress &lhs, const block::StdAddress &rhs);
std::strong_ordering operator<=>(const schema::AddressVar &, const schema::AddressVar &);
