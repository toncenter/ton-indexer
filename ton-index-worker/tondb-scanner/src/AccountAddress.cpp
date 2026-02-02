#include "AccountAddress.h"

//
// comparators
//
std::strong_ordering operator<=>(const schema::AddressNone &, const schema::AddressNone &) {
  return std::strong_ordering::equal;
}

std::strong_ordering operator<=>(const schema::AddressExtern &lhs, const schema::AddressExtern &rhs) {
  if (lhs.len != rhs.len) {
    return lhs.len <=> rhs.len;
  }
  if (lhs.external_address.get() == rhs.external_address.get()) {
    return std::strong_ordering::equal;
  }
  return lhs.external_address.get() < rhs.external_address.get()
           ? std::strong_ordering::greater
           : std::strong_ordering::less;
}

std::strong_ordering operator<=>(const block::StdAddress &lhs, const block::StdAddress &rhs) {
  if (lhs.workchain != rhs.workchain) {
    return lhs.workchain <=> rhs.workchain;
  }
  if (lhs.addr == rhs.addr) {
    return std::strong_ordering::equal;
  }
  return lhs.addr < rhs.addr ? std::strong_ordering::greater : std::strong_ordering::less;
}

std::strong_ordering operator<=>(const schema::AddressVar &, const schema::AddressVar &) {
  return std::strong_ordering::equal;
}
