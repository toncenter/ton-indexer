#include "WalletRequest.h"

#include "TraceLoader.h"

#include "td/utils/base64.h"
#include "vm/boc.h"
#include "vm/cellslice.h"

#include <cstddef>

namespace mch {

namespace {

// signature(512) + opcode(32) + SeqnoHeader(96) = 640 bits, plus the BOC header.
constexpr std::size_t kTgWalletMinRequestBocBytes = 80;

}  // namespace

bool is_tg_wallet_request_opcode(std::uint32_t opcode) {
  switch (opcode) {
    case kTgWalletSendOneMessageInternal:
    case kTgWalletSendOneMessageExternal:
    case kTgWalletSendBulkMessagesInternal:
    case kTgWalletSendBulkMessagesExternal:
    case kTgWalletChangePublicKeyInternal:
    case kTgWalletChangePublicKeyExternal:
      return true;
    default:
      return false;
  }
}

bool is_gasless_request_opcode(std::uint32_t opcode) {
  return opcode == kTgWalletSendOneMessageInternal ||
         opcode == kTgWalletSendBulkMessagesInternal ||
         opcode == kTgWalletChangePublicKeyInternal || opcode == kWalletV5SignedRequestInternal;
}

std::optional<std::uint32_t> get_tg_wallet_request_opcode(const Message *message) {
  if (message == nullptr || !message->content) return std::nullopt;

  auto r_raw = td::base64_decode(td::Slice(message->content->body));
  if (r_raw.is_error() || r_raw.ok().size() < kTgWalletMinRequestBocBytes) {
    return std::nullopt;
  }
  auto r_cell = vm::std_boc_deserialize(r_raw.move_as_ok());
  if (r_cell.is_error()) return std::nullopt;

  try {
    bool special = false;
    vm::CellSlice cs = vm::load_cell_slice_special(r_cell.move_as_ok(), special);
    if (!cs.have(512 + 32)) return std::nullopt;
    cs.advance(512);
    const auto opcode = static_cast<std::uint32_t>(cs.fetch_ulong(32));
    return is_tg_wallet_request_opcode(opcode) ? std::optional<std::uint32_t>(opcode)
                                                : std::nullopt;
  } catch (...) {
    return std::nullopt;
  }
}

}  // namespace mch
