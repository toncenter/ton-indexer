// Wallet request opcodes and the body-level Telegram-wallet opcode recovery
// shared by leaf construction and failed-external decoding.
#pragma once

#include <cstdint>
#include <optional>

namespace mch {

struct Message;

inline constexpr std::uint32_t kTgWalletSendOneMessageInternal = 0x63896E74;
inline constexpr std::uint32_t kTgWalletSendOneMessageExternal = 0x63896E75;
inline constexpr std::uint32_t kTgWalletSendBulkMessagesInternal = 0x73896E74;
inline constexpr std::uint32_t kTgWalletSendBulkMessagesExternal = 0x73896E75;
inline constexpr std::uint32_t kTgWalletChangePublicKeyInternal = 0xFBBA99C7;
inline constexpr std::uint32_t kTgWalletChangePublicKeyExternal = 0xFBBA99C8;
inline constexpr std::uint32_t kWalletV5SignedRequestInternal = 0x73696E74;  // 'sint'

bool is_tg_wallet_request_opcode(std::uint32_t opcode);
bool is_gasless_request_opcode(std::uint32_t opcode);

// Telegram-wallet requests put their opcode after a 512-bit signature, so the
// opcode stored on the message row is only a signature fragment. Returns the
// real request opcode, or nullopt for a missing/malformed/non-TG body.
std::optional<std::uint32_t> get_tg_wallet_request_opcode(const Message *message);

}  // namespace mch
