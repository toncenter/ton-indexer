// Wallet request opcodes and the body-level Telegram-wallet opcode recovery
// shared by leaf construction and the failed-external ghost decoder.
#pragma once

#include <cstdint>
#include <optional>

namespace mch {

struct Message;

// Telegram wallet (tg-wallet-contract) request opcodes. A request body starts
// with a 512-bit signature, so the stored message opcode is a slice of that
// signature; the real opcode follows it. E = external, I = relayed internal.
inline constexpr std::uint32_t kTgWalletSendOneMessageInternal = 0x63896E74;
inline constexpr std::uint32_t kTgWalletSendOneMessageExternal = 0x63896E75;
inline constexpr std::uint32_t kTgWalletSendBulkMessagesInternal = 0x73896E74;
inline constexpr std::uint32_t kTgWalletSendBulkMessagesExternal = 0x73896E75;
inline constexpr std::uint32_t kTgWalletChangePublicKeyInternal = 0xFBBA99C7;
inline constexpr std::uint32_t kTgWalletChangePublicKeyExternal = 0xFBBA99C8;
// Wallet v5 signed request delivered as an internal message ('sint').
inline constexpr std::uint32_t kWalletV5SignedRequestInternal = 0x73696E74;

bool is_tg_wallet_request_opcode(std::uint32_t opcode);

// A signed wallet request that arrived as an internal message: a relayer sent
// the owner's request and attached the TON that pays for the gas.
bool is_gasless_request_opcode(std::uint32_t opcode);

// Returns the real tg-wallet request opcode read from behind the signature, or
// nullopt for a missing, malformed or non-tg-wallet body.
std::optional<std::uint32_t> get_tg_wallet_request_opcode(const Message *message);

}  // namespace mch
