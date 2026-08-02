// Typed records for the shared DEX swap action vocabulary. Protocol hosts derive
// these facts, then call one encoder that
// owns the output field names + null conventions, instead of assembling generic
// `Value` dicts and mutating them (the HostCoffee const_cast is gone).
//
// The leg, peer, and record encoders emit Coffee field order. Coffee and Stonfi
// cores (HostCoffee.cpp / HostStonfi.cpp) share one encoder and ONE leg reader
// (TransferLeg::from_jetton_transfer). Add an ordering variant only for an
// observed wire-format requirement.
#pragma once

#include "Value.h"

#include <string>
#include <vector>

namespace mch {

struct Block;  // BlockTree.h

// {asset, amount}, a peer-swap hop's in/out side.
struct SwapLeg {
  Value asset;
  Value amount;
  Value encode() const;  // Dict{asset, amount}
};

// A full transfer leg {asset, amount, source, source_jetton_wallet,
// destination, destination_jetton_wallet} (the DEX incoming/outgoing transfer).
struct TransferLeg {
  Value asset;
  Value amount;
  Value source;
  Value source_jetton_wallet;
  Value destination;
  Value destination_jetton_wallet;
  Value encode() const;  // Dict, 6 fields in the order above

  // Read the six fields out of a produced `jetton_transfer` block's data dict
  // (asset/amount/sender/sender_wallet/receiver/receiver_wallet). The shared
  // jetton-leg shape reused by Coffee (in + out arms) and the Stonfi cores.
  static TransferLeg from_jetton_transfer(const Block *jt);
};

// One peer-swap hop {in:{asset,amount}, out:{asset,amount}}.
struct PeerSwap {
  SwapLeg in;
  SwapLeg out;
  Value encode() const;  // Dict{in, out}
};

// The top swap record. Field set/order = Coffee's coffee_swap_data (see the
// ORDER NOTE above).
struct SwapRecord {
  std::string dex;
  Value sender;
  Value source_asset;
  Value destination_asset;
  TransferLeg dex_incoming_transfer;
  TransferLeg dex_outgoing_transfer;
  std::vector<PeerSwap> peer_swaps;
  Value referral_amount;
  Value referral_address;
  bool failed{false};

  // Fill each hop's out-asset from the next hop's in-asset, and the final hop's
  // out-asset from `dex_outgoing_transfer.asset`. This is the typed replacement
  // before encode().
  void fill_peer_out_assets();

  // Obj{dex, sender, source_asset, destination_asset, dex_incoming_transfer,
  // dex_outgoing_transfer, peer_swaps, referral_amount, referral_address,
  // failed}. peer_swaps renders as an empty list unless there is >1 hop
  // (Coffee's `size > 1 ? ... : {}` convention).
  Value encode() const;
};

}  // namespace mch
