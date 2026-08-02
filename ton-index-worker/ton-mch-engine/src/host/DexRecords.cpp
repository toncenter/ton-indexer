// Typed DEX swap records + encoders (see host/DexRecords.h).
#include "host/DexRecords.h"

#include "host/HostCommon.h"

namespace mch {

TransferLeg TransferLeg::from_jetton_transfer(const Block *jt) {
  TransferLeg leg;
  leg.asset = data_field(jt, "asset");
  leg.amount = data_field(jt, "amount");
  leg.source = data_field(jt, "sender");
  leg.source_jetton_wallet = data_field(jt, "sender_wallet");
  leg.destination = data_field(jt, "receiver");
  leg.destination_jetton_wallet = data_field(jt, "receiver_wallet");
  return leg;
}

Value SwapLeg::encode() const {
  Value::Fields f;
  f.emplace_back("asset", asset);
  f.emplace_back("amount", amount);
  return Value::make_dict(std::move(f));
}

Value TransferLeg::encode() const {
  Value::Fields f;
  f.emplace_back("asset", asset);
  f.emplace_back("amount", amount);
  f.emplace_back("source", source);
  f.emplace_back("source_jetton_wallet", source_jetton_wallet);
  f.emplace_back("destination", destination);
  f.emplace_back("destination_jetton_wallet", destination_jetton_wallet);
  return Value::make_dict(std::move(f));
}

Value PeerSwap::encode() const {
  Value::Fields f;
  f.emplace_back("in", in.encode());
  f.emplace_back("out", out.encode());
  return Value::make_dict(std::move(f));
}

void SwapRecord::fill_peer_out_assets() {
  if (peer_swaps.empty()) {
    return;
  }
  for (std::size_t i = 0; i + 1 < peer_swaps.size(); i++) {
    peer_swaps[i].out.asset = peer_swaps[i + 1].in.asset;
  }
  peer_swaps.back().out.asset = dex_outgoing_transfer.asset;
}

Value SwapRecord::encode() const {
  std::vector<Value> hops;
  if (peer_swaps.size() > 1) {
    for (const PeerSwap &p : peer_swaps) {
      hops.push_back(p.encode());
    }
  }
  Value::Fields d;
  d.emplace_back("dex", Value::make_str(dex));
  d.emplace_back("sender", sender);
  d.emplace_back("source_asset", source_asset);
  d.emplace_back("destination_asset", destination_asset);
  d.emplace_back("dex_incoming_transfer", dex_incoming_transfer.encode());
  d.emplace_back("dex_outgoing_transfer", dex_outgoing_transfer.encode());
  d.emplace_back("peer_swaps", Value::make_list(std::move(hops)));
  d.emplace_back("referral_amount", referral_amount);
  d.emplace_back("referral_address", referral_address);
  d.emplace_back("failed", Value::make_bool(failed));
  return Value::make_obj(std::move(d));
}

}  // namespace mch
