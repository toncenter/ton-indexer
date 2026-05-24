#pragma once
#include "IndexData.h"


// Detects special cases of Actions like - Jetton transfers and burns, NFT transfers
class ActionDetector: public td::actor::Actor {
private:
  ParsedBlockPtr block_;
  td::Promise<ParsedBlockPtr> promise_;
  td::Timer timer_{true};
public:
  ActionDetector(ParsedBlockPtr block, td::Promise<ParsedBlockPtr> promise): block_(block), promise_(std::move(promise)) {}
  void start_up() override;
  void process_tx(const schema::Transaction& transaction);

  td::Result<schema::JettonTransfer> parse_jetton_transfer(const schema::JettonWalletDataV2& jetton_wallet, const schema::Transaction& transaction, td::Ref<vm::CellSlice> in_msg_body_cs);
  td::Result<schema::JettonBurn> parse_jetton_burn(const schema::JettonWalletDataV2& jetton_wallet, const schema::Transaction& transaction, td::Ref<vm::CellSlice> in_msg_body_cs);
  td::Result<schema::NFTTransfer> parse_nft_transfer(const schema::NFTItemDataV2& nft_item, const schema::Transaction& transaction, td::Ref<vm::CellSlice> in_msg_body_cs);
};
