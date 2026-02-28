#pragma once
#include <td/actor/actor.h>
#include "IndexData.h"

using InsertData = std::variant<schema::AccountState,
  schema::JettonMasterDataV2, schema::JettonWalletDataV2,
  schema::NFTItemDataV2, schema::NFTCollectionDataV2,
  schema::GetGemsNftAuctionData, schema::GetGemsNftFixPriceSaleData,
  schema::MultisigContractData, schema::MultisigOrderData,
  schema::VestingData, schema::DedustPoolData, schema::StonfiPoolV2Data>;

class PostgreSQLInsertManager : public td::actor::Actor {
public:
  PostgreSQLInsertManager(std::string connection_string, std::int32_t batch_size)
    : connection_string_(connection_string), batch_size_(batch_size) {}
  void insert_data(std::vector<InsertData> data, td::Promise<td::Unit> promise);
  void insert_done(size_t cnt);
private:

  std::string connection_string_;
  std::int32_t batch_size_;
  std::vector<std::tuple<InsertData, td::Promise<>>> queue_;

  std::int32_t inserted_count_{0};
  std::int32_t in_progress_{0};
};
