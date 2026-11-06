#pragma once
#include <block/block.h>
#include <td/actor/actor.h>
#include "td/actor/MultiPromise.h"
#include <mc-config.h>
#include "Tokens.h"
#include "NftSale.h"
#include "NftSaleV4.h"
#include "Multisig.h"
#include "Vesting.h"
#include "NominatorPool.h"
#include "Telemint.h"
#include "parse_contract_methods.h"
#include "Statistics.h"
#include <type_traits>

class AccountLookupContext;

struct DetectionOptions {
  bool filter_methods = true;
  std::shared_ptr<AccountLookupContext> account_lookup;
};

template<typename... Detectors>
class InterfacesDetector: public td::actor::Actor {
public:
  using DetectedInterface = std::variant<typename Detectors::Result...>;

  InterfacesDetector(block::StdAddress address, 
                    td::Ref<vm::Cell> code_cell,
                    td::Ref<vm::Cell> data_cell, 
                    AllShardStates shard_states,
                    std::shared_ptr<block::ConfigInfo> config,
                    td::Promise<std::vector<DetectedInterface>> promise,
                    std::shared_ptr<TokenDetectionStatus> token_detection = nullptr,
                    DetectionOptions options = {}) :
      address_(std::move(address)), code_cell_(std::move(code_cell)), data_cell_(std::move(data_cell)), 
      shard_states_(std::move(shard_states)), config_(std::move(config)), promise_(std::move(promise)),
      token_detection_(std::move(token_detection)), options_(std::move(options)) {
  found_interfaces_ = std::make_shared<std::vector<DetectedInterface>>();
}

  void start_up() override {
    if (options_.filter_methods) {
      auto info = get_contract_method_info(code_cell_, config_.get());
      if (info.is_ok() && info.ok()->exhaustive) {
        methods_ = info.move_as_ok();
      } else {
        g_statistics.record_count(DETECTOR_FILTER_FALLBACK);
      }
    }
    auto P = td::PromiseCreator::lambda([&, SelfId=actor_id(this)](td::Result<td::Unit> res) mutable {
      td::actor::send_closure(SelfId, &InterfacesDetector::finish, std::move(res));
    });

    td::MultiPromise mp;
    auto ig = mp.init_guard();
    ig.add_promise(std::move(P));

    start_detection<Detectors...>(ig);
  }
private:
  block::StdAddress address_;
  td::Ref<vm::Cell> code_cell_;
  td::Ref<vm::Cell> data_cell_;
  AllShardStates shard_states_;
  std::shared_ptr<block::ConfigInfo> config_;

  std::shared_ptr<std::vector<DetectedInterface>> found_interfaces_;
  td::Promise<std::vector<DetectedInterface>> promise_;
  std::shared_ptr<TokenDetectionStatus> token_detection_;
  DetectionOptions options_;
  std::shared_ptr<const ContractMethodInfo> methods_;

  template<class Detector>
  bool is_candidate() const {
    if (!methods_) return true;
    // Check the primary required getter only; optional getters do not disqualify
    // a contract. The auction detector has two alternative entry points.
    if constexpr (std::is_same_v<Detector, JettonWalletDetectorR>) return methods_->has(97026);
    if constexpr (std::is_same_v<Detector, JettonMasterDetectorR>) return methods_->has(106029);
    if constexpr (std::is_same_v<Detector, NftItemDetectorR>) return methods_->has(102351);
    if constexpr (std::is_same_v<Detector, NftCollectionDetectorR>) return methods_->has(102491);
    if constexpr (std::is_same_v<Detector, GetGemsNftAuction>) return methods_->has(123140) || methods_->has(72748);
    if constexpr (std::is_same_v<Detector, GetGemsNftFixPriceSale>) return methods_->has(72748);
    if constexpr (std::is_same_v<Detector, GetGemsNftFixPriceSaleV4>) return methods_->has(122483);
    if constexpr (std::is_same_v<Detector, MultisigContract>) return methods_->has(107307);
    if constexpr (std::is_same_v<Detector, MultisigOrder>) return methods_->has(123832);
    if constexpr (std::is_same_v<Detector, TelemintContract>) return methods_->has(69506);
    // Vesting/Nominator/DeDust already have cheap exact-code checks, and
    // NominatorPool parses storage directly rather than requiring a getter.
    return true;
  }

  template<typename Detector>
  void record_detection(std::optional<int> error = std::nullopt) {
    if (!token_detection_) {
      return;
    }
    std::optional<bool>* status = nullptr;
    int non_match = 0;
    if constexpr (std::is_same_v<Detector, JettonWalletDetectorR>) {
      status = &token_detection_->jetton_wallet;
      non_match = Detector::NOT_JETTON_WALLET;
    } else if constexpr (std::is_same_v<Detector, NftItemDetectorR>) {
      status = &token_detection_->nft_item;
      non_match = Detector::NOT_NFT_ITEM;
    }
    if (status) {
      *status = !error ? std::optional<bool>(true)
          : *error == non_match ? std::optional<bool>(false) : std::nullopt;
    }
  }

  template<typename FirstDetector, typename... RemainingDetectors>
  void start_detection(td::MultiPromise::InitGuard& ig) {
    detect_interface<FirstDetector>(ig.get_promise());

    if constexpr (sizeof...(RemainingDetectors) > 0) {
        start_detection<RemainingDetectors...>(ig);
    }
  }

  template<typename Detector>
  void detect_interface(td::Promise<td::Unit> promise) {
    if (!is_candidate<Detector>()) {
      g_statistics.record_count(DETECTOR_SKIPPED);
      if constexpr (std::is_same_v<Detector, JettonWalletDetectorR>) {
        record_detection<Detector>(Detector::NOT_JETTON_WALLET);
      } else if constexpr (std::is_same_v<Detector, NftItemDetectorR>) {
        record_detection<Detector>(Detector::NOT_NFT_ITEM);
      }
      promise.set_value(td::Unit());
      return;
    }
    g_statistics.record_count(DETECTOR_STARTED);
    auto P = td::PromiseCreator::lambda([&, SelfId = actor_id(this), promise = std::move(promise)](td::Result<typename Detector::Result> data) mutable {
      if (data.is_ok()) {
        LOG(DEBUG) << "Detected interface " << typeid(typename Detector::Result).name() << " for " << address_;
        send_lambda(SelfId, [this, data = data.move_as_ok(), promise = std::move(promise)]() mutable {
          record_detection<Detector>();
          found_interfaces_->push_back(data);
          promise.set_value(td::Unit());
        });
      } else {
        send_lambda(SelfId, [this, error = data.move_as_error(), promise = std::move(promise)]() mutable {
          record_detection<Detector>(error.code());
          promise.set_value(td::Unit());
        });
      }
    });
    if constexpr (std::is_same_v<Detector, JettonWalletDetectorR> || std::is_same_v<Detector, NftItemDetectorR> ||
                  std::is_same_v<Detector, MultisigOrder> || std::is_same_v<Detector, DedustPoolDetector>) {
      td::actor::create_actor<Detector>(td::Slice(typeid(Detector).name()), address_, code_cell_, data_cell_,
          shard_states_, config_, std::move(P), options_.account_lookup).release();
    } else {
      td::actor::create_actor<Detector>(td::Slice(typeid(Detector).name()), address_, code_cell_, data_cell_,
          shard_states_, config_, std::move(P)).release();
    }
  }

  void finish(td::Result<td::Unit> res) {
    if (res.is_error()) {
      promise_.set_error(res.move_as_error_prefix("Failed to detect interfaces: "));
    } else {
      promise_.set_value(std::move(*found_interfaces_));
    }
    stop();
  }
};
