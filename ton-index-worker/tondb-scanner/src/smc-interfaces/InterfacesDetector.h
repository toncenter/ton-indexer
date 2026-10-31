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
#include <type_traits>

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
                    std::shared_ptr<TokenDetectionStatus> token_detection = nullptr) :
      address_(std::move(address)), code_cell_(std::move(code_cell)), data_cell_(std::move(data_cell)), 
      shard_states_(std::move(shard_states)), config_(std::move(config)), promise_(std::move(promise)),
      token_detection_(std::move(token_detection)) {
  found_interfaces_ = std::make_shared<std::vector<DetectedInterface>>();
}

  void start_up() override {
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
    td::actor::create_actor<Detector>(td::Slice(typeid(Detector).name()), address_, code_cell_, data_cell_, shard_states_, config_, std::move(P)).release();
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
