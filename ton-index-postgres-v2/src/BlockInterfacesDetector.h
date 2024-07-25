#include <td/actor/actor.h>
#include <td/actor/MultiPromise.h>
#include <block/block.h>
#include "IndexData.h"

struct AccountStateHasher {
    std::size_t operator()(const schema::AccountState& account_state) const {
        return BitArrayHasher()(account_state.hash);
    }
};

class BlockInterfaceProcessor: public td::actor::Actor {
private:
    ParsedBlockPtr block_;
    td::Promise<ParsedBlockPtr> promise_;
    std::unordered_map<block::StdAddress, std::vector<typename Detector::DetectedInterface>, AddressHasher> interfaces_{};
public:
    BlockInterfaceProcessor(ParsedBlockPtr block, td::Promise<ParsedBlockPtr> promise) : 
        block_(std::move(block)), promise_(std::move(promise)) {}

    void start_up() override {
        std::unordered_set<schema::AccountState, AccountStateHasher> account_states_to_detect;
        for (const auto& account_state : block_->account_states_) {
            auto existing = account_states_to_detect.find(account_state);
            if (existing != account_states_to_detect.end() && account_state.last_trans_lt > existing->last_trans_lt) {
                account_states_to_detect.erase(existing);
                account_states_to_detect.insert(account_state);
            } else {
                account_states_to_detect.insert(account_state);
                interfaces_[account_state.account] = {};
            }
        }

        std::vector<td::Ref<vm::Cell>> shard_states;
        for (const auto& shard_state : block_->mc_block_.shard_blocks_) {
            shard_states.push_back(shard_state.block_state);
        }

        auto P = td::PromiseCreator::lambda([&, SelfId=actor_id(this)](td::Result<td::Unit> res) mutable {
            td::actor::send_closure(SelfId, &BlockInterfaceProcessor::finish, std::move(res));
        });

        td::MultiPromise mp;
        auto ig = mp.init_guard();
        ig.add_promise(std::move(P));

        for (const auto& account_state : account_states_to_detect) {
            if (account_state.code.is_null()) {
                continue;
            }
            td::actor::create_actor<Detector>("InterfacesDetector", account_state.account, account_state.code, account_state.data, shard_states, block_->mc_block_.config_, 
                td::PromiseCreator::lambda([&, SelfId = actor_id(this), address = account_state.account, promise = ig.get_promise()](std::vector<typename Detector::DetectedInterface> interfaces) mutable {
                    send_lambda(SelfId, [this, address = std::move(address), interfaces = std::move(interfaces), promise = std::move(promise)]() mutable {
                        interfaces_[address] = std::move(interfaces);
                        promise.set_value(td::Unit());
                    });
            })).release();
        }
    }

    void finish(td::Result<td::Unit> status) {
        if (status.is_error()) {
            promise_.set_error(status.move_as_error_prefix("Failed to detect interfaces: "));
        } else {
            block_->account_interfaces_ = std::move(interfaces_);
            promise_.set_result(std::move(block_));
        }
        stop();
    }
};