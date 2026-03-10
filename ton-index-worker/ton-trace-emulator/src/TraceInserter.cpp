#include "TraceInserter.h"
#include "Serializer.hpp"
#include "Statistics.h"
#include <optional>
#include <queue>

class TraceInserter: public td::actor::Actor {
private:
    static constexpr size_t kMaxExistingTraceFields = 1000; // to avoid reading and inserting into unexpectedly large trace
    sw::redis::Transaction transaction_;
    Trace trace_;
    td::Promise<td::Unit> promise_;
    std::string trace_key_;
    MeasurementPtr measurement_;
public:
    TraceInserter(sw::redis::Transaction&& transaction, Trace trace, td::Promise<td::Unit> promise, const MeasurementPtr& measurement) :
        transaction_(std::move(transaction)), trace_(std::move(trace)), promise_(std::move(promise)), measurement_(measurement) {
    }

    void start_up() override {
        td::Timer timer;
        measurement_->measure_step("trace_inserter__trace_insert_start");
        try {
            std::queue<TraceNode*> queue;
        
            std::vector<std::string> tx_keys_to_delete;
            std::vector<std::pair<std::string, std::string>> addr_keys_to_delete;
            std::vector<std::pair<std::string, RedisTraceNode>> flattened_trace;

            trace_key_ = td::base64_encode(trace_.ext_in_msg_hash_norm.as_slice());
            auto existing_fields_count = transaction_.redis().hlen(trace_key_);
            if (existing_fields_count > static_cast<long long>(kMaxExistingTraceFields)) {
                LOG(WARNING) << "Skipping trace insert for " << trace_key_
                             << ": existing trace hash is too large (" << existing_fields_count << " fields)";
                promise_.set_value(td::Unit());
                stop();
                return;
            }

            if (trace_.root) {
                queue.push(trace_.root.get());
            }

            while (!queue.empty()) {
                TraceNode* current = queue.front();
                queue.pop();
                if (!current) {
                    continue;
                }

                auto redis_node_r = parse_trace_node(*current);
                if (redis_node_r.is_error()) {
                    promise_.set_error(redis_node_r.move_as_error_prefix("Failed to parse trace node: "));
                    stop();
                    return;
                }
                auto redis_node = redis_node_r.move_as_ok();
                auto msg_hash_key = td::base64_encode(redis_node.transaction.in_msg.value().hash.as_slice());

                // if existing node is more advanced (e.g. it's finalized, but we are inserting emulated), skip subtree
                auto existing_node = load_existing_node(trace_key_, msg_hash_key);
                if (existing_node &&
                    static_cast<int>(existing_node->finality) > static_cast<int>(redis_node.finality)) {
                    continue;
                }

                for (auto& child : current->children) {
                    if (child) {
                        queue.push(child.get());
                    }
                }

                if (redis_node.finality != FinalityState::Emulated) {
                    delete_db_subtree(msg_hash_key, tx_keys_to_delete, addr_keys_to_delete,
                                      existing_node ? &*existing_node : nullptr);
                }

                flattened_trace.emplace_back(std::move(msg_hash_key), std::move(redis_node));
            }

            if (flattened_trace.empty()) {
                promise_.set_value(td::Unit());
                stop();
                return;
            }

            // delete previously emulated trace
            if (tx_keys_to_delete.size() > 0) {
                transaction_.hdel(trace_key_, tx_keys_to_delete.begin(), tx_keys_to_delete.end());
            }

            for (const auto& [addr, by_addr_key] : addr_keys_to_delete) {
                transaction_.zrem(addr, by_addr_key);
            }

            std::string commited_txs_hashes = trace_key_ + ":";
            bool has_commited_txs = false;

            // insert new trace
            for (const auto& entry : flattened_trace) {
                const auto& node_key = entry.first;
                const auto& node = entry.second;
                std::stringstream buffer;
                msgpack::pack(buffer, node);

                transaction_.hset(trace_key_, node_key, buffer.str());

                auto addr_raw = std::to_string(node.transaction.account.workchain) + ":" + node.transaction.account.addr.to_hex();
                auto by_addr_key = trace_key_ + ":" + td::base64_encode(node.transaction.in_msg.value().hash.as_slice());
                transaction_.zadd(addr_raw, by_addr_key, node.transaction.lt);

                if (node.finality != FinalityState::Emulated) {
                    if (has_commited_txs) {
                        commited_txs_hashes += ",";
                    }
                    commited_txs_hashes += td::base64_encode(node.transaction.hash.as_slice());
                    has_commited_txs = true;
                }
            }

            // insert interfaces
            for (const auto& [addr, interfaces] : trace_.interfaces) {
                auto interfaces_redis = parse_interfaces(interfaces);
                std::stringstream buffer;
                msgpack::pack(buffer, interfaces_redis);
                auto addr_raw = std::to_string(addr.workchain) + ":" + addr.addr.to_hex();
                transaction_.hset(trace_key_, addr_raw, buffer.str());
            }
            
            for (const auto& [addr, account] : trace_.committed_accounts) {
                auto account_redis_r = parse_account(account);
                if (account_redis_r.is_error()) {
                    promise_.set_error(account_redis_r.move_as_error_prefix("Failed to parse account: "));
                    stop();
                    return;
                }
                auto account_redis = account_redis_r.move_as_ok();
                std::stringstream state_buffer;
                msgpack::pack(state_buffer, account_redis);
                auto addr_raw = std::to_string(addr.workchain) + ":" + addr.addr.to_hex();

                std::stringstream if_buffer;
                auto interfaces = trace_.committed_interfaces.find(addr);
                if (interfaces != trace_.committed_interfaces.end()) {
                    auto interfaces_redis = parse_interfaces(interfaces->second);
                    msgpack::pack(if_buffer, interfaces_redis);
                }
                
                const std::string script = R"(
                    local cur = redis.call('HGET', KEYS[1], 'lt')
                    local cur_num = tonumber(cur)
                    local new_num = tonumber(ARGV[1])
                    if (not cur_num) or (new_num > cur_num) then
                        redis.call('HSET', KEYS[1], 'lt', ARGV[1], 'state', ARGV[2], 'interfaces', ARGV[3])
                        redis.call('PUBLISH', 'new_account_state', KEYS[1])
                    end
                    redis.call('EXPIRE', KEYS[1], 60)
                    return 1
                )";
                std::string key_prefix;
                switch (trace_.root->finality_state) {
                    case FinalityState::Finalized:
                        key_prefix = "account_finalized:";
                        break;
                    case FinalityState::Confirmed:
                        key_prefix = "account_confirmed:";
                        break;
                    default:
                        UNREACHABLE();
                }
                auto key = key_prefix + addr_raw;
                transaction_.eval(script, {key}, {std::to_string(account.last_trans_lt_), state_buffer.str(), if_buffer.str()});
            }

            if (has_commited_txs) {
                if (trace_.root->finality_state == FinalityState::Finalized) {
                    transaction_.publish("new_finalized_txs", commited_txs_hashes);
                } else {
                    transaction_.publish("new_confirmed_txs", commited_txs_hashes);
                }
            }

            if (has_commited_txs) {
                transaction_.publish("new_commited_txs", commited_txs_hashes);
            }

            if (trace_.root) {
                auto it = trace_.emulated_accounts.find(trace_.root->address);
                if (it != trace_.emulated_accounts.end()) {
                    const auto& [address, account] = *it;
                    if (account.code.not_null()) {
                        transaction_.hset(trace_key_,
                                          "root_account_code_hash",
                                          td::base64_encode(account.code->get_hash().as_slice()));
                    }
                }
            }
            transaction_.hset(trace_key_, "root_node", td::base64_encode(trace_.ext_in_msg_hash.as_slice()));
            transaction_.hset(trace_key_, "depth_limit_exceeded", trace_.tx_limit_exceeded ? "1" : "0");
            transaction_.hset(trace_key_, "measurement_id", std::to_string(measurement_->id()));
            
            transaction_.set("tr_in_msg:" + td::base64_encode(trace_.ext_in_msg_hash.as_slice()), trace_key_);
            transaction_.expire("tr_in_msg:" + td::base64_encode(trace_.ext_in_msg_hash.as_slice()), 600);

            transaction_.publish("new_trace", trace_key_);
            
            transaction_.exec();
            measurement_->measure_step("trace_inserter__trace_insert_complete");

            promise_.set_value(td::Unit());
        } catch (const vm::VmError &e) {
            promise_.set_error(td::Status::Error("Got VmError while inserting trace: " + std::string(e.get_msg())));
        } catch (const std::exception &e) {
            promise_.set_error(td::Status::Error("Got exception while inserting trace: " + std::string(e.what())));
        }
        g_statistics.record_time(INSERT_TRACE, timer.elapsed() * 1e3);
        stop();
    }
    void delete_db_subtree(std::string key,
                           std::vector<std::string>& tx_keys,
                           std::vector<std::pair<std::string, std::string>>& addr_keys,
                           const RedisTraceNode* cached_node = nullptr) {
        struct Frame {
            std::string key;
            std::optional<RedisTraceNode> node;
            bool expanded{false};
        };

        std::vector<Frame> stack;
        stack.push_back(Frame{std::move(key), std::nullopt, false});
        if (cached_node) {
            stack.back().node = *cached_node;
        }

        while (!stack.empty()) {
            auto& frame = stack.back();

            if (!frame.node.has_value()) {
                auto node_raw = transaction_.redis().hget(trace_key_, frame.key);
                if (!node_raw) {
                    stack.pop_back();
                    continue;
                }
                msgpack::unpacked result;
                msgpack::unpack(result, node_raw->data(), node_raw->size());
                frame.node.emplace();
                result.get().convert(*frame.node);
            }

            if (!frame.expanded) {
                frame.expanded = true;
                std::vector<std::string> child_keys;
                child_keys.reserve(frame.node->transaction.out_msgs.size());
                for (const auto& out_msg : frame.node->transaction.out_msgs) {
                    child_keys.push_back(td::base64_encode(out_msg.hash.as_slice()));
                }
                // Reverse order keeps child processing equivalent to recursive DFS.
                for (auto it = child_keys.rbegin(); it != child_keys.rend(); ++it) {
                    stack.push_back(Frame{std::move(*it), std::nullopt, false});
                }
                continue;
            }

            tx_keys.push_back(frame.key);
            auto addr_raw = std::to_string(frame.node->transaction.account.workchain) + ":" + frame.node->transaction.account.addr.to_hex();
            auto by_addr_key = trace_key_ + ":" + td::base64_encode(frame.node->transaction.in_msg.value().hash.as_slice());
            addr_keys.push_back(std::make_pair(addr_raw, by_addr_key));

            stack.pop_back();
        }
    }

    std::optional<RedisTraceNode> load_existing_node(const std::string& trace_key, const std::string& node_key) {
        auto existing = transaction_.redis().hget(trace_key, node_key);
        if (!existing) {
            return std::nullopt;
        }
        try {
            RedisTraceNode node;
            msgpack::unpacked result;
            msgpack::unpack(result, existing->data(), existing->size());
            result.get().convert(node);
            return node;
        } catch (const std::exception& e) {
            LOG(ERROR) << "Failed to parse existing trace node for " << node_key << ": " << e.what();
            return std::nullopt;
        }
    }
};

void RedisInsertManager::insert(Trace trace, td::Promise<td::Unit> promise, MeasurementPtr measurement) {
    td::actor::create_actor<TraceInserter>("TraceInserter", redis_.transaction(), std::move(trace), std::move(promise), measurement).release();
}
