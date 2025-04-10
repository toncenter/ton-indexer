#include "TraceInserter.h"
#include "Serializer.hpp"
#include "Statistics.h"

class TraceInserter: public td::actor::Actor {
private:
    sw::redis::Transaction transaction_;
    Trace trace_;
    td::Promise<td::Unit> promise_;

public:
    TraceInserter(sw::redis::Transaction&& transaction, Trace trace, td::Promise<td::Unit> promise) :
        transaction_(std::move(transaction)), trace_(std::move(trace)), promise_(std::move(promise)) {
    }

    void start_up() override {
        td::Timer timer;
        try {
            std::queue<std::reference_wrapper<TraceNode>> queue;
        
            std::vector<std::string> tx_keys_to_delete;
            std::vector<std::pair<std::string, std::string>> addr_keys_to_delete;
            std::vector<RedisTraceNode> flattened_trace;

            queue.push(*trace_.root);

            while (!queue.empty()) {
                TraceNode& current = queue.front();

                for (auto& child : current.children) {
                    queue.push(*child);
                }

                auto redis_node_r = parse_trace_node(current);
                if (redis_node_r.is_error()) {
                    promise_.set_error(redis_node_r.move_as_error_prefix("Failed to parse trace node: "));
                    stop();
                    return;
                }
                auto redis_node = redis_node_r.move_as_ok();

                if (!current.emulated) {
                    delete_db_subtree(td::base64_encode(redis_node.transaction.in_msg.value().hash.as_slice()), tx_keys_to_delete, addr_keys_to_delete);
                }

                flattened_trace.push_back(std::move(redis_node));

                queue.pop();
            }

            // delete previously emulated trace
            if (tx_keys_to_delete.size() > 0) {
                transaction_.hdel(td::base64_encode(trace_.ext_in_msg_hash_norm.as_slice()), tx_keys_to_delete.begin(), tx_keys_to_delete.end());
            }

            for (const auto& [addr, by_addr_key] : addr_keys_to_delete) {
                transaction_.zrem(addr, by_addr_key);
            }

            // insert new trace
            for (const auto& node : flattened_trace) {
                std::stringstream buffer;
                msgpack::pack(buffer, std::move(node));

                transaction_.hset(td::base64_encode(trace_.ext_in_msg_hash_norm.as_slice()), td::base64_encode(node.transaction.in_msg.value().hash.as_slice()), buffer.str());

                auto addr_raw = std::to_string(node.transaction.account.workchain) + ":" + node.transaction.account.addr.to_hex();
                auto by_addr_key = td::base64_encode(trace_.ext_in_msg_hash_norm.as_slice()) + ":" + td::base64_encode(node.transaction.in_msg.value().hash.as_slice());
                transaction_.zadd(addr_raw, by_addr_key, node.transaction.lt);
            }

            // insert interfaces
            for (const auto& [addr, interfaces] : trace_.interfaces) {
                auto interfaces_redis = parse_interfaces(interfaces);
                std::stringstream buffer;
                msgpack::pack(buffer, interfaces_redis);
                auto addr_raw = std::to_string(addr.workchain) + ":" + addr.addr.to_hex();
                transaction_.hset(td::base64_encode(trace_.ext_in_msg_hash_norm.as_slice()), addr_raw, buffer.str());
            }
            transaction_.hset(td::base64_encode(trace_.ext_in_msg_hash_norm.as_slice()), "root_node", td::base64_encode(trace_.ext_in_msg_hash.as_slice()));
            transaction_.hset(td::base64_encode(trace_.ext_in_msg_hash_norm.as_slice()), "depth_limit_exceeded", trace_.tx_limit_exceeded ? "1" : "0");
            
            if (!trace_.root->emulated) {
                transaction_.set("tr_root_tx:" + td::base64_encode(trace_.root_tx_hash.as_slice()), td::base64_encode(trace_.ext_in_msg_hash_norm.as_slice()));
                transaction_.expire("tr_root_tx:" + td::base64_encode(trace_.root_tx_hash.as_slice()), 600);
            }
            transaction_.set("tr_in_msg:" + td::base64_encode(trace_.ext_in_msg_hash.as_slice()), td::base64_encode(trace_.ext_in_msg_hash_norm.as_slice()));
            transaction_.expire("tr_in_msg:" + td::base64_encode(trace_.ext_in_msg_hash.as_slice()), 600);

            transaction_.publish("new_trace", td::base64_encode(trace_.ext_in_msg_hash_norm.as_slice()));
            transaction_.exec();

            promise_.set_value(td::Unit());
        } catch (const vm::VmError &e) {
            promise_.set_error(td::Status::Error("Got VmError while inserting trace: " + std::string(e.get_msg())));
        } catch (const std::exception &e) {
            promise_.set_error(td::Status::Error("Got exception while inserting trace: " + std::string(e.what())));
        }
        g_statistics.record_time(INSERT_TRACE, timer.elapsed() * 1e3);
        stop();
    }
    void delete_db_subtree(std::string key, std::vector<std::string>& tx_keys, std::vector<std::pair<std::string, std::string>>& addr_keys) {
        auto emulated_in_db = transaction_.redis().hget(td::base64_encode(trace_.ext_in_msg_hash_norm.as_slice()), key);
        if (emulated_in_db) {
            auto serialized = emulated_in_db.value();
            RedisTraceNode node;
            msgpack::unpacked result;
            msgpack::unpack(result, serialized.data(), serialized.size());
            result.get().convert(node);
            for (const auto& out_msg : node.transaction.out_msgs) {
                delete_db_subtree(td::base64_encode(out_msg.hash.as_slice()), tx_keys, addr_keys);
            }
            tx_keys.push_back(key);

            auto addr_raw = std::to_string(node.transaction.account.workchain) + ":" + node.transaction.account.addr.to_hex();
            auto by_addr_key = td::base64_encode(trace_.ext_in_msg_hash_norm.as_slice()) + ":" + td::base64_encode(node.transaction.in_msg.value().hash.as_slice());
            addr_keys.push_back(std::make_pair(addr_raw, by_addr_key));
        }
    }
};

void RedisInsertManager::insert(Trace trace, td::Promise<td::Unit> promise) {
    td::actor::create_actor<TraceInserter>("TraceInserter", redis_.transaction(), std::move(trace), std::move(promise)).release();
}