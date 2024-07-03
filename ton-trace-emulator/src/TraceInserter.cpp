#include "TraceInserter.h"
#include "Serializer.hpp"

void TraceInserter::start_up() {
    try {
        std::queue<std::reference_wrapper<Trace>> queue;
        std::unordered_map<std::string, typeof(trace_->interfaces)> addr_interfaces;

        queue.push(*trace_);

        while (!queue.empty()) {
            Trace& current = queue.front();
            queue.pop();

            for (auto& child : current.children) {
                queue.push(*child);
            }

            auto tx_r = parse_tx(current.transaction_root, current.workchain);
            if (tx_r.is_error()) {
                promise_.set_error(tx_r.move_as_error_prefix("Failed to parse transaction: "));
                stop();
                return;
            }
            auto tx = tx_r.move_as_ok();

            if (!current.emulated) {
                delete_db_subtree(tx.in_msg.value().hash.to_hex());
            }

            TraceNode node{tx, current.emulated};
            std::stringstream buffer;
            msgpack::pack(buffer, std::move(node));

            redis_.hset(trace_->id.to_hex(), tx.in_msg.value().hash.to_hex(), buffer.str());

            auto addr_raw = std::to_string(tx.account.workchain) + ":" + tx.account.addr.to_hex();
            auto by_addr_key = trace_->id.to_hex() + ":" + tx.in_msg.value().hash.to_hex();
            redis_.zadd(addr_raw, {std::make_pair(by_addr_key, tx.lt)});

            addr_interfaces[addr_raw] = current.interfaces;
        }

        for (const auto& [addr_raw, interfaces] : addr_interfaces) {
            auto interfaces_redis = parse_interfaces(interfaces);
            std::stringstream buffer;
            msgpack::pack(buffer, interfaces_redis);
            redis_.hset(trace_->id.to_hex(), addr_raw, buffer.str());
        }

        redis_.publish("new_trace", trace_->id.to_hex());
        promise_.set_value(td::Unit());
    } catch (const std::exception &e) {
        promise_.set_error(td::Status::Error("Got exception while inserting trace: " + std::string(e.what())));
    }
    stop();
}

void TraceInserter::delete_db_subtree(std::string key) {
    auto emulated_in_db = redis_.hget(trace_->id.to_hex(), key);
    if (emulated_in_db) {
        auto serialized = emulated_in_db.value();
        TraceNode node;
        msgpack::unpacked result;
        msgpack::unpack(result, serialized.data(), serialized.size());
        result.get().convert(node);
        for (const auto& out_msg : node.transaction.out_msgs) {
            delete_db_subtree(out_msg.hash.to_hex());
        }
        redis_.hdel(trace_->id.to_hex(), key);

        auto addr_raw = std::to_string(node.transaction.account.workchain) + ":" + node.transaction.account.addr.to_hex();
        auto by_addr_key = trace_->id.to_hex() + ":" + node.transaction.in_msg.value().hash.to_hex();
        redis_.zrem(addr_raw, by_addr_key);
    }
}