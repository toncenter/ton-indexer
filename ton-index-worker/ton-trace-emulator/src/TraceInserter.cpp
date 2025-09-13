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
            if (trace_.root) {
                auto it = trace_.emulated_accounts.find(trace_.root->address);
                if (it != trace_.emulated_accounts.end()) {
                    const auto& [address, account] = *it;
                    if (account.code.not_null()) {
                        transaction_.hset(td::base64_encode(trace_.ext_in_msg_hash_norm.as_slice()),
                                          "root_account_code_hash",
                                          td::base64_encode(account.code->get_hash().as_slice()));
                    }
                }
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

// inserts latest state of the account, it's interfaces and transactions
class AccTxsInserter: public td::actor::Actor {
private:
    sw::redis::Transaction transaction_;
    CommittedAccountTxs acc_txs_;
    td::Promise<td::Unit> promise_;
public:
    AccTxsInserter(sw::redis::Transaction&& transaction, CommittedAccountTxs acc_txs, td::Promise<td::Unit> promise) :
        transaction_(std::move(transaction)), acc_txs_(std::move(acc_txs)), promise_(std::move(promise)) {
    }

    void start_up() override {
        td::Timer timer;
        try {
            auto addr_raw = std::to_string(acc_txs_.account.workchain) + ":" + acc_txs_.account.addr.to_hex();

            // 1. Upsert account state to key "acct:<acc_addr>:state"
            // First, we need to get the account state from redis
            auto account_state_r = parse_account(acc_txs_.account);
            if (account_state_r.is_error()) {
                promise_.set_error(account_state_r.move_as_error_prefix("Failed to parse account state: "));
                stop();
                return;
            }
            auto account_state = account_state_r.move_as_ok();
            bool should_insert = true;
            auto account_state_redis = transaction_.redis().get("acct:" + addr_raw + ":state");
            if (account_state_redis) {
                AccountState existing_account_state;
                auto serialized = account_state_redis.value();
                msgpack::unpacked result;
                msgpack::unpack(result, serialized.data(), serialized.size());
                result.get().convert(existing_account_state);
                if (existing_account_state.last_trans_lt > account_state.last_trans_lt) {
                    should_insert = false; // existing state is newer, skip
                }
            }
            if (should_insert) {
                std::stringstream buffer;
                msgpack::pack(buffer, account_state);
                transaction_.set("acct:" + addr_raw + ":state", buffer.str());
                transaction_.expire("acct:" + addr_raw + ":state", 30); // 30 seconds expiration
            }
            
            // 2. Insert committed transactions to redis key "comm:tx:<tx_hash>"
            std::vector<std::string> tx_hashes;
            for (const auto& tx_info : acc_txs_.txs) {
                auto tx_r = parse_tx(tx_info.root, tx_info.account.workchain);
                if (tx_r.is_error()) {
                    promise_.set_error(tx_r.move_as_error_prefix("Failed to parse transaction: "));
                    stop();
                    return;
                }
                auto tx = tx_r.move_as_ok();
                
                std::stringstream buffer;
                msgpack::pack(buffer, tx);
                
                auto tx_hash_b64 = td::base64_encode(tx.hash.as_slice());
                transaction_.set("comm:tx:" + tx_hash_b64, buffer.str());
                transaction_.expire("comm:tx:" + tx_hash_b64, 30); // 30 seconds expiration
                tx_hashes.push_back(tx_hash_b64);
            }
            
            // 3. Insert account interfaces to key "acct:<acc>:ifaces"
            if (!acc_txs_.interfaces.empty()) {
                auto interfaces_redis = parse_interfaces(acc_txs_.interfaces);
                std::stringstream interfaces_buffer;
                msgpack::pack(interfaces_buffer, interfaces_redis);
                transaction_.set("acct:" + addr_raw + ":ifaces", interfaces_buffer.str());
                transaction_.expire("acct:" + addr_raw + ":ifaces", 30); // 30 seconds expiration
            }
            
            // 4. Publish new committed transactions to channel "new_commited_txs"
            if (!tx_hashes.empty()) {
                std::string message = addr_raw;
                for (const auto& tx_hash : tx_hashes) {
                    message += "|" + tx_hash;
                }
                transaction_.publish("new_commited_txs", message);
            }
            
            transaction_.exec();
            promise_.set_value(td::Unit());
        } catch (const vm::VmError &e) {
            promise_.set_error(td::Status::Error("Got VmError while inserting committed transactions: " + std::string(e.get_msg())));
        } catch (const std::exception &e) {
            promise_.set_error(td::Status::Error("Got exception while inserting committed transactions: " + std::string(e.what())));
        }
        g_statistics.record_time(INSERT_TRACE, timer.elapsed() * 1e3);
        stop();
    }
};

void RedisInsertManager::insert(Trace trace, td::Promise<td::Unit> promise) {
    td::actor::create_actor<TraceInserter>("TraceInserter", redis_.transaction(), std::move(trace), std::move(promise)).release();
}

void RedisInsertManager::insert_committed(CommittedAccountTxs acc_txs, td::Promise<td::Unit> promise) {
    td::actor::create_actor<AccTxsInserter>("AccTxsInserter", redis_.transaction(), std::move(acc_txs), std::move(promise)).release();
}
