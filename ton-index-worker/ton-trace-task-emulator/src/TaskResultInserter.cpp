#include "TaskResultInserter.h"
#include "Serializer.hpp"
#include "Statistics.h"


class TaskResultInserter: public td::actor::Actor {
private:
    sw::redis::Transaction transaction_;
    TraceEmulationResult result_;
    td::Promise<td::Unit> promise_;

public:
    TaskResultInserter(sw::redis::Transaction&& transaction, TraceEmulationResult result, td::Promise<td::Unit> promise) :
        transaction_(std::move(transaction)), result_(std::move(result)), promise_(std::move(promise)) {
    }

    void start_up() override {
        td::Timer timer;
        auto result_channel = "emulator_channel_" + result_.task.id;
        try {
            if (result_.trace.is_error()) {
                transaction_.set("emulator_error_" + result_.task.id, result_.trace.error().message().str());
                transaction_.expire("emulator_error_" + result_.task.id, 60);
                transaction_.publish(result_channel, "error");
                transaction_.exec();
                promise_.set_value(td::Unit());
                stop();
                return;
            }
            auto& trace = result_.trace.ok();

            std::queue<std::reference_wrapper<TraceNode>> queue;
        
            std::vector<std::string> tx_keys_to_delete;
            std::vector<std::pair<std::string, std::string>> addr_keys_to_delete;
            std::vector<RedisTraceNode> nodes;

            queue.push(*trace.root);

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
                nodes.push_back(std::move(redis_node));

                queue.pop();
            }


            // insert new trace
            for (const auto& node : nodes) {
                std::stringstream buffer;
                msgpack::pack(buffer, std::move(node));

                transaction_.hset("result_" + result_.task.id, td::base64_encode(node.transaction.in_msg.value().hash.as_slice()), buffer.str());
            }
            transaction_.hset("result_" + result_.task.id, "root_node", td::base64_encode(result_.trace.ok().root->node_id.as_slice()));
            transaction_.hset("result_" + result_.task.id, "mc_block_seqno", std::to_string(result_.mc_block_id.value().seqno));
            transaction_.hset("result_" + result_.task.id, "rand_seed", td::base64_encode(result_.trace.ok().rand_seed.as_slice()));
            transaction_.hset("result_" + result_.task.id, "depth_limit_exceeded", result_.trace.ok().tx_limit_exceeded ? "1" : "0");

            std::unordered_map<td::Bits256, AccountState> account_states;
            std::unordered_map<td::Bits256, std::string> code_cells;
            std::unordered_map<td::Bits256, std::string> data_cells;

            for (const auto& [addr, account] : trace.emulated_accounts) {
                auto account_parsed_r = parse_account(account);
                if (account_parsed_r.is_error()) {
                    promise_.set_error(account_parsed_r.move_as_error_prefix("Failed to parse account: "));
                    stop();
                    return;
                }
                auto account_state = account_parsed_r.move_as_ok();
                account_states[account_state.hash] = account_state;
                if (account_state.code_hash && code_cells.find(account_state.code_hash.value()) == code_cells.end()) {
                    auto code_boc = convert::to_bytes(account_state.code_cell);
                    if (code_boc.is_ok() && code_boc.ok().has_value()) {
                        code_cells[account_state.code_hash.value()] = code_boc.ok().value();
                    } else {
                        LOG(ERROR) << "Failed to convert code cell " << account_state.code_hash.value() << " to bytes. This code cell boc won't be included in response";
                    }
                }
                if (account_state.data_hash && data_cells.find(account_state.data_hash.value()) == data_cells.end()) {
                    auto data_boc = convert::to_bytes(account_state.data_cell);
                    if (data_boc.is_ok() && data_boc.ok().has_value()) {
                        data_cells[account_state.data_hash.value()] = data_boc.ok().value();
                    } else {
                        LOG(ERROR) << "Failed to convert data cell " << account_state.data_hash.value() << " to bytes. This data cell boc won't be included in response";
                    }
                }
            }
            {
                std::stringstream buffer;
                msgpack::pack(buffer, account_states);
                transaction_.hset("result_" + result_.task.id, "account_states", buffer.str());
            }
            
            if (result_.task.include_code_data) {
                std::stringstream code_cells_buffer;
                msgpack::pack(code_cells_buffer, code_cells);
                transaction_.hset("result_" + result_.task.id, "code_cells", code_cells_buffer.str());

                std::stringstream data_cells_buffer;
                msgpack::pack(data_cells_buffer, data_cells);
                transaction_.hset("result_" + result_.task.id, "data_cells", data_cells_buffer.str());
            }

            for (const auto& [addr, interfaces] : trace.interfaces) {
                auto interfaces_redis = parse_interfaces(interfaces);
                std::stringstream buffer;
                msgpack::pack(buffer, interfaces_redis);
                auto addr_raw = std::to_string(addr.workchain) + ":" + addr.addr.to_hex();
                transaction_.hset("result_" + result_.task.id, addr_raw, buffer.str());
            }

            transaction_.expire("result_" + result_.task.id, 60);

            transaction_.publish(result_channel, "success");
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

};

void RedisTaskResultInsertManager::insert(TraceEmulationResult result, td::Promise<td::Unit> promise) {
    td::actor::create_actor<TaskResultInserter>("TraceInserter", redis_.transaction(), std::move(result), std::move(promise)).release();
}