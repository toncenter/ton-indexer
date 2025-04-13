#include <map>
#include <filesystem>
#include "TraceAssembler.h"
#include "td/utils/JsonBuilder.h"
#include "td/utils/filesystem.h"
#include "td/utils/port/path.h"
#include "common/delay.h"
#include "convert-utils.h"
#include "Statistics.h"

namespace fs = std::filesystem;

schema::TraceEdge TraceEdgeImpl::to_schema() const {
    schema::TraceEdge result;
    result.trace_id = trace_id;
    result.msg_hash = msg_hash;
    result.msg_lt = msg_lt;
    result.left_tx = left_tx;
    result.right_tx = right_tx;
    result.type = type;
    result.incomplete = incomplete;
    result.broken = broken;
    return result;
}

schema::Trace TraceImpl::to_schema() const {
    schema::Trace result;
    result.trace_id = trace_id;
    result.external_hash = external_hash;
    result.mc_seqno_start = mc_seqno_start;
    result.mc_seqno_end = mc_seqno_end;
    result.start_lt = start_lt;
    result.start_utime = start_utime;
    result.end_lt = end_lt;
    result.end_utime = end_utime;
    result.state = state;
    result.pending_edges_ = pending_edges;
    result.edges_ = edges;
    result.nodes_ = nodes;
    return result;
}

TraceAssembler::TraceAssembler(std::string db_path, size_t gc_distance) : 
        db_path_(db_path), gc_distance_(gc_distance) {
    td::mkdir(db_path).ensure();
}

void TraceAssembler::start_up() {
    alarm_timestamp() = td::Timestamp::in(10.0);
}

td::Status gc_states(std::string db_path, ton::BlockSeqno current_seqno, size_t keep_last) {
    td::Timer timer;
    std::map<int, fs::path, std::greater<int>> fileMap;

    for (const auto& entry : fs::directory_iterator(db_path)) {
        if (fs::is_regular_file(entry.status())) {
            auto filename = entry.path().stem().string();
            auto extension = entry.path().extension().string();

            if (extension == ".tastate") {
                try {
                    int seqno = std::stoi(filename);
                    fileMap[seqno] = entry.path();
                } catch (const std::exception& e) {
                    LOG(ERROR) << "Error reading seqno of trace assembler state " << entry.path().string();
                }
            }
        }
    }

    int count = 0;
    for (const auto& [seqno, filepath] : fileMap) {
        if (seqno > current_seqno) {
            LOG(WARNING) << "Deleting state " << filepath.string() << " that is higher than currently processing seqno " << current_seqno;
            fs::remove(filepath);
        } else if (count >= keep_last) {
            fs::remove(filepath);
            LOG(DEBUG) << "Deleting old state: " << filepath.string();
        } else {
            LOG(DEBUG) << "Keeping state: " << filepath.string();
            count++;
        }
    }
    g_statistics.record_time(TRACE_ASSEMBLER_GC_STATE, timer.elapsed() * 1e6);
    return td::Status::OK();
}

td::Status save_state(std::string db_path, ton::BlockSeqno seqno, 
                      std::unordered_map<td::Bits256, TraceImplPtr, Bits256Hasher> pending_traces,
                      std::unordered_map<td::Bits256, TraceEdgeImpl, Bits256Hasher> pending_edges) {
    td::Timer timer;
    std::stringstream buffer;
    msgpack::pack(buffer, pending_traces);
    msgpack::pack(buffer, pending_edges);
 
    auto path = db_path + "/" + std::to_string(seqno) + ".tastate";
    auto result = td::atomic_write_file(path, buffer.str());
    g_statistics.record_time(TRACE_ASSEMBLER_SAVE_STATE, timer.elapsed() * 1e6);
    return result;
}

void TraceAssembler::alarm() {
    alarm_timestamp() = td::Timestamp::in(10.0);

    if (expected_seqno_ == 0) {
        return;
    }

    ton::delay_action([db_path = this->db_path_, seqno = expected_seqno_ - 1, pending_traces = pending_traces_, pending_edges = pending_edges_]() {
        auto S = save_state(db_path, seqno, pending_traces, pending_edges);
        if (S.is_error()) {
            LOG(ERROR) << "Error while saving Trace Assembler state: " << S.move_as_error();
        }
    }, td::Timestamp::now());

    ton::delay_action([this]() {
        auto S = gc_states(this->db_path_, this->expected_seqno_, 100);
        if (S.is_error()) {
            LOG(ERROR) << "Error while garbage collecting Trace Assembler states: " << S.move_as_error();
        }
    }, td::Timestamp::now());

    LOG(INFO) << "Expected seqno: " << expected_seqno_
              << " Pending traces: " << pending_traces_.size()
              << " Pending edges: " << pending_edges_.size()
              << " Broken traces: " << broken_count_;
}

void TraceAssembler::assemble(ton::BlockSeqno seqno, ParsedBlockPtr block, td::Promise<ParsedBlockPtr> promise) {
    if (seqno < expected_seqno_) {
        LOG(FATAL) << "TraceAssembler received seqno " << seqno << " that is lower than expected " << expected_seqno_;
        return;
    }
    queue_.emplace(seqno, Task{seqno, std::move(block), std::move(promise)});

    process_queue();
}

void TraceAssembler::set_expected_seqno(ton::BlockSeqno new_expected_seqno) { 
    expected_seqno_ = new_expected_seqno;
}

void TraceAssembler::reset_state() {
    pending_traces_.clear();
    pending_edges_.clear();
    broken_count_ = 0;
}

td::Result<ton::BlockSeqno> TraceAssembler::restore_state(ton::BlockSeqno seqno) {
    std::map<int, fs::path, std::greater<int>> fileMap;
    try {
        for (const auto& entry : fs::directory_iterator(db_path_)) {
            if (fs::is_regular_file(entry.status())) {
                auto filename = entry.path().stem().string();  // Get filename without extension
                auto extension = entry.path().extension().string();  // Get file extension

                if (extension == ".tastate") {
                    try {
                        int seqno = std::stoi(filename);
                        fileMap[seqno] = entry.path();
                    } catch (const std::exception& e) {
                        LOG(ERROR) << "Error reading seqno of trace assembler state " << entry.path().string();
                    }
                }
            }
        }
    } catch (const std::exception& e) {
        return td::Status::Error(PSLICE() << "Error while searching for TraceAssembler states: " << e.what());
    }

    for (const auto& [state_seqno, path] : fileMap) {
        LOG(INFO) << "Found TA state seqno: " << state_seqno << " - path: " << path.string() << '\n';
        if (state_seqno > seqno) {
            LOG(WARNING) << "Found trace assembler state " << state_seqno << " newer than requested " << seqno;
            continue;
        }
        auto buffer_r = td::read_file(path.string());
        if (buffer_r.is_error()) {
            LOG(ERROR) << "Failed to read trace assembler state file " << path.string() << ": " << buffer_r.move_as_error();
            continue;
        }
        auto buffer = buffer_r.move_as_ok();

        std::unordered_map<td::Bits256, TraceImplPtr, Bits256Hasher> pending_traces;
        std::unordered_map<td::Bits256, TraceEdgeImpl, Bits256Hasher> pending_edges;    
        try {
            size_t offset = 0;
            msgpack::unpacked pending_traces_res;
            msgpack::unpack(pending_traces_res, buffer.data(), buffer.size(), offset);
            msgpack::object pending_traces_obj = pending_traces_res.get();

            msgpack::unpacked pending_edges_res;
            msgpack::unpack(pending_edges_res, buffer.data(), buffer.size(), offset);
            msgpack::object pending_edges_obj = pending_edges_res.get();

            pending_traces_obj.convert(pending_traces);
            pending_edges_obj.convert(pending_edges);
        } catch (const std::exception &e) {
            LOG(ERROR) << "Failed to unpack state for seqno " << state_seqno << ": " << e.what();
            continue;
        }
        
        pending_traces_ = std::move(pending_traces);
        pending_edges_ = std::move(pending_edges);

        return state_seqno;
    }

    return td::Status::Error(ton::ErrorCode::warning, "TraceAssembler state not found");    
}


void TraceAssembler::process_queue() {
    auto it = queue_.find(expected_seqno_);
    while(it != queue_.end()) {
        td::Timer timer;
        process_block(it->second.seqno_, it->second.block_);
        g_statistics.record_time(TRACE_ASSEMBLER_PROCESS_BLOCK, timer.elapsed() * 1e6);
        it->second.promise_.set_result(it->second.block_);

        // block processed
        queue_.erase(it);

        expected_seqno_ += 1;
        it = queue_.find(expected_seqno_);
    }
}

void TraceAssembler::process_block(ton::BlockSeqno seqno, ParsedBlockPtr block) {
    // sort transactions by lt
    std::vector<std::reference_wrapper<schema::Transaction>> sorted_txs;
    for(auto& blk: block->blocks_) {
        for(auto& tx: blk.transactions) {
            sorted_txs.push_back(tx);
        }
    }
    std::sort(sorted_txs.begin(), sorted_txs.end(), [](auto& lhs, auto& rhs){
        if (lhs.get().lt != rhs.get().lt) {
            return lhs.get().lt < rhs.get().lt;
        }
        if (lhs.get().account.workchain != rhs.get().account.workchain) {
            return (lhs.get().account.workchain < rhs.get().account.workchain);
        }
        return (lhs.get().account.addr < rhs.get().account.addr);
    });

    // process transactions
    std::vector<TraceEdgeImpl> completed_edges;
    std::unordered_set<td::Bits256, Bits256Hasher> updated_traces;
    std::unordered_set<td::Bits256, Bits256Hasher> pending_edges_added;
    for(auto &tx : sorted_txs) {
        process_transaction(seqno, tx.get(), completed_edges, updated_traces, pending_edges_added);
    }
    std::unordered_map<td::Bits256, schema::Trace, Bits256Hasher> trace_map;
    for (auto &trace_id : updated_traces) {
        auto trace = pending_traces_[trace_id];
        if (trace->pending_edges == 0) {
            if (trace->state != TraceImpl::State::broken) {
                trace->state = TraceImpl::State::complete;
            }
            pending_traces_.erase(trace->trace_id);
        } else if (trace->pending_edges < 0) {
            trace->state = TraceImpl::State::broken;
        }
        trace_map[trace_id] = trace->to_schema();
    }
    std::vector<TraceEdgeImpl> block_all_edges = completed_edges;
    for (const auto &edge_hash : pending_edges_added) {
        const auto &edge = pending_edges_[edge_hash];
        assert(edge.incomplete);
        block_all_edges.push_back(edge);
    }
    for (const auto &edge : block_all_edges) {
        // update trace
        auto &trace_schema = trace_map[edge.trace_id];
        trace_schema.edges.push_back(edge.to_schema());
    }
    for (auto &[_, trace] : trace_map) {
        block->traces_.push_back(std::move(trace));
    }
}

void TraceAssembler::process_transaction(ton::BlockSeqno seqno, schema::Transaction& tx, std::vector<TraceEdgeImpl>& completed_edges, 
        std::unordered_set<td::Bits256, Bits256Hasher>& updated_traces, std::unordered_set<td::Bits256, Bits256Hasher>& pending_edges_added) {
    TraceImplPtr trace = nullptr;
    if (tx.in_msg.has_value()) {
        auto &msg = tx.in_msg.value();
        TraceEdgeImpl edge;
        {
            auto edge_it = pending_edges_.find(msg.hash);
            if (edge_it == pending_edges_.end()) {
                // edge doesn't exist
                if (!msg.source) {
                    // external
                    edge.trace_id = tx.hash;
                    edge.msg_hash = msg.hash;
                    edge.msg_lt = (msg.created_lt ? msg.created_lt.value() : 0);
                    edge.left_tx = std::nullopt;
                    edge.right_tx = tx.hash;
                    edge.type = TraceEdgeImpl::Type::ext;
                    edge.incomplete = false;
                    edge.broken = false;
                } else if (msg.source.value() == "-1:0000000000000000000000000000000000000000000000000000000000000000") {
                    // system
                    edge.trace_id = tx.hash;
                    edge.msg_hash = msg.hash;
                    edge.msg_lt = (msg.created_lt ? msg.created_lt.value() : 0);
                    edge.left_tx = std::nullopt;
                    edge.right_tx = tx.hash;
                    edge.type = TraceEdgeImpl::Type::sys;
                    edge.incomplete = false;
                    edge.broken = false;
                } else {
                    // broken edge
                    edge.trace_id = tx.hash;
                    edge.msg_hash = msg.hash;
                    edge.msg_lt = (msg.created_lt ? msg.created_lt.value() : 0);
                    edge.left_tx = std::nullopt;
                    edge.right_tx = tx.hash;
                    edge.type = TraceEdgeImpl::Type::ord;
                    edge.incomplete = true;
                    edge.broken = true;
                }

                // trace
                trace = std::make_shared<TraceImpl>(seqno, tx);
                trace->edges += !edge.incomplete;
                trace->pending_edges += edge.incomplete;
                if(edge.broken) {
                    trace->state = TraceImpl::State::broken;
                    ++broken_count_;
                }
                if (edge.incomplete) {
                    pending_edges_.insert_or_assign(edge.msg_hash, edge);
                    pending_edges_added.insert(edge.msg_hash);
                } else {
                    completed_edges.push_back(edge);
                }
                pending_traces_.insert_or_assign(trace->trace_id, trace);
            } else {
                // edge exists
                edge_it->second.right_tx = tx.hash;
                edge_it->second.incomplete = false;
                edge_it->second.broken = false;
                edge = edge_it->second;

                // trace
                {
                    auto trace_it = pending_traces_.find(edge.trace_id);
                    if (trace_it == pending_traces_.end()) {
                        // LOG(ERROR) << "Broken trace for in_msg of tx: " << tx.hash;
                        // create a broken trace
                        trace = std::make_shared<TraceImpl>(seqno, tx);
                        trace->edges += !edge.incomplete;
                        trace->pending_edges += edge.incomplete;
                        trace->state = TraceImpl::State::broken;
                        
                        ++broken_count_;
                        edge.trace_id = trace->trace_id;
                        pending_traces_.insert_or_assign(trace->trace_id, trace);
                    } else {
                        trace = trace_it->second;
                    }
                }

                completed_edges.push_back(edge);
                pending_edges_.erase(edge_it);
                pending_edges_added.erase(edge.msg_hash);

                trace->pending_edges -= 1;
                trace->edges += 1;
                trace->nodes += 1;
            }
        }
        updated_traces.insert(trace->trace_id);

        tx.trace_id = trace->trace_id;
        msg.trace_id = trace->trace_id;
    } else {
        trace = std::make_shared<TraceImpl>(seqno, tx);
        pending_traces_.insert_or_assign(trace->trace_id, trace);
        updated_traces.insert(trace->trace_id);

        tx.trace_id = trace->trace_id;
    }
    // update trace meta
    trace->mc_seqno_end = std::max(trace->mc_seqno_end, seqno);
    trace->end_lt = std::max(trace->end_lt, tx.lt);
    trace->end_utime = std::max(trace->end_utime, tx.now);

    // out_msgs
    for(auto & msg : tx.out_msgs) {
        auto edge = TraceEdgeImpl{trace->trace_id, msg.hash, (msg.created_lt ? msg.created_lt.value() : 0), tx.hash, std::nullopt, TraceEdgeImpl::Type::ord, true, false};
        if (!msg.destination) {
            edge.type = TraceEdgeImpl::Type::logs;
            edge.incomplete = false;
            edge.broken = false;
        }
        
        trace->pending_edges += edge.incomplete;
        trace->edges += !edge.incomplete;
        if (edge.incomplete) {
            pending_edges_.insert_or_assign(edge.msg_hash, edge);
            pending_edges_added.insert(edge.msg_hash);
        } else {
            completed_edges.push_back(edge);
        }
        msg.trace_id = trace->trace_id;
    }
}
