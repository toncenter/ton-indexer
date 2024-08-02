#include <map>
#include <functional>

#include "TraceAssembler.h"
#include "convert-utils.h"

//
// Utils
//
#define B64HASH(x) (td::base64_encode((x).as_slice()))

template<class T>
std::optional<T> to_std_optional(td::optional<T> item) {
    return (item ? std::optional<T>(item.value()) : std::optional<T>());
}

//
// Utils
//
std::string TraceEdgeImpl::str() const {
    td::StringBuilder sb;
    sb << "TraceEdge("
       << trace_id << ", "
       << msg_hash << ", " 
       << (left_tx.has_value() ? B64HASH(left_tx.value()) : "null") << ", "
       << (right_tx.has_value() ? B64HASH(right_tx.value()) : "null") << ")";
    return sb.as_cslice().str();
}

std::string TraceImpl::str() const {
    td::StringBuilder sb;
    sb << "Trace(" << trace_id 
       << ", nodes="  << nodes_ 
       << ", edges=" << edges_ 
       << ", pending_edges=" << pending_edges_ << ")";
    return sb.as_cslice().str();
}

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

TraceEdgeImpl TraceEdgeImpl::from_schema(const schema::TraceEdge& edge) {
    TraceEdgeImpl edge_impl;
    edge_impl.trace_id = edge.trace_id;
    edge_impl.msg_hash = edge.msg_hash;
    edge_impl.msg_lt = edge.msg_lt;
    edge_impl.left_tx = edge.left_tx;
    edge_impl.right_tx = edge.right_tx;
    edge_impl.type = edge.type;
    edge_impl.incomplete = edge.incomplete;
    edge_impl.broken = edge.broken;
    return std::move(edge_impl);
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
    result.pending_edges_ = pending_edges_;
    result.edges_ = edges_;
    result.nodes_ = nodes_;
    return result;
}

TraceImplPtr TraceImpl::from_schema(const schema::Trace& trace) {
    TraceImplPtr trace_impl = std::make_shared<TraceImpl>();
    trace_impl->trace_id = trace.trace_id;
    trace_impl->external_hash = trace.external_hash;
    trace_impl->mc_seqno_start = trace.mc_seqno_start;
    trace_impl->mc_seqno_end = trace.mc_seqno_end;
    trace_impl->start_lt = trace.start_lt;
    trace_impl->start_utime = trace.start_utime;
    trace_impl->end_lt = trace.end_lt;
    trace_impl->end_utime = trace.end_utime;
    trace_impl->state = trace.state;
    trace_impl->pending_edges_ = trace.pending_edges_;
    trace_impl->edges_ = trace.edges_;
    trace_impl->nodes_ = trace.nodes_;
    return std::move(trace_impl);
}

//
// TraceAssembler
//
void TraceAssembler::start_up() {
    alarm_timestamp() = td::Timestamp::in(10.0);
}

void TraceAssembler::alarm() {
    alarm_timestamp() = td::Timestamp::in(10.0);

    LOG(INFO) << " Pending traces: " << pending_traces_.size()
              << " Pending edges: " << pending_edges_.size()
              << " Broken traces: " << broken_count_;
}

void TraceAssembler::assemble(std::int32_t seqno, ParsedBlockPtr block, td::Promise<ParsedBlockPtr> promise) {
    queue_.emplace(seqno, Task{seqno, std::move(block), std::move(promise)});

    if (is_ready_) {
        process_queue();
    }
}

void TraceAssembler::update_expected_seqno(std::int32_t new_expected_seqno) { 
    expected_seqno_ = new_expected_seqno;
    LOG(INFO) << "Updating the expected senqo. New expected seqno is " << expected_seqno_;
    if (is_ready_) {
        process_queue();
    }
}

void TraceAssembler::restore_trace_assembler_state(schema::TraceAssemblerState state) {
    LOG(INFO) << "Got TraceAssemblerState with " << state.pending_traces_.size() 
              << " pending traces and " << state.pending_edges_.size() << " pending edges.";

    for (auto &trace : state.pending_traces_) {
        pending_traces_.emplace(trace.trace_id, TraceImpl::from_schema(trace));
    }
    for (auto &edge : state.pending_edges_) {
        pending_edges_.emplace(edge.msg_hash, TraceEdgeImpl::from_schema(edge));
    }
    is_ready_ = true;
    process_queue();
}

void TraceAssembler::process_queue() {
    auto it = queue_.find(expected_seqno_);
    while(it != queue_.end()) {
        process_block(it->second.seqno_, it->second.block_);
        it->second.promise_.set_result(it->second.block_);

        // block processed
        queue_.erase(it);
        expected_seqno_ += 1;
        it = queue_.find(expected_seqno_);
    }
}

void TraceAssembler::process_block(std::int32_t seqno, ParsedBlockPtr block) {
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
    std::vector<TraceEdgeImpl> edges_found_;
    std::unordered_set<td::Bits256, Bits256Hasher> updated_traces_;
    std::unordered_set<td::Bits256, Bits256Hasher> updated_edges_;
    for(auto &tx : sorted_txs) {
        process_transaction(seqno, tx.get(), edges_found_, updated_traces_, updated_edges_);
    }
    std::unordered_map<td::Bits256, schema::Trace, Bits256Hasher> trace_map;
    for (auto & trace_id : updated_traces_) {
        auto trace = pending_traces_[trace_id];
        if(trace->pending_edges_ == 0) {
            if (trace->state != TraceImpl::State::broken) {
                trace->state = TraceImpl::State::complete;
            }
            pending_traces_.erase(trace->trace_id);
        } else if (trace->pending_edges_ < 0) {
            trace->state = TraceImpl::State::broken;
        }
        trace_map[trace_id] = trace->to_schema();
    }
    for (auto & edge_hash : updated_edges_) {
        auto edge = pending_edges_.find(edge_hash);
        if (edge == pending_edges_.end()) {
            LOG(ERROR) << "No edge found!";
            std::_Exit(42);
        }
        edges_found_.push_back(edge->second);
        if (!edge->second.incomplete) {
            LOG(WARNING) << "Complete edge in pending_edges_!";
            pending_edges_.erase(edge);
        }
    }
    for (auto &edge : edges_found_) {
        // update trace
        auto &trace_schema = trace_map[edge.trace_id];
        trace_schema.edges.push_back(edge.to_schema());
    }
    for (auto &[_, trace] : trace_map) {
        block->traces_.push_back(std::move(trace));
    }
}

void TraceAssembler::process_transaction(std::int32_t seqno, schema::Transaction& tx, std::vector<TraceEdgeImpl>& edges_found_, 
        std::unordered_set<td::Bits256, Bits256Hasher>& updated_traces_, std::unordered_set<td::Bits256, Bits256Hasher>& updated_edges_) {
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
                } else if (msg.source && msg.source.value() == "-1:0000000000000000000000000000000000000000000000000000000000000000") {
                    // system
                    edge.trace_id = tx.hash;
                    edge.msg_hash = msg.hash;
                    edge.msg_lt = (msg.created_lt ? msg.created_lt.value() : 0);
                    edge.left_tx = std::nullopt;
                    edge.right_tx = tx.hash;
                    edge.type = TraceEdgeImpl::Type::sys;
                    edge.incomplete = tx.out_msgs.size() > 0;
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
                trace->edges_ += !edge.incomplete;
                trace->pending_edges_ += edge.incomplete;
                if(edge.broken) {
                    trace->state = TraceImpl::State::broken;
                    ++broken_count_;
                }
                if (edge.incomplete) {
                    pending_edges_.insert_or_assign(edge.msg_hash, edge);
                    updated_edges_.insert(edge.msg_hash);
                } else {
                    edges_found_.push_back(edge);
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
                        trace->edges_ += !edge.incomplete;
                        trace->pending_edges_ += edge.incomplete;
                        trace->state = TraceImpl::State::broken;
                        
                        ++broken_count_;
                        edge.trace_id = trace->trace_id;
                        pending_traces_.insert_or_assign(trace->trace_id, trace);
                    } else {
                        trace = trace_it->second;
                    }
                }

                edges_found_.push_back(edge);
                pending_edges_.erase(edge_it);
                if (updated_edges_.find(edge.msg_hash) != updated_edges_.end()) {
                    updated_edges_.erase(edge.msg_hash);
                }

                trace->pending_edges_ -= 1;
                trace->edges_ += 1;
                trace->nodes_ += 1;
            }
        }
        updated_traces_.insert(trace->trace_id);

        tx.trace_id = trace->trace_id;
        msg.trace_id = trace->trace_id;
    } else {
        trace = std::make_shared<TraceImpl>(seqno, tx);
        pending_traces_.insert_or_assign(trace->trace_id, trace);
        updated_traces_.insert(trace->trace_id);

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
        
        trace->pending_edges_ += edge.incomplete;
        trace->edges_ += !edge.incomplete;
        if (edge.incomplete) {
            pending_edges_.insert_or_assign(edge.msg_hash, edge);
            updated_edges_.insert(edge.msg_hash);
        } else {
            edges_found_.push_back(edge);
        }
        msg.trace_id = trace->trace_id;
    }
}
