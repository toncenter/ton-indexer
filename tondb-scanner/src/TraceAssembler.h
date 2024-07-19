#pragma once
#include "IndexData.h"
#include <queue>


// 
// Utils
//
struct Bits256Hasher {
  std::size_t operator()(const td::Bits256& k) const {
    std::size_t seed = 0;
    for(const auto& el : k.as_array()) {
        seed ^= std::hash<td::uint8>{}(el) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    }
    return seed;
  }
};

//
// Trace datatypes
//
struct TraceEdgeImpl {
    using Type = schema::TraceEdge::Type;

    td::Bits256 trace_id;
    td::Bits256 msg_hash;
    std::uint64_t msg_lt;
    std::optional<td::Bits256> left_tx;
    std::optional<td::Bits256> right_tx;

    Type type{Type::ord};
    bool incomplete{false};
    bool broken{false};
    
    // methods
    std::string str() const;
    schema::TraceEdge to_schema() const;
    static TraceEdgeImpl from_schema(const schema::TraceEdge& edge);
};

struct TraceImpl;
using TraceImplPtr = std::shared_ptr<TraceImpl>;
struct TraceImpl {
    using State = schema::Trace::State;

    td::Bits256 trace_id;

    // info
    std::optional<td::Bits256> external_hash;
    std::int32_t mc_seqno_start;
    std::int32_t mc_seqno_end;

    std::uint64_t start_lt;
    std::uint32_t start_utime;

    std::uint64_t end_lt{0};
    std::uint32_t end_utime{0};

    State state{State::pending};

    std::int64_t pending_edges_{0};
    std::int64_t edges_{0};
    std::int64_t nodes_{0};

    // methods
    TraceImpl() {}
    TraceImpl(std::int32_t seqno, const schema::Transaction &tx) :
        trace_id(tx.hash), external_hash((tx.in_msg.has_value() ? std::optional<td::Bits256>(tx.in_msg.value().hash) : std::nullopt)),
        mc_seqno_start(seqno), mc_seqno_end(seqno), start_lt(tx.lt), start_utime(tx.now), end_lt(tx.lt), end_utime(tx.now),
        state(State::pending), nodes_(1) {}
    
    std::string str() const;
    schema::Trace to_schema() const;
    static TraceImplPtr from_schema(const schema::Trace& trace);
};

//
// TraceAssembler
//
class TraceAssembler: public td::actor::Actor {
    struct Task {
        std::int32_t seqno_;
        ParsedBlockPtr block_;
        td::Promise<ParsedBlockPtr> promise_;
    };

    // assembler state and queue
    std::int32_t expected_seqno_;
    std::map<std::int32_t, Task> queue_;
    bool is_ready_{false};
    std::int64_t broken_count_{0};

    // trace assembly
    std::unordered_map<td::Bits256, TraceImplPtr, Bits256Hasher> pending_traces_;
    std::unordered_map<td::Bits256, TraceEdgeImpl, Bits256Hasher> pending_edges_;
public:
    TraceAssembler(std::int32_t expected_seqno, bool is_ready = false) : 
        expected_seqno_(expected_seqno), is_ready_(is_ready) {}
    
    void assemble(int mc_seqno, ParsedBlockPtr mc_block_, td::Promise<ParsedBlockPtr> promise);
    void update_expected_seqno(std::int32_t new_expected_seqno);
    void restore_trace_assembler_state(schema::TraceAssemblerState state);
    void process_queue();

    void start_up() override;
    void alarm() override;
private:
    void process_block(std::int32_t seqno, ParsedBlockPtr block);
    void process_transaction(std::int32_t seqno, schema::Transaction& tx, std::vector<TraceEdgeImpl>& edges_found_, 
        std::unordered_set<td::Bits256, Bits256Hasher>& updated_traces_, std::unordered_set<td::Bits256, Bits256Hasher>& updated_edges_);
};
