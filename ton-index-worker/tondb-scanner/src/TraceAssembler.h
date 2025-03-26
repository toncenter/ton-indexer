#pragma once
#include "msgpack-utils.h"
#include "IndexData.h"


struct Bits256Hasher {
  std::size_t operator()(const td::Bits256& k) const {
    std::size_t seed = 0;
    for(const auto& el : k.as_array()) {
        seed ^= std::hash<td::uint8>{}(el) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    }
    return seed;
  }
};

struct TraceEdgeImpl {
    using Type = schema::TraceEdge::Type;

    td::Bits256 trace_id;
    td::Bits256 msg_hash;
    uint64_t msg_lt;
    std::optional<td::Bits256> left_tx;
    std::optional<td::Bits256> right_tx;

    Type type{Type::ord};
    bool incomplete{false};
    bool broken{false};
    
    // methods
    schema::TraceEdge to_schema() const;
    static TraceEdgeImpl from_schema(const schema::TraceEdge& edge);

    MSGPACK_DEFINE(trace_id, msg_hash, msg_lt, left_tx, right_tx, type, incomplete, broken);
};
MSGPACK_ADD_ENUM(TraceEdgeImpl::Type);

struct TraceImpl;
using TraceImplPtr = std::shared_ptr<TraceImpl>;
struct TraceImpl {
    using State = schema::Trace::State;

    td::Bits256 trace_id;

    // info
    std::optional<td::Bits256> external_hash;
    ton::BlockSeqno mc_seqno_start;
    ton::BlockSeqno mc_seqno_end;

    uint64_t start_lt;
    uint32_t start_utime;

    uint64_t end_lt{0};
    uint32_t end_utime{0};

    State state{State::pending};

    size_t pending_edges{0};
    size_t edges{0};
    size_t nodes{0};

    // methods
    TraceImpl() {}
    TraceImpl(ton::BlockSeqno seqno, const schema::Transaction &tx) :
        trace_id(tx.hash), external_hash((tx.in_msg.has_value() ? std::optional<td::Bits256>(tx.in_msg.value().hash) : std::nullopt)),
        mc_seqno_start(seqno), mc_seqno_end(seqno), start_lt(tx.lt), start_utime(tx.now), end_lt(tx.lt), end_utime(tx.now),
        state(State::pending), nodes(1) {}
    
    schema::Trace to_schema() const;
    static TraceImplPtr from_schema(const schema::Trace& trace);

    MSGPACK_DEFINE(trace_id, external_hash, mc_seqno_start, mc_seqno_end, start_lt, start_utime, end_lt, end_utime, state, pending_edges, edges, nodes);
};
MSGPACK_ADD_ENUM(TraceImpl::State);


class TraceAssembler: public td::actor::Actor {
    struct Task {
        ton::BlockSeqno seqno_;
        ParsedBlockPtr block_;
        td::Promise<ParsedBlockPtr> promise_;
    };

    std::string db_path_;
    size_t gc_distance_;
    ton::BlockSeqno expected_seqno_{0};
    std::map<ton::BlockSeqno, Task> queue_;
    size_t broken_count_{0};

    std::unordered_map<td::Bits256, TraceImplPtr, Bits256Hasher> pending_traces_;
    std::unordered_map<td::Bits256, TraceEdgeImpl, Bits256Hasher> pending_edges_;
public:
    TraceAssembler(std::string db_path, size_t gc_distance);
    
    void assemble(ton::BlockSeqno mc_seqno, ParsedBlockPtr mc_block_, td::Promise<ParsedBlockPtr> promise);
    
    td::Result<ton::BlockSeqno> restore_state(ton::BlockSeqno expected_seqno);
    void reset_state();
    void set_expected_seqno(ton::BlockSeqno expected_seqno);
    void start_up() override;
    void alarm() override;
private:
    void process_queue();
    void process_block(ton::BlockSeqno seqno, ParsedBlockPtr block);
    void process_transaction(ton::BlockSeqno seqno, schema::Transaction& tx, std::vector<TraceEdgeImpl>& edges_found_, 
        std::unordered_set<td::Bits256, Bits256Hasher>& updated_traces_, std::unordered_set<td::Bits256, Bits256Hasher>& updated_edges_);
};
