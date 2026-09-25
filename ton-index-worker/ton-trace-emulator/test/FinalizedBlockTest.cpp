#include "BlockEmulator.h"
#include "TraceTestUtils.h"

struct McBlockEmulatorTest {
  static void check() {
    auto external = trace_test::message(901, true);
    auto outgoing = trace_test::message(902);
    auto root = trace_test::node(external, {outgoing}, FinalityState::Finalized);
    auto child = trace_test::node(outgoing, {}, FinalityState::Finalized, 110);
    TransactionInfo tx;
    tx.root = root->transaction_root;
    tx.account = root->address;
    tx.in_msg_hash = external->get_hash().bits();
    tx.hash = tx.root->get_hash().bits();
    tx.block_id = root->block_id;
    tx.mc_block_seqno = 100;
    tx.out_msgs.push_back(OutMsgInfo{outgoing->get_hash().bits(), outgoing});
    McBlockEmulator parser({}, [](ton::BlockSeqno) {}, {}, {});
    std::vector<EmuRequest> requests;
    auto partial = parser.construct_commited_trace(tx, requests, {}, 1);
    ASSERT_EQ(1u, requests.size());
    ASSERT_EQ(1u, partial->children.size());
    ASSERT_TRUE(partial->finality_state == FinalityState::Finalized);
    TransactionInfo receiving;
    receiving.root = child->transaction_root;
    receiving.account = child->address;
    receiving.in_msg_hash = outgoing->get_hash().bits();
    receiving.hash = receiving.root->get_hash().bits();
    receiving.block_id = child->block_id;
    receiving.mc_block_seqno = 100;
    parser.tx_by_in_msg_hash_.emplace(receiving.in_msg_hash, std::move(receiving));
    requests.clear();
    auto complete = parser.construct_commited_trace(tx, requests, {}, 1);
    ASSERT_TRUE(requests.empty());
    ASSERT_EQ(1u, complete->children.size());
    ASSERT_TRUE(complete->children[0] != nullptr);
    ASSERT_EQ(2, complete->transactions_count());
    ASSERT_EQ(0, complete->emulated_transactions_count());
  }
};

TEST(FinalizedBlock, ordinary_emulator_keeps_tail_requests_and_committed_children) {
  McBlockEmulatorTest::check();
}
