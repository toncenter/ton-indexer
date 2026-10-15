package reader

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"google.golang.org/protobuf/proto"
)

func testHash(b byte) models.HashType {
	return models.HashType(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{b}, 32)))
}

func ptr[T any](v T) *T { return &v }

const basechainShard = models.ShardId(-9223372036854775808) // 0x8000000000000000

func testRound() ([]models.Block, []models.Block, []models.Transaction) {
	mc := models.Block{
		Workchain: -1, Shard: basechainShard, Seqno: 100,
		RootHash: testHash(1), FileHash: testHash(2), RandSeed: testHash(3), CreatedBy: testHash(4),
		GenUtime: 1760000000, StartLt: 61000000000000, EndLt: 61000000000004,
		ValidatorListHashShort: -1, TxCount: 1,
		MasterchainBlockRef: models.BlockId{Workchain: -1, Shard: basechainShard, Seqno: 100},
		PrevBlocks:          []models.BlockId{{Workchain: -1, Shard: basechainShard, Seqno: 99}},
	}
	shardHigh := models.Block{Workchain: 0, Shard: basechainShard, Seqno: 501, RootHash: testHash(5), FileHash: testHash(6), RandSeed: testHash(7), CreatedBy: testHash(8), TxCount: 0}
	shardLow := models.Block{Workchain: 0, Shard: 0x4000000000000000, Seqno: 500, RootHash: testHash(9), FileHash: testHash(10), RandSeed: testHash(11), CreatedBy: testHash(12), TxCount: 0}

	tx := models.Transaction{
		Account: "0:83DFD552E63729B472FCBCC8C45EBCC6691702558B68EC7527E1BA403A0F31A8",
		Hash:    testHash(20), Lt: 61000000000001, Now: 1760000000, McSeqno: 100,
		TraceId: ptr(testHash(21)), PrevTransHash: testHash(22), PrevTransLt: 60000000000001,
		OrigStatus: "active", EndStatus: "active", TotalFees: 1234,
		TotalFeesExtraCurrencies: map[string]string{"239": "100000000000000000000000"},
		Descr: models.TransactionDescr{
			Type:      "ord",
			Aborted:   ptr(false),
			ComputePh: &models.ComputePhase{Success: ptr(true), GasUsed: ptr(int64(3308)), ExitCode: ptr(int32(0))},
			Action:    &models.ActionPhase{TotActions: ptr(int32(1)), TotMsgSize: &models.MsgSize{Cells: ptr(int64(1)), Bits: ptr(int64(705))}},
		},
		BlockRef: models.BlockId{Workchain: -1, Shard: basechainShard, Seqno: 100},
		InMsg: &models.Message{
			MsgHash: testHash(30), Destination: ptr(models.AccountAddress("0:83DFD552E63729B472FCBCC8C45EBCC6691702558B68EC7527E1BA403A0F31A8")),
			Opcode: ptr(models.OpcodeType(0x0f8a7ea5)), BodyHash: ptr(testHash(31)),
			MessageContent: &models.MessageContent{Hash: ptr(testHash(31)), Body: ptr(models.BytesType("te6cckEBAQEAAgAAAEysuc0="))},
			MsgHashNorm:    ptr(testHash(32)),
		},
		OutMsgs: []*models.Message{{
			MsgHash: testHash(40), Value: ptr(int64(1000000000)), CreatedLt: ptr(uint64(61000000000002)),
			BodyHash:       ptr(testHash(41)),
			MessageContent: &models.MessageContent{Hash: ptr(testHash(41)), Body: ptr(models.BytesType("te6cckEBAQEAAgAAAEysuc0="))},
		}},
		AccountStateAfter: &models.AccountState{Hash: testHash(50), Balance: ptr("5000000000"), AccountStatus: ptr("active"), CodeHash: ptr(testHash(51))},
	}
	return []models.Block{shardHigh, mc, shardLow}, []models.Block{shardHigh, shardLow}, []models.Transaction{tx}
}

func TestBuildBlock(t *testing.T) {
	blocks, shards, txs := testRound()
	blk, err := buildBlock(100, blocks, shards, txs)
	if err != nil {
		t.Fatal(err)
	}

	mc := blk.MasterchainBlock
	if mc.Seqno != 100 || mc.Shard != 0x8000000000000000 || mc.ValidatorListHashShort != 0xffffffff {
		t.Errorf("unexpected masterchain header %v", mc)
	}
	if len(blk.ShardBlocks) != 2 || blk.ShardBlocks[0].Seqno != 500 || blk.ShardBlocks[1].Seqno != 501 {
		t.Errorf("shard blocks are not ordered by shard: %v", blk.ShardBlocks)
	}
	if len(blk.Shards) != 2 || blk.Shards[0].Shard != 0x4000000000000000 {
		t.Errorf("unexpected shards %v", blk.Shards)
	}

	tx := blk.Transactions[0]
	if tx.Lt != 61000000000001 || tx.TotalFeesExtraCurrencies[239] != "100000000000000000000000" {
		t.Errorf("unexpected transaction %v", tx)
	}
	if !bytes.Equal(tx.TraceId, bytes.Repeat([]byte{21}, 32)) {
		t.Errorf("trace_id = %x", tx.TraceId)
	}
	if got := tx.InMsg.GetOpcode(); got != 0x0f8a7ea5 {
		t.Errorf("opcode = %#x", got)
	}
	if tx.InMsg.Source != nil || len(tx.InMsg.MessageContent.Body) == 0 {
		t.Errorf("unexpected in_msg %v", tx.InMsg)
	}
	if tx.OutMsgs[0].GetValue() != 1000000000 || tx.Description.ComputePh.GetGasUsed() != 3308 {
		t.Errorf("unexpected out_msgs or description")
	}
	if tx.AccountStateBefore != nil || tx.AccountStateAfter.GetBalance() != 5000000000 {
		t.Errorf("unexpected account states")
	}
}

func TestBuildBlockRejectsIncompleteRound(t *testing.T) {
	blocks, shards, txs := testRound()
	if _, err := buildBlock(100, blocks, shards, nil); err == nil || !strings.Contains(err.Error(), "read 0 transactions") {
		t.Errorf("missing transactions: err = %v", err)
	}

	txs[0].OutMsgs[0].MessageContent = nil
	if _, err := buildBlock(100, blocks, shards, txs); err == nil || !strings.Contains(err.Error(), "body") {
		t.Errorf("missing body: err = %v", err)
	}

	if _, err := buildBlock(101, blocks, shards, nil); err == nil {
		t.Error("wrong masterchain seqno is accepted")
	}
}

func TestBstreamBlockIsDeterministic(t *testing.T) {
	blocks, shards, txs := testRound()
	var payloads [][]byte
	for range 3 {
		blk, err := buildBlock(100, blocks, shards, txs)
		if err != nil {
			t.Fatal(err)
		}
		bblk, err := toBstreamBlock(blk, "aa")
		if err != nil {
			t.Fatal(err)
		}
		if bblk.Number != 100 || bblk.LibNum != 100 || bblk.ParentNum != 99 || bblk.ParentId != "aa" {
			t.Errorf("unexpected metadata %v", bblk)
		}
		if bblk.Id != hex.EncodeToString(bytes.Repeat([]byte{1}, 32)) {
			t.Errorf("id = %s", bblk.Id)
		}
		if bblk.Timestamp.AsTime().Unix() != 1760000000 {
			t.Errorf("timestamp = %v", bblk.Timestamp.AsTime())
		}
		payloads = append(payloads, bblk.Payload.Value)
	}
	for _, p := range payloads[1:] {
		if !bytes.Equal(p, payloads[0]) {
			t.Fatal("the same round serialized to different bytes")
		}
	}

	blk, _ := buildBlock(100, blocks, shards, txs)
	decoded, _ := buildBlock(100, blocks, shards, txs)
	if err := proto.Unmarshal(payloads[0], decoded); err != nil || !proto.Equal(blk, decoded) {
		t.Errorf("payload does not round-trip: %v", err)
	}
}
