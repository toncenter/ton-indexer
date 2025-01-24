package index

import (
	"context"
	"encoding/base64"
	"fmt"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/gofiber/fiber/v2/log"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton/jetton"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"math"
	"math/big"
	"slices"
	"time"
)

var timings = make([]time.Duration, 0)

type ShortMessage struct {
	Hash            *HashType
	TransactionHash *HashType
	Source          *AccountAddress
	Destination     *AccountAddress
	Opcode          *OpcodeType
	Value           *int64
	Direction       *string
	FwdFee          *uint64
	Body            *string
	Transaction     *ShortTransaction
}

type ShortTransaction struct {
	Hash      HashType
	Account   AccountAddress
	TotalFees *int64
	Lt        int64

	OutMsgs []*ShortMessage
	InMsg   *ShortMessage
}

type Node struct {
	Msg            *ShortMessage
	TxHash         HashType
	NextNodes      []*Node
	PreviousNode   *Node
	BalanceChanges *BalanceChanges
}

func (n *Node) AddNextNode(node *Node) {
	n.NextNodes = append(n.NextNodes, node)
	node.SetPreviousNode(n)
}

func (n *Node) SetPreviousNode(node *Node) {
	n.PreviousNode = node
}

type BalanceChanges struct {
	Value   map[AccountAddress]int64
	Fees    map[AccountAddress]int64
	Jettons map[AccountAddress]map[AccountAddress]*big.Int
}

func (v *BalanceChanges) add_value(addr AccountAddress, value int64) {
	if _, ok := v.Value[addr]; !ok {
		v.Value[addr] = 0
	}
	v.Value[addr] += value
}

func (v *BalanceChanges) add_fees(addr AccountAddress, value int64) {
	if _, ok := v.Fees[addr]; !ok {
		v.Fees[addr] = 0
	}
	v.Fees[addr] += value
}

func (v *BalanceChanges) merge(other_changes *BalanceChanges) {
	for addr, value := range other_changes.Value {
		if _, ok := v.Value[addr]; !ok {
			v.Value[addr] = 0
		}
		v.Value[addr] += value
	}
	for addr, value := range other_changes.Fees {
		if _, ok := v.Fees[addr]; !ok {
			v.Fees[addr] = 0
		}
		v.Fees[addr] += value
	}
	for addr, jettons := range other_changes.Jettons {
		if _, ok := v.Jettons[addr]; !ok {
			v.Jettons[addr] = make(map[AccountAddress]*big.Int)
		}
		for jetton, value := range jettons {
			if _, ok := v.Jettons[addr][jetton]; !ok {
				newInt := big.NewInt(0)
				v.Jettons[addr][jetton] = newInt
			}
			v.Jettons[addr][jetton].Add(v.Jettons[addr][jetton], value)
		}
	}
}

func (v *BalanceChanges) add_jetton(addr AccountAddress, jetton AccountAddress, value *big.Int, negate bool) {
	if _, ok := v.Jettons[addr]; !ok {
		v.Jettons[addr] = make(map[AccountAddress]*big.Int)
	}
	if _, ok := v.Jettons[addr][jetton]; !ok {
		v.Jettons[addr][jetton] = big.NewInt(0)
	}
	if negate {
		v.Jettons[addr][jetton].Sub(v.Jettons[addr][jetton], value)
	} else {
		v.Jettons[addr][jetton].Add(v.Jettons[addr][jetton], value)
	}
}

func (v *BalanceChanges) get_summarized_balance_changes() map[AccountAddress]int64 {
	balances := make(map[AccountAddress]int64)
	for addr, value := range v.Value {
		balances[addr] = value
	}
	for addr, value := range v.Fees {
		if _, ok := balances[addr]; !ok {
			balances[addr] = 0
		}
		balances[addr] -= value
	}
	return balances
}

func compare(t1 *ShortTransaction, t2 *ShortTransaction) int {
	if t1.Lt < t2.Lt {
		return 1
	}
	if t1.Lt > t2.Lt {
		return -1
	}
	return 0
}

func CalculateBalanceChanges(traceId HashType, conn *pgxpool.Conn) (*BalanceChanges, map[HashType]*BalanceChanges, error) {
	tx_map, err := fetchTrace(traceId, conn)
	if err != nil {
		return nil, nil, err
	}
	if len(tx_map) == 0 {
		return nil, nil, fmt.Errorf("trace not found")
	}
	msg_tx_map := make(map[HashType]HashType)
	nodes := make(map[HashType]*Node)
	txs := make([]*ShortTransaction, 0)
	for _, tx := range tx_map {
		txs = append(txs, tx)
	}
	slices.SortFunc(txs, compare)

	var maxInt int32 = math.MaxInt32
	actionsQuery, err := buildActionsQuery(ActionRequest{TraceId: []HashType{traceId}}, UtimeRequest{}, LtRequest{}, LimitRequest{
		Limit: &maxInt,
	}, RequestSettings{
		MaxLimit: int(maxInt),
	})
	if err != nil {
		return nil, nil, err
	}
	actions, err := queryRawActionsImpl(actionsQuery, conn, RequestSettings{
		Timeout: time.Second,
	})
	for _, tx := range txs {
		if _, ok := nodes[tx.Hash]; !ok {
			nodes[tx.Hash] = &Node{
				TxHash: tx.Hash,
				Msg:    tx.InMsg,
			}
			if tx.InMsg != nil {
				msg_tx_map[*tx.InMsg.Hash] = tx.Hash
			}
			for _, msg := range tx.OutMsgs {
				if msg.Destination == nil {
					nodes[tx.Hash].AddNextNode(&Node{
						TxHash: tx.Hash,
						Msg:    msg,
					})
					msg_tx_map[*msg.Hash] = tx.Hash
				} else {
					nodes[tx.Hash].AddNextNode(nodes[msg_tx_map[*msg.Hash]])
				}
			}
		}
	}
	root := nodes[txs[len(txs)-1].Hash]

	queue := make([]*Node, 0)
	queue = append(queue, root)
	jetton_wallet_candidates := mapset.NewSet[AccountAddress]()
	trace_balance_changes := &BalanceChanges{
		Value:   make(map[AccountAddress]int64),
		Fees:    make(map[AccountAddress]int64),
		Jettons: make(map[AccountAddress]map[AccountAddress]*big.Int),
	}
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		if node.Msg != nil {
			if node.BalanceChanges == nil {
				node.BalanceChanges = &BalanceChanges{
					Value:   make(map[AccountAddress]int64),
					Fees:    make(map[AccountAddress]int64),
					Jettons: make(map[AccountAddress]map[AccountAddress]*big.Int),
				}
			}
			if node.PreviousNode != nil && node.PreviousNode.Msg != nil {
				for _, outMsg := range node.PreviousNode.Msg.Transaction.OutMsgs {
					if *outMsg.Hash == *node.Msg.Hash {
						node.BalanceChanges.add_fees(*node.Msg.Source, int64(*outMsg.FwdFee))
						break
					}
				}
			}
			if *node.Msg.Direction == "in" {
				node.BalanceChanges.add_fees(*node.Msg.Destination, *node.Msg.Transaction.TotalFees)

				//node.BalanceChanges.add_fees(*node.Msg.Source, int64(*node.Msg.FwdFee))
				if node.Msg.Value != nil {
					node.BalanceChanges.add_value(*node.Msg.Source, -*node.Msg.Value)
					node.BalanceChanges.add_value(*node.Msg.Destination, *node.Msg.Value)

				}
				for _, outMsg := range node.Msg.Transaction.OutMsgs {
					if outMsg.FwdFee != nil && outMsg.Destination == nil {
						node.BalanceChanges.add_fees(*node.Msg.Destination, int64(*outMsg.FwdFee))
					}
				}
			}
			if node.Msg.Opcode != nil && *node.Msg.Opcode == 0x0f8a7ea5 {
				jetton_wallet_candidates.Add(*node.Msg.Destination)
			}
			if node.Msg.Opcode != nil && *node.Msg.Opcode == 0x178d4519 {
				jetton_wallet_candidates.Add(*node.Msg.Source)
				jetton_wallet_candidates.Add(*node.Msg.Destination)
			}
		}

		for _, next := range node.NextNodes {
			queue = append(queue, next)
		}
	}

	wallet_infos, err := checkJettonWallets(jetton_wallet_candidates, conn)
	if err != nil {
		return nil, nil, err
	}
	owner_wallets := map[AccountAddress]map[AccountAddress]walletInfo{}
	for w := range wallet_infos.Iter() {
		if _, ok := owner_wallets[w.jettonMaster]; !ok {
			owner_wallets[w.jettonMaster] = make(map[AccountAddress]walletInfo)
		}
		owner_wallets[w.jettonMaster][w.owner] = w
	}

	queue = append(queue, root)
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		if node.Msg != nil {
			if node.Msg.Opcode != nil && *node.Msg.Opcode == 0x0f8a7ea5 && node.Msg.Body != nil {
				body, err := base64.StdEncoding.DecodeString(*node.Msg.Body)
				if err != nil {
					log.Error(err)
					continue
				}
				c, err := cell.FromBOC(body)
				if err != nil {
					log.Error(err)
					continue
				}
				var transfer jetton.TransferPayload
				err = tlb.LoadFromCell(&transfer, c.BeginParse())
				destination_raw := transfer.Destination.String()
				var destination AccountAddress
				addr_loc := AccountAddressConverter(destination_raw)
				if addr_loc.IsValid() {
					if v, ok := addr_loc.Interface().(AccountAddress); ok {
						destination = v
					}
				}
				source := *node.Msg.Source
				var jetton_master AccountAddress
				source_jetton_wallet := *node.Msg.Destination
				for v := range wallet_infos.Iter() {
					if v.address == source_jetton_wallet {
						jetton_master = v.jettonMaster
						break
					}
				}
				sender := owner_wallets[jetton_master][source]
				receiver := owner_wallets[jetton_master][destination]
				node.BalanceChanges.add_jetton(sender.owner, jetton_master, transfer.Amount.Nano(), true)
				node.BalanceChanges.add_jetton(receiver.owner, jetton_master, transfer.Amount.Nano(), false)
				if err != nil {
					log.Error(err)
					continue
				}
			}
		}
		if node.BalanceChanges != nil {
			trace_balance_changes.merge(node.BalanceChanges)
		}

		for _, next := range node.NextNodes {
			queue = append(queue, next)
		}
	}

	action_balance_changes := make(map[HashType]*BalanceChanges)
	for _, action := range actions {
		if action.Type == "contract_deploy" {
			continue
		}
		transactions := make([]*ShortTransaction, 0)
		for _, tx := range action.TxHashes {
			transactions = append(transactions, tx_map[tx])
		}
		slices.SortFunc(transactions, compare)
		earliest_tx := transactions[len(transactions)-1]
		if earliest_tx.InMsg.Source != nil {
			transactions = transactions[0 : len(transactions)-1]
		}
		balance_changes := &BalanceChanges{
			Value:   make(map[AccountAddress]int64),
			Fees:    make(map[AccountAddress]int64),
			Jettons: make(map[AccountAddress]map[AccountAddress]*big.Int),
		}
		for _, node := range nodes {
			for _, tx := range transactions {
				if node.TxHash == tx.Hash {
					balance_changes.merge(node.BalanceChanges)
				}
			}
		}
		action_balance_changes[action.ActionId] = balance_changes
	}
	return trace_balance_changes, action_balance_changes, nil
}

type walletInfo struct {
	address      AccountAddress
	owner        AccountAddress
	jettonMaster AccountAddress
}

func (v *walletInfo) Equals(other *walletInfo) bool {
	return v.address == other.address && v.owner == other.owner && v.jettonMaster == other.jettonMaster
}

func checkJettonWallets(jetton_wallet_candidates mapset.Set[AccountAddress], conn *pgxpool.Conn) (mapset.Set[walletInfo], error) {
	var query = `
		SELECT address, owner, jetton
		FROM jetton_wallets
		WHERE address = ANY($1)`
	rows, err := conn.Query(context.Background(), query, jetton_wallet_candidates.ToSlice())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	jetton_wallets := mapset.NewSet[walletInfo]()
	for rows.Next() {
		var addr AccountAddress
		var owner AccountAddress
		var jetton_master AccountAddress
		if err := rows.Scan(&addr, &owner, &jetton_master); err != nil {
			return nil, err
		}
		jetton_wallets.Add(walletInfo{
			address:      addr,
			owner:        owner,
			jettonMaster: jetton_master,
		})
	}
	return jetton_wallets, nil
}

func fetchTrace(trace_id HashType, conn *pgxpool.Conn) (map[HashType]*ShortTransaction, error) {
	var query = `
		SELECT T.trace_id, Tx.hash, Tx.account, Tx.lt, Tx.total_fees, M.msg_hash, M.tx_hash,
		M.source, M.destination, M.opcode, M.value, M.direction, M.fwd_fee, MC.body
		FROM traces T
		JOIN transactions Tx ON T.trace_id = Tx.trace_id
		LEFT JOIN messages M ON Tx.hash = M.tx_hash
		LEFT JOIN message_contents MC ON M.body_hash = MC.hash
		WHERE T.trace_id = $1`
	rows, err := conn.Query(context.Background(), query, trace_id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var (
		txs  = make(map[HashType]*ShortTransaction)
		msgs = make(map[HashType]*ShortMessage)
	)
	for rows.Next() {
		var (
			t = ShortTransaction{}
			m = ShortMessage{}
		)
		if err := rows.Scan(&trace_id, &t.Hash, &t.Account, &t.Lt, &t.TotalFees, &m.Hash, &m.TransactionHash,
			&m.Source, &m.Destination, &m.Opcode, &m.Value, &m.Direction, &m.FwdFee, &m.Body); err != nil {
			return nil, err
		}
		if _, ok := txs[t.Hash]; !ok {
			txs[t.Hash] = &t
		}
		if m.Hash != nil {
			tx := txs[*m.TransactionHash]
			msgs[*m.Hash] = &m
			if *m.Direction == "out" {
				tx.OutMsgs = append(tx.OutMsgs, &m)
			} else {
				tx.InMsg = &m
			}
			m.Transaction = tx
		}
	}
	return txs, nil
}
