package v2

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	wsclient "github.com/fasthttp/websocket"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/websocket/v2"
	"github.com/redis/go-redis/v9"
	accountModels "github.com/toncenter/ton-indexer/ton-emulate-go/models"
	models "github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttputil"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func replayRedis(t *testing.T) *redis.Client {
	t.Helper()
	binary, err := exec.LookPath("redis-server")
	if err != nil {
		t.Skip("redis-server is needed for replay integration tests")
	}
	dir := t.TempDir()
	socket := filepath.Join(dir, "redis.sock")
	logfile, err := os.Create(filepath.Join(dir, "redis.log"))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, "--port", "0", "--unixsocket", socket, "--save", "", "--appendonly", "no")
	cmd.Stdout = logfile
	cmd.Stderr = logfile
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait(); _ = logfile.Close() })
	rdb := redis.NewClient(&redis.Options{Network: "unix", Addr: socket, DialTimeout: time.Second, MaxRetries: -1})
	t.Cleanup(func() { _ = rdb.Close() })
	deadline := time.Now().Add(3 * time.Second)
	for rdb.Ping(context.Background()).Err() != nil {
		if time.Now().After(deadline) {
			t.Fatal("test Redis did not start; socket permissions may be required")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return rdb
}

func packReplay(t *testing.T, v any) string {
	t.Helper()
	b, err := msgpack.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
func replayAddress(b byte) models.AccountAddress {
	return models.AccountAddress(fmt.Sprintf("0:%064X", b))
}
func replayHash(b byte) models.HashType {
	return models.HashType(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{b}, 32)))
}
func replayBody(b byte) (string, models.HashType) {
	c := cell.BeginCell().MustStoreUInt(uint64(b), 32).EndCell()
	return base64.StdEncoding.EncodeToString(c.ToBOC()), models.HashType(base64.StdEncoding.EncodeToString(c.Hash()))
}

// A relayer root followed by an internal message to the user's wallet. Both
// inbound bodies are represented exactly as in the emulator's Redis snapshots.
func seedReplayTrace(t *testing.T, rdb *redis.Client, tail models.FinalityState) (models.HashType, models.HashType, models.HashType) {
	t.Helper()
	ctx := context.Background()
	key, root, child := replayHash(9), replayHash(1), replayHash(2)
	rootBody, rootBodyHash := replayBody(11)
	childBody, childBodyHash := replayBody(22)
	msg := func(hash models.HashType, body string, source any, dest models.AccountAddress) map[string]any {
		b, _ := base64.StdEncoding.DecodeString(hash.String())
		return map[string]any{"hash": b, "body_boc": body, "source": source, "destination": dest.String()}
	}
	rootMsg := msg(root, rootBody, nil, replayAddress(1))
	childMsg := msg(child, childBody, string(replayAddress(1)), replayAddress(2))
	node := func(id byte, account models.AccountAddress, in map[string]any, out []any, finality models.FinalityState) map[string]any {
		return map[string]any{"transaction": map[string]any{"hash": bytes.Repeat([]byte{id}, 32), "account": account.String(), "lt": uint64(id) * 100, "in_msg": in, "out_msgs": out, "orig_status": 2, "end_status": 2, "description": map[string]any{"compute_ph": []any{uint8(0), map[string]any{"reason": 0}}}}, "emulated": finality == models.FinalityStatePending, "finality": uint8(finality)}
	}
	raw := map[string]any{"root_node": root.String(), "update_seq": "7", "streaming_actions_updated": "1", root.String(): packReplay(t, node(3, replayAddress(1), rootMsg, []any{childMsg}, models.FinalityStateFinalized)), child.String(): packReplay(t, node(4, replayAddress(2), childMsg, []any{}, tail)), "actions": packReplay(t, []any{map[string]any{"action_id": string(replayHash(7)), "type": "ton_transfer", "accounts": []string{string(replayAddress(1)), string(replayAddress(2))}, "source": string(replayAddress(1)), "destination": string(replayAddress(2)), "value": "100", "success": true, "finality": uint8(tail), "tx_hashes": []string{string(replayHash(3)), string(replayHash(4))}, "ton_transfer_data": map[string]any{}}})}
	if err := rdb.HSet(ctx, key.String(), raw).Err(); err != nil {
		t.Fatal(err)
	}
	for i, address := range []models.AccountAddress{replayAddress(1), replayAddress(2)} {
		member := key.String() + ":" + []models.HashType{root, child}[i].String()
		if err := rdb.ZAdd(ctx, address.String(), redis.Z{Score: 100, Member: member}).Err(); err != nil {
			t.Fatal(err)
		}
		if err := rdb.ZAdd(ctx, "_aai:"+address.String(), redis.Z{Score: 100, Member: key.String() + ":" + string(replayHash(7))}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	return key, rootBodyHash, childBodyHash
}

func replayClient(manager *ClientManager, s Subscription) (*Client, *replaySession) {
	c := &Client{ID: "replay-test", Connected: true, Subscription: s, done: make(chan struct{}), sendChan: make(chan clientMessage, 1024)}
	c.TracesForPotentialInvalidation = make(map[models.HashType]bool)
	session := manager.requestReplayLocked(c)
	addTestClient(manager, c)
	return c, session
}
func replayEvents(t *testing.T, c *Client) []map[string]any {
	t.Helper()
	var messages []clientMessage
	for len(c.sendChan) > 0 {
		messages = append(messages, <-c.sendChan)
	}
	return decodeReplayEvents(t, messages)
}

func decodeReplayEvents(t *testing.T, messages []clientMessage) []map[string]any {
	t.Helper()
	var events []map[string]any
	for _, message := range messages {
		var event map[string]any
		if err := json.Unmarshal(message.data, &event); err != nil {
			t.Fatal(err)
		}
		events = append(events, event)
	}
	return events
}

func pumpReplay(s *replaySession, existing bool) []clientMessage {
	done := make(chan struct{})
	go func() { defer close(done); s.run(existing) }()
	var messages []clientMessage
	for {
		select {
		case message := <-s.client.sendChan:
			if message.flushed != nil {
				close(message.flushed)
			} else {
				messages = append(messages, message)
			}
		case <-done:
			return messages
		}
	}
}

func runReplay(t *testing.T, s *replaySession, existing bool) []map[string]any {
	t.Helper()
	return decodeReplayEvents(t, pumpReplay(s, existing))
}

func TestReplayBodyHashAndTraceFinality(t *testing.T) {
	rdb := replayRedis(t)
	key, external, internal := seedReplayTrace(t, rdb, models.FinalityStateFinalized)
	for _, tc := range []struct {
		name    string
		address models.AccountAddress
		body    models.HashType
		want    models.HashType
	}{
		{"external", replayAddress(1), external, replayHash(3)}, {"gasless internal", replayAddress(2), internal, replayHash(4)}, {"outgoing does not match", replayAddress(1), internal, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager := NewClientManager(rdb)
			c, s := replayClient(manager, Subscription{SubscribedAddresses: AddressSet{tc.address: {}}, EventTypes: makeEventSet([]EventType{EventTransactions}), MinFinality: models.FinalityStateFinalized, MsgBodyHash: &tc.body})
			defer s.cancel()
			events := runReplay(t, s, true)
			if tc.want == "" {
				if len(events) != 0 {
					t.Fatalf("outbound matched: %v", events)
				}
				return
			}
			if len(events) != 1 {
				t.Fatalf("want one latest event, got %v", events)
			}
			txs := events[0]["transactions"].([]any)
			if len(txs) != 1 || txs[0].(map[string]any)["hash"] != tc.want.String() {
				t.Fatalf("wrong transaction: %v", txs)
			}
			// The same current snapshot routed through the live path has identical JSON.
			manager.broadcast = make(chan notificationDelivery, 4)
			ProcessTransactionHint(context.Background(), rdb, transactionHint{TraceKey: key, UpdateSeq: 7, UpdateFinality: models.FinalityStateFinalized, TraceFinality: models.FinalityStateFinalized, Accounts: []models.AccountAddress{tc.address}}, manager, "test")
			select {
			case delivery := <-manager.broadcast:
				event := delivery.notification.AdjustForClient(c)
				b, _ := json.Marshal(event)
				var live map[string]any
				_ = json.Unmarshal(b, &live)
				if !reflect.DeepEqual(events[0], live) {
					t.Fatalf("live/replay differ: %s", b)
				}
			default:
				t.Fatal("live event missing")
			}
		})
	}
	seedReplayTrace(t, rdb, models.FinalityStatePending)
	for _, minimum := range []models.FinalityState{models.FinalityStatePending, models.FinalityStateConfirmed, models.FinalityStateFinalized} {
		manager := NewClientManager(rdb)
		_, s := replayClient(manager, Subscription{SubscribedAddresses: AddressSet{replayAddress(1): {}}, EventTypes: makeEventSet([]EventType{EventTransactions}), MinFinality: minimum, MsgBodyHash: &external})
		events := runReplay(t, s, true)
		if minimum != models.FinalityStatePending {
			if len(events) > 0 {
				t.Fatal("a finalized matching transaction bypassed whole-trace min_finality")
			}
			continue
		}
		if len(events) != 1 || events[0]["finality"] != "pending" {
			t.Fatalf("expected pending trace: %v", events)
		}
	}
}

func TestReplayActionsTracesAndExpiredMembers(t *testing.T) {
	rdb := replayRedis(t)
	key, _, _ := seedReplayTrace(t, rdb, models.FinalityStateFinalized)
	manager := NewClientManager(rdb)
	sub := Subscription{SubscribedAddresses: AddressSet{replayAddress(1): {}, replayAddress(2): {}}, SubscribedTraces: TraceSet{key: {}}, EventTypes: makeEventSet([]EventType{EventActions, EventTrace}), SupportedActionTypes: []string{"ton_transfer"}, MinFinality: models.FinalityStateFinalized}
	_, s := replayClient(manager, sub)
	events := runReplay(t, s, true)
	if len(events) != 2 || events[0]["type"] != "actions" || events[1]["type"] != "trace" {
		t.Fatalf("duplicates or missing events: %v", events)
	}
	// An old retained blob must not be emitted after classification failed.
	if err := rdb.HSet(context.Background(), key.String(), "streaming_actions_updated", "0").Err(); err != nil {
		t.Fatal(err)
	}
	_, s = replayClient(manager, sub)
	events = runReplay(t, s, true)
	if len(events) != 1 || events[0]["type"] != "trace" || len(events[0]["actions"].([]any)) != 0 {
		t.Fatalf("stale actions leaked: %v", events)
	}
	if err := rdb.Del(context.Background(), key.String()).Err(); err != nil {
		t.Fatal(err)
	}
	_, s = replayClient(manager, sub)
	if events := runReplay(t, s, true); len(events) != 0 {
		t.Fatalf("expired trace emitted: %v", events)
	}
}

func TestReplayPurePendingMatchesLive(t *testing.T) {
	rdb := replayRedis(t)
	key, body, _ := seedReplayTrace(t, rdb, models.FinalityStatePending)
	root := string(replayHash(1))
	raw, err := rdb.HGet(context.Background(), key.String(), root).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	var node map[string]any
	if err := msgpack.Unmarshal(raw, &node); err != nil {
		t.Fatal(err)
	}
	node["emulated"], node["finality"] = true, uint8(0)
	if err := rdb.HSet(context.Background(), key.String(), root, packReplay(t, node)).Err(); err != nil {
		t.Fatal(err)
	}
	manager := NewClientManager(rdb)
	c, s := replayClient(manager, Subscription{SubscribedAddresses: AddressSet{replayAddress(1): {}}, EventTypes: makeEventSet([]EventType{EventTransactions}), MsgBodyHash: &body})
	events := runReplay(t, s, true)
	if len(events) != 1 {
		t.Fatalf("pending replay missing: %v", events)
	}
	manager.broadcast = make(chan notificationDelivery, 1)
	ProcessTransactionHint(context.Background(), rdb, transactionHint{TraceKey: key, UpdateSeq: 7, UpdateFinality: models.FinalityStatePending, TraceFinality: models.FinalityStatePending, Accounts: []models.AccountAddress{replayAddress(1)}}, manager, "test")
	select {
	case delivery := <-manager.broadcast:
		data, _ := json.Marshal(delivery.notification.AdjustForClient(c))
		var live map[string]any
		if err := json.Unmarshal(data, &live); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(events[0], live) {
			t.Fatal("pending replay differs from live message decoding")
		}
	default:
		t.Fatal("live pending event missing")
	}
}

func seedReplayAccount(t *testing.T, rdb *redis.Client, account, owner models.AccountAddress, lt uint64, f models.FinalityState) {
	t.Helper()
	state := accountModels.AccountState{AccountStatus: "active", Balance: lt, LastTransLt: &lt}
	interfaces := ""
	if owner != "" {
		interfaces = packReplay(t, map[string]any{"interfaces": []any{[]any{uint8(0), map[string]any{"address": account.String(), "owner": owner.String(), "jetton": string(replayAddress(99)), "balance": "123"}}}})
	}
	key := accountStateRedisKey(accountStateHint{Account: account, Finality: f})
	if err := rdb.HSet(context.Background(), key, map[string]any{"lt": lt, "state": packReplay(t, state), "interfaces": interfaces}).Err(); err != nil {
		t.Fatal(err)
	}
}

func TestReplayAccountStatesAndJettonWallet(t *testing.T) {
	rdb := replayRedis(t)
	owner, wallet := replayAddress(10), replayAddress(20)
	for _, f := range []models.FinalityState{models.FinalityStateConfirmed, models.FinalityStateFinalized} {
		seedReplayAccount(t, rdb, owner, "", 100, f)
		seedReplayAccount(t, rdb, wallet, owner, 100, f)
	}
	sub := Subscription{SubscribedAddresses: AddressSet{owner: {}}, EventTypes: makeEventSet([]EventType{EventAccountStateChange, EventJettonsChange}), MinFinality: models.FinalityStateConfirmed}
	manager := NewClientManager(rdb)
	_, s := replayClient(manager, sub)
	events := runReplay(t, s, true)
	if len(events) != 1 || events[0]["type"] != "account_state_change" || events[0]["account"] != string(owner) {
		t.Fatalf("owner subscription must replay only the owner's account state: %v", events)
	}
	sub.SubscribedAddresses = AddressSet{wallet: {}}
	_, s = replayClient(manager, sub)
	events = runReplay(t, s, true)
	if len(events) != 2 {
		t.Fatalf("equal LT must return finalized account and jetton states: %v", events)
	}
	for _, event := range events {
		if event["finality"] != "finalized" {
			t.Fatal(event)
		}
	}
	seedReplayAccount(t, rdb, wallet, owner, 101, models.FinalityStateConfirmed)
	_, s = replayClient(manager, sub)
	events = runReplay(t, s, true)
	if len(events) != 4 {
		t.Fatalf("newer confirmed account or jetton state missing: %v", events)
	}
	// A subscribed account whose state has expired contributes no events.
	sub.SubscribedAddresses[replayAddress(22)] = struct{}{}
	_, s = replayClient(manager, sub)
	if len(runReplay(t, s, true)) != 4 {
		t.Fatal("missing account state changed the replay results")
	}
}

func replayTransaction(seq uint64) *TransactionsNotification {
	return &TransactionsNotification{version: deliveryVersion{seq: seq}, Type: EventTransactions, TraceExternalHashNorm: replayHash(9), Finality: models.FinalityStatePending, Transactions: []models.Transaction{{Account: replayAddress(1), Hash: replayHash(3)}}}
}
func TestReplayLiveBoundaryAndInvalidation(t *testing.T) {
	sub := Subscription{SubscribedAddresses: AddressSet{replayAddress(1): {}}, EventTypes: makeEventSet([]EventType{EventTransactions})}
	manager := NewClientManager()
	c, s := replayClient(manager, sub)

	if err := s.send(replayTransaction(7), true); err != nil {
		t.Fatal(err)
	}
	for _, n := range []Notification{replayTransaction(7), replayTransaction(8)} {
		c.mu.Lock()
		err := c.acceptNotificationLocked(n)
		c.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	}
	if events := runReplay(t, s, false); len(events) != 2 {
		t.Fatalf("same-version duplicate or newer pending update lost: %v", events)
	}
	c, s = replayClient(manager, sub)

	_ = s.send(replayTransaction(7), true)
	c.mu.Lock()
	_ = c.acceptNotificationLocked(replayTransaction(8))
	_ = c.acceptNotificationLocked(&TraceInvalidatedNotification{Type: EventTraceInvalidated, TraceExternalHashNorm: replayHash(9)})
	c.mu.Unlock()
	if err := s.send(replayTransaction(8), true); err != nil {
		t.Fatal(err)
	}
	events := runReplay(t, s, false)
	if len(events) != 3 || events[0]["type"] != "transactions" || events[1]["type"] != "transactions" || events[2]["type"] != "trace_invalidated" {
		t.Fatalf("snapshots must be followed by the buffered invalidation: %v", events)
	}
}

func TestSubscriptionChangeKeepsReplayAndQueuedEvents(t *testing.T) {
	manager := NewClientManager()
	c, s := replayClient(manager, Subscription{SubscribedAddresses: AddressSet{replayAddress(1): {}}, EventTypes: makeEventSet([]EventType{EventTransactions})})
	defer s.cancel()
	c.sendChan = make(chan clientMessage, 1)
	c.sendChan <- clientMessage{data: []byte("already queued")}
	sent := make(chan error, 1)
	go func() { sent <- s.send(replayTransaction(7), true) }()
	deadline := time.Now().Add(time.Second)
	for {
		c.mu.Lock()
		prepared := len(s.snapshots) > 0
		c.mu.Unlock()
		if prepared {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("replay did not prepare its event")
		}
		time.Sleep(time.Millisecond)
	}
	if err := manager.updateSubscription(c, func(sub *Subscription) error {
		sub.Replace([]models.AccountAddress{replayAddress(2)}, []EventType{EventTransactions})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if s.ctx.Err() != nil || c.replay != s {
		t.Fatal("subscription change cancelled replay")
	}
	<-c.sendChan
	select {
	case err := <-sent:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("prepared replay event did not finish")
	}
	events := replayEvents(t, c)
	if len(events) != 1 {
		t.Fatal("prepared event was discarded")
	}
	// Subsequent event preparation uses the current filters, even in this replay.
	if err := s.send(replayTransaction(8), true); err != nil {
		t.Fatal(err)
	}
	if len(c.sendChan) != 0 {
		t.Fatal("replay kept the previous address filter")
	}
	current := replayTransaction(9)
	current.Transactions[0].Account = replayAddress(2)
	if err := s.send(current, true); err != nil {
		t.Fatal(err)
	}
	if len(replayEvents(t, c)) != 1 {
		t.Fatal("current filter was not applied")
	}
}

func TestDisconnectStillCancelsBlockedReplay(t *testing.T) {
	manager := NewClientManager()
	c, s := replayClient(manager, Subscription{SubscribedAddresses: AddressSet{replayAddress(1): {}}, EventTypes: makeEventSet([]EventType{EventTransactions})})
	c.sendChan = make(chan clientMessage, 1)
	c.sendChan <- clientMessage{}
	stopped := make(chan error, 1)
	go func() { stopped <- s.send(replayTransaction(7), true) }()
	disconnectClient(manager, c, nil)
	select {
	case err := <-stopped:
		if err == nil {
			t.Fatal("disconnected replay still sent an event")
		}
	case <-time.After(time.Second):
		t.Fatal("disconnected replay did not stop")
	}
}

func TestReplayInvalidationAllowsSequenceRestart(t *testing.T) {
	for _, duringReplay := range []bool{true, false} {
		t.Run(fmt.Sprintf("during_replay=%v", duringReplay), func(t *testing.T) {
			manager := NewClientManager()
			c, s := replayClient(manager, Subscription{SubscribedAddresses: AddressSet{replayAddress(1): {}}, EventTypes: makeEventSet([]EventType{EventTransactions})})
			defer s.cancel()
			if err := s.send(replayTransaction(7), true); err != nil {
				t.Fatal(err)
			}
			if duringReplay {
				replayEvents(t, c)
			} else {
				runReplay(t, s, false)
			}

			// Invalidation precedes the recreated trace in live delivery order.
			recreated := replayTransaction(1)
			recreated.Transactions[0].Hash = replayHash(4)
			for _, n := range []Notification{
				&TraceInvalidatedNotification{Type: EventTraceInvalidated, TraceExternalHashNorm: replayHash(9)},
				recreated,
				recreated, // The reset must still deduplicate subsequent live events.
			} {
				c.mu.Lock()
				err := c.acceptNotificationLocked(n)
				c.mu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
			}
			var events []map[string]any
			if duringReplay {
				events = runReplay(t, s, false)
			} else {
				events = replayEvents(t, c)
			}
			if len(events) != 2 || events[0]["type"] != "trace_invalidated" || events[1]["type"] != "transactions" {
				t.Fatalf("expected invalidation followed by one recreated trace: %v", events)
			}
			txs := events[1]["transactions"].([]any)
			if len(txs) != 1 || txs[0].(map[string]any)["hash"] != string(replayHash(4)) {
				t.Fatalf("recreated transaction was lost: %v", txs)
			}
		})
	}
}

func TestReplayBufferOverflowIsExplicit(t *testing.T) {
	c, s := replayClient(NewClientManager(), Subscription{})
	defer s.cancel()
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; i < maxReplayBufferEvents; i++ {
		if err := c.acceptNotificationLocked(replayTransaction(uint64(i + 1))); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.acceptNotificationLocked(replayTransaction(9999)); !errors.Is(err, errSlowConsumer) {
		t.Fatalf("overflow did not report a slow consumer: %v", err)
	}
	if len(s.pending) != maxReplayBufferEvents {
		t.Fatal("overflow changed the buffered event count")
	}
}

func TestMessageBodyHashValidation(t *testing.T) {
	hash := strings.Repeat("ab", 32)
	for _, tc := range []struct {
		addresses []models.AccountAddress
		types     []EventType
		hash      string
		valid     bool
	}{
		{[]models.AccountAddress{replayAddress(1)}, []EventType{EventTransactions}, hash, true},
		{nil, []EventType{EventTransactions}, hash, false},
		{[]models.AccountAddress{replayAddress(1)}, []EventType{EventActions}, hash, false},
		{[]models.AccountAddress{replayAddress(1)}, []EventType{EventTransactions}, "no", false},
	} {
		_, err := validateMessageBodyHash(&tc.hash, tc.addresses, tc.types)
		if (err == nil) != tc.valid {
			t.Fatalf("unexpected validation: %v", err)
		}
	}
}

func TestReplaySSEBackpressure(t *testing.T) {
	rdb := replayRedis(t)
	manager := NewClientManager(rdb)
	go manager.Run()
	addresses := make([]string, 150)
	for i := range addresses {
		a := replayAddress(byte(i + 1))
		addresses[i] = a.String()
		seedReplayAccount(t, rdb, a, "", uint64(i+1), models.FinalityStateFinalized)
	}
	body, _ := json.Marshal(map[string]any{"types": []string{"account_state_change"}, "addresses": addresses, "replay_existing": true})
	app := fiber.New()
	request := &fasthttp.RequestCtx{}
	request.Request.Header.SetMethod("POST")
	request.Request.Header.SetContentType("application/json")
	request.Request.SetBody(body)
	ctx := app.AcquireCtx(request)
	err := SSEHandler(manager)(ctx)
	app.ReleaseCtx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	stream := request.Response.BodyStream().(io.ReadCloser)
	defer stream.Close()
	result := make(chan error, 1)
	go func() {
		reader := bufio.NewReader(stream)
		count := 0
		for count < 150 {
			line, err := reader.ReadString('\n')
			if err != nil {
				result <- err
				return
			}
			if strings.HasPrefix(line, "data: ") {
				var event map[string]any
				if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event); err != nil {
					result <- err
					return
				}
				if event["type"] == "account_state_change" {
					count++
					time.Sleep(time.Millisecond)
				}
			}
		}
		result <- nil
	}()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("large SSE replay stalled or lost events")
	}
}

func TestReplayWebSocketSubscribeAndReplace(t *testing.T) {
	rdb := replayRedis(t)
	_, _, body := seedReplayTrace(t, rdb, models.FinalityStateFinalized)
	manager := NewClientManager(rdb)
	go manager.Run()
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	app.Get("/ws", websocket.New(WebSocketHandler(manager)))
	listener := fasthttputil.NewInmemoryListener()
	done := make(chan error, 1)
	go func() { done <- app.Listener(listener) }()
	defer func() { _ = listener.Close(); _ = app.ShutdownWithTimeout(time.Second); <-done }()
	dialer := wsclient.Dialer{NetDial: func(_, _ string) (net.Conn, error) { return listener.Dial() }}
	conn, _, err := dialer.Dial("ws://localhost/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	read := func() map[string]any {
		t.Helper()
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		var n map[string]any
		if err := json.Unmarshal(raw, &n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	for i := 0; i < 2; i++ {
		request := map[string]any{"operation": "subscribe", "types": []string{"transactions"}, "addresses": []string{string(replayAddress(2))}, "msg_body_hash": body.String(), "replay_existing": true}
		if err := conn.WriteJSON(request); err != nil {
			t.Fatal(err)
		}
		if event := read(); event["status"] != "subscribed" {
			t.Fatalf("event preceded ack: %v", event)
		}
		if event := read(); event["type"] != "transactions" {
			t.Fatalf("replay missing: %v", event)
		}
	}
	// A new subscription resets the body filter; replay defaults to false.
	if err := conn.WriteJSON(map[string]any{"operation": "subscribe", "types": []string{"transactions"}, "addresses": []string{string(replayAddress(1))}}); err != nil {
		t.Fatal(err)
	}
	if event := read(); event["status"] != "subscribed" {
		t.Fatal(event)
	}
	_ = conn.WriteJSON(map[string]any{"operation": "ping"})
	if event := read(); event["status"] != "pong" {
		t.Fatalf("unexpected implicit replay: %v", event)
	}
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	for _, c := range manager.clients {
		c.mu.Lock()
		if c.Subscription.MsgBodyHash != nil {
			t.Error("body filter survived subscription replacement")
		}
		c.mu.Unlock()
	}
}

func TestReplayHistoryIsLimitedToInitialEntities(t *testing.T) {
	manager := NewClientManager()
	c, s := replayClient(manager, Subscription{SubscribedAddresses: AddressSet{replayAddress(1): {}}, EventTypes: makeEventSet([]EventType{EventTransactions})})
	if err := s.send(replayTransaction(7), true); err != nil {
		t.Fatal(err)
	}
	runReplay(t, s, false)
	if s.active || len(s.pending) != 0 {
		t.Fatal("temporary replay state was retained")
	}
	c.mu.Lock()
	for i := 0; i < 1000; i++ {
		n := replayTransaction(1)
		n.TraceExternalHashNorm = models.HashType(fmt.Sprintf("new-live-trace-%d", i))
		if err := c.acceptNotificationLocked(n); err != nil {
			t.Fatal(err)
		}
		if err := c.acceptNotificationLocked(&TraceInvalidatedNotification{Type: EventTraceInvalidated, TraceExternalHashNorm: n.TraceExternalHashNorm}); err != nil {
			t.Fatal(err)
		}
		for len(c.sendChan) > 0 {
			<-c.sendChan
		}
	}
	if len(s.snapshots) != 1 {
		t.Fatal("live events grew the replay history")
	}
	_ = c.acceptNotificationLocked(replayTransaction(6))
	if len(c.sendChan) != 0 {
		t.Fatal("late pre-replay version was delivered")
	}
	_ = c.acceptNotificationLocked(replayTransaction(8))
	c.mu.Unlock()
	if len(replayEvents(t, c)) != 1 {
		t.Fatal("new version of an initial trace was suppressed")
	}
}

func TestReplayRemembersSnapshotExcludedByBodyFilter(t *testing.T) {
	wanted, other := replayHash(1), replayHash(2)
	manager := NewClientManager()
	c, s := replayClient(manager, Subscription{SubscribedAddresses: AddressSet{replayAddress(1): {}}, EventTypes: makeEventSet([]EventType{EventTransactions}), MsgBodyHash: &wanted})
	snapshot := replayTransaction(8)
	snapshot.Transactions[0].InMsg = &models.Message{BodyHash: &other}
	if err := s.send(snapshot, true); err != nil {
		t.Fatal(err)
	}
	runReplay(t, s, false)
	late := replayTransaction(7)
	late.Transactions[0].InMsg = &models.Message{BodyHash: &wanted}
	c.mu.Lock()
	err := c.acceptNotificationLocked(late)
	c.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if len(replayEvents(t, c)) != 0 {
		t.Fatal("a delayed older body matched after its newer snapshot was replayed")
	}
}

func TestLiveOverflowReportsSlowConsumer(t *testing.T) {
	c := &Client{ID: "slow-live", Connected: true, Subscription: Subscription{SubscribedAddresses: AddressSet{replayAddress(1): {}}, EventTypes: makeEventSet([]EventType{EventTransactions})}, sendChan: make(chan clientMessage, 1), TracesForPotentialInvalidation: make(map[models.HashType]bool)}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.acceptNotificationLocked(replayTransaction(1)); err != nil {
		t.Fatal(err)
	}
	if err := c.acceptNotificationLocked(replayTransaction(2)); !errors.Is(err, errSlowConsumer) {
		t.Fatalf("overflow did not report a slow consumer: %v", err)
	}
	if len(c.sendChan) != 1 {
		t.Fatal("queue grew past its bound")
	}
}

func TestSSELiveOverflowDisconnectsAndReleasesQuota(t *testing.T) {
	manager := NewClientManager()
	app := fiber.New()
	request := &fasthttp.RequestCtx{}
	request.Request.Header.SetMethod("POST")
	request.Request.Header.SetContentType("application/json")
	request.Request.Header.Set("X-Limiting-Key", "sse-slow")
	request.Request.Header.Set("X-Max-Parallel-Connections", "1")
	request.Request.Header.Set("X-Max-Subscribed-Addr", "1")
	request.Request.SetBodyString(`{"types":["account_state_change"],"addresses":["` + string(replayAddress(1)) + `"]}`)
	ctx := app.AcquireCtx(request)
	err := SSEHandler(manager)(ctx)
	app.ReleaseCtx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if request.Response.StatusCode() != 200 {
		t.Fatalf("subscription failed: %s", request.Response.Body())
	}
	stream := request.Response.BodyStream().(io.ReadCloser)
	defer stream.Close()
	registration := <-manager.register
	client := registration.client
	registration.ready = make(chan struct{})
	manager.register <- registration
	go manager.Run()
	select {
	case <-registration.ready:
	case <-time.After(time.Second):
		t.Fatal("SSE client registration stalled")
	}
	// Leave the HTTP reader idle and fill the shared queue through live delivery.
	for i := 0; i < clientSendQueueSize+16; i++ {
		manager.sendNotification(&AccountStateNotification{
			Type: EventAccountStateChange, Finality: models.FinalityStateFinalized, Account: replayAddress(1),
		}, clientSet{client.ID: {}})
	}
	select {
	case <-client.done:
	case <-time.After(time.Second):
		t.Fatal("SSE client did not disconnect on overflow")
	}
	if err := manager.rateLimiter.RegisterConnection("sse-slow", "replacement", RateLimitConfig{MaxParallelConnections: 1}); err != nil {
		t.Fatalf("slow SSE still occupies its quota: %v", err)
	}
}

type replayReadBarrier struct {
	key       string
	nextKey   string
	started   chan struct{}
	release   chan struct{}
	once      sync.Once
	nextReads atomic.Int32
}

func (h *replayReadBarrier) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *replayReadBarrier) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *replayReadBarrier) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "hgetall" {
			if cmd.Args()[1] == h.nextKey {
				h.nextReads.Add(1)
			}
			if cmd.Args()[1] == h.key {
				h.once.Do(func() {
					close(h.started)
					select {
					case <-h.release:
					case <-ctx.Done():
					}
				})
			}
		}
		return next(ctx, cmd)
	}
}

func TestReplayBusyRequestsReadCurrentSubscriptionOnceMore(t *testing.T) {
	rdb := replayRedis(t)
	first, second := replayAddress(1), replayAddress(2)
	seedReplayAccount(t, rdb, first, "", 100, models.FinalityStateFinalized)
	seedReplayAccount(t, rdb, second, "", 200, models.FinalityStateFinalized)
	hook := &replayReadBarrier{key: "account_finalized:" + string(first), nextKey: "account_finalized:" + string(second), started: make(chan struct{}), release: make(chan struct{})}
	rdb.AddHook(hook)
	manager := NewClientManager(rdb)
	c, s := replayClient(manager, Subscription{SubscribedAddresses: AddressSet{first: {}}, EventTypes: makeEventSet([]EventType{EventAccountStateChange}), MinFinality: models.FinalityStateFinalized})
	defer s.cancel()
	done := make(chan []clientMessage, 1)
	go func() { done <- pumpReplay(s, true) }()
	select {
	case <-hook.started:
	case <-time.After(time.Second):
		t.Fatal("replay did not start reading the first address")
	}
	for i := 0; i < 3; i++ {
		err := manager.updateSubscription(c, func(sub *Subscription) error {
			sub.Replace([]models.AccountAddress{second}, []EventType{EventAccountStateChange})
			if another := manager.requestReplayLocked(c); another != nil {
				t.Error("started a parallel replay worker")
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if s.ctx.Err() != nil {
		t.Fatal("subscription update cancelled the active read")
	}
	close(hook.release)
	var messages []clientMessage
	select {
	case messages = <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("queued replay did not complete")
	}
	events := decodeReplayEvents(t, messages)
	if len(events) != 1 || events[0]["account"] != string(second) {
		t.Fatalf("replay used saved filters or missed the latest subscription: %v", events)
	}
	if hook.nextReads.Load() != 1 {
		t.Fatalf("busy requests were not combined: %d reads", hook.nextReads.Load())
	}
}

func TestReplayCanRepeatSameSnapshotWithCurrentFilters(t *testing.T) {
	manager := NewClientManager()
	c, s := replayClient(manager, Subscription{SubscribedAddresses: AddressSet{replayAddress(1): {}}, EventTypes: makeEventSet([]EventType{EventTransactions})})
	defer s.cancel()
	n := replayTransaction(7)
	n.Transactions = append(n.Transactions, models.Transaction{Account: replayAddress(2), Hash: replayHash(4)})
	if err := s.send(n, true); err != nil {
		t.Fatal(err)
	}
	if err := manager.updateSubscription(c, func(sub *Subscription) error {
		sub.Replace([]models.AccountAddress{replayAddress(2)}, []EventType{EventTransactions})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.send(n, true); err != nil {
		t.Fatal(err)
	}
	events := replayEvents(t, c)
	if len(events) != 2 {
		t.Fatal("new replay was suppressed because the data version did not change")
	}
	for i, event := range events {
		txs := event["transactions"].([]any)
		if len(txs) != 1 || txs[0].(map[string]any)["account"] != string(replayAddress(byte(i+1))) {
			t.Fatalf("wrong filter applied: %v", event)
		}
	}
}
