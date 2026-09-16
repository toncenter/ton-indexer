package v2

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	models "github.com/toncenter/ton-indexer/ton-index-go/index/models"
	"github.com/valyala/fasthttp"
)

func TestReplayWaitsForInflightWritesAndBufferedLive(t *testing.T) {
	manager := NewClientManager()
	client, s := replayClient(manager, Subscription{SubscribedAddresses: AddressSet{replayAddress(1): {}}, EventTypes: makeEventSet([]EventType{EventTransactions})})
	client.sendChan = make(chan clientMessage, 64)
	firstWrite := make(chan struct{})
	releaseFirst := make(chan struct{})
	firstFlush := make(chan struct{})
	releaseFlush := make(chan struct{})
	var writeOnce, flushOnce sync.Once
	var writes atomic.Int32
	client.SendEvent = func(msg clientMessage) error {
		if msg.flushed != nil {
			flushOnce.Do(func() {
				close(firstFlush)
				select {
				case <-releaseFlush:
				case <-client.done:
				}
			})
			close(msg.flushed)
			return nil
		}
		writeOnce.Do(func() {
			close(firstWrite)
			select {
			case <-releaseFirst:
			case <-client.done:
			}
		})
		writes.Add(1)
		return nil
	}
	defer disconnectClient(manager, client, nil)
	client.startSender(manager)
	first := replayTransaction(1)
	first.TraceExternalHashNorm = replayHash(16)
	if err := s.send(first, true); err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstWrite:
	case <-time.After(time.Second):
		t.Fatal("sender did not start")
	}
	for i := 0; i < 64; i++ {
		n := replayTransaction(1)
		n.TraceExternalHashNorm = replayHash(byte(i + 32))
		if err := s.send(n, true); err != nil {
			t.Fatal(err)
		}
	}
	finished := make(chan struct{})
	go func() { defer close(finished); s.run(false) }()
	// A momentary writer delay must not end replay while its queue is full.
	select {
	case <-finished:
		t.Fatal("replay finished before its writes completed")
	case <-time.After(20 * time.Millisecond):
	}
	client.mu.Lock()
	active := s.active
	live := replayTransaction(1)
	live.TraceExternalHashNorm = replayHash(200)
	err := client.acceptNotificationLocked(live)
	client.mu.Unlock()
	if !active || err != nil {
		t.Fatalf("live event was treated as a slow consumer while replay was still queued: active=%v err=%v", active, err)
	}
	close(releaseFirst)
	select {
	case <-firstFlush:
	case <-time.After(time.Second):
		t.Fatal("sender never reached the delivery marker")
	}
	if len(client.sendChan) != 0 {
		t.Fatal("expected an empty queue with the delivery marker still in flight")
	}
	// An empty channel is insufficient: the last transport operation is not done.
	client.mu.Lock()
	active = s.active
	latest := replayTransaction(1)
	latest.TraceExternalHashNorm = replayHash(201)
	err = client.acceptNotificationLocked(latest)
	client.mu.Unlock()
	if !active || err != nil {
		t.Fatalf("replay ended before transport confirmation: active=%v err=%v", active, err)
	}
	select {
	case <-finished:
		t.Fatal("replay completed without transport confirmation")
	default:
	}
	close(releaseFlush)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("replay did not drain buffered live events")
	}
	if writes.Load() != 67 {
		t.Fatalf("lost an event at handoff: wrote %d, want 67", writes.Load())
	}
	client.mu.Lock()
	connected, active := client.Connected, s.active
	client.mu.Unlock()
	if !connected || active {
		t.Fatalf("invalid final state: connected=%v active=%v", connected, active)
	}
}

func TestReplaySSEWaitsForHTTPWriter(t *testing.T) {
	rdb := replayRedis(t)
	manager := NewClientManager(rdb)
	addresses := make([]string, 10)
	for i := range addresses {
		a := replayAddress(byte(i + 1))
		addresses[i] = string(a)
		seedReplayAccount(t, rdb, a, "", uint64(i+1), models.FinalityStateFinalized)
	}
	payload, _ := json.Marshal(map[string]any{"addresses": addresses, "types": []string{"account_state_change"}, "replay_existing": true})
	app := fiber.New()
	request := &fasthttp.RequestCtx{}
	request.Request.Header.SetMethod("POST")
	request.Request.Header.SetContentType("application/json")
	request.Request.SetBody(payload)
	ctx := app.AcquireCtx(request)
	err := SSEHandler(manager)(ctx)
	app.ReleaseCtx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	stream := request.Response.BodyStream().(io.ReadCloser)
	defer stream.Close()
	registration := <-manager.register
	client := registration.client
	markers := make(chan chan struct{}, 4)
	send := client.SendEvent
	client.SendEvent = func(msg clientMessage) error {
		err := send(msg)
		if err == nil && msg.flushed != nil {
			markers <- msg.flushed
		}
		return err
	}
	manager.register <- registration
	go manager.Run()
	var marker chan struct{}
	select {
	case marker = <-markers:
	case <-time.After(2 * time.Second):
		t.Fatal("delivery marker did not reach the SSE queue")
	}
	// The HTTP reader has consumed nothing; the pipe contains at most four
	// writes. Ten events therefore cannot have passed the SSE writer yet.
	select {
	case <-marker:
		t.Fatal("SSE acknowledged before writing the queued events")
	default:
	}
	client.mu.Lock()
	active := client.replay.active
	queued := len(client.sendChan)
	live := &AccountStateNotification{Type: EventAccountStateChange, Finality: models.FinalityStateFinalized, Account: replayAddress(1), version: deliveryVersion{seq: 100}}
	err = client.acceptNotificationLocked(live)
	client.mu.Unlock()
	if !active || queued != 0 || err != nil {
		t.Fatalf("premature SSE handoff: active=%v queued=%d err=%v", active, queued, err)
	}
	readDone := make(chan error, 1)
	go func() {
		reader := bufio.NewReader(stream)
		for count := 0; count < 11; {
			line, err := reader.ReadString('\n')
			if err != nil {
				readDone <- err
				return
			}
			if len(line) < 6 || line[:6] != "data: " {
				continue
			}
			var event map[string]any
			if err := json.Unmarshal([]byte(line[6:]), &event); err != nil {
				readDone <- err
				return
			}
			if event["status"] == "subscribed" {
				continue
			}
			if event["type"] != "account_state_change" {
				readDone <- fmt.Errorf("internal marker leaked into SSE: %v", event)
				return
			}
			count++
		}
		readDone <- nil
	}()
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("SSE lost events or stalled at handoff")
	}
	deadline := time.Now().Add(time.Second)
	for {
		client.mu.Lock()
		active = client.replay != nil && client.replay.active
		connected := client.Connected
		client.mu.Unlock()
		if !connected {
			t.Fatal("healthy SSE client was disconnected")
		}
		if !active {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("SSE replay did not finish after its writer drained")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestReplayDeliveryWaitStopsOnDisconnectOrTimeout(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		t.Run(fmt.Sprint(timeout), func(t *testing.T) {
			manager := NewClientManager()
			client, s := replayClient(manager, Subscription{})
			if timeout {
				s.cancel()
				s.ctx, s.cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
			}
			defer s.cancel()
			finished := make(chan struct{})
			go func() { defer close(finished); s.run(false) }()
			select {
			case msg := <-client.sendChan:
				if msg.flushed == nil {
					t.Fatal("expected a delivery marker")
				}
			case <-time.After(time.Second):
				t.Fatal("delivery wait did not start")
			}
			if !timeout {
				disconnectClient(manager, client, nil)
			}
			select {
			case <-finished:
			case <-time.After(time.Second):
				t.Fatal("delivery wait did not stop")
			}
			client.mu.Lock()
			connected := client.Connected
			client.mu.Unlock()
			if connected {
				t.Fatal("undeliverable replay left the connection open")
			}
		})
	}
}

func TestReplayRequestDuringDeliveryWaitIsNotLost(t *testing.T) {
	rdb := replayRedis(t)
	account := replayAddress(2)
	seedReplayAccount(t, rdb, account, "", 200, models.FinalityStateFinalized)
	manager := NewClientManager(rdb)
	client, s := replayClient(manager, Subscription{})
	defer disconnectClient(manager, client, nil)
	finished := make(chan struct{})
	go func() { defer close(finished); s.run(false) }()
	var marker clientMessage
	select {
	case marker = <-client.sendChan:
	case <-time.After(time.Second):
		t.Fatal("delivery marker missing")
	}
	if marker.flushed == nil {
		t.Fatal("expected a delivery marker")
	}
	err := manager.updateSubscription(client, func(sub *Subscription) error {
		sub.Replace([]models.AccountAddress{account}, []EventType{EventAccountStateChange})
		sub.MinFinality = models.FinalityStateFinalized
		if manager.requestReplayLocked(client) != nil {
			t.Error("started a second replay worker")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	close(marker.flushed)
	select {
	case message := <-client.sendChan:
		if message.flushed != nil {
			t.Fatal("new replay request was skipped")
		}
		var event map[string]any
		if err := json.Unmarshal(message.data, &event); err != nil {
			t.Fatal(err)
		}
		if event["account"] != string(account) {
			t.Fatalf("wrong replay result: %v", event)
		}
	case <-finished:
		t.Fatal("replay finished without handling the new request")
	case <-time.After(time.Second):
		t.Fatal("new replay did not produce an event")
	}
	select {
	case marker = <-client.sendChan:
	case <-time.After(time.Second):
		t.Fatal("second delivery marker missing")
	}
	if marker.flushed == nil {
		t.Fatal("expected another delivery marker")
	}
	close(marker.flushed)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("replay failed to complete")
	}
}
