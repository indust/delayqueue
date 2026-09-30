package delayqueue

import (
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newMiniQueue(t *testing.T, cb interface{}) (*DelayQueue, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	cli := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = cli.Close() })
	return NewQueue("inspect", cli, cb, UseCustomPrefix("test:dp")), server
}

func TestScanPendingAndPayloads(t *testing.T) {
	queue, _ := newMiniQueue(t, func(string) bool { return true })
	want := map[string]string{}
	for _, payload := range []string{"a", "b", "c"} {
		msg, err := queue.SendScheduleMsgV2(payload, time.Now().Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		want[msg.ID()] = payload
	}
	var ids []string
	var cursor uint64
	for {
		batch, next, err := queue.ScanPending(cursor, 2)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, batch...)
		if cursor = next; cursor == 0 {
			break
		}
	}
	sort.Strings(ids)
	if len(ids) != len(want) {
		t.Fatalf("scan returned %v", ids)
	}
	payloads, err := queue.GetPayloads(append(ids, "missing"))
	if err != nil {
		t.Fatal(err)
	}
	if len(payloads) != len(want) {
		t.Fatalf("payloads = %v", payloads)
	}
	for id, payload := range want {
		if payloads[id] != payload {
			t.Fatalf("payload of %s = %q, want %q", id, payloads[id], payload)
		}
	}
}

func TestInterceptByPersistedID(t *testing.T) {
	var consumed atomic.Int32
	queue, _ := newMiniQueue(t, func(string) bool { consumed.Add(1); return true })
	msg, err := queue.SendScheduleMsgV2("x", time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	result, err := queue.TryIntercept(NewMessageInfo(msg.ID()))
	if err != nil || !result.Intercepted || result.State != StatePending {
		t.Fatalf("intercept result=%+v err=%v", result, err)
	}
	if ids, _, _ := queue.ScanPending(0, 10); len(ids) != 0 {
		t.Fatalf("intercepted message still pending: %v", ids)
	}
	if payloads, _ := queue.GetPayloads([]string{msg.ID()}); len(payloads) != 0 {
		t.Fatalf("intercepted payload still exists: %v", payloads)
	}
}

func TestMsgTTLOptionDoesNotChangeQueueDefault(t *testing.T) {
	queue, server := newMiniQueue(t, func(string) bool { return true })
	if _, err := queue.SendScheduleMsgV2("long", time.Now(), WithMsgTTL(24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	msg, err := queue.SendScheduleMsgV2("default", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if ttl := server.TTL(queue.genMsgKey(msg.ID())); ttl > queue.msgTTL+time.Second {
		t.Fatalf("message ttl %v, want queue default %v", ttl, queue.msgTTL)
	}
}

// A message whose payload disappeared must not leak a fetch slot, otherwise
// consuming stops forever once fetchLimit leaked slots accumulated.
func TestMissingPayloadReleasesFetchSlot(t *testing.T) {
	var consumed atomic.Int32
	queue, server := newMiniQueue(t, func(string) bool { consumed.Add(1); return true })
	queue.WithFetchLimit(1).WithFetchInterval(10 * time.Millisecond)
	lost, err := queue.SendScheduleMsgV2("lost", time.Now().Add(-time.Second))
	if err != nil {
		t.Fatal(err)
	}
	server.Del(queue.genMsgKey(lost.ID()))
	if _, err := queue.SendScheduleMsgV2("ok", time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	done := queue.StartConsume()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		processing, _ := queue.GetProcessingCount()
		if consumed.Load() == 1 && processing == 0 && atomic.LoadInt32(&queue.fetchCount) == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	queue.StopConsume()
	<-done
	if consumed.Load() != 1 {
		t.Fatalf("consumed %d messages, fetch slot leaked", consumed.Load())
	}
	if n := atomic.LoadInt32(&queue.fetchCount); n != 0 {
		t.Fatalf("fetchCount = %d", n)
	}
	if count, _ := queue.GetProcessingCount(); count != 0 {
		t.Fatalf("lost message still unacked: %d", count)
	}
}

func TestCallbackWithID(t *testing.T) {
	got := make(chan [2]string, 1)
	queue, _ := newMiniQueue(t, func(id, payload string) bool { got <- [2]string{id, payload}; return true })
	queue.WithFetchInterval(10 * time.Millisecond)
	msg, err := queue.SendScheduleMsgV2("x", time.Now().Add(-time.Second))
	if err != nil {
		t.Fatal(err)
	}
	done := queue.StartConsume()
	defer func() { queue.StopConsume(); <-done }()
	select {
	case delivery := <-got:
		if delivery != [2]string{msg.ID(), "x"} {
			t.Fatalf("delivery = %v, want id %s", delivery, msg.ID())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("not consumed")
	}
}
