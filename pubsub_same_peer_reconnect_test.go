package pubsub

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

func newLoopbackHost(t *testing.T, opts ...libp2p.Option) host.Host {
	t.Helper()

	h, err := libp2p.New(append([]libp2p.Option{libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0")}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { h.Close() })

	return h
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}

	return cond()
}

// TestTopicStateSurvivesSameIDReconnect covers a node with a fixed identity
// being restarted while its old process is still up, as in a rolling update.
// The new process connects and announces its subscriptions over its own
// stream; then the old process exits. The old connection carried our outbound
// stream, so its death runs the dead-peer path. That path must not discard the
// subscriptions the new process announced on the inbound stream that is still
// open, otherwise the peer stays connected but absent from every topic.
func TestTopicStateSurvivesSameIDReconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const topicName = "same-id-reconnect"

	skB, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatal(err)
	}

	hA := newLoopbackHost(t)
	psA := getGossipsub(ctx, hA)

	topicA, err := psA.Join(topicName)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := topicA.Subscribe(); err != nil {
		t.Fatal(err)
	}

	startB := func() (host.Host, *Subscription) {
		h := newLoopbackHost(t, libp2p.Identity(skB))
		ps := getGossipsub(ctx, h)

		topic, err := ps.Join(topicName)
		if err != nil {
			t.Fatal(err)
		}

		sub, err := topic.Subscribe()
		if err != nil {
			t.Fatal(err)
		}

		if err := h.Connect(ctx, peer.AddrInfo{ID: hA.ID(), Addrs: hA.Addrs()}); err != nil {
			t.Fatal(err)
		}

		return h, sub
	}

	bID, err := peer.IDFromPrivateKey(skB)
	if err != nil {
		t.Fatal(err)
	}

	inTopic := func() bool { return slices.Contains(psA.ListPeers(topicName), bID) }

	hB1, _ := startB()
	if !waitUntil(t, 5*time.Second, inTopic) {
		t.Fatal("first process never joined the topic")
	}

	// Let A's outbound stream settle on the first process's connection.
	time.Sleep(time.Second)

	_, subB2 := startB()
	if !waitUntil(t, 5*time.Second, func() bool { return len(hA.Network().ConnsToPeer(hB1.ID())) > 1 }) {
		t.Fatal("second process never connected alongside the first")
	}

	// Give A time to process the second process's subscriptions.
	time.Sleep(time.Second)

	hB1.Close()

	if !waitUntil(t, 10*time.Second, func() bool {
		return len(hA.Network().ConnsToPeer(hB1.ID())) == 1
	}) {
		t.Fatal("old connection never went away")
	}

	// Allow the dead-peer path and the respawned writer to run.
	time.Sleep(2 * time.Second)

	if !slices.Contains(psA.ListPeers(topicName), hB1.ID()) {
		t.Fatalf("peer %s is still connected but missing from topic %q after its old process exited", hB1.ID(), topicName)
	}

	// And messages must still reach the new process.
	if err := topicA.Publish(ctx, []byte("after-restart")); err != nil {
		t.Fatal(err)
	}

	recvCtx, recvCancel := context.WithTimeout(ctx, 5*time.Second)
	defer recvCancel()

	msg, err := subB2.Next(recvCtx)
	if err != nil {
		t.Fatalf("message never reached the new process: %v", err)
	}
	if string(msg.Data) != "after-restart" {
		t.Fatalf("unexpected message %q", msg.Data)
	}
}
