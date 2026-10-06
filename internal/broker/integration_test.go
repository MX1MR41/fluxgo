package broker_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/MX1MR41/fluxgo/internal/broker"
	cfg "github.com/MX1MR41/fluxgo/internal/config"
	"github.com/MX1MR41/fluxgo/internal/offset"
	proto "github.com/MX1MR41/fluxgo/internal/protocol"
	"github.com/MX1MR41/fluxgo/internal/store"
)

// testServer starts a broker on an ephemeral port backed by a temp data dir.
func testServer(t *testing.T) (addr string, stop func()) {
	t.Helper()
	dataDir := t.TempDir()
	config := &cfg.ServerConfig{
		Server: cfg.ServerSettings{
			ListenAddress: "127.0.0.1:0",
			ReadTimeout:   5 * time.Second,
			WriteTimeout:  5 * time.Second,
			MaxFrameBytes: 1 << 20,
		},
		Log: cfg.LogSettings{
			DataDir:         dataDir,
			MaxSegmentBytes: 512,
			MaxLogBytes:     0,
			FileSync:        false,
		},
	}
	logStore, err := store.NewStore(dataDir, config, nil)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	om, err := offset.NewManager(dataDir, nil)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	srv := broker.NewServer(config, logStore, om, nil)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Start(ctx) }()

	// Wait until the listener is up.
	deadline := time.Now().Add(5 * time.Second)
	for srv.Addr() == nil {
		if time.Now().After(deadline) {
			t.Fatalf("server did not start listening")
		}
		time.Sleep(5 * time.Millisecond)
	}

	return srv.Addr().String(), func() {
		cancel()
		<-errCh
		logStore.Close()
	}
}

func dial(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("SetDeadline: %v", err)
	}
	return conn
}

func produce(t *testing.T, conn net.Conn, topic string, partition uint32, msg string) uint64 {
	t.Helper()
	req := proto.NewEncoder(64)
	req.String(topic)
	req.Uint32(partition)
	req.Bytes([]byte(msg))
	if err := proto.WriteFrame(conn, proto.CmdProduce, req.Payload()); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	code, resp, err := proto.ReadFrame(conn, proto.DefaultMaxFrameBytes)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if code != proto.ErrCodeNone {
		t.Fatalf("produce %q: error 0x%02X: %s", msg, code, resp)
	}
	return proto.NewDecoder(resp).Uint64()
}

func fetch(t *testing.T, conn net.Conn, topic string, partition uint32, off uint64, maxRecords uint32) (byte, []byte) {
	t.Helper()
	req := proto.NewEncoder(64)
	req.String(topic)
	req.Uint32(partition)
	req.Uint64(off)
	req.Uint32(maxRecords)
	req.Uint32(1 << 20)
	if err := proto.WriteFrame(conn, proto.CmdFetch, req.Payload()); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	code, resp, err := proto.ReadFrame(conn, proto.DefaultMaxFrameBytes)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	return code, resp
}

func TestProduceFetchEndToEnd(t *testing.T) {
	addr, stop := testServer(t)
	defer stop()
	conn := dial(t, addr)
	defer conn.Close()

	// Produce enough messages to force segment rollover (segment is 512B).
	const n = 60
	for i := 0; i < n; i++ {
		if off := produce(t, conn, "orders", 0, fmt.Sprintf("order-%02d", i)); off != uint64(i) {
			t.Fatalf("produce %d: got offset %d", i, off)
		}
	}

	code, resp := fetch(t, conn, "orders", 0, 0, n)
	if code != proto.ErrCodeNone {
		t.Fatalf("fetch: error 0x%02X: %s", code, resp)
	}
	d := proto.NewDecoder(resp)
	high := d.Uint64()
	start := d.Uint64()
	count := int(d.Uint32())
	if high != n || start != 0 || count != n {
		t.Fatalf("fetch header: high=%d start=%d count=%d, want %d/0/%d", high, start, count, n, n)
	}
	for i := 0; i < count; i++ {
		rec := d.Bytes()
		if want := fmt.Sprintf("order-%02d", i); string(rec) != want {
			t.Fatalf("record %d: got %q, want %q", i, rec, want)
		}
	}
	if err := d.Err(); err != nil {
		t.Fatalf("decode fetch response: %v", err)
	}

	// Fetch from the middle.
	code, resp = fetch(t, conn, "orders", 0, 42, 4)
	if code != proto.ErrCodeNone {
		t.Fatalf("fetch middle: error 0x%02X", code)
	}
	d = proto.NewDecoder(resp)
	d.Uint64()
	if start := d.Uint64(); start != 42 {
		t.Fatalf("middle fetch start = %d, want 42", start)
	}
	if count := int(d.Uint32()); count != 4 {
		t.Fatalf("middle fetch count = %d, want 4", count)
	}
}

func TestFetchPastEndAndUnknownTopic(t *testing.T) {
	addr, stop := testServer(t)
	defer stop()
	conn := dial(t, addr)
	defer conn.Close()

	produce(t, conn, "t", 0, "x")

	code, resp := fetch(t, conn, "t", 0, 100, 1)
	if code != proto.ErrCodeOffsetPastEnd {
		t.Fatalf("fetch past end: code = 0x%02X, want ErrCodeOffsetPastEnd", code)
	}
	if high := proto.NewDecoder(resp).Uint64(); high != 1 {
		t.Fatalf("past-end payload high watermark = %d, want 1", high)
	}

	code, _ = fetch(t, conn, "ghost", 0, 0, 1)
	if code != proto.ErrCodeTopicNotFound {
		t.Fatalf("fetch unknown topic: code = 0x%02X, want ErrCodeTopicNotFound", code)
	}
}

func TestOffsetCommitFetchOverWire(t *testing.T) {
	addr, stop := testServer(t)
	defer stop()
	conn := dial(t, addr)
	defer conn.Close()

	// Fetch before any commit.
	req := proto.NewEncoder(32)
	req.String("group-a")
	req.String("orders")
	req.Uint32(0)
	if err := proto.WriteFrame(conn, proto.CmdFetchOffset, req.Payload()); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	code, _, err := proto.ReadFrame(conn, proto.DefaultMaxFrameBytes)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if code != proto.ErrCodeOffsetNotFound {
		t.Fatalf("fetch-offset before commit: code = 0x%02X, want ErrCodeOffsetNotFound", code)
	}

	// Commit and read back.
	req = proto.NewEncoder(32)
	req.String("group-a")
	req.String("orders")
	req.Uint32(0)
	req.Uint64(17)
	if err := proto.WriteFrame(conn, proto.CmdCommitOffset, req.Payload()); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	code, resp, _ := proto.ReadFrame(conn, proto.DefaultMaxFrameBytes)
	if code != proto.ErrCodeNone {
		t.Fatalf("commit: code = 0x%02X: %s", code, resp)
	}

	req = proto.NewEncoder(32)
	req.String("group-a")
	req.String("orders")
	req.Uint32(0)
	proto.WriteFrame(conn, proto.CmdFetchOffset, req.Payload())
	code, resp, _ = proto.ReadFrame(conn, proto.DefaultMaxFrameBytes)
	if code != proto.ErrCodeNone {
		t.Fatalf("fetch-offset: code = 0x%02X", code)
	}
	if off := proto.NewDecoder(resp).Uint64(); off != 17 {
		t.Fatalf("fetch-offset = %d, want 17", off)
	}
}

func TestListTopics(t *testing.T) {
	addr, stop := testServer(t)
	defer stop()
	conn := dial(t, addr)
	defer conn.Close()

	produce(t, conn, "beta", 0, "1")
	produce(t, conn, "alpha", 0, "1")
	produce(t, conn, "alpha", 1, "2")

	if err := proto.WriteFrame(conn, proto.CmdListTopics, nil); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	code, resp, err := proto.ReadFrame(conn, proto.DefaultMaxFrameBytes)
	if err != nil || code != proto.ErrCodeNone {
		t.Fatalf("list-topics: code=0x%02X err=%v", code, err)
	}
	d := proto.NewDecoder(resp)
	n := int(d.Uint16())
	if n != 2 {
		t.Fatalf("topic count = %d, want 2", n)
	}
	first, second := d.String(), d.String()
	if first != "alpha" || second != "beta" {
		t.Fatalf("topics = [%s %s], want [alpha beta]", first, second)
	}
}

func TestMalformedRequestsDoNotCrashServer(t *testing.T) {
	addr, stop := testServer(t)
	defer stop()
	conn := dial(t, addr)
	defer conn.Close()

	// Truncated payloads for every command, plus an unknown command.
	for _, frame := range [][]byte{
		{0x01, 0x00},            // produce: claims 0-length topic, then nothing
		{0x02, 0x00, 0x05, 't'}, // fetch: claims 5-char topic, provides 1
		{0x03, 0xFF, 0xFF},      // commit: absurd group length
		{0x04},                  // fetch-offset: empty
		{0x7F, 'x'},             // unknown command
	} {
		if err := proto.WriteFrame(conn, frame[0], frame[1:]); err != nil {
			t.Fatalf("WriteFrame: %v", err)
		}
		code, _, err := proto.ReadFrame(conn, proto.DefaultMaxFrameBytes)
		if err != nil {
			t.Fatalf("server dropped connection on malformed frame %v: %v", frame, err)
		}
		if code == proto.ErrCodeNone {
			t.Fatalf("malformed frame %v returned success", frame)
		}
	}

	// The server must still be healthy afterwards.
	if off := produce(t, conn, "alive", 0, "still here"); off != 0 {
		t.Fatalf("produce after malformed frames: off=%d", off)
	}
}

func TestInvalidTopicRejected(t *testing.T) {
	addr, stop := testServer(t)
	defer stop()
	conn := dial(t, addr)
	defer conn.Close()

	req := proto.NewEncoder(32)
	req.String("../escape")
	req.Uint32(0)
	req.Bytes([]byte("boom"))
	if err := proto.WriteFrame(conn, proto.CmdProduce, req.Payload()); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	code, _, err := proto.ReadFrame(conn, proto.DefaultMaxFrameBytes)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if code != proto.ErrCodeInvalidTopic {
		t.Fatalf("path traversal produce: code = 0x%02X, want ErrCodeInvalidTopic", code)
	}
}
