package store

import (
	"testing"

	cfg "github.com/MX1MR41/fluxgo/internal/config"
)

func testServerConfig(t *testing.T) *cfg.ServerConfig {
	t.Helper()
	return &cfg.ServerConfig{
		Server: cfg.ServerSettings{
			ListenAddress: "127.0.0.1:0",
			MaxFrameBytes: 1 << 20,
		},
		Log: cfg.LogSettings{
			DataDir:         t.TempDir(),
			MaxSegmentBytes: 1024,
			MaxLogBytes:     0,
			FileSync:        false,
		},
	}
}

func TestFormatParseLogNameRoundTrip(t *testing.T) {
	cases := []struct {
		topic     string
		partition uint32
	}{
		{"orders", 0},
		{"my_stream", 12}, // topics with underscores must round-trip
		{"a.b-c_d", 999},
	}
	for _, c := range cases {
		name := FormatLogName(c.topic, c.partition)
		topic, partition, err := ParseLogName(name)
		if err != nil {
			t.Fatalf("ParseLogName(%q): %v", name, err)
		}
		if topic != c.topic || partition != c.partition {
			t.Fatalf("ParseLogName(%q) = (%q, %d), want (%q, %d)",
				name, topic, partition, c.topic, c.partition)
		}
	}
}

func TestParseLogNameRejectsGarbage(t *testing.T) {
	for _, name := range []string{"", "nounderscorepartition", "topic_", "_1", "topic_x", "../x_1"} {
		if _, _, err := ParseLogName(name); err == nil {
			t.Fatalf("ParseLogName(%q) unexpectedly succeeded", name)
		}
	}
}

func TestGetOrCreateAndReload(t *testing.T) {
	config := testServerConfig(t)
	s, err := NewStore(config.Log.DataDir, config, nil)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}

	lg, err := s.GetOrCreateLog("orders", 0)
	if err != nil {
		t.Fatalf("GetOrCreateLog: %v", err)
	}
	if _, err := lg.Append([]byte("hello")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	// Same log instance on second call.
	lg2, err := s.GetOrCreateLog("orders", 0)
	if err != nil || lg2 != lg {
		t.Fatalf("GetOrCreateLog returned different instance")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen: data must survive.
	s2, err := NewStore(config.Log.DataDir, config, nil)
	if err != nil {
		t.Fatalf("NewStore (reopen): %v", err)
	}
	defer s2.Close()
	lg3 := s2.GetLog("orders", 0)
	if lg3 == nil {
		t.Fatalf("log not found after reopen")
	}
	rec, err := lg3.Read(0)
	if err != nil || string(rec) != "hello" {
		t.Fatalf("Read after reopen: rec=%q err=%v", rec, err)
	}
}

func TestGetLogDoesNotCreate(t *testing.T) {
	config := testServerConfig(t)
	s, err := NewStore(config.Log.DataDir, config, nil)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer s.Close()
	if lg := s.GetLog("nope", 0); lg != nil {
		t.Fatalf("GetLog created a log for unknown topic")
	}
}

func TestInvalidTopicNamesRejected(t *testing.T) {
	config := testServerConfig(t)
	s, err := NewStore(config.Log.DataDir, config, nil)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer s.Close()

	for _, topic := range []string{"", "../evil", "a/b", "a\\b", "..", ".", "white space", "null\x00byte"} {
		if _, err := s.GetOrCreateLog(topic, 0); err == nil {
			t.Fatalf("GetOrCreateLog(%q) unexpectedly succeeded", topic)
		}
	}
}

func TestTopicsAndPartitions(t *testing.T) {
	config := testServerConfig(t)
	s, err := NewStore(config.Log.DataDir, config, nil)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer s.Close()

	for _, tp := range [][2]any{{"orders", 0}, {"orders", 1}, {"events", 0}} {
		if _, err := s.GetOrCreateLog(tp[0].(string), uint32(tp[1].(int))); err != nil {
			t.Fatalf("GetOrCreateLog: %v", err)
		}
	}

	topics := s.Topics()
	if len(topics) != 2 || topics[0] != "events" || topics[1] != "orders" {
		t.Fatalf("Topics() = %v, want [events orders]", topics)
	}
	parts := s.Partitions("orders")
	if len(parts) != 2 || parts[0] != 0 || parts[1] != 1 {
		t.Fatalf("Partitions(orders) = %v, want [0 1]", parts)
	}
	if p := s.Partitions("unknown"); len(p) != 0 {
		t.Fatalf("Partitions(unknown) = %v, want empty", p)
	}
}

func TestClosedStore(t *testing.T) {
	config := testServerConfig(t)
	s, _ := NewStore(config.Log.DataDir, config, nil)
	s.GetOrCreateLog("orders", 0)
	s.Close()

	if lg := s.GetLog("orders", 0); lg != nil {
		t.Fatalf("GetLog after close returned a log")
	}
	if _, err := s.GetOrCreateLog("orders", 1); err == nil {
		t.Fatalf("GetOrCreateLog after close succeeded")
	}
}
