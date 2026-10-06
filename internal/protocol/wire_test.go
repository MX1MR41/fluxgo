package protocol

import (
	"bytes"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	payload := []byte("hello world")
	if err := WriteFrame(&buf, CmdProduce, payload); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	code, got, err := ReadFrame(&buf, DefaultMaxFrameBytes)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if code != CmdProduce {
		t.Fatalf("code = %d, want %d", code, CmdProduce)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload = %q, want %q", got, payload)
	}
}

func TestFrameEmptyPayload(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteFrame(&buf, CmdListTopics, nil); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	code, got, err := ReadFrame(&buf, DefaultMaxFrameBytes)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if code != CmdListTopics || len(got) != 0 {
		t.Fatalf("got code=%d payload=%q", code, got)
	}
}

func TestReadFrameRejectsOversized(t *testing.T) {
	var buf bytes.Buffer
	big := make([]byte, 1024)
	if err := WriteFrame(&buf, CmdProduce, big); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	if _, _, err := ReadFrame(&buf, 100); err == nil {
		t.Fatalf("ReadFrame accepted oversized frame")
	}
}

func TestReadFrameRejectsTruncated(t *testing.T) {
	var buf bytes.Buffer
	WriteFrame(&buf, CmdProduce, []byte("payload"))
	truncated := buf.Bytes()[:6] // length prefix + 2 bytes only
	if _, _, err := ReadFrame(bytes.NewReader(truncated), DefaultMaxFrameBytes); err == nil {
		t.Fatalf("ReadFrame accepted truncated frame")
	}
}

func TestEncoderDecoderRoundTrip(t *testing.T) {
	e := NewEncoder(64)
	e.Uint8(7)
	e.Uint16(65535)
	e.Uint32(1 << 30)
	e.Uint64(1 << 60)
	e.String("topic-name")
	e.Bytes([]byte{0x01, 0x02, 0x03})
	e.String("")
	e.Bytes(nil)

	d := NewDecoder(e.Payload())
	if v := d.Uint8(); v != 7 {
		t.Fatalf("Uint8 = %d", v)
	}
	if v := d.Uint16(); v != 65535 {
		t.Fatalf("Uint16 = %d", v)
	}
	if v := d.Uint32(); v != 1<<30 {
		t.Fatalf("Uint32 = %d", v)
	}
	if v := d.Uint64(); v != 1<<60 {
		t.Fatalf("Uint64 = %d", v)
	}
	if s := d.String(); s != "topic-name" {
		t.Fatalf("String = %q", s)
	}
	if b := d.Bytes(); !bytes.Equal(b, []byte{1, 2, 3}) {
		t.Fatalf("Bytes = %v", b)
	}
	if s := d.String(); s != "" {
		t.Fatalf("empty String = %q", s)
	}
	if b := d.Bytes(); len(b) != 0 {
		t.Fatalf("empty Bytes = %v", b)
	}
	if err := d.Err(); err != nil {
		t.Fatalf("Err = %v", err)
	}
	if d.Remaining() != 0 {
		t.Fatalf("Remaining = %d, want 0", d.Remaining())
	}
}

func TestDecoderShortReads(t *testing.T) {
	d := NewDecoder([]byte{0x01})
	_ = d.Uint64() // needs 8 bytes, only 1 available
	if d.Err() == nil {
		t.Fatalf("expected error on short read")
	}
	// Errors are sticky: subsequent reads keep failing.
	_ = d.String()
	if d.Err() == nil {
		t.Fatalf("expected sticky error")
	}
}

func TestErrorCodeToStringCoversAll(t *testing.T) {
	for _, code := range []byte{
		ErrCodeNone, ErrCodeUnknownCommand, ErrCodeMalformedRequest,
		ErrCodeMessageTooLarge, ErrCodeInvalidTopic, ErrCodeTopicNotFound,
		ErrCodeOffsetOutOfRange, ErrCodeOffsetPastEnd, ErrCodeOffsetNotFound,
		ErrCodeInternal, ErrCodeUnavailable,
	} {
		s := ErrorCodeToString(code)
		if s == "" || s == "unrecognized error code" {
			t.Fatalf("code 0x%02X has no description", code)
		}
	}
	if s := ErrorCodeToString(0xEE); s == "" {
		t.Fatalf("unknown code should still produce a description")
	}
}
