package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

// WriteFrame writes a single length-prefixed frame to w.
// The caller is responsible for deadlines (e.g. net.Conn.SetWriteDeadline).
func WriteFrame(w io.Writer, code byte, payload []byte) error {
	totalLen := uint32(CodeSize + len(payload))
	frame := make([]byte, LenPrefixSize+totalLen)
	binary.BigEndian.PutUint32(frame[:LenPrefixSize], totalLen)
	frame[LenPrefixSize] = code
	copy(frame[LenPrefixSize+CodeSize:], payload)
	if _, err := w.Write(frame); err != nil {
		return fmt.Errorf("protocol: failed to write frame: %w", err)
	}
	return nil
}

// ReadFrame reads a single length-prefixed frame from r and returns the code
// byte and the payload. maxFrameBytes bounds the frame size; pass
// DefaultMaxFrameBytes if you do not have your own limit.
func ReadFrame(r io.Reader, maxFrameBytes uint32) (code byte, payload []byte, err error) {
	lenBuf := make([]byte, LenPrefixSize)
	if _, err := io.ReadFull(r, lenBuf); err != nil {
		return 0, nil, fmt.Errorf("protocol: failed to read length prefix: %w", err)
	}
	frameLen := binary.BigEndian.Uint32(lenBuf)
	if frameLen < CodeSize {
		return 0, nil, fmt.Errorf("protocol: frame length %d too small", frameLen)
	}
	if maxFrameBytes > 0 && frameLen > maxFrameBytes {
		return 0, nil, fmt.Errorf("protocol: frame length %d exceeds limit %d", frameLen, maxFrameBytes)
	}
	buf := make([]byte, frameLen)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, nil, fmt.Errorf("protocol: failed to read frame body (len %d): %w", frameLen, err)
	}
	return buf[0], buf[1:], nil
}

// ---------------------------------------------------------------------------
// Payload encoder
// ---------------------------------------------------------------------------

// Encoder incrementally builds a request/response payload. All integers are
// written big-endian.
type Encoder struct {
	buf []byte
}

// NewEncoder returns an Encoder with the given capacity hint.
func NewEncoder(capacityHint int) *Encoder {
	return &Encoder{buf: make([]byte, 0, capacityHint)}
}

func (e *Encoder) Uint8(v uint8)   { e.buf = append(e.buf, v) }
func (e *Encoder) Uint16(v uint16) { e.buf = binary.BigEndian.AppendUint16(e.buf, v) }
func (e *Encoder) Uint32(v uint32) { e.buf = binary.BigEndian.AppendUint32(e.buf, v) }
func (e *Encoder) Uint64(v uint64) { e.buf = binary.BigEndian.AppendUint64(e.buf, v) }

// String appends a uint16 length-prefixed string. Strings longer than 65535
// bytes are truncated; callers validate names well below that limit.
func (e *Encoder) String(s string) {
	e.Uint16(uint16(len(s)))
	e.buf = append(e.buf, s...)
}

// Bytes appends a uint32 length-prefixed byte slice.
func (e *Encoder) Bytes(b []byte) {
	e.Uint32(uint32(len(b)))
	e.buf = append(e.buf, b...)
}

// Payload returns the encoded payload.
func (e *Encoder) Payload() []byte { return e.buf }

// Len returns the current encoded length.
func (e *Encoder) Len() int { return len(e.buf) }

// ---------------------------------------------------------------------------
// Payload decoder
// ---------------------------------------------------------------------------

// Decoder incrementally reads a payload. It records the first error and
// short-circuits subsequent reads, so callers can decode a full struct and
// check Err() once at the end.
type Decoder struct {
	buf []byte
	off int
	err error
}

// ErrPayloadShort is returned when a decode runs past the end of the payload.
var ErrPayloadShort = fmt.Errorf("protocol: payload too short")

// NewDecoder wraps buf for decoding.
func NewDecoder(buf []byte) *Decoder { return &Decoder{buf: buf} }

func (d *Decoder) Uint8() uint8 {
	if d.err != nil {
		return 0
	}
	if d.off+1 > len(d.buf) {
		d.err = ErrPayloadShort
		return 0
	}
	v := d.buf[d.off]
	d.off++
	return v
}

func (d *Decoder) Uint16() uint16 {
	if d.err != nil {
		return 0
	}
	if d.off+2 > len(d.buf) {
		d.err = ErrPayloadShort
		return 0
	}
	v := binary.BigEndian.Uint16(d.buf[d.off:])
	d.off += 2
	return v
}

func (d *Decoder) Uint32() uint32 {
	if d.err != nil {
		return 0
	}
	if d.off+4 > len(d.buf) {
		d.err = ErrPayloadShort
		return 0
	}
	v := binary.BigEndian.Uint32(d.buf[d.off:])
	d.off += 4
	return v
}

func (d *Decoder) Uint64() uint64 {
	if d.err != nil {
		return 0
	}
	if d.off+8 > len(d.buf) {
		d.err = ErrPayloadShort
		return 0
	}
	v := binary.BigEndian.Uint64(d.buf[d.off:])
	d.off += 8
	return v
}

// String reads a uint16 length-prefixed string.
func (d *Decoder) String() string {
	n := int(d.Uint16())
	if d.err != nil {
		return ""
	}
	if d.off+n > len(d.buf) {
		d.err = ErrPayloadShort
		return ""
	}
	s := string(d.buf[d.off : d.off+n])
	d.off += n
	return s
}

// Bytes reads a uint32 length-prefixed byte slice. The returned slice
// aliases the decoder's buffer.
func (d *Decoder) Bytes() []byte {
	n := int(d.Uint32())
	if d.err != nil {
		return nil
	}
	if n < 0 || d.off+n > len(d.buf) {
		d.err = ErrPayloadShort
		return nil
	}
	b := d.buf[d.off : d.off+n]
	d.off += n
	return b
}

// Err returns the first decode error encountered, if any.
func (d *Decoder) Err() error { return d.err }

// Remaining reports how many bytes are left undecoded.
func (d *Decoder) Remaining() int { return len(d.buf) - d.off }
