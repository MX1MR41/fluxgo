// Package protocol defines the FluxGo binary wire format (V2) and the
// helpers used by both the broker and clients to encode and decode frames.
//
// Frame layout (all integers are big-endian):
//
//	Request:  [4B length N][1B command code][N-1 bytes payload]
//	Response: [4B length M][1B error code][M-1 bytes payload]
//
// The length prefix covers the command/error code byte plus the payload.
package protocol

// Field widths used throughout the protocol.
const (
	LenPrefixSize   = 4
	CodeSize        = 1 // command code (request) / error code (response)
	TopicLenSize    = 2
	GroupIDLenSize  = 2
	PartitionIDSize = 4
	OffsetSize      = 8
	CountSize       = 4
	RecordLenSize   = 4
)

// DefaultMaxFrameBytes bounds a single frame. The broker enforces its own
// configured limit; this is the fallback used by clients.
const DefaultMaxFrameBytes = 16 * 1024 * 1024

// Command codes (request frame, first byte after the length prefix).
const (
	CmdProduce      byte = 0x01 // append one record, returns its offset
	CmdFetch        byte = 0x02 // read a batch of records starting at an offset
	CmdCommitOffset byte = 0x03 // persist a consumer-group offset
	CmdFetchOffset  byte = 0x04 // read back a consumer-group offset
	CmdListTopics   byte = 0x05 // metadata: list known topics
)

// Error codes (response frame, first byte after the length prefix).
const (
	ErrCodeNone             byte = 0x00
	ErrCodeUnknownCommand   byte = 0x01
	ErrCodeMalformedRequest byte = 0x02
	ErrCodeMessageTooLarge  byte = 0x03
	ErrCodeInvalidTopic     byte = 0x04
	ErrCodeTopicNotFound    byte = 0x05
	// ErrCodeOffsetOutOfRange means the requested offset is older than the
	// earliest record still retained. The payload carries the low watermark.
	ErrCodeOffsetOutOfRange byte = 0x06
	// ErrCodeOffsetPastEnd means the requested offset is at or beyond the
	// next offset to be assigned, i.e. there is no new data yet. The payload
	// carries the high watermark. This is a normal polling outcome.
	ErrCodeOffsetPastEnd  byte = 0x07
	ErrCodeOffsetNotFound byte = 0x08 // no committed offset for the group
	ErrCodeInternal       byte = 0x09
	ErrCodeUnavailable    byte = 0x0A // broker is shutting down
)

// ErrorCodeToString renders a human-readable description of an error code.
func ErrorCodeToString(code byte) string {
	switch code {
	case ErrCodeNone:
		return "success"
	case ErrCodeUnknownCommand:
		return "unknown command"
	case ErrCodeMalformedRequest:
		return "malformed request"
	case ErrCodeMessageTooLarge:
		return "message too large"
	case ErrCodeInvalidTopic:
		return "invalid topic name"
	case ErrCodeTopicNotFound:
		return "topic or partition not found"
	case ErrCodeOffsetOutOfRange:
		return "offset out of range (data already deleted)"
	case ErrCodeOffsetPastEnd:
		return "offset past end (no new data)"
	case ErrCodeOffsetNotFound:
		return "no committed offset for group/topic/partition"
	case ErrCodeInternal:
		return "internal broker error"
	case ErrCodeUnavailable:
		return "broker unavailable"
	default:
		return "unrecognized error code"
	}
}
