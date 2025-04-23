package protocol

import "fmt"

const (
	CmdProduce      byte = 0x01
	CmdConsume      byte = 0x02
	CmdCommitOffset byte = 0x04
	CmdFetchOffset  byte = 0x05
)

const (
	ErrCodeNone            byte = 0x00
	ErrCodeUnknown         byte = 0xFF
	ErrCodeUnknownCommand  byte = 0xFE
	ErrCodeInvalidRequest  byte = 0xFD
	ErrCodePayloadTooShort byte = 0xFC
	ErrCodeMessageTooLarge byte = 0xFB

	ErrCodeProduceTopicLen     byte = 0x11
	ErrCodeProduceMissingData  byte = 0x12
	ErrCodeProduceLogAccess    byte = 0x13
	ErrCodeProduceAppendFailed byte = 0x14

	ErrCodeConsumeTopicLen      byte = 0x21
	ErrCodeConsumeTopicNotFound byte = 0x22
	ErrCodeConsumeOffsetInvalid byte = 0x23
	ErrCodeConsumeReadFailed    byte = 0x24

	ErrCodeOffsetGroupIDLen   byte = 0x30
	ErrCodeOffsetTopicLen     byte = 0x31
	ErrCodeOffsetCommitFailed byte = 0x32
	ErrCodeOffsetFetchFailed  byte = 0x33
	ErrCodeOffsetNotFound     byte = 0x34
)

func ErrorCodeToString(code byte) string {
	switch code {
	case ErrCodeNone:
		return "Success"
	case ErrCodeUnknown:
		return "Unknown error"
	case ErrCodeUnknownCommand:
		return "Unknown command code"
	case ErrCodeInvalidRequest:
		return "Invalid request format"
	case ErrCodePayloadTooShort:
		return "Payload too short"
	case ErrCodeMessageTooLarge:
		return "Message too large"
	case ErrCodeProduceTopicLen:
		return "Produce Error: Invalid topic length in payload"
	case ErrCodeProduceMissingData:
		return "Produce Error: Missing message data in payload"
	case ErrCodeProduceLogAccess:
		return "Produce Error: Cannot access log"
	case ErrCodeProduceAppendFailed:
		return "Produce Error: Failed to append message"
	case ErrCodeConsumeTopicLen:
		return "Consume Error: Invalid topic length in payload"
	case ErrCodeConsumeTopicNotFound:
		return "Consume Error: Topic or partition not found"
	case ErrCodeConsumeOffsetInvalid:
		return "Consume Error: Offset out of range"
	case ErrCodeConsumeReadFailed:
		return "Consume Error: Failed to read message"

	case ErrCodeOffsetGroupIDLen:
		return "Offset Error: Invalid Group ID length/name in payload"
	case ErrCodeOffsetTopicLen:
		return "Offset Error: Invalid Topic length/name in payload"
	case ErrCodeOffsetCommitFailed:
		return "Offset Error: Failed to commit offset"
	case ErrCodeOffsetFetchFailed:
		return "Offset Error: Failed to fetch offset"
	case ErrCodeOffsetNotFound:
		return "Offset Info: No offset committed for group/topic/partition"
	default:
		return fmt.Sprintf("Unrecognized error code: 0x%X", code)
	}
}
