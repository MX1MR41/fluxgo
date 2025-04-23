package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"
)

const (
	LenPrefixSize = 4

	CmdCodeSize = 1

	ErrCodeSize = 1

	TopicLenSize    = 2
	PartitionIDSize = 8
	OffsetSize      = 8
	GroupIDLenSize  = 2
)

func WriteFrame(conn net.Conn, writeTimeout time.Duration, commandCode byte, payload []byte) error {
	totalLen := uint32(CmdCodeSize + len(payload))
	frameBuf := make([]byte, LenPrefixSize+totalLen)

	binary.BigEndian.PutUint32(frameBuf[0:LenPrefixSize], totalLen)

	frameBuf[LenPrefixSize] = commandCode

	copy(frameBuf[LenPrefixSize+CmdCodeSize:], payload)

	if writeTimeout > 0 {
		conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	}

	_, err := conn.Write(frameBuf)
	if err != nil {
		return fmt.Errorf("failed to write frame: %w", err)
	}
	return nil
}

func ReadFrame(conn net.Conn, readTimeout time.Duration, maxMessageSize uint32) (errorCode byte, payload []byte, err error) {

	if readTimeout > 0 {
		conn.SetReadDeadline(time.Now().Add(readTimeout))
	}

	lenBuf := make([]byte, LenPrefixSize)
	_, err = io.ReadFull(conn, lenBuf)
	if err != nil {
		return 0xFF, nil, fmt.Errorf("failed to read length prefix: %w", err)
	}
	messageLen := binary.BigEndian.Uint32(lenBuf)

	if messageLen == 0 || messageLen < ErrCodeSize {
		return 0xFF, nil, fmt.Errorf("invalid message length received: %d", messageLen)
	}
	if maxMessageSize > 0 && messageLen > maxMessageSize {

		return 0xFF, nil, fmt.Errorf("message length %d exceeds maximum %d", messageLen, maxMessageSize)
	}

	payloadWithErr := make([]byte, messageLen)
	_, err = io.ReadFull(conn, payloadWithErr)
	if err != nil {
		return 0xFF, nil, fmt.Errorf("failed to read error/payload (len %d): %w", messageLen, err)
	}

	errorCode = payloadWithErr[0]
	payload = payloadWithErr[1:]

	return errorCode, payload, nil
}
