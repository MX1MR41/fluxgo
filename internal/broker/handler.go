package broker

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	clog "github.com/MX1MR41/fluxgo/internal/commitlog"
	offset "github.com/MX1MR41/fluxgo/internal/offset"
	proto "github.com/MX1MR41/fluxgo/internal/protocol"
	store "github.com/MX1MR41/fluxgo/internal/store"
)

type Handler struct {
	store         *store.Store
	offsetManager *offset.Manager
}

func NewHandler(s *store.Store, om *offset.Manager) *Handler {
	return &Handler{
		store:         s,
		offsetManager: om,
	}
}

func (h *Handler) Handle(conn net.Conn, readTimeout, writeTimeout time.Duration) {
	remoteAddr := conn.RemoteAddr().String()
	fmt.Printf("Handler: Handling connection from %s\n", remoteAddr)
	defer fmt.Printf("Handler: Finished handling connection from %s\n", remoteAddr)

	reader := bufio.NewReader(conn)

	for {

		if readTimeout > 0 {
			conn.SetReadDeadline(time.Now().Add(readTimeout))
		}

		lenBuf := make([]byte, proto.LenPrefixSize)
		_, err := io.ReadFull(reader, lenBuf)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {

			} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				fmt.Printf("Handler: Read timeout for %s\n", remoteAddr)
			} else {
				fmt.Fprintf(os.Stderr, "Handler: Error reading length prefix from %s: %v\n", remoteAddr, err)
			}
			return
		}
		messageLen := binary.BigEndian.Uint32(lenBuf)

		if messageLen > 1024*1024*10 {
			fmt.Fprintf(os.Stderr, "Handler: Message length %d exceeds limit from %s\n", messageLen, remoteAddr)
			return
		}
		if messageLen < proto.CmdCodeSize {
			fmt.Fprintf(os.Stderr, "Handler: Message length %d too short from %s\n", messageLen, remoteAddr)
			return
		}

		payloadWithCmd := make([]byte, messageLen)
		_, err = io.ReadFull(reader, payloadWithCmd)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				fmt.Printf("Handler: Read timeout while reading payload from %s\n", remoteAddr)
			} else {
				fmt.Fprintf(os.Stderr, "Handler: Error reading command/payload from %s: %v\n", remoteAddr, err)
			}
			return
		}

		commandCode := payloadWithCmd[0]
		payload := payloadWithCmd[1:]

		var responsePayload []byte
		var responseErrCode byte

		switch commandCode {
		case proto.CmdProduce:
			responsePayload, responseErrCode = h.handleProduce(payload)
		case proto.CmdConsume:
			responsePayload, responseErrCode = h.handleConsume(payload)
		case proto.CmdCommitOffset:
			responsePayload, responseErrCode = h.handleCommitOffset(payload)
		case proto.CmdFetchOffset:
			responsePayload, responseErrCode = h.handleFetchOffset(payload)
		default:
			fmt.Fprintf(os.Stderr, "Handler: Unknown command code 0x%X from %s\n", commandCode, remoteAddr)
			responseErrCode = proto.ErrCodeUnknownCommand
			responsePayload = []byte("Unknown command code")
		}

		respLen := uint32(proto.ErrCodeSize + len(responsePayload))
		respBuf := make([]byte, proto.LenPrefixSize+respLen)

		binary.BigEndian.PutUint32(respBuf[0:proto.LenPrefixSize], respLen)
		respBuf[proto.LenPrefixSize] = responseErrCode
		copy(respBuf[proto.LenPrefixSize+proto.ErrCodeSize:], responsePayload)

		if writeTimeout > 0 {
			conn.SetWriteDeadline(time.Now().Add(writeTimeout))
		}
		_, err = conn.Write(respBuf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				fmt.Printf("Handler: Write timeout for %s\n", remoteAddr)
			} else {
				fmt.Fprintf(os.Stderr, "Handler: Error writing response to %s: %v\n", remoteAddr, err)
			}
			return
		}
	}
}

func (h *Handler) handleProduce(payload []byte) (responsePayload []byte, errorCode byte) {
	fmt.Println("Handler: Received Produce request")

	minLen := proto.TopicLenSize + proto.PartitionIDSize
	if len(payload) < minLen {
		return []byte("Invalid produce payload: too short"), proto.ErrCodePayloadTooShort
	}

	topicLen := int(binary.BigEndian.Uint16(payload[0:proto.TopicLenSize]))
	if len(payload) < minLen+topicLen {
		return []byte("Invalid produce payload: too short for topic"), proto.ErrCodeProduceTopicLen
	}
	topicName := string(payload[proto.TopicLenSize : proto.TopicLenSize+topicLen])

	partitionOffset := proto.TopicLenSize + topicLen
	partitionID := binary.BigEndian.Uint64(payload[partitionOffset : partitionOffset+proto.PartitionIDSize])

	messageDataOffset := partitionOffset + proto.PartitionIDSize
	if messageDataOffset > len(payload) {
		return []byte("Invalid produce payload: missing message data"), proto.ErrCodeProduceMissingData
	}
	messageData := payload[messageDataOffset:]

	log, err := h.store.GetOrCreateLog(topicName, partitionID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Handler: Failed to get/create log for %s_%d: %v\n", topicName, partitionID, err)
		return []byte(fmt.Sprintf("Failed to access log: %s", err)), proto.ErrCodeProduceLogAccess
	}

	offset, err := log.Append(clog.Record(messageData))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Handler: Failed to append to log %s_%d: %v\n", topicName, partitionID, err)
		return []byte(fmt.Sprintf("Failed to append message: %s", err)), proto.ErrCodeProduceAppendFailed
	}

	respData := make([]byte, proto.OffsetSize)
	binary.BigEndian.PutUint64(respData, offset)

	fmt.Printf("Handler: Produced to %s_%d at offset %d\n", topicName, partitionID, offset)
	return respData, proto.ErrCodeNone
}

func (h *Handler) handleConsume(payload []byte) (responsePayload []byte, errorCode byte) {
	fmt.Println("Handler: Received Consume request")

	if len(payload) < proto.TopicLenSize {
		return []byte("Invalid consume payload: too short for topic length"), proto.ErrCodePayloadTooShort
	}

	topicLen := int(binary.BigEndian.Uint16(payload[0:proto.TopicLenSize]))
	expectedLen := proto.TopicLenSize + topicLen + proto.PartitionIDSize + proto.OffsetSize
	if len(payload) < expectedLen {
		return []byte("Invalid consume payload: too short for full header"), proto.ErrCodeConsumeTopicLen
	}
	topicName := string(payload[proto.TopicLenSize : proto.TopicLenSize+topicLen])

	partitionOffset := proto.TopicLenSize + topicLen
	partitionID := binary.BigEndian.Uint64(payload[partitionOffset : partitionOffset+proto.PartitionIDSize])

	offsetOffset := partitionOffset + proto.PartitionIDSize
	requestedOffset := binary.BigEndian.Uint64(payload[offsetOffset : offsetOffset+proto.OffsetSize])

	log := h.store.GetLog(topicName, partitionID)
	if log == nil {
		return []byte("Topic or partition not found"), proto.ErrCodeConsumeTopicNotFound
	}

	record, err := log.Read(requestedOffset)
	if err != nil {
		if errors.Is(err, clog.ErrOffsetNotFound) || errors.Is(err, clog.ErrReadPastEnd) {
			fmt.Printf("Handler: Consume request for offset %d on %s_%d: Offset not found/past end.\n",
				requestedOffset, topicName, partitionID)
			return nil, proto.ErrCodeConsumeOffsetInvalid
		}
		fmt.Fprintf(os.Stderr, "Handler: Failed to read from log %s_%d at offset %d: %v\n",
			topicName, partitionID, requestedOffset, err)
		return []byte(fmt.Sprintf("Failed to read message: %s", err)), proto.ErrCodeConsumeReadFailed
	}

	fmt.Printf("Handler: Consumed from %s_%d at offset %d (size %d)\n",
		topicName, partitionID, requestedOffset, len(record))
	return record, proto.ErrCodeNone
}

func (h *Handler) handleCommitOffset(payload []byte) (responsePayload []byte, errorCode byte) {

	if len(payload) < proto.GroupIDLenSize+proto.TopicLenSize+proto.PartitionIDSize+proto.OffsetSize {
		return []byte("Invalid commit payload: too short for headers"), proto.ErrCodePayloadTooShort
	}

	cursor := 0
	groupIDLen := int(binary.BigEndian.Uint16(payload[cursor : cursor+proto.GroupIDLenSize]))
	cursor += proto.GroupIDLenSize
	if len(payload) < cursor+groupIDLen+proto.TopicLenSize+proto.PartitionIDSize+proto.OffsetSize {
		return []byte("Invalid commit payload: too short for group ID"), proto.ErrCodeOffsetGroupIDLen
	}
	groupID := string(payload[cursor : cursor+groupIDLen])
	cursor += groupIDLen

	topicLen := int(binary.BigEndian.Uint16(payload[cursor : cursor+proto.TopicLenSize]))
	cursor += proto.TopicLenSize
	if len(payload) < cursor+topicLen+proto.PartitionIDSize+proto.OffsetSize {
		return []byte("Invalid commit payload: too short for topic"), proto.ErrCodeOffsetTopicLen
	}
	topicName := string(payload[cursor : cursor+topicLen])
	cursor += topicLen

	if len(payload) < cursor+proto.PartitionIDSize+proto.OffsetSize {
		return []byte("Invalid commit payload: too short for partition/offset"), proto.ErrCodePayloadTooShort
	}
	partitionID := binary.BigEndian.Uint64(payload[cursor : cursor+proto.PartitionIDSize])
	cursor += proto.PartitionIDSize

	committedOffset := binary.BigEndian.Uint64(payload[cursor : cursor+proto.OffsetSize])
	cursor += proto.OffsetSize

	err := h.offsetManager.Commit(groupID, topicName, partitionID, committedOffset)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Handler: Failed to commit offset %d for %s/%s_%d: %v\n",
			committedOffset, groupID, topicName, partitionID, err)
		return []byte("Failed to commit offset"), proto.ErrCodeOffsetCommitFailed
	}

	return nil, proto.ErrCodeNone
}

func (h *Handler) handleFetchOffset(payload []byte) (responsePayload []byte, errorCode byte) {

	if len(payload) < proto.GroupIDLenSize+proto.TopicLenSize+proto.PartitionIDSize {
		return []byte("Invalid fetch payload: too short for headers"), proto.ErrCodePayloadTooShort
	}

	cursor := 0
	groupIDLen := int(binary.BigEndian.Uint16(payload[cursor : cursor+proto.GroupIDLenSize]))
	cursor += proto.GroupIDLenSize
	if len(payload) < cursor+groupIDLen+proto.TopicLenSize+proto.PartitionIDSize {
		return []byte("Invalid fetch payload: too short for group ID"), proto.ErrCodeOffsetGroupIDLen
	}
	groupID := string(payload[cursor : cursor+groupIDLen])
	cursor += groupIDLen

	topicLen := int(binary.BigEndian.Uint16(payload[cursor : cursor+proto.TopicLenSize]))
	cursor += proto.TopicLenSize
	if len(payload) < cursor+topicLen+proto.PartitionIDSize {
		return []byte("Invalid fetch payload: too short for topic"), proto.ErrCodeOffsetTopicLen
	}
	topicName := string(payload[cursor : cursor+topicLen])
	cursor += topicLen

	if len(payload) < cursor+proto.PartitionIDSize {
		return []byte("Invalid fetch payload: too short for partition"), proto.ErrCodePayloadTooShort
	}
	partitionID := binary.BigEndian.Uint64(payload[cursor : cursor+proto.PartitionIDSize])
	cursor += proto.PartitionIDSize

	fetchedOffset, err := h.offsetManager.Fetch(groupID, topicName, partitionID)
	if err != nil {
		if errors.Is(err, offset.ErrOffsetNotFound) {

			return nil, proto.ErrCodeOffsetNotFound
		}

		fmt.Fprintf(os.Stderr, "Handler: Failed to fetch offset for %s/%s_%d: %v\n",
			groupID, topicName, partitionID, err)
		return []byte("Failed to fetch offset"), proto.ErrCodeOffsetFetchFailed
	}

	respData := make([]byte, proto.OffsetSize)
	binary.BigEndian.PutUint64(respData, fetchedOffset)

	return respData, proto.ErrCodeNone
}
