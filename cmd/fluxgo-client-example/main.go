package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"net"
	"os"

	"strings"
	"time"

	proto "github.com/MX1MR41/fluxgo/internal/protocol"
)

var (
	serverAddr = flag.String("addr", "127.0.0.1:9898", "FluxGo server address")
	action     = flag.String("action", "produce", "Action to perform: produce, consume, commit, fetch")
	topic      = flag.String("topic", "test-topic", "Topic name")
	partition  = flag.Uint64("partition", 0, "Partition ID")
	group      = flag.String("group", "", "Consumer group ID (required for commit/fetch)")
	message    = flag.String("message", "Hello FluxGo!", "Message to produce (for produce action)")
	offset     = flag.Uint64("offset", 0, "Offset to consume from or commit (for consume/commit action)")
	timeout    = flag.Duration("timeout", 10*time.Second, "Connection read/write timeout")
)

func main() {
	flag.Parse()

	*action = strings.ToLower(*action)
	validActions := map[string]bool{
		"produce": true,
		"consume": true,
		"commit":  true,
		"fetch":   true,
	}
	if !validActions[*action] {
		fmt.Fprintf(os.Stderr, "Error: Invalid action '%s'. Must be one of: produce, consume, commit, fetch.\n", *action)
		flag.Usage()
		os.Exit(1)
	}

	if (*action == "commit" || *action == "fetch") && *group == "" {
		fmt.Fprintf(os.Stderr, "Error: -group flag is required for commit and fetch actions.\n")
		flag.Usage()
		os.Exit(1)
	}

	fmt.Printf("Connecting to server %s...\n", *serverAddr)
	conn, err := net.DialTimeout("tcp", *serverAddr, *timeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to server: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()
	fmt.Println("Connected.")

	switch *action {
	case "produce":
		produceMessage(conn, *topic, *partition, []byte(*message))
	case "consume":
		consumeMessage(conn, *topic, *partition, *offset)
	case "commit":
		commitOffset(conn, *group, *topic, *partition, *offset)
	case "fetch":
		fetchOffset(conn, *group, *topic, *partition)
	}
}

func produceMessage(conn net.Conn, topic string, partition uint64, message []byte) {
	fmt.Printf("Producing to Topic: %s, Partition: %d, Message: \"%s\"\n", topic, partition, string(message))
	topicBytes := []byte(topic)
	topicLen := uint16(len(topicBytes))
	payloadSize := proto.TopicLenSize + len(topicBytes) + proto.PartitionIDSize + len(message)
	payload := make([]byte, payloadSize)
	offset := 0
	binary.BigEndian.PutUint16(payload[offset:], topicLen)
	offset += proto.TopicLenSize
	copy(payload[offset:], topicBytes)
	offset += len(topicBytes)
	binary.BigEndian.PutUint64(payload[offset:], partition)
	offset += proto.PartitionIDSize
	copy(payload[offset:], message)
	err := proto.WriteFrame(conn, *timeout, proto.CmdProduce, payload)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error sending produce request: %v\n", err)
		return
	}
	maxRespSize := uint32(proto.LenPrefixSize + proto.ErrCodeSize + proto.OffsetSize + 100)
	errCode, respPayload, err := proto.ReadFrame(conn, *timeout, maxRespSize)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading produce response: %v\n", err)
		return
	}
	if errCode != proto.ErrCodeNone {
		errMsg := ""
		if len(respPayload) > 0 {
			errMsg = string(respPayload)
		}
		fmt.Printf("Produce failed: Code=0x%X (%s), Message: %s\n",
			errCode, proto.ErrorCodeToString(errCode), errMsg)
	} else {
		if len(respPayload) < proto.OffsetSize {
			fmt.Printf("Produce succeeded, but response payload is too short (%d bytes)\n", len(respPayload))
		} else {
			assignedOffset := binary.BigEndian.Uint64(respPayload[0:proto.OffsetSize])
			fmt.Printf("Produce successful! Assigned Offset: %d\n", assignedOffset)
		}
	}
}

func consumeMessage(conn net.Conn, topic string, partition uint64, offset uint64) {
	fmt.Printf("Consuming from Topic: %s, Partition: %d, Offset: %d\n", topic, partition, offset)
	topicBytes := []byte(topic)
	topicLen := uint16(len(topicBytes))
	payloadSize := proto.TopicLenSize + len(topicBytes) + proto.PartitionIDSize + proto.OffsetSize
	payload := make([]byte, payloadSize)
	payloadOffset := 0
	binary.BigEndian.PutUint16(payload[payloadOffset:], topicLen)
	payloadOffset += proto.TopicLenSize
	copy(payload[payloadOffset:], topicBytes)
	payloadOffset += len(topicBytes)
	binary.BigEndian.PutUint64(payload[payloadOffset:], partition)
	payloadOffset += proto.PartitionIDSize
	binary.BigEndian.PutUint64(payload[payloadOffset:], offset)
	err := proto.WriteFrame(conn, *timeout, proto.CmdConsume, payload)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error sending consume request: %v\n", err)
		return
	}
	maxRespSize := uint32(1024*1024*10 + 100)
	errCode, respPayload, err := proto.ReadFrame(conn, *timeout, maxRespSize)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading consume response: %v\n", err)
		return
	}
	if errCode == proto.ErrCodeNone {
		fmt.Printf("Consume successful! Message: \"%s\"\n", string(respPayload))
	} else if errCode == proto.ErrCodeConsumeOffsetInvalid {
		fmt.Printf("Consume failed: Offset %d not found or out of range (Code=0x%X)\n", offset, errCode)
	} else {
		errMsg := ""
		if len(respPayload) > 0 {
			errMsg = string(respPayload)
		}
		fmt.Printf("Consume failed: Code=0x%X (%s), Message: %s\n",
			errCode, proto.ErrorCodeToString(errCode), errMsg)
	}
}

func commitOffset(conn net.Conn, groupID, topic string, partition uint64, offsetToCommit uint64) {
	fmt.Printf("Committing Offset for Group: %s, Topic: %s, Partition: %d, Offset: %d\n",
		groupID, topic, partition, offsetToCommit)

	groupIDBytes := []byte(groupID)
	groupIDLen := uint16(len(groupIDBytes))
	topicBytes := []byte(topic)
	topicLen := uint16(len(topicBytes))

	payloadSize := proto.GroupIDLenSize + len(groupIDBytes) + proto.TopicLenSize + len(topicBytes) + proto.PartitionIDSize + proto.OffsetSize
	payload := make([]byte, payloadSize)

	cursor := 0
	binary.BigEndian.PutUint16(payload[cursor:], groupIDLen)
	cursor += proto.GroupIDLenSize
	copy(payload[cursor:], groupIDBytes)
	cursor += len(groupIDBytes)

	binary.BigEndian.PutUint16(payload[cursor:], topicLen)
	cursor += proto.TopicLenSize
	copy(payload[cursor:], topicBytes)
	cursor += len(topicBytes)

	binary.BigEndian.PutUint64(payload[cursor:], partition)
	cursor += proto.PartitionIDSize
	binary.BigEndian.PutUint64(payload[cursor:], offsetToCommit)

	err := proto.WriteFrame(conn, *timeout, proto.CmdCommitOffset, payload)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error sending commit request: %v\n", err)
		return
	}

	maxRespSize := uint32(proto.LenPrefixSize + proto.ErrCodeSize + 256)
	errCode, respPayload, err := proto.ReadFrame(conn, *timeout, maxRespSize)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading commit response: %v\n", err)
		return
	}

	if errCode == proto.ErrCodeNone {
		fmt.Println("Offset committed successfully.")
	} else {
		errMsg := ""
		if len(respPayload) > 0 {
			errMsg = string(respPayload)
		}
		fmt.Printf("Offset commit failed: Code=0x%X (%s), Message: %s\n",
			errCode, proto.ErrorCodeToString(errCode), errMsg)
	}
}

func fetchOffset(conn net.Conn, groupID, topic string, partition uint64) {
	fmt.Printf("Fetching Offset for Group: %s, Topic: %s, Partition: %d\n",
		groupID, topic, partition)

	groupIDBytes := []byte(groupID)
	groupIDLen := uint16(len(groupIDBytes))
	topicBytes := []byte(topic)
	topicLen := uint16(len(topicBytes))

	payloadSize := proto.GroupIDLenSize + len(groupIDBytes) + proto.TopicLenSize + len(topicBytes) + proto.PartitionIDSize
	payload := make([]byte, payloadSize)

	cursor := 0
	binary.BigEndian.PutUint16(payload[cursor:], groupIDLen)
	cursor += proto.GroupIDLenSize
	copy(payload[cursor:], groupIDBytes)
	cursor += len(groupIDBytes)

	binary.BigEndian.PutUint16(payload[cursor:], topicLen)
	cursor += proto.TopicLenSize
	copy(payload[cursor:], topicBytes)
	cursor += len(topicBytes)

	binary.BigEndian.PutUint64(payload[cursor:], partition)

	err := proto.WriteFrame(conn, *timeout, proto.CmdFetchOffset, payload)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error sending fetch request: %v\n", err)
		return
	}

	maxRespSize := uint32(proto.LenPrefixSize + proto.ErrCodeSize + proto.OffsetSize + 256)
	errCode, respPayload, err := proto.ReadFrame(conn, *timeout, maxRespSize)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading fetch response: %v\n", err)
		return
	}

	if errCode == proto.ErrCodeNone {
		if len(respPayload) < proto.OffsetSize {
			fmt.Printf("Fetch succeeded, but response payload is too short (%d bytes)\n", len(respPayload))
		} else {
			fetchedOffset := binary.BigEndian.Uint64(respPayload[0:proto.OffsetSize])
			fmt.Printf("Fetch successful! Last committed Offset: %d\n", fetchedOffset)
		}
	} else if errCode == proto.ErrCodeOffsetNotFound {
		fmt.Printf("No offset found committed for this group/topic/partition (Code=0x%X).\n", errCode)
	} else {
		errMsg := ""
		if len(respPayload) > 0 {
			errMsg = string(respPayload)
		}
		fmt.Printf("Offset fetch failed: Code=0x%X (%s), Message: %s\n",
			errCode, proto.ErrorCodeToString(errCode), errMsg)
	}
}
