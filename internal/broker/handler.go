package broker

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	clog "github.com/MX1MR41/fluxgo/internal/commitlog"
	"github.com/MX1MR41/fluxgo/internal/offset"
	proto "github.com/MX1MR41/fluxgo/internal/protocol"
	"github.com/MX1MR41/fluxgo/internal/store"
	"github.com/MX1MR41/fluxgo/internal/validate"
)

// Hard caps applied to fetch requests regardless of what the client asks for.
const (
	maxFetchRecords = 4096
	maxFetchBytes   = 32 * 1024 * 1024
)

// Handler serves a single client connection.
type Handler struct {
	store         *store.Store
	offsetManager *offset.Manager
	logger        *slog.Logger
	maxFrameBytes uint32
	readTimeout   time.Duration
	writeTimeout  time.Duration
}

// NewHandler builds a Handler.
func NewHandler(s *store.Store, om *offset.Manager, logger *slog.Logger,
	maxFrameBytes int64, readTimeout, writeTimeout time.Duration) *Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{
		store:         s,
		offsetManager: om,
		logger:        logger,
		maxFrameBytes: uint32(maxFrameBytes),
		readTimeout:   readTimeout,
		writeTimeout:  writeTimeout,
	}
}

// Handle processes requests on conn until the peer disconnects, a protocol
// error occurs, or the broker shuts down. A panic while serving one
// connection is contained: it is logged and the connection is dropped, but
// the broker keeps running.
func (h *Handler) Handle(conn net.Conn) {
	remote := conn.RemoteAddr().String()
	log := h.logger.With("remote", remote)
	log.Debug("connection opened")
	defer func() {
		if r := recover(); r != nil {
			log.Error("panic while serving connection, dropping it", "panic", r)
		}
		log.Debug("connection closed")
	}()

	reader := bufio.NewReader(conn)
	for {
		if h.readTimeout > 0 {
			conn.SetReadDeadline(time.Now().Add(h.readTimeout))
		}
		code, payload, err := h.readRequest(reader)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) && !isTimeout(err) {
				log.Warn("failed to read request", "error", err)
			}
			return
		}

		respCode, respPayload := h.dispatch(code, payload, log)

		if h.writeTimeout > 0 {
			conn.SetWriteDeadline(time.Now().Add(h.writeTimeout))
		}
		if err := proto.WriteFrame(conn, respCode, respPayload); err != nil {
			if !errors.Is(err, net.ErrClosed) && !isTimeout(err) {
				log.Warn("failed to write response", "error", err)
			}
			return
		}
	}
}

// readRequest reads one request frame and enforces the frame size limit.
func (h *Handler) readRequest(reader *bufio.Reader) (byte, []byte, error) {
	code, payload, err := proto.ReadFrame(reader, h.maxFrameBytes)
	if err != nil {
		return 0, nil, err
	}
	return code, payload, nil
}

func (h *Handler) dispatch(cmd byte, payload []byte, log *slog.Logger) (byte, []byte) {
	switch cmd {
	case proto.CmdProduce:
		return h.handleProduce(payload, log)
	case proto.CmdFetch:
		return h.handleFetch(payload, log)
	case proto.CmdCommitOffset:
		return h.handleCommitOffset(payload, log)
	case proto.CmdFetchOffset:
		return h.handleFetchOffset(payload, log)
	case proto.CmdListTopics:
		return h.handleListTopics()
	default:
		return proto.ErrCodeUnknownCommand, []byte(fmt.Sprintf("unknown command 0x%X", cmd))
	}
}

// handleProduce: [u16 topic][u32 partition][u32 msgLen][msg] -> [u64 offset].
func (h *Handler) handleProduce(payload []byte, log *slog.Logger) (byte, []byte) {
	d := proto.NewDecoder(payload)
	topic := d.String()
	partition := d.Uint32()
	message := d.Bytes()
	if err := d.Err(); err != nil {
		return proto.ErrCodeMalformedRequest, []byte("malformed produce request")
	}

	lg, err := h.store.GetOrCreateLog(topic, partition)
	if err != nil {
		log.Warn("produce: cannot get/create log", "topic", topic, "partition", partition, "error", err)
		if isInvalidName(err) {
			return proto.ErrCodeInvalidTopic, []byte(err.Error())
		}
		return proto.ErrCodeInternal, []byte("failed to access log")
	}

	offset, err := lg.Append(clog.Record(message))
	if err != nil {
		log.Error("produce: append failed", "topic", topic, "partition", partition, "error", err)
		return proto.ErrCodeInternal, []byte("failed to append message")
	}

	resp := proto.NewEncoder(proto.OffsetSize)
	resp.Uint64(offset)
	return proto.ErrCodeNone, resp.Payload()
}

// handleFetch: [u16 topic][u32 partition][u64 offset][u32 maxRecords][u32 maxBytes]
// -> [u64 highWatermark][u64 startOffset][u32 count][u32 len + record]...
//
// Offset errors carry watermarks so clients can resync:
// past-end -> [u64 highWatermark], out-of-range -> [u64 lowWatermark].
func (h *Handler) handleFetch(payload []byte, log *slog.Logger) (byte, []byte) {
	d := proto.NewDecoder(payload)
	topic := d.String()
	partition := d.Uint32()
	offset := d.Uint64()
	maxRecords := int(d.Uint32())
	maxBytes := int(d.Uint32())
	if err := d.Err(); err != nil {
		return proto.ErrCodeMalformedRequest, []byte("malformed fetch request")
	}

	lg := h.store.GetLog(topic, partition)
	if lg == nil {
		return proto.ErrCodeTopicNotFound, []byte("topic or partition not found")
	}

	if maxRecords < 1 {
		maxRecords = 1
	}
	maxRecords = min(maxRecords, maxFetchRecords)
	if maxBytes <= 0 || maxBytes > maxFetchBytes {
		maxBytes = maxFetchBytes
	}

	records, err := lg.ReadBatch(offset, maxRecords, maxBytes)
	if err != nil {
		switch {
		case errors.Is(err, clog.ErrOffsetOutOfRange):
			resp := proto.NewEncoder(proto.OffsetSize)
			resp.Uint64(lg.LowestOffset())
			return proto.ErrCodeOffsetOutOfRange, resp.Payload()
		case errors.Is(err, clog.ErrReadPastEnd):
			resp := proto.NewEncoder(proto.OffsetSize)
			resp.Uint64(lg.HighestOffset())
			return proto.ErrCodeOffsetPastEnd, resp.Payload()
		case errors.Is(err, clog.ErrLogClosed):
			return proto.ErrCodeUnavailable, []byte("log is closed")
		default:
			log.Error("fetch: read failed", "topic", topic, "partition", partition,
				"offset", offset, "error", err)
			return proto.ErrCodeInternal, []byte("failed to read")
		}
	}

	size := proto.OffsetSize*2 + proto.CountSize
	for _, rec := range records {
		size += proto.RecordLenSize + len(rec)
	}
	resp := proto.NewEncoder(size)
	resp.Uint64(lg.HighestOffset()) // high watermark
	resp.Uint64(offset)             // offset of the first record
	resp.Uint32(uint32(len(records)))
	for _, rec := range records {
		resp.Bytes(rec)
	}
	return proto.ErrCodeNone, resp.Payload()
}

// handleCommitOffset: [u16 group][u16 topic][u32 partition][u64 offset] -> (empty).
func (h *Handler) handleCommitOffset(payload []byte, log *slog.Logger) (byte, []byte) {
	d := proto.NewDecoder(payload)
	group := d.String()
	topic := d.String()
	partition := d.Uint32()
	off := d.Uint64()
	if err := d.Err(); err != nil {
		return proto.ErrCodeMalformedRequest, []byte("malformed commit request")
	}

	if err := h.offsetManager.Commit(group, topic, partition, off); err != nil {
		if isInvalidName(err) {
			return proto.ErrCodeInvalidTopic, []byte(err.Error())
		}
		log.Error("offset commit failed", "group", group, "topic", topic,
			"partition", partition, "error", err)
		return proto.ErrCodeInternal, []byte("failed to commit offset")
	}
	return proto.ErrCodeNone, nil
}

// handleFetchOffset: [u16 group][u16 topic][u32 partition] -> [u64 offset].
func (h *Handler) handleFetchOffset(payload []byte, log *slog.Logger) (byte, []byte) {
	d := proto.NewDecoder(payload)
	group := d.String()
	topic := d.String()
	partition := d.Uint32()
	if err := d.Err(); err != nil {
		return proto.ErrCodeMalformedRequest, []byte("malformed fetch-offset request")
	}

	off, err := h.offsetManager.Fetch(group, topic, partition)
	if err != nil {
		switch {
		case errors.Is(err, offset.ErrOffsetNotFound):
			return proto.ErrCodeOffsetNotFound, nil
		case isInvalidName(err):
			return proto.ErrCodeInvalidTopic, []byte(err.Error())
		default:
			log.Error("offset fetch failed", "group", group, "topic", topic,
				"partition", partition, "error", err)
			return proto.ErrCodeInternal, []byte("failed to fetch offset")
		}
	}

	resp := proto.NewEncoder(proto.OffsetSize)
	resp.Uint64(off)
	return proto.ErrCodeNone, resp.Payload()
}

// handleListTopics: (empty) -> [u16 count][u16 len + topic]...
func (h *Handler) handleListTopics() (byte, []byte) {
	topics := h.store.Topics()
	resp := proto.NewEncoder(64)
	resp.Uint16(uint16(len(topics)))
	for _, t := range topics {
		resp.String(t)
	}
	return proto.ErrCodeNone, resp.Payload()
}

func isTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func isInvalidName(err error) bool {
	return errors.Is(err, validate.ErrInvalidName)
}
