// Command fluxgo-client is a CLI for interacting with a FluxGo broker:
// producing records, fetching batches, managing consumer-group offsets, and
// listing topics.
package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"time"

	proto "github.com/MX1MR41/fluxgo/internal/protocol"
)

var (
	serverAddr = flag.String("addr", "127.0.0.1:9898", "FluxGo server address")
	action     = flag.String("action", "produce", "one of: produce, fetch, commit, fetch-offset, topics")
	topic      = flag.String("topic", "test-topic", "topic name")
	partition  = flag.Uint("partition", 0, "partition ID")
	group      = flag.String("group", "", "consumer group ID (required for commit/fetch-offset)")
	message    = flag.String("message", "Hello FluxGo!", "message to produce")
	offset     = flag.Uint64("offset", 0, "offset to fetch from / to commit")
	count      = flag.Int("count", 1, "max records to fetch")
	maxBytes   = flag.Int("max-bytes", 1<<20, "max bytes to fetch per request")
	follow     = flag.Bool("follow", false, "keep polling for new records after a fetch")
	timeout    = flag.Duration("timeout", 10*time.Second, "dial/read/write timeout")
)

func main() {
	flag.Parse()

	conn, err := net.DialTimeout("tcp", *serverAddr, *timeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: cannot connect to %s: %v\n", *serverAddr, err)
		os.Exit(1)
	}
	defer conn.Close()

	var runErr error
	switch *action {
	case "produce":
		runErr = produce(conn)
	case "fetch":
		runErr = fetch(conn)
	case "commit":
		runErr = commit(conn)
	case "fetch-offset":
		runErr = fetchOffsetCmd(conn)
	case "topics":
		runErr = listTopics(conn)
	default:
		fmt.Fprintf(os.Stderr, "error: invalid action %q (produce, fetch, commit, fetch-offset, topics)\n", *action)
		os.Exit(2)
	}
	if runErr != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", runErr)
		os.Exit(1)
	}
}

// roundTrip sends one request and returns the response, applying deadlines.
func roundTrip(conn net.Conn, cmd byte, payload []byte) (byte, []byte, error) {
	if *timeout > 0 {
		deadline := time.Now().Add(*timeout)
		conn.SetDeadline(deadline)
	}
	if err := proto.WriteFrame(conn, cmd, payload); err != nil {
		return 0, nil, err
	}
	code, resp, err := proto.ReadFrame(conn, proto.DefaultMaxFrameBytes)
	if err != nil {
		return 0, nil, err
	}
	if code != proto.ErrCodeNone {
		return code, resp, fmt.Errorf("broker returned %s (0x%02X): %s",
			proto.ErrorCodeToString(code), code, string(resp))
	}
	return code, resp, nil
}

func produce(conn net.Conn) error {
	req := proto.NewEncoder(len(*topic) + len(*message) + 16)
	req.String(*topic)
	req.Uint32(uint32(*partition))
	req.Bytes([]byte(*message))

	_, resp, err := roundTrip(conn, proto.CmdProduce, req.Payload())
	if err != nil {
		return err
	}
	d := proto.NewDecoder(resp)
	assigned := d.Uint64()
	if err := d.Err(); err != nil {
		return fmt.Errorf("malformed produce response")
	}
	fmt.Printf("produced to %s_%d at offset %d\n", *topic, *partition, assigned)
	return nil
}

func fetch(conn net.Conn) error {
	next := *offset
	for {
		req := proto.NewEncoder(64)
		req.String(*topic)
		req.Uint32(uint32(*partition))
		req.Uint64(next)
		req.Uint32(uint32(*count))
		req.Uint32(uint32(*maxBytes))

		code, resp, err := roundTrip(conn, proto.CmdFetch, req.Payload())
		if err != nil {
			if code == proto.ErrCodeOffsetPastEnd {
				if !*follow {
					fmt.Println("no new data (past end of log)")
					return nil
				}
				time.Sleep(500 * time.Millisecond)
				continue
			}
			if code == proto.ErrCodeOffsetOutOfRange {
				d := proto.NewDecoder(resp)
				low := d.Uint64()
				fmt.Printf("offset %d is out of range; earliest available is %d\n", next, low)
				if !*follow {
					return fmt.Errorf("offset out of range")
				}
				next = low
				continue
			}
			return err
		}

		d := proto.NewDecoder(resp)
		high := d.Uint64()
		start := d.Uint64()
		n := int(d.Uint32())
		for i := 0; i < n; i++ {
			rec := d.Bytes()
			fmt.Printf("[offset %d] %s\n", start+uint64(i), string(rec))
		}
		if err := d.Err(); err != nil {
			return fmt.Errorf("malformed fetch response")
		}
		next = start + uint64(n)
		if !*follow {
			return nil
		}
		if next >= high {
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func commit(conn net.Conn) error {
	if *group == "" {
		return fmt.Errorf("-group is required for commit")
	}
	req := proto.NewEncoder(64)
	req.String(*group)
	req.String(*topic)
	req.Uint32(uint32(*partition))
	req.Uint64(*offset)

	if _, _, err := roundTrip(conn, proto.CmdCommitOffset, req.Payload()); err != nil {
		return err
	}
	fmt.Printf("committed offset %d for group %q on %s_%d\n", *offset, *group, *topic, *partition)
	return nil
}

func fetchOffsetCmd(conn net.Conn) error {
	if *group == "" {
		return fmt.Errorf("-group is required for fetch-offset")
	}
	req := proto.NewEncoder(64)
	req.String(*group)
	req.String(*topic)
	req.Uint32(uint32(*partition))

	code, resp, err := roundTrip(conn, proto.CmdFetchOffset, req.Payload())
	if err != nil {
		if code == proto.ErrCodeOffsetNotFound {
			fmt.Println("no committed offset for this group/topic/partition")
			return nil
		}
		return err
	}
	d := proto.NewDecoder(resp)
	committed := d.Uint64()
	if err := d.Err(); err != nil {
		return fmt.Errorf("malformed fetch-offset response")
	}
	fmt.Printf("committed offset for group %q on %s_%d: %d\n", *group, *topic, *partition, committed)
	return nil
}

func listTopics(conn net.Conn) error {
	_, resp, err := roundTrip(conn, proto.CmdListTopics, nil)
	if err != nil {
		return err
	}
	d := proto.NewDecoder(resp)
	n := int(d.Uint16())
	fmt.Printf("%d topic(s):\n", n)
	for i := 0; i < n; i++ {
		fmt.Printf("  %s\n", d.String())
	}
	return d.Err()
}
