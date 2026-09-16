package events

import (
	"bufio"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nabutabu/herdr-observr/internal/client"
)

// Subscriber owns a single dedicated connection to the Herdr socket used
// exclusively for events.subscribe. This type exposes no write surface after
// construction: RPCs (reconciliation, report
// calls) must open their own short-lived connection via client.Call and must
// never reuse this one.
type Subscriber struct {
	conn   net.Conn
	events chan NormalizedEvent
	err    chan error
	done   chan struct{}

	stopped   atomic.Bool
	closeOnce sync.Once
}

func NewSubscriber(params map[string]any) (*Subscriber, error) {
	conn, err := client.Dial()
	if err != nil {
		return nil, fmt.Errorf("dialing unix socket %s: %w", client.SocketPath(), err)
	}

	conn.SetWriteDeadline(time.Now().Add(client.WRITE_DEADLINE))
	if err := client.WriteFrame(conn, &client.Request{
		ID:     "herdr-observr",
		Method: "events.subscribe",
		Params: params,
	}); err != nil {
		conn.Close()
		return nil, fmt.Errorf("writing subscribe request: %w", err)
	}
	// The connection becomes a long-lived read-only stream; no stale write
	// deadline should survive into the read loop.
	conn.SetDeadline(time.Time{})

	s := &Subscriber{
		conn:   conn,
		events: make(chan NormalizedEvent, 64),
		err:    make(chan error, 1),
		done:   make(chan struct{}),
	}

	go s.run()
	return s, nil
}

// Events delivers each pushed event verbatim, exactly as received on the wire.
func (s *Subscriber) Events() <-chan NormalizedEvent { return s.events }

// Err reports a terminal stream failure (socket error or EOF).
func (s *Subscriber) Err() <-chan error { return s.err }

// Close tears down the subscription connection and waits for the reader
// goroutine to exit. Safe to call multiple times.
func (s *Subscriber) Close() error {
	var err error
	s.closeOnce.Do(func() {
		s.stopped.Store(true)
		err = s.conn.Close()
		<-s.done
	})
	return err
}

func (s *Subscriber) run() {
	defer func() {
		s.conn.Close()
		close(s.events)
		close(s.err)
		close(s.done)
	}()

	reader := bufio.NewReader(s.conn)
	for {
		raw, err := client.ReadFrame(reader)
		if err != nil {
			if !s.stopped.Load() {
				slog.Error("subscription stream error", "error", err)
				select {
				case s.err <- fmt.Errorf("reading subscription stream: %w", err):
				default:
				}
			}
			return
		}

		kind, ev, err := parseFrame(raw)
		if err != nil {
			slog.Warn("unrecognized event; skipping", "raw", string(raw), "error", err)
			continue
		}

		switch kind {
		case frameEvent:
			slog.Debug("pushed event", "raw", string(raw))
			select {
			case s.events <- ev:
			default:
				slog.Warn("event channel full; dropping event", "raw", string(raw))
			}
		case frameAck:
			slog.Info("subscribe response", "raw", string(raw))
		default:
			slog.Warn("unrecognized frame; skipping", "raw", string(raw))
		}
	}
}
