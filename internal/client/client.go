package client

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"time"
)

const WRITE_DEADLINE = 10 * time.Second

func SocketPath() string {
	return os.Getenv("HERDR_SOCKET_PATH")
}

func Call(ctx context.Context, method string, params map[string]any) (json.RawMessage, error) {
	conn, err := Dial()
	if err != nil {
		return nil, fmt.Errorf("dialing unix socket %s: %w", SocketPath(), err)
	}
	defer conn.Close()

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(WRITE_DEADLINE)
	}
	conn.SetDeadline(deadline)

	if err := WriteFrame(conn, &Request{
		ID:     "herdr-observr",
		Method: method,
		Params: params,
	}); err != nil {
		return nil, fmt.Errorf("writing request: %w", err)
	}

	raw, err := ReadFrame(bufio.NewReader(conn))
	if err != nil {
		return nil, fmt.Errorf("reading response: %w", err)
	}

	var resp Response
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("parsing response %q: %w", string(raw), err)
	}

	if resp.Error != nil {
		return nil, resp.Error
	}

	return resp.Result, nil
}

func Dial() (net.Conn, error) {
	path := SocketPath()
	if path == "" {
		return nil, fmt.Errorf("HERDR_SOCKET_PATH environment variable is not set")
	}
	return net.Dial("unix", path)
}
