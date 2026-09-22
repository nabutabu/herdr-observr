package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// stubSockPath returns a Unix socket path short enough for macOS's 104-byte
// sockaddr_un.sun_path limit. t.TempDir() embeds the full test name and can
// alone exceed the limit on macOS runners, so use a short random-named dir.
func stubSockPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "herdr-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return filepath.Join(dir, "h.sock")
}

func startStubServer(t *testing.T, handler func(*bufio.Reader, net.Conn) error) string {
	t.Helper()
	sockPath := stubSockPath(t)
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		if err := handler(bufio.NewReader(conn), conn); err != nil {
			t.Errorf("stub server: %v", err)
		}
	}()

	return sockPath
}

func setSocketPath(t *testing.T, path string) {
	t.Helper()
	t.Setenv("HERDR_SOCKET_PATH", path)
}

func TestWriteFrame(t *testing.T) {
	var buf bytes.Buffer
	req := &Request{ID: "herdr-observr", Method: "ping", Params: map[string]any{"k": "v"}}
	if err := WriteFrame(&buf, req); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}

	got := buf.Bytes()
	if len(got) == 0 || got[len(got)-1] != '\n' {
		t.Errorf("frame not newline-terminated: %q", got)
	}

	var parsed Request
	if err := json.Unmarshal(TrimNewline(got), &parsed); err != nil {
		t.Fatalf("unmarshal frame: %v", err)
	}
	if parsed.ID != "herdr-observr" || parsed.Method != "ping" {
		t.Errorf("unexpected frame: %+v", parsed)
	}
	if parsed.Params["k"] != "v" {
		t.Errorf("unexpected params: %v", parsed.Params)
	}
}

func TestReadFrame(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("{\"result\":\"pong\"}\n{\"result\":\"second\"}\n"))

	first, err := ReadFrame(r)
	if err != nil {
		t.Fatalf("first ReadFrame: %v", err)
	}
	if string(first) != `{"result":"pong"}` {
		t.Errorf("first frame = %q", first)
	}

	second, err := ReadFrame(r)
	if err != nil {
		t.Fatalf("second ReadFrame: %v", err)
	}
	if string(second) != `{"result":"second"}` {
		t.Errorf("second frame = %q", second)
	}
}

func TestReadFrameCRLF(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("{\"result\":\"pong\"}\r\n"))

	got, err := ReadFrame(r)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if string(got) != `{"result":"pong"}` {
		t.Errorf("frame = %q", got)
	}
}

func TestReadFrameEOFBeforeNewline(t *testing.T) {
	r := bufio.NewReader(strings.NewReader(`{"result":"partial"`))

	if _, err := ReadFrame(r); err == nil {
		t.Fatal("expected error on unterminated frame")
	}
}

func TestCallPing(t *testing.T) {
	sock := startStubServer(t, func(r *bufio.Reader, conn net.Conn) error {
		line, err := r.ReadBytes('\n')
		if err != nil {
			return err
		}
		var req Request
		if err := json.Unmarshal(TrimNewline(line), &req); err != nil {
			return fmt.Errorf("parsing request: %w", err)
		}
		if req.ID != "herdr-observr" || req.Method != "ping" {
			return fmt.Errorf("unexpected request: %+v", req)
		}
		_, err = conn.Write([]byte(`{"id":"herdr-observr","result":"pong"}` + "\n"))
		return err
	})
	setSocketPath(t, sock)

	raw, err := Call(context.Background(), "ping", map[string]any{})
	if err != nil {
		t.Fatalf("Call: %v", err)
	}

	var result string
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatalf("unmarshal result %q: %v", raw, err)
	}
	if result != "pong" {
		t.Errorf("result = %q, want %q", result, "pong")
	}
}

func TestCallRPCError(t *testing.T) {
	sock := startStubServer(t, func(r *bufio.Reader, conn net.Conn) error {
		if _, err := r.ReadBytes('\n'); err != nil {
			return err
		}
		_, err := conn.Write([]byte(`{"id":"herdr-observr","error":{"code":"MethodNotFound","message":"no such method: ping"}}` + "\n"))
		return err
	})
	setSocketPath(t, sock)

	raw, err := Call(context.Background(), "ping", map[string]any{})
	if raw != nil {
		t.Errorf("raw = %q, want nil", raw)
	}
	rpcErr, ok := err.(*RPCError)
	if !ok {
		t.Fatalf("err = %T (%v), want *RPCError", err, err)
	}
	if rpcErr.Code != "MethodNotFound" || rpcErr.Message != "no such method: ping" {
		t.Errorf("unexpected RPCError: %v", rpcErr)
	}
}

func TestCallMalformedJSON(t *testing.T) {
	sock := startStubServer(t, func(r *bufio.Reader, conn net.Conn) error {
		if _, err := r.ReadBytes('\n'); err != nil {
			return err
		}
		_, err := conn.Write([]byte("this is not json\n"))
		return err
	})
	setSocketPath(t, sock)

	if _, err := Call(context.Background(), "ping", map[string]any{}); err == nil {
		t.Fatal("expected error for malformed response")
	} else if !strings.Contains(err.Error(), "parsing response") {
		t.Errorf("error = %v, want parse context", err)
	}
}

func TestCallMissingSocket(t *testing.T) {
	setSocketPath(t, stubSockPath(t))

	_, err := Call(context.Background(), "ping", map[string]any{})
	if err == nil {
		t.Fatal("expected error for missing socket")
	}
	if !strings.Contains(err.Error(), "dialing unix socket") {
		t.Errorf("error = %v, want dial context", err)
	}
}

func TestCallContextTimeout(t *testing.T) {
	sock := startStubServer(t, func(r *bufio.Reader, conn net.Conn) error {
		if _, err := r.ReadBytes('\n'); err != nil {
			return err
		}
		time.Sleep(200 * time.Millisecond)
		// Client has hung up by now; ignore the write error.
		conn.Write([]byte(`{"id":"herdr-observr","result":"pong"}` + "\n"))
		return nil
	})
	setSocketPath(t, sock)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if _, err := Call(ctx, "ping", map[string]any{}); err == nil {
		t.Fatal("expected timeout error")
	}
}
