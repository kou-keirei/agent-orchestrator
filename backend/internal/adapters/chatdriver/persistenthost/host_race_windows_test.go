//go:build windows

package persistenthost

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"testing"
	"time"
)

// Exercise the stale-probe boundary without POSIX shells or process groups.
// Prepare stops each observer before spawning; the test publishes a winner
// between the two probe responses and verifies its native ownership files stay
// intact. The POSIX companion additionally exercises real competing launches.
func TestConnectOrStartWindowsStaleProbePreservesReplacementOwnership(t *testing.T) {
	dataDir := t.TempDir()
	sessionID := "windows-stale-race"
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	stale := Descriptor{
		Version: ProtocolVersion, SessionID: sessionID, Address: ln.Addr().String(),
		Token: "stale", PID: 2147483647, StartedAt: time.Now().UTC(),
	}
	if err := writeDescriptor(dataDir, stale); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	accepted := make(chan net.Conn, 2)
	go func() {
		for i := 0; i < 2; i++ {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
			var req hello
			if json.NewDecoder(conn).Decode(&req) != nil {
				_ = conn.Close()
				return
			}
			accepted <- conn
		}
	}()
	stopBeforeSpawn := errors.New("fixture stops before provider launch")
	cfg := Config{SessionID: sessionID, DataDir: dataDir, Workdir: t.TempDir(),
		Prepare: func(context.Context) (PreparedProvider, error) {
			return PreparedProvider{}, stopBeforeSpawn
		}}
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			_, err := ConnectOrStart(ctx, cfg)
			results <- err
		}()
	}
	probes := make([]net.Conn, 0, 2)
	for i := 0; i < 2; i++ {
		select {
		case conn := <-accepted:
			t.Cleanup(func() { _ = conn.Close() })
			probes = append(probes, conn)
		case <-ctx.Done():
			t.Fatal("both stale probes did not reach handshake")
		}
	}
	releaseProbe := func(conn net.Conn) {
		t.Helper()
		if err := json.NewEncoder(conn).Encode(helloResponse{Error: ErrUnauthorized.Error()}); err != nil {
			t.Fatal(err)
		}
		_ = conn.Close()
		select {
		case err := <-results:
			if !errors.Is(err, stopBeforeSpawn) {
				t.Fatalf("stale observer: %v, want fixture stop before spawn", err)
			}
		case <-ctx.Done():
			t.Fatal("stale observer did not finish")
		}
	}
	releaseProbe(probes[0])

	releaseLock, err := acquireHostLock(dataDir, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(releaseLock)
	winner := stale
	winner.PID = os.Getpid()
	winner.Token = "replacement"
	if err := writeDescriptor(dataDir, winner); err != nil {
		t.Fatal(err)
	}
	lock, _ := lockPath(dataDir, sessionID)
	before, err := os.ReadFile(lock)
	if err != nil {
		t.Fatal(err)
	}
	releaseProbe(probes[1])
	if got, err := readDescriptor(dataDir, sessionID); err != nil || got != winner {
		t.Fatalf("replacement descriptor changed: got %+v, err %v, want %+v", got, err, winner)
	}
	if after, err := os.ReadFile(lock); err != nil || string(after) != string(before) {
		t.Fatalf("replacement lock changed: got %q, err %v, want %q", after, err, before)
	}
	if release, err := acquireHostLock(dataDir, sessionID); !errors.Is(err, ErrHostExists) {
		if release != nil {
			release()
		}
		t.Fatalf("rival lock acquisition: %v, want ErrHostExists", err)
	}
}
