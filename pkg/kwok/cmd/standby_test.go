/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cmd

import (
	"context"
	"io"
	"net"
	"net/http"
	"testing"
)

func freeAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	return address
}

func TestServeStandbyHealthz(t *testing.T) {
	address := freeAddress(t)
	stop, err := serveStandbyHealthz(context.Background(), address)
	if err != nil {
		t.Fatalf("serveStandbyHealthz() error = %v", err)
	}

	for _, path := range []string{"/healthz", "/readyz", "/livez"} {
		resp, err := http.Get("http://" + address + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK || string(body) != "ok" {
			t.Errorf("GET %s = %d %q, want 200 \"ok\"", path, resp.StatusCode, body)
		}
	}

	resp, err := http.Get("http://" + address + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("GET /metrics = %d, want 404 on a standby", resp.StatusCode)
	}

	stop()
	stop()

	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("address still bound after stop: %v", err)
	}
	_ = listener.Close()
}

func TestServeStandbyHealthzStopRightAfterStart(t *testing.T) {
	address := freeAddress(t)
	stop, err := serveStandbyHealthz(context.Background(), address)
	if err != nil {
		t.Fatalf("serveStandbyHealthz() error = %v", err)
	}
	stop()

	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("address still bound after stop: %v", err)
	}
	_ = listener.Close()
}

func TestServeStandbyHealthzReportsBindErrors(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()

	_, err = serveStandbyHealthz(context.Background(), listener.Addr().String())
	if err == nil {
		t.Fatal("serveStandbyHealthz() succeeded on an address already in use")
	}
}
