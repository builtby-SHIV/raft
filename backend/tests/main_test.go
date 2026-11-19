package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"raft/raft"
)

func freePortAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to reserve port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// TestPutHandlerCommitsWithoutPanic guards the path that previously panicked:
// POST /put dereferenced a nil NodeState and then read its own commit
// notification from the shared request channel.
func TestPutHandlerCommitsWithoutPanic(t *testing.T) {
	peers := []string{freePortAddr(t), freePortAddr(t), freePortAddr(t)}

	oldPeers := raft.DefaultPeers
	raft.DefaultPeers = peers
	defer func() { raft.DefaultPeers = oldPeers }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodes := make([]*raft.NodeState, 0, len(peers))
	for i, addr := range peers {
		role := raft.RoleFollower
		if i == 0 {
			role = raft.RoleLeader
		}
		ns := raft.NewNodeState(uint64(i+1), ":0", addr, t.TempDir(), role, ctx, nil)
		if ns == nil {
			t.Fatalf("failed to initialize node %d", i+1)
		}
		go ns.Start()
		nodes = append(nodes, ns)
	}
	defer func() {
		for _, ns := range nodes {
			_ = ns.Close()
		}
	}()

	srv := &Server{ns: nodes[0]}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /put", srv.add)
	mux.HandleFunc("GET /get", srv.get)
	mux.HandleFunc("DELETE /del", srv.delete)

	ts := httptest.NewServer(mux)
	defer ts.Close()

	client := &http.Client{Timeout: 15 * time.Second}

	resp, err := client.Post(ts.URL+"/put", "application/json", strings.NewReader(`{"Key":"alpha","Val":"beta"}`))
	if err != nil {
		t.Fatalf("POST /put: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST /put status = %d, want %d", resp.StatusCode, http.StatusCreated)
	}

	getResp, err := client.Get(ts.URL + "/get?key=alpha")
	if err != nil {
		t.Fatalf("GET /get: %v", err)
	}
	body := struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}{}
	if err := json.NewDecoder(getResp.Body).Decode(&body); err != nil {
		t.Fatalf("decode GET /get: %v", err)
	}
	_ = getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK || body.Value != "beta" {
		t.Fatalf("GET /get status = %d, value = %q", getResp.StatusCode, body.Value)
	}

	delReq, err := http.NewRequest(http.MethodDelete, ts.URL+"/del?key=alpha", nil)
	if err != nil {
		t.Fatalf("build DELETE: %v", err)
	}
	delResp, err := client.Do(delReq)
	if err != nil {
		t.Fatalf("DELETE /del: %v", err)
	}
	_ = delResp.Body.Close()
	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE /del status = %d, want %d", delResp.StatusCode, http.StatusOK)
	}

	missingResp, err := client.Get(ts.URL + "/get?key=alpha")
	if err != nil {
		t.Fatalf("GET /get after delete: %v", err)
	}
	_ = missingResp.Body.Close()
	if missingResp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET /get after delete status = %d, want %d", missingResp.StatusCode, http.StatusNotFound)
	}
}

func TestHandlersRejectMissingNode(t *testing.T) {
	srv := &Server{}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /put", srv.add)
	mux.HandleFunc("GET /get", srv.get)
	mux.HandleFunc("DELETE /del", srv.delete)

	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/put", "application/json", strings.NewReader(`{"Key":"alpha","Val":"beta"}`))
	if err != nil {
		t.Fatalf("POST /put: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("POST /put status = %d, want %d", resp.StatusCode, http.StatusServiceUnavailable)
	}
}
