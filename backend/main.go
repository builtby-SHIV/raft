package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"raft/proto"
	"raft/raft"
	"raft/storage"
)

type Response struct {
	Message string
}

type AddRequest struct {
	Key string
	Val string
}

type NonAddRequest struct {
	Key string
}

type Server struct {
	ns *raft.NodeState
}

const commandTimeout = 10 * time.Second

type ServerConfig struct {
	NodeID   uint64
	HTTPAddr string
	TCPAddr  string
	Role     string
}

func parseRole(s string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "leader":
		return raft.RoleLeader, nil
	case "follower":
		return raft.RoleFollower, nil
	case "candidate":
		return raft.RoleCandidate, nil
	case "observer":
		return raft.RoleFollower, nil
	case "unknown", "":
		return raft.RoleUnknown, nil
	default:
		return raft.RoleUnknown, fmt.Errorf("unknown role %q: must be one of leader, follower, observer", s)
	}
}

func (s *Server) add(w http.ResponseWriter, r *http.Request) {
	var req AddRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.Key) == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}
	if s.ns == nil {
		http.Error(w, "node is not initialized", http.StatusServiceUnavailable)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), commandTimeout)
	defer cancel()

	err := s.ns.Apply(ctx, &raft.ClientCmd{Command: proto.Command_PUT, Key: req.Key, Value: req.Val})
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, raft.ErrNotLeader) {
			status = http.StatusServiceUnavailable
		}
		http.Error(w, err.Error(), status)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(Response{Message: "Key-Val pair created"})
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	var req NonAddRequest
	if key := r.URL.Query().Get("key"); key != "" {
		req.Key = key
	} else if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}
	if req.Key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}
	if s.ns == nil {
		http.Error(w, "node is not initialized", http.StatusServiceUnavailable)
		return
	}

	value, ok := s.ns.Get(req.Key)
	w.Header().Set("Content-Type", "application/json")
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"key": req.Key, "error": "key not found"})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]string{"key": req.Key, "value": value})
}

func (s *Server) delete(w http.ResponseWriter, r *http.Request) {
	var req NonAddRequest
	if key := r.URL.Query().Get("key"); key != "" {
		req.Key = key
	} else if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}
	if req.Key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}
	if s.ns == nil {
		http.Error(w, "node is not initialized", http.StatusServiceUnavailable)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), commandTimeout)
	defer cancel()

	err := s.ns.Apply(ctx, &raft.ClientCmd{Command: proto.Command_DELETE, Key: req.Key})
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, raft.ErrNotLeader) {
			status = http.StatusServiceUnavailable
		}
		http.Error(w, err.Error(), status)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"msg": "Key successfully deleted"})
}

// state exposes a read-only snapshot of the consensus state for the
// visualization UI. It never mutates the node.
func (s *Server) state(w http.ResponseWriter, r *http.Request) {
	if s.ns == nil {
		http.Error(w, "node is not initialized", http.StatusServiceUnavailable)
		return
	}
	limit := 250
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil {
			limit = parsed
		}
	}
	if limit < 0 {
		limit = 0
	}
	if limit > 5000 {
		limit = 5000
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(s.ns.SnapshotState(limit))
}

// events returns the node's recent consensus events as JSON.
func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	if s.ns == nil {
		http.Error(w, "node is not initialized", http.StatusServiceUnavailable)
		return
	}
	var since uint64
	if raw := r.URL.Query().Get("since"); raw != "" {
		if parsed, err := strconv.ParseUint(raw, 10, 64); err == nil {
			since = parsed
		}
	}
	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"events": s.ns.RecentEvents(since, limit),
	})
}

func normalizeAddr(addr string) (string, string, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return "", "", errors.New("empty address")
	}
	if p, err := strconv.Atoi(addr); err == nil && p > 0 && p <= 65535 {
		return ":" + addr, addr, nil
	}
	if strings.Contains(addr, ":") {
		host, port, err := net.SplitHostPort(addr)
		if err == nil && port != "" {
			if p, err := strconv.Atoi(port); err == nil && p > 0 && p <= 65535 {
				if host == "" {
					return ":" + port, port, nil
				}
				return addr, port, nil
			}
		}
		last := strings.LastIndex(addr, ":")
		if last != -1 && last < len(addr)-1 {
			candidate := addr[last+1:]
			if p, err := strconv.Atoi(candidate); err == nil && p > 0 && p <= 65535 {
				return addr, candidate, nil
			}
		}
	}
	return "", "", fmt.Errorf("invalid address or port %q", addr)
}

func extractPort(addr string) (string, error) {
	_, port, err := normalizeAddr(addr)
	return port, err
}

func parsePort(addr string) (int, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return 0, nil
	}
	portStr, err := extractPort(addr)
	if err != nil {
		return 0, err
	}
	p, err := strconv.Atoi(portStr)
	if err != nil {
		return 0, err
	}
	return p, nil
}

func resolveConfig(
	idFlag,
	httpFlag,
	tcpFlag,
	roleFlag string) (*ServerConfig, error) {
	if strings.TrimSpace(idFlag) == "" {
		return nil, errors.New("--id is required")
	}
	nodeID, err := strconv.ParseUint(strings.TrimSpace(idFlag), 10, 64)
	if err != nil || nodeID == 0 {
		return nil, fmt.Errorf("invalid --id=%q: must be a positive integer", idFlag)
	}

	if strings.TrimSpace(httpFlag) == "" {
		return nil, errors.New("--http is required")
	}
	normHTTP, _, err := normalizeAddr(httpFlag)
	if err != nil {
		return nil, fmt.Errorf("invalid --http=%q: %w", httpFlag, err)
	}

	if strings.TrimSpace(tcpFlag) == "" {
		return nil, errors.New("--tcp is required")
	}
	normTCP, _, err := normalizeAddr(tcpFlag)
	if err != nil {
		return nil, fmt.Errorf("invalid --tcp=%q: %w", tcpFlag, err)
	}

	if strings.TrimSpace(roleFlag) == "" {
		return nil, errors.New("--role is required")
	}
	role, err := parseRole(roleFlag)
	if err != nil {
		return nil, fmt.Errorf("invalid --role=%q: %w", roleFlag, err)
	}

	return &ServerConfig{
		NodeID:   nodeID,
		HTTPAddr: normHTTP,
		TCPAddr:  normTCP,
		Role:     role,
	}, nil
}

func main() {
	idFlag := flag.String("id", "", "Node identifier")
	httpFlag := flag.String("http", "", "HTTP listen address")
	tcpFlag := flag.String("tcp", "", "TCP listen address")
	roleFlag := flag.String("role", "", "Node role: leader | follower | observer")
	flag.Parse()

	cfg, err := resolveConfig(*idFlag, *httpFlag, *tcpFlag, *roleFlag)
	if err != nil {
		log.Fatalf("[startup] %v", err)
	}

	baseDir := storage.DefaultStorageBaseDir()
	processDir, err := storage.ProcessStorageDir(baseDir, cfg.NodeID, cfg.TCPAddr)
	if err != nil {
		log.Fatalf("[startup] resolve storage directory: %v", err)
	}
	layout := storage.NewStorageLayout(processDir)
	if err := layout.Ensure(); err != nil {
		log.Fatalf("[startup] %v", err)
	}

	procCfg, err := storage.LoadOrInitProcessConfig(layout.Root, *httpFlag, *tcpFlag)
	if err != nil {
		log.Fatalf("[startup] load or init process config: %v", err)
	}

	log.Printf("[node] id=%d  role=%s  http=%s  tcp=%s  term=%d  votedFor=%d  storage=%s",
		cfg.NodeID, cfg.Role, cfg.HTTPAddr, cfg.TCPAddr, procCfg.CurrentTerm, procCfg.VotedFor, layout.Root)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	raftState := &proto.RaftState{Term: procCfg.CurrentTerm}
	if procCfg.VotedFor > 0 {
		raftState.VotedFor = strconv.FormatInt(procCfg.VotedFor, 10)
	}

	ns := raft.NewNodeState(cfg.NodeID, cfg.HTTPAddr, cfg.TCPAddr, layout.Root, cfg.Role, ctx, raftState)
	if ns == nil {
		log.Fatalf("[startup] failed to initialize node state in %s", layout.Root)
	}
	go ns.Start()
	defer func() {
		if err := ns.Close(); err != nil {
			log.Printf("[node] close: %v", err)
		}
	}()

	var httpServer *http.Server

	if cfg.HTTPAddr != "" {
		srv := &Server{ns: ns}
		mux := http.NewServeMux()

		mux.HandleFunc("POST /put", srv.add)
		mux.HandleFunc("GET /get", srv.get)
		mux.HandleFunc("DELETE /del", srv.delete)
		mux.HandleFunc("GET /state", srv.state)
		mux.HandleFunc("GET /events", srv.events)

		httpServer = &http.Server{Addr: cfg.HTTPAddr, Handler: mux}
		go func() {
			if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Fatalf("[http] %v", err)
			}
		}()
		log.Printf("[http] listening on %s", cfg.HTTPAddr)
	}

	<-ctx.Done()

	log.Println("[node] shutting down…")
	if httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(ctx)
	}
}
