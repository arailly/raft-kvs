package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"

	"github.com/arailly/raft-kvs/pkg/kvs"
)

type Server struct {
	addr       string
	raftAddr   string
	httpServer *http.Server
	service    *kvs.KeyValueStore
}

func NewServer(id string, port int, raftPort int) *Server {
	addr := "127.0.0.1:" + strconv.Itoa(port)
	raftAddr := "127.0.0.1:" + strconv.Itoa(raftPort)

	httpServer := &http.Server{}
	service := kvs.NewKeyValueStore(id, addr)
	return &Server{
		addr:       addr,
		raftAddr:   raftAddr,
		httpServer: httpServer,
		service:    service,
	}
}

func (s *Server) Addr() string {
	return s.addr
}

func (s *Server) RaftAddr() string {
	return s.raftAddr
}

func (s *Server) Serve() error {
	raftListener, err := net.Listen("tcp", s.raftAddr)
	if err != nil {
		return err
	}
	s.service.Setup(raftListener)

	appListener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			s.handleGet(w, r)
		case http.MethodPut:
			s.handlePut(w, r)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	http.HandleFunc("/join", func(w http.ResponseWriter, r *http.Request) {
		fmt.Println(r)
		switch r.Method {
		case http.MethodPost:
			s.handleJoin(w, r)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	return s.httpServer.Serve(appListener)
}

func (s *Server) Shutdown() error {
	if err := s.service.Shutdown(); err != nil {
		return err
	}
	return s.httpServer.Shutdown(context.Background())
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path[1:]
	value, err := s.service.Get(key)
	slog.Info("Get", "key", key, "value", value)
	if err != nil {
		if errors.Is(err, kvs.ErrNotFound) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		slog.Error("Get", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write(value)
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path[1:]
	value, err := io.ReadAll(r.Body)
	if err != nil {
		slog.Error("Put", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	slog.Info("Put", "key", key, "value", value)
	err = s.service.Set(key, value)
	if err != nil {
		slog.Error("Put", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

type JoinRequest struct {
	ID       string `json:"id"`
	RaftAddr string `json:"raft_addr"`
}

func (s *Server) handleJoin(w http.ResponseWriter, r *http.Request) {
	var req JoinRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		slog.Error("Join", "error", err, "request", req)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	err = s.service.Join(req.ID, req.RaftAddr)
	if err != nil {
		slog.Error("Join", "error", err, "request", req)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}
