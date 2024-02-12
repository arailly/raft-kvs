package kvs

import (
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"time"

	"github.com/arailly/raft-kvs/pkg/repository"
	"github.com/hashicorp/raft"
)

const (
	raftTimeout = 1 * time.Second
)

var (
	ErrNotFound  = errors.New("not found")
	ErrNotLeader = errors.New("not leader")
)

// KeyValueStore is a key-value store service.
// KeyValueStore meets the raft.FSM interface.
type KeyValueStore struct {
	id         string
	addr       string
	repository *repository.Repository
	raft       *raft.Raft
}

var _ raft.FSM = (*KeyValueStore)(nil)

func NewKeyValueStore(id string, addr string) *KeyValueStore {
	return &KeyValueStore{
		repository: repository.NewRepository(),
		id:         id,
	}
}

func (s *KeyValueStore) Setup(ln net.Listener) error {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(s.id)
	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()
	snapshotStore := raft.NewInmemSnapshotStore()

	config.HeartbeatTimeout = 500 * time.Millisecond
	config.ElectionTimeout = 500 * time.Millisecond
	config.LeaderLeaseTimeout = 500 * time.Millisecond
	config.CommitTimeout = 100 * time.Millisecond

	s.addr = ln.Addr().String()
	transport := raft.NewNetworkTransport(
		NewStreamLayer(ln),
		10,
		5*time.Second,
		os.Stdout,
	)

	raft_, err := raft.NewRaft(
		config,
		s,
		logStore,
		stableStore,
		snapshotStore,
		transport,
	)
	if err != nil {
		return err
	}

	hasState, err := raft.HasExistingState(
		logStore,
		stableStore,
		snapshotStore,
	)
	if err != nil {
		return err
	}
	if !hasState {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		raft_.BootstrapCluster(configuration)
	}
	s.raft = raft_
	return nil
}

func (s *KeyValueStore) Addr() string {
	return s.addr
}

func (s *KeyValueStore) Get(key string) ([]byte, error) {
	if s.raft.State() != raft.Leader {
		return nil, ErrNotLeader
	}
	value, err := s.repository.Get(key)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return value, nil
}

func (s *KeyValueStore) Set(key string, value []byte) error {
	if s.raft.State() != raft.Leader {
		return ErrNotLeader
	}
	s.repository.Set(key, value)

	data, err := json.Marshal(raftLogData{
		Key:   key,
		Value: value,
	})
	if err != nil {
		return err
	}
	f := s.raft.Apply(data, raftTimeout)
	return f.Error()
}

func (s *KeyValueStore) Join(id string, addr string) error {
	if s.raft.State() != raft.Leader {
		return ErrNotLeader
	}
	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}
	serverID := raft.ServerID(id)
	serverAddr := raft.ServerAddress(addr)
	for _, server := range configFuture.Configuration().Servers {
		if server.ID == raft.ServerID(id) ||
			server.Address == raft.ServerAddress(addr) {
			if server.ID == serverID && server.Address == serverAddr {
				return nil
			}
			removeFuture := s.raft.RemoveServer(serverID, 0, 0)
			if err := removeFuture.Error(); err != nil {
				return err
			}
		}
	}
	addFuture := s.raft.AddVoter(serverID, serverAddr, 0, 0)
	if err := addFuture.Error(); err != nil {
		return err
	}
	return nil
}

func (s *KeyValueStore) Leave(id string) error {
	if s.raft.State() != raft.Leader {
		return ErrNotLeader
	}
	serverID := raft.ServerID(id)
	removeFuture := s.raft.RemoveServer(serverID, 0, 0)
	if err := removeFuture.Error(); err != nil {
		return err
	}
	return nil
}

func (s *KeyValueStore) LeaderCh() <-chan bool {
	return s.raft.LeaderCh()
}

func (s *KeyValueStore) Role() string {
	return s.raft.State().String()
}

func (s *KeyValueStore) IsLeader() bool {
	return s.raft.State() == raft.Leader
}

func (s *KeyValueStore) Shutdown() error {
	return s.raft.Shutdown().Error()
}

// Implement raft.FSM interface

func (s *KeyValueStore) Apply(log *raft.Log) any {
	var data raftLogData
	err := json.Unmarshal(log.Data, &data)
	if err != nil {
		return err
	}
	s.repository.Set(data.Key, data.Value)
	return nil
}

type raftLogData struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

func (s *KeyValueStore) Snapshot() (raft.FSMSnapshot, error) {
	return &snapshot{
		repository: s.repository,
	}, nil
}

func (s *KeyValueStore) Restore(sink io.ReadCloser) error {
	defer sink.Close()
	var data map[string][]byte
	err := json.NewDecoder(sink).Decode(&data)
	if err != nil {
		return err
	}
	s.repository.Restore(data)
	return nil
}

// Implement raft.FSMSnapshot interface

type snapshot struct {
	repository *repository.Repository
}

var _ raft.FSMSnapshot = (*snapshot)(nil)

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		err := json.NewEncoder(sink).Encode(s.repository.Dump())
		if err != nil {
			return err
		}
		return sink.Close()
	}()
	if err != nil {
		sink.Cancel()
		return err
	}
	return nil
}

func (s *snapshot) Release() {}

type streamLayer struct {
	ln net.Listener
}

var _ raft.StreamLayer = (*streamLayer)(nil)

func NewStreamLayer(ln net.Listener) *streamLayer {
	return &streamLayer{
		ln: ln,
	}
}

func (s *streamLayer) Dial(
	address raft.ServerAddress,
	timeout time.Duration,
) (net.Conn, error) {
	return net.DialTimeout("tcp", string(address), timeout)
}

func (s *streamLayer) Accept() (net.Conn, error) {
	return s.ln.Accept()
}

func (s *streamLayer) Close() error {
	return s.ln.Close()
}

func (s *streamLayer) Addr() net.Addr {
	return s.ln.Addr()
}
