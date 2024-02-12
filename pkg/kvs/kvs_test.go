package kvs_test

import (
	"net"
	"testing"
	"time"

	"github.com/arailly/raft-kvs/pkg/kvs"
	"github.com/stretchr/testify/require"
)

func setup(t *testing.T) (
	*kvs.KeyValueStore,
	*kvs.KeyValueStore,
	*kvs.KeyValueStore,
) {
	leaderAddr := "127.0.0.1:10001"
	leader := kvs.NewKeyValueStore("leader", leaderAddr)
	ln, err := net.Listen("tcp", leaderAddr)
	require.NoError(t, err)
	err = leader.Setup(ln)
	require.NoError(t, err)

	gotLeaderShip := <-leader.LeaderCh()
	require.True(t, gotLeaderShip)

	followerAddr1 := "127.0.0.1:10002"
	follower1 := kvs.NewKeyValueStore("follower1", followerAddr1)
	ln, err = net.Listen("tcp", followerAddr1)
	require.NoError(t, err)
	err = follower1.Setup(ln)
	require.NoError(t, err)
	err = leader.Join("follower1", follower1.Addr())
	require.NoError(t, err)

	followerAddr2 := "127.0.0.1:10003"
	follower2 := kvs.NewKeyValueStore("follower2", followerAddr2)
	ln, err = net.Listen("tcp", followerAddr2)
	require.NoError(t, err)
	err = follower2.Setup(ln)
	require.NoError(t, err)
	err = leader.Join("follower2", follower2.Addr())
	require.NoError(t, err)

	return leader, follower1, follower2
}

func TestSetAndGet(t *testing.T) {
	leader, _, _ := setup(t)

	key := "foo"
	value := []byte("bar")
	err := leader.Set(key, value)
	require.NoError(t, err)

	actual, err := leader.Get(key)
	require.NoError(t, err)
	require.Equal(t, value, actual)
}

func TestFailover(t *testing.T) {
	leader, follower1, follower2 := setup(t)

	key := "foo"
	value := "bar"
	err := leader.Set(key, []byte(value))
	require.NoError(t, err)

	// Kill the leader
	err = leader.Shutdown()
	require.NoError(t, err)

	// Wait for a new leader to be elected
	var newLeader *kvs.KeyValueStore
	select {
	case promote := <-follower1.LeaderCh():
		if promote {
			newLeader = follower1
		}
	case promote := <-follower2.LeaderCh():
		if promote {
			newLeader = follower2
		}
	}
	require.NotNil(t, newLeader)

	require.Eventually(t, func() bool {
		actual, err := newLeader.Get(key)
		if err != nil {
			return false
		}
		return string(actual) == value
	}, 1*time.Second, 100*time.Millisecond)
}
