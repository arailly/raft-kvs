package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"

	"github.com/arailly/raft-kvs/pkg/server"
	"golang.org/x/sync/errgroup"
)

func main() {
	var (
		id         string
		port       int
		raftPort   int
		leaderAddr string
	)
	flag.StringVar(&id, "id", "node", "node id")
	flag.IntVar(&port, "port", 8080, "port to listen on")
	flag.IntVar(&raftPort, "raft-port", 10000, "port to listen on")
	flag.StringVar(&leaderAddr, "leader-addr", "", "leader address")
	flag.Parse()

	var g errgroup.Group
	srv := server.NewServer(id, port, raftPort)
	g.Go(srv.Serve)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	g.Go(func() error {
		<-signalChan
		return srv.Shutdown()
	})

	if leaderAddr != "" {
		req := &server.JoinRequest{
			ID:       id,
			RaftAddr: srv.RaftAddr(),
		}
		if err := sendJoinRequest(leaderAddr, req); err != nil {
			fmt.Println(err)
			return
		}
	}

	g.Wait()
}

func sendJoinRequest(leaderAddr string, req *server.JoinRequest) error {
	reqJson, err := json.Marshal(req)
	if err != nil {
		return err
	}
	var client http.Client
	for i := 0; i < 5; i++ {
		resp, err := client.Post(
			"http://"+leaderAddr+"/join",
			"application/json",
			bytes.NewReader(reqJson),
		)
		if err != nil {
			fmt.Println("failed to request join, retrying...")
			continue
		}
		io.Copy(io.Discard, resp.Body)
		if resp.StatusCode == http.StatusOK {
			return nil
		}
		resp.Body.Close()
		fmt.Println("failed to request join, retrying...")
	}
	return fmt.Errorf("failed to request join")
}
