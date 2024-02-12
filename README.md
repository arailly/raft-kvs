# An Implementation Example of Distributed Key-Value Store with [hashicorp/raft](https://github.com/hashicorp/raft)

## Usage

### Build

```bash
make
```

### Start Server

```bash
# leader
./kvs-server --id node0 --port 8080 --raft-port 10000

# follower 1
./kvs-server --id node1 --port 8081 --raft-port 10001 -
-leader-addr 127.0.0.1:8080

# follower 2
./kvs-server --id node2 --port 8082 --raft-port 10002 
--leader-addr 127.0.0.1:8080
```

### Add/Get Value

```bash
# Add
curl -X PUT -d bar 127.0.0.1:8080/foo

# Get
curl 127.0.0.1:8080/foo
```
