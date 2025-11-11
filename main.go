package main

import (
	proto "Ricart_and_Agrawala_implementation/grpc" // adjust to your generated package path
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	logFile *os.File
	logMu   sync.Mutex
	logger  *log.Logger
)

const nodeCount = 10000

var startPort = 8080

type connection struct {
	conn    *grpc.ClientConn
	stream  proto.NodeService_SendClient
	replyCh chan *proto.Reply
}

type Node struct {
	proto.UnimplementedNodeServiceServer
	mutex     sync.Mutex // lock for lamport stuff
	state     int
	timestamp int64
	id        int
	port      int

	connections map[int]*connection
	queue       []proto.NodeService_SendServer // queue of streams to reply to
}

func main() {
	// set up logging
	var err error
	logFile, err = os.OpenFile("nodes.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal(err)
	}

	flags := log.Ldate | log.Ltime | log.Lmicroseconds
	logger = log.New(logFile, "", flags)
	defer logFile.Close()

	// initial log
	logEvent(-1, 0, "Logger Initialized")

	nodes := make([]*Node, nodeCount)
	for i := 0; i < nodeCount; i++ {
		port := startPort + i
		n := &Node{
			timestamp:   0,
			port:        port,
			id:          i + 1,
			state:       0,
			connections: make(map[int]*connection),
			queue:       nil,
		}
		nodes[i] = n
		go n.startServer() // listener
		logEvent(n.id, n.state, "started listening on its port")
	}

	// allow servers and connections to be made before running
	time.Sleep(100 * time.Millisecond)

	// start connecting and run loops
	for _, n := range nodes {
		go n.run()
	}

	// block forever
	select {}
}

func (n *Node) run() {
	// POSSIBLY ISSUE: Connecting before all servers are started, can be tested with logging
	for i := startPort; i < startPort+nodeCount; i++ {
		if n.port == i {
			continue
		}
		go n.connect(i)
	}

	for {
		if n.state == 2 {
			n.setLeader()
		}

		if n.state == 0 {
			n.enter()
		}
		// another wait cycle
		time.Sleep(10 * time.Millisecond)
	}
}

func (n *Node) enter() {
	// set state to 1 for wanted and start sending requests
	n.mutex.Lock()
	n.state = 1
	n.mutex.Unlock()
	n.sendToAll()
}

func (n *Node) sendToAll() {
	n.mutex.Lock()
	msg := &proto.Request{
		Timestamp: n.timestamp,
		Port:      int64(n.port),
	}
	n.mutex.Unlock()

	// used to stop holding the actual map hostage
	n.mutex.Lock()
	connectionsCopy := make(map[int]*connection, len(n.connections))
	for k, v := range n.connections {
		connectionsCopy[k] = v
	}
	n.mutex.Unlock()

	// sends message to all connections (Notice that we take a connection from the map, reference their stream, and send to that specifically)
	for port, connection := range connectionsCopy {
		fmt.Printf("Sending request from port [%d] to port [%d]\n", n.port, port)

		if err := connection.stream.Send(msg); err != nil {
			// log failed to send
			continue
		}
		n.mutex.Lock()
		n.timestamp++
		n.mutex.Unlock()
	}

	// wait to receive all "yes"
	expected := len(connectionsCopy)
	received := 0

	for received < expected {
		for _, p := range connectionsCopy {
			select {
			case <-p.replyCh:
				received++
			default:
			}
			if received >= expected {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}

	n.mutex.Lock()
	n.state = 2
	n.mutex.Unlock()
}

// This is deceiving: its called "server stream" but what it is, is the stream that was used to send
// the request that we queued.
func (n *Node) exit() {
	n.mutex.Lock()
	for _, s := range n.queue {
		_ = s.Send(&proto.Reply{}) // name doesnt matter
	}
	n.queue = nil
	fmt.Printf("[Node %d] has exited the critical process\n", n.port)
	n.state = 0
	n.mutex.Unlock()
}

func (n *Node) setLeader() {
	fmt.Printf("[Node %d] has entered the critical process\n", n.port)
	// some critical process...
	time.Sleep(100 * time.Millisecond)
	n.exit()
}

// connects to another client with their port
func (n *Node) connect(port int) {
	addr := fmt.Sprintf("localhost:%d", port) // so its not hardcoded anymore

	for {
		if port == n.port {
			return
		} // Make sure we don't connect to our selves

		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			// logging
			time.Sleep(100 * time.Millisecond)
			continue
		}

		client := proto.NewNodeServiceClient(conn)

		// Create a bidirectional stream
		stream, err := client.Send(context.Background())
		if err != nil {
			// logging
			conn.Close()
			time.Sleep(100 * time.Millisecond)
			continue
		}

		c := &connection{
			conn:    conn,
			stream:  stream,
			replyCh: make(chan *proto.Reply, 100),
		}

		n.mutex.Lock()
		n.connections[port] = c
		n.mutex.Unlock()

		// start receiver goroutine for that connection (reads server replies)
		go n.Receive(port, c)
		return
	}
}

func (n *Node) Receive(port int, c *connection) {
	for {
		rep, err := c.stream.Recv()
		if err != nil {
			log.Printf("[Node %d] recv error from %d: %v\n", n.port, port, err)
			// connection broken -> cleanup and attempt reconnect
			n.mutex.Lock()
			if c.conn != nil {
				c.conn.Close()
			}
			delete(n.connections, port)
			n.mutex.Unlock()
			// try reconnect
			go n.connect(port)
			return
		}
		// update timestamp on receive (Lamport)
		n.mutex.Lock()
		if rep != nil {
			// example: no timestamp in Reply currently, but if it existed you'd merge here
		}
		n.mutex.Unlock()

		// push to connections's replyCh (non-blocking)
		select {
		case c.replyCh <- rep:
		default:
		}
	}
}

// Send is bidirectional
func (n *Node) Send(stream proto.NodeService_SendServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		if n.state == 2 || n.state == 1 && req.Timestamp > n.timestamp {
			n.queue = append(n.queue, stream) // we save the stream of the sender for later use
		} else {
			if err := stream.Send(&proto.Reply{}); err != nil {
				return err
			}
		}
	}
}

// Should start server for a different address each time (8080, 8081...) one for each node
// Server only handles receive and reply
func (n *Node) startServer() {
	addr := fmt.Sprintf(":%d", n.port) // so its not hardcoded anymore
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		// logging
	}

	// Makes new GRPC server
	grpcServer := grpc.NewServer()

	// Registers the grpc server with the System struct
	proto.RegisterNodeServiceServer(grpcServer, n)
	log.Printf("node %d listening on %s\n", n.port, addr)

	// this blocks ???
	err = grpcServer.Serve(listener)
	if err != nil {
		// logging
	}

	// logging like, "server started at... for node..."
}

func logEvent(nodeId int, state int, msg string) {
	logMu.Lock()
	defer logMu.Unlock()
	logger.Printf("[NODE %d | %d] %s\n", nodeId, state, msg)
}
