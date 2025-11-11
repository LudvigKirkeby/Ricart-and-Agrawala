package main

import (
	proto "Ricart-and-Agrawala/grpc" // adjust to your generated package path
	"context"
	"log"

	//"context"
	//"log"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"net"
	"sync"
)

//TIP <p>To run your code, right-click the code and select <b>Run</b>.</p> <p>Alternatively, click
// the <icon src="AllIcons.Actions.Execute"/> icon in the gutter and select the <b>Run</b> menu item from here.</p>

// used for each server for each node
// this is a server
type Node struct {
	proto.UnimplementedNodeServiceServer

	mutex     sync.Mutex // lock for lamport stuff
	state     int
	timestamp int64
	id        int
	port      int

	node_map map[int]proto.NodeService_SendClient
	queue    []proto.NodeService_SendServer // queue of streams to reply to
}

var node_count = 5
var start_port = 8080

func main() {
	for i := start_port; i < node_count+start_port; i++ {
		new_node := Node{
			timestamp: 0,
			port:      i,
			id:        i - (start_port - 1), // we want id to start at 1
			state:     0,
			node_map:  make(map[int]proto.NodeService_SendClient),
			queue:     []proto.NodeService_SendServer{},
		}
		go new_node.run()
	}
}

func (n *Node) run() {
	go n.startServer(n.port)
	// POSSIBLY ISSUE: Connecting before all servers are started, can be tested with logging
	for i := start_port; i < node_count+start_port; i++ {
		if n.port == i {
			continue
		}
		go n.connect(i)
	}

	n.state = 0

	if n.state == 2 {
		n.setLeader()
	}

	n.enter()

}

func (n *Node) enter() {
	// set state to 1 for wanted and start sending requests
	n.state = 1
	n.sendToAll()
}

func (n *Node) sendToAll() {

	msg := &proto.Request{
		Timestamp: n.timestamp,
		Port:      n.port,
	}

	// sends message to all other nodes
	for port, stream := range n.node_map {
		fmt.Printf("Sending request from port [%d] to port [%d]", n.port, port)

		if err := stream.Send(msg); err != nil {
			// log failed to send
		}

		n.mutex.Lock()
		n.timestamp++
		n.mutex.Unlock()
	}

	// wait to receive all "yes"
	replyCh := make(chan *proto.Reply, len(n.node_map))

	for port, stream := range n.node_map {
		go func(p int, s proto.NodeService_SendClient) {
			for {
				rep, err := s.Recv() // block until a reply comes
				if err != nil {
					log.Printf("[node %d] error receiving from %d: %v", n.port, p, err)
					return
				}
				replyCh <- rep
			}
		}(port, stream) // ??? Kinda confused
	}

	// wait for replies from all peers (exclude self if present in node_map)
	expectedReplies := len(n.node_map)
	received := 0
	for received < expectedReplies {
		<-replyCh
		received++
	}
}

// This is deceiving: its called "server stream" but what it is, is the stream that was used to send
// the request that we queued.
func (n *Node) exit() {
	for _, server_stream := range n.queue {
		server_stream.Send(&proto.Reply{}) // gets thrown out immediately,so can be blank
	}
	n.queue = n.queue[:0]
}

func (n *Node) setLeader() {
	fmt.Printf("%v is the leader\n", n.id)
	// some critical process...
	n.state = 0
	n.exit()
}

// connects to another client with their port
func (n *Node) connect(port int) {
	if port == n.port {
		return
	} // Make sure we don't connect to our selves

	addr := fmt.Sprintf("localhost:%d", port) // so its not hardcoded anymore
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		// logging
	}
	defer conn.Close() // keeps connnection open

	client := proto.NewNodeServiceClient(conn)

	// Create a bidirectional stream
	stream, err := client.Send(context.Background())
	if err != nil {
		// logging
		return
	}

	n.node_map[port] = stream // map port to open stream
}

func (n *Node) Send(stream proto.NodeService_SendServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		// timestamp stuff + lock
		if n.state == 2 || n.state == 1 && req.Timestamp > n.timestamp {
			n.queue = append(n.queue, stream) // we save the stream of the sender for later use
		} else {
			reply := &proto.Reply{}                    // can be blank, gets thrown out immediately anyway
			if err := stream.Send(reply); err != nil { // we reply directly on sender stream
				return err
			}
		}
	}
}

// Should start server for a different address each time (8080, 8081...) one for each node
// Server only handles receive and reply
func (n *Node) startServer(port int) {
	addr := fmt.Sprintf(":%d", port) // so its not hardcoded anymore
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		// logging
	}

	// Makes new GRPC server
	grpc_server := grpc.NewServer()

	// Registers the grpc server with the System struct
	proto.RegisterNodeServiceServer(grpc_server, n)

	// this blocks ???
	err = grpc_server.Serve(listener)
	if err != nil {
		// logging
	}
	// logging like, "server started at... for node..."
}
