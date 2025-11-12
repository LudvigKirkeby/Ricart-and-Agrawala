This is an implementation of the Ricart and Agrawala algorithm written in Go, using gRPC.

How to run the program:

0. **Configuration**
    - In main.go, the variable node_count can be set to a given value. This is the number of Nodes active in the program.
    - It has been hardcoded as 3 since this is the minimum requirement.

1. **Start-up**
   - cd to the root directory
   - run command: go run main.go
   - Each node starts a gRPC server on a unique port.
   - Once all nodes are connected, they begin sending requests.

2. **Termination**
   - Press ctrl + c to terminate the program :-)

3. **Log**
   - The resulting log after running the program can be found in nodes.log.
   - If the file doesn't already exist, it is generated.