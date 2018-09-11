# Sincronia

Implementation for [Sincronia](https://dl.acm.org/citation.cfm?id=3230569), a near-optimal network design for coflows

Sincronia implementation mainly employs RPCs over TCP, with DiffServ for packet prioritization. It consists of 3 main parts: 

1. **scheduler** directory contains the source for a binary implementing a central sincronia scehduler. It provides the coflow ordering (used to generate flow-priorities) to the clients.  
2. **client** directory contains the shim library used by the applications running on clients to exchange the coflow information and priorities with the central scheduler.
3. **apps** directory contains the applications running on the clients and sending coflows amongst each other.

#### Running the implementation

##### Central Scheduler

First, generate and run the central scheduler binary running on a server dedicated for the same. This scheduler will keep running in the background, waiting for the appmaster to first register the coflows information (number of flows and the corresponding sebders and receivers for each flow). Then later wait for different clients to request sending the flows of each coflow and [provide priorities](https://dl.acm.org/citation.cfm?id=3230569) to each of the clients for the flows. 

In the **scheduler** directory, run

```
make all
./sincronia-scheduler
```

To compile, the server should have Gurobi solver. Make the changes to gurobi include flags in the Makefile according to the installed location and source OS in the Makefile [https://www.gurobi.com/documentation/7.0/quickstart_linux/cpp_building_and_running_t.html].

Requires Gurobi version >= 7.0.2. If using version different from 7.0.2, replace the files "gurobi_c++.h" and "gurobi_c.h" with the corresponding ones found in the include folder of the install directory. Gurobi provides free academic licenses [https://user.gurobi.com/download/licenses/free-academic].

Also, the [Cap'n Proto](https://capnproto.org/install.html) RPC library is required.

##### Appmaster

The appmaster can be run on the same server as the central scheduler. It supplies the scheduler coflow information for registration, such that the scheduler could later identify when it has received requests to send flows from all clients involved for a coflow. 

To run the Appmaster, we first require installing go libraries -- a tracefile **parser** library and sincronia shim library **client**, which further requires 

```
go get github.com/sincronia-coflow/implementation/app/parser
go get github.com/sincronia-coflow/implementation/client
```

Now build and run the app master using following inside the **app/master/** directory
```
go build master.go
./master [path to coflow tracefile]
```

The argument above is the path to the coflow tracefile, with the format
The input format of the traces looks like follows

```
Line1: <NUM_INP_PORTS> <NUM_COFLOWS>
Line2: <Coflow 1ID> <Arrival Time (in millisec)> <Number of Flows in Coflow 1> <Number of Sources in Coflow 1> <Number of Destinations in Coflow 1> <Flow 1 Source ID> <Flow 1 Destination ID> <Flow 1 Size (in MB)> ... <Flow N Source ID> <Flow N1 Destination ID> <Flow N1 size (in MB)>
...
...
Line i+1:  <Coflow iID> <Arrival Time (in millisec)> <Number of Flows in Coflow i> <Number of Sources in Coflow i> <Number of Destinations in Coflow i> <Flow 1 Source ID> <Flow 1 Destination ID> <Flow 1 Size (in MB)> ... <Flow N Source ID> <Flow Ni Destination ID> <Flow Ni size (in MB)>
...
```

##### Client App

The client app requires the same go libraries on the client servers
```
go get github.com/sincronia-coflow/implementation/app/parser
go get github.com/sincronia-coflow/implementation/client
```

Then build and run the client app using
```
go build client.go
./client [path to coflow tracefile] [node id]
```

Node tracefile needs to be obtained for each client using `coflow-parser.py` before running the client app
```
python coflow-parser.py [path to coflow tracefile] [node id] [path to node tracefile]
```

Note that each node should be preassigned a node id and corresponding ip for each node should be stored in a `nodes` map in file **client.go**. Also the [path to node tracefile] should be in format "node-[node id].txt".

The output log is store as file `coflow-done.txt` in the **scheduler** directory at the central scheduler server and contains the coflow id, start time and end time for each coflow.
