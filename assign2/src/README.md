# Distributed and Partitioned Key-Value Store

### Starting rmi registry

By default starts at port 1099, but can be specified.
```
rmiregistry &
```

### Starting Storage Node

```
java -Djava.rmi.server.codebase=file:./ StorageNode <IP_mcast_addr> <IP_mcast_port> <node_addr>
```

### Start Client

```
java Client <node_ap> <operation> [<opnd>]
```
node_ap is an address.<br>
operations is one of:
- join
- leave
- put
- get
- delete

opnd is a filename


### Selecting Network Interface

By default, each node will use the loopback interface.<br>
In order to use another interface, a file "NetworkInterface.txt" 
should be created in the same directory as the StorageNode.class, with the desired interface.