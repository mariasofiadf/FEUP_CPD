# Distributed and Partitioned Key-Value Store

## To run and test the Store

Go to build/classes/java/main folder

### Starting rmi registry

By default starts at port 1099, but can be specified.
```
rmiregistry &
```

### Starting Storage Node

```
java -Djava.rmi.server.codebase=file:./ StorageNode
```

### Start Client

```
java Client
```




### Join

1. Every time a node joins or leaves the cluster, the node must send via IP multicast a JOIN/LEAVE message, respectively.
2. Messages:
    - membership counter (initially 0) - increases everytime the node join or leaves the cluster
        if even the node is joining the cluster, if odd the node is leaving
        The membership counter must survive node crashes and therefore should be stored in non-volatile memory.
3. Add events (join/leave) to the membership log, this must contain:
    - node id
    - membership counter
4. Whenever a node JOINS initialize the cluster membership aka some nodes will send a Membership message, which includes:
    - list of current cluster members
    - the 32 most recent membership events (put, join, leave, etc) in its log.
    Note: Use TCP to pass membership information to nodes joining the cluster
5. Since a new member starts accepting connections on a PORT before sending JOIN messages, it sends the port number on these messages.
6. X time after receiving the join message, a cluster member send the membership information.
    Note: The new member stops accepting connections JOINafter receiving 3 MEMBERSHIP messages.
7.