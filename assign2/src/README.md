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