
# NanoKV

This project demonstrates how to build very simple distributed key-value store  on top of [NanoRaft](https://github.com/nishihpandey/NanoRaft). Please note that this is not production-ready
code.

## Code pointers

[`NanoKV-server`](https://github.com/MicroRaft/NanoKV/tree/master/NanoKV-server) directory contains the server code and [`NanoKV-client`](https://github.com/MicroRaft/NanoKV/tree/master/NanoKV-client)
directory contains the client code.

Each NanoKV server runs a [`RaftNode`](https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/RaftNode.java) and the communication between servers
and the client-server communication happens through gRPC.

[`NanoKV-server/src/main/proto/NanoKVRaft.proto`](https://github.com/MicroRaft/NanoKV/blob/master/NanoKV-server/src/main/proto/NanoKVRaft.proto) contains the Protobufs definitions
for [MicroRaft's model and network abstractions](https://microraft.io/docs/main-abstractions/), and the key-value operations of
NanoKV's key-value API. NanoKV servers talk to each other via
the `RaftService` defined in this file. This service is
implemented at [`NanoKV-server/src/main/java/io/NanoKV/internal/rpc/impl/RaftRpcServiceImpl.java`](https://github.com/MicroRaft/NanoKV/blob/master/NanoKV-server/src/main/java/io/microraft/NanoKV/internal/rpc/impl/RaftRpcServiceImpl.java).

[`NanoKV-server/src/main/java/io/NanoKV/internal/raft/model`](https://github.com/MicroRaft/NanoKV/tree/master/NanoKV-server/src/main/java/io/microraft/NanoKV/internal/raft/model) package contains
implementing MicroRaft's model abstractions using the Protobufs definitions
defined in the above file.

[`NanoKV-server/src/main/java/io/NanoKV/internal/rpc/RaftRpcService.java`](https://github.com/MicroRaft/NanoKV/blob/master/NanoKV-server/src/main/java/io/microraft/NanoKV/internal/rpc/RaftRpcService.java) implements
MicroRaft's [`Transport`](https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/transport/Transport.java) abstraction and makes NanoKV servers talk to each
other with gRPC.

[`NanoKV-server/src/main/java/io/NanoKV/internal/raft/impl/KVStoreStateMachine.java`](https://github.com/MicroRaft/NanoKV/blob/master/NanoKV-server/src/main/java/io/microraft/NanoKV/internal/raft/KVStoreStateMachine.java)
contains the state machine implementation of NanoKV's key-value API and it
implements MicroRaft's [`StateMachine`](https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/statemachine/StateMachine.java) interface.

[`NanoKV-commons/src/main/proto/KV.proto`](https://github.com/MicroRaft/NanoKV/blob/master/NanoKV-commons/src/main/proto/KV.proto) defines the protocol between
NanoKV clients and servers. The server side counterpart is at
[`NanoKV-server/src/main/java/io/NanoKV/internal/rpc/impl/KVService.java`](https://github.com/MicroRaft/NanoKV/blob/master/NanoKV-server/src/main/java/io/microraft/NanoKV/internal/rpc/impl/KVService.java).
It handles requests sent by clients and passes them to `RaftNode`.

[`NanoKV-server/src/main/proto/NanoKVAdmin.proto`](https://github.com/MicroRaft/NanoKV/blob/master/NanoKV-server/src/main/proto/NanoKVAdmin.proto) contains the Protobufs
definitions for management operations on NanoKV clusters, such as
adding / removing servers, querying RaftNode reports. Operators can manage
NanoKV clusters by making gRPC calls to the `AdminService`
service defined in this file. Its server side counter part is at
[`NanoKV-server/src/main/java/io/NanoKV/internal/rpc/impl/AdminService.java`](https://github.com/MicroRaft/NanoKV/blob/master/NanoKV-server/src/main/java/io/microraft/NanoKV/internal/rpc/impl/AdminService.java).
It handles requests sent by clients and passes them to `RaftNode`.

That is enough pointers for curious readers.

MicroRaft provides a [SQLite-based `RaftStore` implementation](https://github.com/MicroRaft/MicroRaft/blob/master/microraft-store-sqlite/src/main/java/io/microraft/store/sqlite/RaftSqliteStore.java). NanoKV injects its [SerDe class](https://github.com/MicroRaft/NanoKV/blob/master/NanoKV-server/src/main/java/io/microraft/NanoKV/internal/raft/model/ProtoStateStoreSerializer.java) to persist [its Protocol Buffers based implementations](https://github.com/MicroRaft/NanoKV/tree/master/NanoKV-server/src/main/java/io/microraft/NanoKV/internal/raft/model) of [the RaftModel interfaces](https://github.com/MicroRaft/MicroRaft/tree/master/microraft/src/main/java/io/microraft/model).

## How to start a 3-node NanoKV cluster

Configuration is built with the [Typesafe Config](https://github.com/lightbend/config)
library. You can also create config programmatically. Please see
[`NanoKV-server/src/main/java/io/NanoKV/config/NanoKVConfig.java`](https://github.com/MicroRaft/NanoKV/blob/master/NanoKV-server/src/main/java/io/microraft/NanoKV/config/NanoKVConfig.java).

You need to pass a config to start an NanoKV server. The config should
provide the Raft endpoint (id and address) and the initial member list of
the cluster. [`NanoKV-server/src/test/resources`](https://github.com/MicroRaft/NanoKV/tree/master/NanoKV-server/src/test/resources) contains example config files to
start a 3 node NanoKV cluster.

`mvn clean package`

`java -jar  NanoKV-server/target/NanoKV-server-fat.jar NanoKV-server/src/test/resources/node1.conf &`

`java -jar  NanoKV-server/target/NanoKV-server-fat.jar NanoKV-server/src/test/resources/node2.conf &`

`java -jar  NanoKV-server/target/NanoKV-server-fat.jar NanoKV-server/src/test/resources/node3.conf &`

## Adding a new server to a running cluster

Once you start your NanoKV cluster, you can add new servers at runtime.
For this, you need to provide address of one of the running servers via
the "join-to" config field for the new server.

`java -jar  NanoKV-server/target/NanoKV-server-fat.jar NanoKV-server/src/test/resources/node4.conf &`

## Key-value API

[`NanoKV-client/src/main/java/io/NanoKV/client/kv/KV.java`](https://github.com/MicroRaft/NanoKV/blob/master/NanoKV-client/src/main/java/io/microraft/NanoKV/client/kv/KV.java) contains the
key-value interface. Keys have `String` type and values can be one of `String`,
`long`, or `byte[]`.

## Client API

You can start a client with [`NanoKVClient.newInstance(config)`](https://github.com/MicroRaft/NanoKV/blob/master/NanoKV-client/src/main/java/io/microraft/NanoKV/client/NanoKVClient.java#L29) to
write and read key-value pairs on the cluster. Please see
[`NanoKV-client/src/main/java/io/NanoKV/client/NanoKVClientConfig.java`](https://github.com/MicroRaft/NanoKV/blob/master/NanoKV-client/src/main/java/io/microraft/NanoKV/client/config/NanoKVClientConfig.java)
for configuration options. You need to pass a server address.
If `singleConnection` is `false`, which is the default value, the client will
discover all servers and connect to them.

## Key-value CLI for experimentation

`java  -jar NanoKV-client-cli/target/NanoKV-client-cli-fat.jar --server localhost:6701 --key name --value basri set`

Output: `Ordered{commitIndex=2, value=null}`

`java  -jar NanoKV-client-cli/target/NanoKV-client-cli-fat.jar --server localhost:6701 --key name get`

Output: `Ordered{commitIndex=2, value=basri}`

See [`NanoKV-client-cli/src/main/java/io/NanoKV/client/NanoKVClientCliRunner.java`](https://github.com/MicroRaft/NanoKV/blob/master/NanoKV-client-cli/src/main/java/io/microraft/NanoKV/client/NanoKVClientCliRunner.java) for more options.
