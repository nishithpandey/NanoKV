/*
 * Copyright (c) 2020, NanoKV.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.microraft.nanokv.internal.rpc.impl;

import io.microraft.nanokv.kv.proto.ClearRequest;
import io.microraft.nanokv.kv.proto.ContainsRequest;
import io.microraft.nanokv.kv.proto.DeleteRequest;
import io.microraft.nanokv.kv.proto.GetRequest;
import io.microraft.nanokv.kv.proto.KVServiceGrpc.KVServiceImplBase;
import io.microraft.nanokv.kv.proto.KVResponse;
import io.microraft.nanokv.kv.proto.PutRequest;
import io.microraft.nanokv.kv.proto.RemoveRequest;
import io.microraft.nanokv.kv.proto.ReplaceRequest;
import io.microraft.nanokv.kv.proto.SetRequest;
import io.microraft.nanokv.kv.proto.SizeRequest;
import io.microraft.nanokv.raft.proto.SizeOp;
import io.microraft.nanokv.raft.proto.PutOp;
import io.microraft.nanokv.raft.proto.GetOp;
import io.microraft.nanokv.raft.proto.RemoveOp;
import io.microraft.nanokv.raft.proto.ReplaceOp;
import io.microraft.nanokv.raft.proto.ClearOp;
import io.microraft.Ordered;
import io.grpc.stub.StreamObserver;
import io.microraft.RaftNode;

import static io.microraft.nanokv.internal.di.NanoKVModule.RAFT_NODE_SUPPLIER_KEY;

import javax.inject.Inject;
import javax.inject.Singleton;

import javax.inject.Named;
import java.util.function.Supplier;

import static io.microraft.nanokv.internal.utils.Exceptions.wrap;
import static io.microraft.QueryPolicy.EVENTUAL_CONSISTENCY;
import static io.microraft.QueryPolicy.LINEARIZABLE;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class KVService extends KVServiceImplBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(KVService.class);

    private final RaftNode raftNode;

    @Inject
    public KVService(@Named(RAFT_NODE_SUPPLIER_KEY) Supplier<RaftNode> raftNodeSupplier) {
        this.raftNode = raftNodeSupplier.get();
    }

    @Override
    public void put(PutRequest request, StreamObserver<KVResponse> responseObserver) {
        // TODO (basri) build the proxy logic here (invocation logic)
        PutOp op = PutOp.newBuilder().setKey(request.getKey()).setVal(request.getVal())
                .setPutIfAbsent(request.getPutIfAbsent()).build();
        raftNode.<io.microraft.nanokv.raft.proto.PutResult>replicate(op)
                .whenComplete((Ordered<io.microraft.nanokv.raft.proto.PutResult> result, Throwable throwable) -> {
                    // TODO [basri] bottleneck. offload to IO thread...
                    if (throwable == null) {
                        responseObserver.onNext(KVResponse.newBuilder().setCommitIndex(result.getCommitIndex())
                                .setPutResult(io.microraft.nanokv.kv.proto.PutResult.newBuilder()
                                        .setOldVal(result.getResult().getOldVal()).build())
                                .build());
                    } else {
                        responseObserver.onError(wrap(throwable));
                    }
                    responseObserver.onCompleted();
                });
    }

    @Override
    public void set(SetRequest request, StreamObserver<KVResponse> responseObserver) {
        PutOp op = PutOp.newBuilder().setKey(request.getKey()).setVal(request.getVal()).build();
        raftNode.<io.microraft.nanokv.raft.proto.PutResult>replicate(op)
                .whenComplete((Ordered<io.microraft.nanokv.raft.proto.PutResult> result, Throwable throwable) -> {
                    // TODO [basri] bottleneck. offload to IO thread...
                    if (throwable == null) {
                        responseObserver.onNext(KVResponse.newBuilder().setCommitIndex(result.getCommitIndex())
                                .setSetResult(io.microraft.nanokv.kv.proto.SetResult.newBuilder()
                                        .setOldValExisted(result.getResult().hasOldVal()).build())
                                .build());
                    } else {
                        responseObserver.onError(wrap(throwable));
                    }
                    responseObserver.onCompleted();
                });
    }

    @Override
    public void get(GetRequest request, StreamObserver<KVResponse> responseObserver) {
        GetOp op = GetOp.newBuilder().setKey(request.getKey()).build();
        raftNode.<io.microraft.nanokv.raft.proto.GetResult>query(op,
                request.getMinCommitIndex() == -1 ? LINEARIZABLE : EVENTUAL_CONSISTENCY,
                Math.max(0, request.getMinCommitIndex()))
                .whenComplete((Ordered<io.microraft.nanokv.raft.proto.GetResult> result, Throwable throwable) -> {
                    // TODO [basri] bottleneck. offload to IO thread...
                    if (throwable == null) {
                        responseObserver.onNext(KVResponse.newBuilder().setCommitIndex(result.getCommitIndex())
                                .setGetResult(io.microraft.nanokv.kv.proto.GetResult.newBuilder()
                                        .setVal(result.getResult().getVal()))
                                .build());
                    } else {
                        responseObserver.onError(wrap(throwable));
                    }
                    responseObserver.onCompleted();
                });
    }

    @Override
    public void contains(ContainsRequest request, StreamObserver<KVResponse> responseObserver) {
        GetOp op = GetOp.newBuilder().setKey(request.getKey()).build();
        raftNode.<io.microraft.nanokv.raft.proto.GetResult>query(op,
                request.getMinCommitIndex() == -1 ? LINEARIZABLE : EVENTUAL_CONSISTENCY,
                Math.max(0, request.getMinCommitIndex()))
                .whenComplete((Ordered<io.microraft.nanokv.raft.proto.GetResult> result, Throwable throwable) -> {
                    // TODO [basri] bottleneck. offload to IO thread...
                    if (throwable == null) {
                        io.microraft.nanokv.kv.proto.ContainsResult.Builder builder2 = io.microraft.nanokv.kv.proto.ContainsResult
                                .newBuilder();
                        if (!result.getResult().hasVal()) {
                            builder2.setSuccess(false);
                        } else if (request.hasVal()) {
                            builder2.setSuccess(request.getVal().equals(result.getResult().getVal()));
                        } else {
                            builder2.setSuccess(true);
                        }
                        responseObserver.onNext(KVResponse.newBuilder().setCommitIndex(result.getCommitIndex())
                                .setContainsResult(builder2.build()).build());
                    } else {
                        responseObserver.onError(wrap(throwable));
                    }
                    responseObserver.onCompleted();
                });
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<KVResponse> responseObserver) {
        RemoveOp op = RemoveOp.newBuilder().setKey(request.getKey()).build();
        raftNode.<io.microraft.nanokv.raft.proto.RemoveResult>replicate(op)
                .whenComplete((Ordered<io.microraft.nanokv.raft.proto.RemoveResult> result, Throwable throwable) -> {
                    // TODO [basri] bottleneck. offload to IO thread...
                    if (throwable == null) {
                        responseObserver.onNext(KVResponse.newBuilder().setCommitIndex(result.getCommitIndex())
                                .setDeleteResult(io.microraft.nanokv.kv.proto.DeleteResult.newBuilder()
                                        .setSuccess(result.getResult().getSuccess()).build())
                                .build());
                    } else {
                        responseObserver.onError(wrap(throwable));
                    }
                    responseObserver.onCompleted();
                });
    }

    @Override
    public void remove(RemoveRequest request, StreamObserver<KVResponse> responseObserver) {
        RemoveOp.Builder builder = RemoveOp.newBuilder().setKey(request.getKey());
        if (request.hasVal()) {
            builder.setVal(request.getVal());
        }
        raftNode.<io.microraft.nanokv.raft.proto.RemoveResult>replicate(builder.build())
                .whenComplete((Ordered<io.microraft.nanokv.raft.proto.RemoveResult> result, Throwable throwable) -> {
                    // TODO [basri] bottleneck. offload to IO thread...
                    if (throwable == null) {
                        io.microraft.nanokv.kv.proto.RemoveResult.Builder builder2 = io.microraft.nanokv.kv.proto.RemoveResult
                                .newBuilder().setSuccess(result.getResult().getSuccess());
                        if (!request.hasVal() && result.getResult().hasOldVal()) {
                            builder2.setOldVal(result.getResult().getOldVal());
                        }
                        responseObserver.onNext(KVResponse.newBuilder().setCommitIndex(result.getCommitIndex())
                                .setRemoveResult(builder2.build()).build());
                    } else {
                        responseObserver.onError(wrap(throwable));
                    }
                    responseObserver.onCompleted();
                });
    }

    @Override
    public void replace(ReplaceRequest request, StreamObserver<KVResponse> responseObserver) {
        ReplaceOp op = ReplaceOp.newBuilder().setKey(request.getKey()).setOldVal(request.getOldVal())
                .setNewVal(request.getNewVal()).build();
        raftNode.<io.microraft.nanokv.raft.proto.ReplaceResult>replicate(op)
                .whenComplete((Ordered<io.microraft.nanokv.raft.proto.ReplaceResult> result, Throwable throwable) -> {
                    // TODO [basri] bottleneck. offload to IO thread...
                    if (throwable == null) {
                        responseObserver.onNext(KVResponse.newBuilder().setCommitIndex(result.getCommitIndex())
                                .setReplaceResult(io.microraft.nanokv.kv.proto.ReplaceResult.newBuilder()
                                        .setSuccess(result.getResult().getSuccess()).build())
                                .build());
                    } else {
                        responseObserver.onError(wrap(throwable));
                    }
                    responseObserver.onCompleted();
                });
    }

    @Override
    public void size(SizeRequest request, StreamObserver<KVResponse> responseObserver) {
        raftNode.<io.microraft.nanokv.raft.proto.SizeResult>query(SizeOp.getDefaultInstance(),
                request.getMinCommitIndex() == -1 ? LINEARIZABLE : EVENTUAL_CONSISTENCY,
                Math.max(0, request.getMinCommitIndex()))
                .whenComplete((Ordered<io.microraft.nanokv.raft.proto.SizeResult> result, Throwable throwable) -> {
                    // TODO [basri] bottleneck. offload to IO thread...
                    if (throwable == null) {
                        responseObserver.onNext(KVResponse.newBuilder().setCommitIndex(result.getCommitIndex())
                                .setSizeResult(io.microraft.nanokv.kv.proto.SizeResult.newBuilder()
                                        .setSize(result.getResult().getSize()))
                                .build());
                    } else {
                        responseObserver.onError(wrap(throwable));
                    }
                    responseObserver.onCompleted();
                });
    }

    @Override
    public void clear(ClearRequest request, StreamObserver<KVResponse> responseObserver) {
        raftNode.<io.microraft.nanokv.raft.proto.ClearResult>replicate(ClearOp.getDefaultInstance())
                .whenComplete((Ordered<io.microraft.nanokv.raft.proto.ClearResult> result, Throwable throwable) -> {
                    // TODO [basri] bottleneck. offload to IO thread...
                    if (throwable == null) {
                        responseObserver.onNext(KVResponse.newBuilder().setCommitIndex(result.getCommitIndex())
                                .setClearResult(io.microraft.nanokv.kv.proto.ClearResult.newBuilder()
                                        .setSize(result.getResult().getSize()).build())
                                .build());
                    } else {
                        responseObserver.onError(wrap(throwable));
                    }
                    responseObserver.onCompleted();
                });
    }

}
