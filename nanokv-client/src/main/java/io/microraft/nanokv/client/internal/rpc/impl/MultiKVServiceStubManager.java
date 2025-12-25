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

package io.microraft.nanokv.client.internal.rpc.impl;

import io.microraft.nanokv.client.NanoKVClientException;
import io.microraft.nanokv.client.config.NanoKVClientConfig;
import io.microraft.nanokv.client.internal.channel.ChannelManager;
import io.microraft.nanokv.client.internal.rpc.InvocationService;
import io.microraft.nanokv.cluster.proto.NanoKVClusterEndpoints;
import io.microraft.nanokv.cluster.proto.NanoKVClusterEndpointsRequest;
import io.microraft.nanokv.cluster.proto.NanoKVClusterEndpointsResponse;
import io.microraft.nanokv.cluster.proto.NanoKVClusterServiceGrpc;
import io.microraft.nanokv.cluster.proto.NanoKVClusterServiceGrpc.NanoKVClusterServiceStub;
import io.microraft.nanokv.kv.proto.KVServiceGrpc;
import io.microraft.nanokv.kv.proto.KVServiceGrpc.KVServiceFutureStub;
import io.microraft.nanokv.kv.proto.KVResponse;
import io.grpc.ManagedChannel;
import java.util.function.Function;
import io.microraft.nanokv.kv.proto.KVResponse;
import io.grpc.stub.StreamObserver;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import javax.annotation.PreDestroy;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ExecutionException;
import com.google.rpc.Code;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Throwables.getRootCause;
import static io.microraft.nanokv.client.internal.di.NanoKVClientModule.CONFIG_KEY;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@Singleton
public class MultiKVServiceStubManager implements InvocationService {

    private static final Logger LOGGER = LoggerFactory.getLogger(MultiKVServiceStubManager.class);
    private static final long START_TIMEOUT_SECONDS = 60;

    private static class StubHolder {
        final KVServiceFutureStub stub;
        final String serverId;

        StubHolder(KVServiceFutureStub stub, String serverId) {
            this.stub = stub;
            this.serverId = serverId;
        }
    }

    private final NanoKVClientConfig config;
    private final int rpcTimeoutSecs;
    private final ChannelManager channelManager;
    private final AtomicReference<NanoKVClusterEndpoints> endpointsRef = new AtomicReference<>();
    private final ConcurrentMap<String, NanoKVClusterServiceStub> clusterStubs = new ConcurrentHashMap<>();
    private final CountDownLatch startLatch = new CountDownLatch(1);
    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor();

    private volatile StubHolder stubHolder;

    @Inject
    public MultiKVServiceStubManager(@Named(CONFIG_KEY) NanoKVClientConfig config, ChannelManager channelManager) {
        this.config = config;
        this.rpcTimeoutSecs = config.getRpcTimeoutSecs();
        this.channelManager = channelManager;
    }

    @Override
    public CompletableFuture<KVResponse> invoke(Function<KVServiceFutureStub, ListenableFuture<KVResponse>> func) {
        return new Invocation(func).invoke();
    }

    @PostConstruct
    public void start() {
        createClusterStubIfAbsent(config.getServerAddress());
        try {
            if (!startLatch.await(START_TIMEOUT_SECONDS, SECONDS)) {
                throw new NanoKVClientException("Could not connect to the leader endpoint!");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new NanoKVClientException("Could not connect to the leader endpoint because interrupted!");
        }
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdown();
    }

    private void tryUpdateClusterEndpoints(NanoKVClusterEndpoints newEndpoints) {
        while (true) {
            NanoKVClusterEndpoints currentEndpoints = endpointsRef.get();
            if (currentEndpoints == null || currentEndpoints.getTerm() < newEndpoints.getTerm()
                    || currentEndpoints.getEndpointsCommitIndex() < newEndpoints.getEndpointsCommitIndex()
                    || (currentEndpoints.getTerm() == newEndpoints.getTerm()
                            && currentEndpoints.getEndpointsCommitIndex() == newEndpoints.getEndpointsCommitIndex()
                            && isNullOrEmpty(currentEndpoints.getLeaderId())
                            && !isNullOrEmpty(newEndpoints.getLeaderId()))) {
                if (endpointsRef.compareAndSet(currentEndpoints, newEndpoints)) {
                    LOGGER.info("{} updated cluster endpoints to: {} with commit index: {} and leader: {}",
                            config.getClientId(), newEndpoints.getEndpointMap(), newEndpoints.getEndpointsCommitIndex(),
                            newEndpoints.getLeaderId());
                    synchronized (endpointsRef) {
                        if (endpointsRef.get().getEndpointsCommitIndex() != newEndpoints.getEndpointsCommitIndex()) {
                            return;
                        }

                        channelManager.retainChannels(endpointsRef.get().getEndpointMap().values());
                        tryUpdateKVStub(newEndpoints);
                    }

                    break;
                }
            } else {
                break;
            }
        }

        endpointsRef.get().getEndpointMap().values().forEach(this::createClusterStubIfAbsent);
    }

    private void tryUpdateKVStub(NanoKVClusterEndpoints newEndpoints) {
        if (!isNullOrEmpty(newEndpoints.getLeaderId())
                && (stubHolder == null || !newEndpoints.getLeaderId().equals(stubHolder.serverId))) {
            LOGGER.info("{} switching the KV stub to the new leader: {}", config.getClientId(),
                    newEndpoints.getLeaderId());

            String leaderAddress = newEndpoints.getEndpointMap().get(newEndpoints.getLeaderId());
            // stub =
            // KVServiceGrpc.newBlockingStub(channelManager.getOrCreateChannel(leaderAddress));
            stubHolder = new StubHolder(KVServiceGrpc.newFutureStub(channelManager.getOrCreateChannel(leaderAddress))
            // .withDeadlineAfter(rpcTimeoutSecs, SECONDS)
                    , newEndpoints.getLeaderId());
            startLatch.countDown();
        }
    }

    private void createClusterStubIfAbsent(String address) {
        ManagedChannel channel = channelManager.getOrCreateChannel(address);
        NanoKVClusterServiceStub stub = NanoKVClusterServiceGrpc.newStub(channel);

        if (clusterStubs.putIfAbsent(address, stub) != null) {
            return;
        }

        LOGGER.info("{} created the cluster service stub for address: {}", config.getClientId(), address);
        NanoKVClusterEndpointsRequest request = NanoKVClusterEndpointsRequest.newBuilder()
                .setClientId(config.getClientId()).build();
        stub.listenClusterEndpoints(request, new NanoKVClusterEndpointsResponseObserver(address, channel, stub));
    }

    private void removeClusterStub(String address, NanoKVClusterServiceStub stub) {
        if (clusterStubs.remove(address, stub)) {
            LOGGER.warn("{} removed cluster stub to: {}.", config.getClientId(), address);
        }
    }

    private class NanoKVClusterEndpointsResponseObserver implements StreamObserver<NanoKVClusterEndpointsResponse> {
        final String address;
        final ManagedChannel channel;
        final NanoKVClusterServiceStub stub;

        NanoKVClusterEndpointsResponseObserver(String address, ManagedChannel channel, NanoKVClusterServiceStub stub) {
            this.address = address;
            this.channel = channel;
            this.stub = stub;
        }

        @Override
        public void onNext(NanoKVClusterEndpointsResponse response) {
            NanoKVClusterEndpoints endpoints = response.getEndpoints();
            try {
                LOGGER.debug("{} received {} from {}.", config.getClientId(), response, address);
                tryUpdateClusterEndpoints(endpoints);
            } catch (Exception e) {
                LOGGER.error(config.getClientId() + " handling of " + endpoints.getEndpointMap()
                        + " with commit index: " + endpoints.getEndpointsCommitIndex() + " term: " + endpoints.getTerm()
                        + " leader: " + endpoints.getLeaderId() + " failed.", e);
            }
        }

        @Override
        public void onError(Throwable t) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.error(config.getClientId() + " cluster observer of " + address + " failed.", t);
            } else {
                LOGGER.error("{} cluster observer of {} failed. Exception: {} Message: {}", config.getClientId(),
                        address, t.getClass().getSimpleName(), t.getMessage());
            }

            removeClusterStub(address, stub);
            channelManager.checkChannel(address, channel);
        }

        @Override
        public void onCompleted() {
            LOGGER.error(config.getClientId() + " cluster observer of " + address + " completed.");
            removeClusterStub(address, stub);
            channelManager.checkChannel(address, channel);
        }
    }

    private class Invocation {
        final CompletableFuture<KVResponse> future = new CompletableFuture<>();
        final Function<KVServiceFutureStub, ListenableFuture<KVResponse>> func;

        Invocation(Function<KVServiceFutureStub, ListenableFuture<KVResponse>> func) {
            this.func = func;
        }

        CompletableFuture<KVResponse> invoke() {
            if (stubHolder == null) {
                executor.schedule(this::invoke, 10, MILLISECONDS);
                return future;
            }

            ListenableFuture<KVResponse> rawFuture = func.apply(stubHolder.stub);
            rawFuture.addListener(new Runnable() {
                @Override
                public void run() {
                    handleRpcResult(rawFuture);
                }
            }, executor);

            return future;
        }

        void handleRpcResult(ListenableFuture<KVResponse> rawFuture) {
            try {
                future.complete(rawFuture.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                future.completeExceptionally(e);
            } catch (ExecutionException e) {
                Throwable t = getRootCause(e);

                if (t instanceof StatusRuntimeException && t.getMessage().contains("RAFT_ERROR")) {
                    StatusRuntimeException ex = (StatusRuntimeException) t;
                    if (ex.getStatus().getCode() == Status.Code.FAILED_PRECONDITION
                            || ex.getStatus().getCode() == Status.Code.RESOURCE_EXHAUSTED) {
                        executor.schedule(this::invoke, 10, MILLISECONDS);
                        return;
                    }
                }

                future.completeExceptionally(new NanoKVClientException(t));
            }
        }
    }

}
