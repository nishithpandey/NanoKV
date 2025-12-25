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

package io.microraft.nanokv.internal.di;

import static com.google.inject.name.Names.named;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import io.microraft.nanokv.admin.proto.AdminServiceGrpc.AdminServiceImplBase;
import io.microraft.nanokv.config.NanoKVConfig;
import io.microraft.nanokv.internal.lifecycle.ProcessTerminationLogger;
import io.microraft.nanokv.internal.lifecycle.impl.ProcessTerminationLoggerImpl;
import io.microraft.nanokv.internal.raft.NanoKVClusterEndpointsPublisher;
import io.microraft.nanokv.internal.raft.KVStoreStateMachine;
import io.microraft.nanokv.internal.raft.RaftNodeReportSupplier;
import io.microraft.nanokv.internal.raft.RaftNodeSupplier;
import io.microraft.nanokv.internal.raft.RaftStoreSupplier;
import io.microraft.nanokv.internal.raft.model.ProtoRaftModelFactory;
import io.microraft.nanokv.internal.raft.model.ProtoStateStoreSerializer;
import io.microraft.nanokv.internal.rpc.RaftRpcService;
import io.microraft.nanokv.internal.rpc.RpcServer;
import io.microraft.nanokv.internal.rpc.impl.AdminService;
import io.microraft.nanokv.internal.rpc.impl.KVService;
import io.microraft.nanokv.internal.rpc.impl.RaftRpcServiceImpl;
import io.microraft.nanokv.internal.rpc.impl.RaftService;
import io.microraft.nanokv.internal.rpc.impl.RpcServerImpl;
import io.microraft.nanokv.kv.proto.KVServiceGrpc.KVServiceImplBase;
import io.microraft.nanokv.raft.proto.RaftServiceGrpc.RaftServiceImplBase;
import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.impl.state.RaftState;
import io.microraft.model.RaftModelFactory;
import io.microraft.persistence.RaftStore;
import io.microraft.statemachine.StateMachine;
import io.microraft.persistence.RaftStoreSerializer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public class NanoKVModule extends AbstractModule {

    public static final String CONFIG_KEY = "Config";
    public static final String LOCAL_ENDPOINT_KEY = "LocalEndpoint";
    public static final String INITIAL_ENDPOINTS_KEY = "InitialEndpoints";
    public static final String RAFT_ENDPOINT_ADDRESSES_KEY = "RaftEndpointAddresses";
    public static final String RAFT_NODE_SUPPLIER_KEY = "RaftNodeSupplier";

    private final NanoKVConfig config;
    private final RaftEndpoint localEndpoint;
    private final List<RaftEndpoint> initialEndpoints;
    private final Map<RaftEndpoint, String> addresses;
    private final AtomicBoolean processTerminationFlag;

    public NanoKVModule(NanoKVConfig config, RaftEndpoint localEndpoint, List<RaftEndpoint> initialEndpoints,
            Map<RaftEndpoint, String> addresses, AtomicBoolean processTerminationFlag) {
        this.config = config;
        this.localEndpoint = localEndpoint;
        this.initialEndpoints = initialEndpoints;
        this.addresses = addresses;
        this.processTerminationFlag = processTerminationFlag;
    }

    @Override
    protected void configure() {
        bind(AtomicBoolean.class).annotatedWith(named(ProcessTerminationLoggerImpl.PROCESS_TERMINATION_FLAG_KEY))
                .toInstance(processTerminationFlag);
        bind(ProcessTerminationLogger.class).to(ProcessTerminationLoggerImpl.class);

        bind(NanoKVConfig.class).annotatedWith(named(CONFIG_KEY)).toInstance(config);
        bind(RaftEndpoint.class).annotatedWith(named(LOCAL_ENDPOINT_KEY)).toInstance(localEndpoint);
        bind(new TypeLiteral<Collection<RaftEndpoint>>() {
        }).annotatedWith(named(INITIAL_ENDPOINTS_KEY)).toInstance(initialEndpoints);
        bind(new TypeLiteral<Map<RaftEndpoint, String>>() {
        }).annotatedWith(named(RAFT_ENDPOINT_ADDRESSES_KEY)).toInstance(addresses);

        bind(RaftNodeReportSupplier.class).to(NanoKVClusterEndpointsPublisher.class);
        bind(StateMachine.class).to(KVStoreStateMachine.class);
        bind(RaftModelFactory.class).to(ProtoRaftModelFactory.class);
        bind(RaftServiceImplBase.class).to(RaftService.class);
        bind(RpcServer.class).to(RpcServerImpl.class);
        bind(RaftRpcService.class).to(RaftRpcServiceImpl.class);
        bind(KVServiceImplBase.class).to(KVService.class);
        bind(AdminServiceImplBase.class).to(AdminService.class);
        bind(RaftStoreSerializer.class).to(ProtoStateStoreSerializer.class);
        bind(new TypeLiteral<Supplier<RaftStore>>() {
        }).to(RaftStoreSupplier.class);
        bind(new TypeLiteral<Supplier<RaftNode>>() {
        }).annotatedWith(named(RAFT_NODE_SUPPLIER_KEY)).to(RaftNodeSupplier.class);
    }
}
