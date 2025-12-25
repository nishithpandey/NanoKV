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

package io.microraft.nanokv.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.microraft.nanokv.NanoKVException;
import io.microraft.RaftConfig;

import javax.annotation.Nonnull;

import static io.microraft.HoconRaftConfigParser.parseConfig;
import static io.microraft.RaftConfig.DEFAULT_RAFT_CONFIG;
import static java.util.Objects.requireNonNull;

public final class NanoKVConfig {

    private Config config;
    private NanoKVEndpointConfig localEndpointConfig;
    private RaftGroupConfig raftGroupConfig;
    private RaftConfig raftConfig;
    private RpcConfig rpcConfig;
    private PersistenceConfig persistenceConfig;

    private NanoKVConfig() {
    }

    @Nonnull
    public static NanoKVConfig from(@Nonnull Config config) {
        return newBuilder().setConfig(requireNonNull(config)).build();
    }

    @Nonnull
    public static NanoKVConfigBuilder newBuilder() {
        return new NanoKVConfigBuilder();
    }

    @Nonnull
    public Config getConfig() {
        return config;
    }

    @Nonnull
    public NanoKVEndpointConfig getLocalEndpointConfig() {
        return localEndpointConfig;
    }

    @Nonnull
    public RaftGroupConfig getRaftGroupConfig() {
        return raftGroupConfig;
    }

    @Nonnull
    public RaftConfig getRaftConfig() {
        return raftConfig;
    }

    @Nonnull
    public RpcConfig getRpcConfig() {
        return rpcConfig;
    }

    @Nonnull
    public PersistenceConfig getPersistenceConfig() {
        return persistenceConfig;
    }

    @Override
    public String toString() {
        return "NanoKVConfig{" + "config=" + config + ", localEndpointConfig=" + localEndpointConfig
                + ", raftGroupConfig=" + raftGroupConfig + ", raftConfig=" + raftConfig + ", rpcConfig=" + rpcConfig
                + ", persistenceConfig=" + persistenceConfig + '}';
    }

    public static class NanoKVConfigBuilder {

        private NanoKVConfig nanoDBConfig = new NanoKVConfig();

        @Nonnull
        public NanoKVConfigBuilder setConfig(@Nonnull Config config) {
            nanoDBConfig.config = requireNonNull(config);
            return this;
        }

        @Nonnull
        public NanoKVConfigBuilder setLocalEndpointConfig(@Nonnull NanoKVEndpointConfig localEndpointConfig) {
            nanoDBConfig.localEndpointConfig = requireNonNull(localEndpointConfig);
            return this;
        }

        @Nonnull
        public NanoKVConfigBuilder setRaftGroupConfig(@Nonnull RaftGroupConfig raftGroupConfig) {
            nanoDBConfig.raftGroupConfig = requireNonNull(raftGroupConfig);
            return this;
        }

        @Nonnull
        public NanoKVConfigBuilder setRaftConfig(@Nonnull RaftConfig raftConfig) {
            nanoDBConfig.raftConfig = requireNonNull(raftConfig);
            return this;
        }

        @Nonnull
        public NanoKVConfigBuilder setRpcConfig(@Nonnull RpcConfig rpcConfig) {
            nanoDBConfig.rpcConfig = requireNonNull(rpcConfig);
            return this;
        }

        @Nonnull
        public NanoKVConfigBuilder setPersistenceConfig(@Nonnull PersistenceConfig persistenceConfig) {
            nanoDBConfig.persistenceConfig = requireNonNull(persistenceConfig);
            return this;
        }

        @Nonnull
        public NanoKVConfig build() {
            if (nanoDBConfig == null) {
                throw new NanoKVException("NanoKVConfig already built!");
            }

            if (nanoDBConfig.config == null) {
                try {
                    nanoDBConfig.config = ConfigFactory.load();
                } catch (Exception e) {
                    throw new NanoKVException("Could not load Config!", e);
                }
            }

            try {
                if (nanoDBConfig.config.hasPath("nanokv")) {
                    Config config = nanoDBConfig.config.getConfig("nanokv");

                    if (nanoDBConfig.localEndpointConfig == null && config.hasPath("local-endpoint")) {
                        nanoDBConfig.localEndpointConfig = NanoKVEndpointConfig
                                .from(config.getConfig("local" + "-endpoint"));
                    }

                    if (nanoDBConfig.raftGroupConfig == null && config.hasPath("group")) {
                        nanoDBConfig.raftGroupConfig = RaftGroupConfig.from(config.getConfig("group"));
                    }

                    if (nanoDBConfig.raftConfig == null) {
                        nanoDBConfig.raftConfig = config.hasPath("raft") ? parseConfig(config) : DEFAULT_RAFT_CONFIG;
                    }

                    if (nanoDBConfig.rpcConfig == null && config.hasPath("rpc")) {
                        nanoDBConfig.rpcConfig = RpcConfig.from(config.getConfig("rpc"));
                    }

                    if (nanoDBConfig.persistenceConfig == null && config.hasPath("persistence")) {
                        nanoDBConfig.persistenceConfig = PersistenceConfig.from(config.getConfig("persistence"));
                    }
                }
            } catch (Exception e) {
                if (e instanceof NanoKVException) {
                    throw (NanoKVException) e;
                }

                throw new NanoKVException("Could not build NanoKVConfig!", e);
            }

            if (nanoDBConfig.localEndpointConfig == null) {
                throw new NanoKVException("Local endpoint config is missing!");
            }

            if (nanoDBConfig.raftGroupConfig == null) {
                throw new NanoKVException("Raft group config is missing!");
            }

            if (nanoDBConfig.raftConfig == null) {
                throw new NanoKVException("Raft config is missing!");
            }

            if (nanoDBConfig.rpcConfig == null) {
                nanoDBConfig.rpcConfig = RpcConfig.newBuilder().build();
            }

            if (nanoDBConfig.persistenceConfig == null) {
                throw new NanoKVException("Persistence config is missing!");
            }

            NanoKVConfig nanoDBConfig = this.nanoDBConfig;
            this.nanoDBConfig = null;
            return nanoDBConfig;
        }

    }

}
