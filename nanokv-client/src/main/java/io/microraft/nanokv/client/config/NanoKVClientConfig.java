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

package io.microraft.nanokv.client.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.microraft.nanokv.client.NanoKVClientException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

public class NanoKVClientConfig {

    public static final boolean DEFAULT_SINGLE_CONNECTION = false;
    public static final int DEFAULT_RPC_TIMEOUT_SECS = 10;

    private Config config;
    private String clientId;
    private String serverAddress;
    private boolean singleConnection;
    private int rpcTimeoutSecs;

    private NanoKVClientConfig() {
    }

    public static NanoKVClientConfig from(Config config) {
        return newBuilder().setConfig(config).build();
    }

    public static NanoKVClientConfigBuilder newBuilder() {
        return new NanoKVClientConfigBuilder();
    }

    @Nonnull
    public Config getConfig() {
        return config;
    }

    @Nullable
    public String getClientId() {
        return clientId;
    }

    public String getServerAddress() {
        return serverAddress;
    }

    public int getRpcTimeoutSecs() {
        return rpcTimeoutSecs;
    }

    @Override
    public String toString() {
        return "NanoKVClientConfig{" + "config=" + config + ", clientId='" + clientId + '\'' + ", serverAddress='"
                + serverAddress + '\'' + ", singleConnection=" + singleConnection + ", rpcTimeoutSecs=" + rpcTimeoutSecs
                + '}';
    }

    public boolean isSingleConnection() {
        return singleConnection;
    }

    public static class NanoKVClientConfigBuilder {

        private NanoKVClientConfig clientConfig = new NanoKVClientConfig();
        private Boolean singleConnection;
        private Integer rpcTimeoutSecs;

        public NanoKVClientConfigBuilder setConfig(@Nonnull Config config) {
            clientConfig.config = requireNonNull(config);
            return this;
        }

        public NanoKVClientConfigBuilder setClientId(@Nonnull String clientId) {
            clientConfig.clientId = requireNonNull(clientId);
            return this;
        }

        public NanoKVClientConfigBuilder setServerAddress(@Nonnull String serverAddress) {
            clientConfig.serverAddress = requireNonNull(serverAddress);
            return this;
        }

        public NanoKVClientConfigBuilder setSingleConnection(boolean singleConnection) {
            this.singleConnection = singleConnection;
            return this;
        }

        public NanoKVClientConfigBuilder setRpcTimeoutSecs(int rpcTimeoutSecs) {
            if (rpcTimeoutSecs < 1) {
                throw new IllegalArgumentException(
                        "Rpc timeout seconds: " + rpcTimeoutSecs + " cannot be non-positive!");
            }
            this.rpcTimeoutSecs = rpcTimeoutSecs;
            return this;
        }

        public NanoKVClientConfig build() {
            if (clientConfig == null) {
                throw new NanoKVClientException("NanoKVClientConfig is already built!");
            }

            if (clientConfig.config == null) {
                try {
                    clientConfig.config = ConfigFactory.load();
                } catch (Exception e) {
                    throw new NanoKVClientException("Could not load Config!", e);
                }
            }

            try {
                if (clientConfig.config.hasPath("nanokv.client")) {
                    Config config = clientConfig.config.getConfig("nanokv.client");

                    if (clientConfig.clientId == null && config.hasPath("id")) {
                        clientConfig.clientId = config.getString("id");
                    }

                    if (clientConfig.serverAddress == null && config.hasPath("server-address")) {
                        clientConfig.serverAddress = config.getString("server-address");
                    }

                    if (singleConnection == null && config.hasPath("single-connection")) {
                        singleConnection = config.getBoolean("single-connection");
                    }

                    if (rpcTimeoutSecs == null && config.hasPath("rpc-timeout-secs")) {
                        rpcTimeoutSecs = config.getInt("rpc-timeout-secs");
                    }
                }
            } catch (Exception e) {
                if (e instanceof NanoKVClientException) {
                    throw (NanoKVClientException) e;
                }

                throw new NanoKVClientException("Could not build NanoKVClientConfig!", e);
            }

            if (clientConfig.clientId == null) {
                clientConfig.clientId = "Client<" + UUID.randomUUID().toString() + ">";
            }

            if (clientConfig.serverAddress == null) {
                throw new NanoKVClientException("Server address is missing!");
            }

            if (singleConnection == null) {
                singleConnection = DEFAULT_SINGLE_CONNECTION;
            }
            clientConfig.singleConnection = singleConnection;

            if (rpcTimeoutSecs == null) {
                rpcTimeoutSecs = DEFAULT_RPC_TIMEOUT_SECS;
            }
            clientConfig.rpcTimeoutSecs = rpcTimeoutSecs;

            NanoKVClientConfig clientConfig = this.clientConfig;
            this.clientConfig = null;
            return clientConfig;
        }

    }

}
