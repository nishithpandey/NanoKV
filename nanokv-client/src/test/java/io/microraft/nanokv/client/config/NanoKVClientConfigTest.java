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
import io.microraft.test.util.BaseTest;
import org.junit.Test;

import static io.microraft.nanokv.client.config.NanoKVClientConfig.newBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class NanoKVClientConfigTest extends BaseTest {

    @Test(expected = NanoKVClientException.class)
    public void when_emptyConfigStringProvided_then_shouldNotCreateConfig() {
        NanoKVClientConfig.from(ConfigFactory.parseString(""));
    }

    @Test(expected = NanoKVClientException.class)
    public void when_nanokvContainerMissingInConfig_then_shouldNotCreateConfig() {
        NanoKVClientConfig.from(ConfigFactory.parseString("client.server-address: \"localhost:6767\""));
    }

    @Test(expected = NanoKVClientException.class)
    public void when_clientContainerMissingInConfig_then_shouldNotCreateConfig() {
        NanoKVClientConfig.from(ConfigFactory.parseString("nanokv.server-address: \"localhost:6767\""));
    }

    @Test(expected = NanoKVClientException.class)
    public void when_requiredFieldsMissingInConfig_then_shouldNotCreateConfig() {
        NanoKVClientConfig.from(ConfigFactory.parseString("nanokv.client {}"));
    }

    @Test
    public void when_requiredFieldsProvidedInConfig_then_shouldCreateConfig() {
        String configString = "nanokv.client.server-address: \"localhost:6701\"";
        NanoKVClientConfig config = NanoKVClientConfig.from(ConfigFactory.parseString(configString));

        assertThat(config.getServerAddress()).isEqualTo("localhost:6701");
    }

    @Test
    public void when_clientIdProvided_then_shouldCreateConfigWithClientId() {
        String configString = "nanokv.client.server-address: \"localhost:6701\"\n" + "nanokv.client.id: \"client1\"";
        NanoKVClientConfig config = NanoKVClientConfig.from(ConfigFactory.parseString(configString));

        assertThat(config.getServerAddress()).isEqualTo("localhost:6701");
        assertThat(config.getClientId()).isEqualTo("client1");
    }

    @Test(expected = NanoKVClientException.class)
    public void when_emptyBuilder_then_shouldNotCreateConfig() {
        newBuilder().build();
    }

    @Test
    public void when_requiredFieldsProvidedToBuilderViaConfig_then_shouldCreateConfig() {
        String configString = "nanokv.client.server-address: \"localhost:6701\"\n" + "nanokv.client.id: \"client1\"";
        NanoKVClientConfig config = newBuilder().setConfig(ConfigFactory.parseString(configString)).build();

        assertThat(config.getClientId()).isEqualTo("client1");
        assertThat(config.getServerAddress()).isEqualTo("localhost:6701");
    }

    @Test
    public void when_requiredFieldsProvidedToBuilder_then_shouldCreateConfig() {
        NanoKVClientConfig config = newBuilder().setServerAddress("localhost:6701").build();

        assertThat(config.getServerAddress()).isEqualTo("localhost:6701");
        assertThat(config.getConfig()).isNotNull();
        assertThat(config.getClientId()).isNotNull();
    }

    @Test
    public void when_emptyConfigProvidedToBuilder_then_shouldCreateConfig() {
        Config config = ConfigFactory.parseString("");
        NanoKVClientConfig clientConfig = newBuilder().setConfig(config).setServerAddress("localhost:6701").build();

        assertThat(clientConfig.getServerAddress()).isEqualTo("localhost:6701");
        assertThat(clientConfig.getConfig()).isSameAs(config);
    }

    @Test
    public void when_rpcTimeoutSecsNotProvided_then_shouldCreateConfigWithDefaultVal() {
        NanoKVClientConfig config = newBuilder().setServerAddress("localhost:6701").build();

        assertThat(config.getRpcTimeoutSecs()).isEqualTo(NanoKVClientConfig.DEFAULT_RPC_TIMEOUT_SECS);
    }

    @Test
    public void when_rpcTimeoutSecsProvidedInConfig_then_shouldCreateConfigWithProvidedVal() {
        int rpcTimeoutSecs = 20;
        String configString = "nanokv.client.rpc-timeout-secs: " + rpcTimeoutSecs + "\nnanokv.client.id: \"client1\"";
        NanoKVClientConfig config = newBuilder().setConfig(ConfigFactory.parseString(configString))
                .setServerAddress("localhost:6701").build();

        assertThat(config.getRpcTimeoutSecs()).isEqualTo(rpcTimeoutSecs);
    }

    @Test
    public void when_rpcTimeoutSecsProvided_then_shouldCreateConfigWithProvidedVal() {
        int rpcTimeoutSecs = 20;
        NanoKVClientConfig config = newBuilder().setServerAddress("localhost:6701").setRpcTimeoutSecs(rpcTimeoutSecs)
                .build();

        assertThat(config.getRpcTimeoutSecs()).isEqualTo(rpcTimeoutSecs);
    }

}
