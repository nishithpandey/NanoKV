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

package io.microraft.nanokv.client.internal;

import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.netflix.governator.guice.LifecycleInjector;
import com.netflix.governator.lifecycle.LifecycleManager;
import io.microraft.nanokv.client.NanoKVClient;
import io.microraft.nanokv.client.NanoKVClientException;
import io.microraft.nanokv.client.config.NanoKVClientConfig;
import io.microraft.nanokv.client.internal.di.NanoKVClientModule;
import io.microraft.nanokv.client.kv.KV;
import io.microraft.nanokv.internal.lifecycle.TerminationAware;

import javax.annotation.Nonnull;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.google.inject.name.Names.named;
import static io.microraft.nanokv.client.internal.di.NanoKVClientModule.CLIENT_ID_KEY;
import static io.microraft.nanokv.client.internal.di.NanoKVClientModule.KV_STORE_KEY;

public class NanoKVClientImpl implements NanoKVClient {

    private final NanoKVClientConfig config;
    private final Injector injector;
    private final LifecycleManager lifecycleManager;
    private final KV kv;
    private final String clientId;
    private final AtomicReference<Status> status = new AtomicReference<>(Status.LATENT);
    private final AtomicBoolean processTerminationFlag = new AtomicBoolean();
    private volatile boolean terminationCompleted;

    public NanoKVClientImpl(NanoKVClientConfig config) {
        this.config = config;
        try {
            Module module = new NanoKVClientModule(config, processTerminationFlag);
            this.injector = LifecycleInjector.builder().withModules(module).build().createInjector();
            this.lifecycleManager = injector.getInstance(LifecycleManager.class);

            lifecycleManager.start();
            status.set(Status.RUNNING);

            Supplier<KV> kvStoreSupplier = injector.getInstance(Key.get(new TypeLiteral<Supplier<KV>>() {
            }, named(KV_STORE_KEY)));
            this.kv = kvStoreSupplier.get();
            this.clientId = injector.getInstance(Key.get(String.class, named(CLIENT_ID_KEY)));

            registerShutdownHook();
        } catch (Throwable t) {
            shutdown();
            throw new NanoKVClientException("Could not start client!", t);
        }
    }

    private void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            processTerminationFlag.set(true);

            if (!isShutdown()) {
                System.out.println(clientId + " shutting down...");
            }

            shutdown();
        }));
    }

    @Nonnull
    @Override
    public NanoKVClientConfig getConfig() {
        return config;
    }

    @Nonnull
    @Override
    public KV getKV() {
        return kv;
    }

    @Override
    public void shutdown() {
        if (status.compareAndSet(Status.RUNNING, Status.SHUTTING_DOWN)) {
            try {
                lifecycleManager.close();
            } finally {
                status.set(Status.SHUT_DOWN);
            }
        } else {
            status.compareAndSet(Status.LATENT, Status.SHUT_DOWN);
        }
    }

    @Override
    public boolean isShutdown() {
        return status.get() == Status.SHUT_DOWN;
    }

    @Override
    public void awaitTermination() {
        if (terminationCompleted) {
            return;
        }

        injector.getAllBindings().values().stream()
                .filter(binding -> binding.getProvider().get() instanceof TerminationAware)
                .map(binding -> (TerminationAware) binding.getProvider().get())
                .forEach(TerminationAware::awaitTermination);
        terminationCompleted = true;
    }

    private enum Status
    {
        LATENT, RUNNING, SHUTTING_DOWN, SHUT_DOWN
    }

}
