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

package io.microraft.nanokv.internal.raft.model;

import io.microraft.nanokv.raft.proto.RaftEndpointProto;
import io.microraft.RaftEndpoint;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class NanoKVEndpoint implements RaftEndpoint {

    private static final ConcurrentMap<String, NanoKVEndpoint> cache = new ConcurrentHashMap<>();
    private RaftEndpointProto endpoint;

    public NanoKVEndpoint(RaftEndpointProto endpoint) {
        this.endpoint = endpoint;
    }

    public static NanoKVEndpoint wrap(@Nonnull RaftEndpointProto endpoint) {
        return cache.computeIfAbsent(endpoint.getId(), id -> new NanoKVEndpoint(endpoint));
    }

    public static RaftEndpointProto unwrap(@Nullable RaftEndpoint endpoint) {
        return endpoint != null ? ((NanoKVEndpoint) endpoint).getEndpoint() : null;
    }

    public RaftEndpointProto getEndpoint() {
        return endpoint;
    }

    @Override
    public int hashCode() {
        return endpoint.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        NanoKVEndpoint that = (NanoKVEndpoint) o;

        return endpoint.equals(that.endpoint);
    }

    @Override
    public String toString() {
        return "NanoKVEndpoint{" + "id=" + getId() + '}';
    }

    @Nonnull
    @Override
    public Object getId() {
        return endpoint.getId();
    }

}
