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

package io.microraft.nanokv.utils;

import io.microraft.nanokv.NanoKV;
import io.microraft.nanokv.config.NanoKVConfig;
import io.microraft.nanokv.internal.NanoKVImpl;
import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.report.RaftGroupMembers;

import java.util.List;

import static com.typesafe.config.ConfigFactory.load;
import static io.microraft.test.util.AssertionUtils.eventually;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

public final class NanoKVTestUtils {

    public static final NanoKVConfig CONFIG_1 = NanoKVConfig.from(load("node1.conf"));
    public static final NanoKVConfig CONFIG_2 = NanoKVConfig.from(load("node2.conf"));
    public static final NanoKVConfig CONFIG_3 = NanoKVConfig.from(load("node3.conf"));

    private NanoKVTestUtils() {
    }

    public static NanoKV getAnyFollower(List<NanoKV> servers) {
        RaftEndpoint leaderEndpoint = getLeaderEndpoint(servers);
        if (leaderEndpoint == null) {
            throw new AssertionError("Group doesn't have a leader yet!");
        }

        for (NanoKV server : servers) {
            if (!server.isShutdown() && !leaderEndpoint.equals(server.getLocalEndpoint())) {
                return server;
            }
        }

        throw new AssertionError("No follower server available!");
    }

    public static RaftEndpoint getLeaderEndpoint(List<NanoKV> servers) {
        RaftEndpoint leader = null;
        for (NanoKV server : servers) {
            if (server.isShutdown()) {
                continue;
            }

            RaftEndpoint leaderEndpoint = getLeaderEndpoint(server);
            if (leader == null) {
                leader = leaderEndpoint;
            } else if (!leader.equals(leaderEndpoint)) {
                throw new AssertionError("Raft group doesn't have a single leader endpoint yet!");
            }
        }

        return leader;
    }

    public static RaftEndpoint getLeaderEndpoint(NanoKV server) {
        return ((NanoKVImpl) server).getRaftNode().getTerm().getLeaderEndpoint();
    }

    public static List<NanoKV> getFollowers(List<NanoKV> servers) {
        NanoKV leader = waitUntilLeaderElected(servers);
        return servers.stream().filter(server -> server != leader).collect(toList());
    }

    public static NanoKV waitUntilLeaderElected(List<NanoKV> servers) {
        NanoKV[] leaderRef = new NanoKV[1];
        eventually(() -> {
            NanoKV leaderServer = getLeader(servers);
            assertThat(leaderServer).isNotNull();

            int leaderTerm = getTerm(leaderServer);

            for (NanoKV server : servers) {
                if (server.isShutdown()) {
                    continue;
                }

                RaftEndpoint leader = getLeaderEndpoint(server);
                assertThat(leader).isEqualTo(leaderServer.getLocalEndpoint());
                assertThat(getTerm(server)).isEqualTo(leaderTerm);
            }

            leaderRef[0] = leaderServer;
        });

        return leaderRef[0];
    }

    public static NanoKV getLeader(List<NanoKV> servers) {
        RaftEndpoint leaderEndpoint = getLeaderEndpoint(servers);
        if (leaderEndpoint == null) {
            return null;
        }

        for (NanoKV server : servers) {
            if (!server.isShutdown() && leaderEndpoint.equals(server.getLocalEndpoint())) {
                return server;
            }
        }

        throw new AssertionError("Leader endpoint is " + leaderEndpoint + ", but leader server could not be found!");
    }

    public static int getTerm(NanoKV server) {
        return ((NanoKVImpl) server).getRaftNode().getTerm().getTerm();
    }

    public static RaftNode getRaftNode(NanoKV server) {
        return ((NanoKVImpl) server).getRaftNode();
    }

    public static RaftGroupMembers getRaftGroupMembers(NanoKV server) {
        return ((NanoKVImpl) server).getRaftNode().getCommittedMembers();
    }

}
