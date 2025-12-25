package io.microraft.nanokv.client;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.microraft.nanokv.NanoKV;
import io.microraft.nanokv.client.config.NanoKVClientConfig;
import io.microraft.nanokv.client.kv.KV;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.microraft.RaftNode;
import io.microraft.test.util.BaseTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Random;

import static com.typesafe.config.ConfigFactory.load;
import static io.microraft.nanokv.utils.NanoKVTestUtils.CONFIG_1;
import static io.microraft.nanokv.utils.NanoKVTestUtils.CONFIG_2;
import static io.microraft.nanokv.utils.NanoKVTestUtils.CONFIG_3;
import static io.microraft.nanokv.utils.NanoKVTestUtils.getAnyFollower;
import static io.microraft.nanokv.utils.NanoKVTestUtils.getRaftNode;
import static io.microraft.nanokv.utils.NanoKVTestUtils.waitUntilLeaderElected;
import static io.microraft.test.util.AssertionUtils.eventually;
import static io.microraft.test.util.AssertionUtils.sleepMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class LeaderServerChangeTest extends BaseTest {

    private static List<NanoKV> servers = new ArrayList<>();
    private static NanoKVClient client;
    private static KV kv;

    private final Random random = new Random();

    @Before
    public void init() {
        servers.add(NanoKV.bootstrap(CONFIG_1));
        servers.add(NanoKV.bootstrap(CONFIG_2));
        servers.add(NanoKV.bootstrap(CONFIG_3));

        String serverAddress = servers.get(random.nextInt(servers.size())).getConfig().getLocalEndpointConfig()
                .getAddress();
        Config config = ConfigFactory.parseString("nanokv.client.server-address: \"" + serverAddress + "\"")
                .withFallback(load("client.conf"));
        client = NanoKVClient.newInstance(NanoKVClientConfig.from(config));
        kv = client.getKV();
    }

    @After
    public void tearDown() {
        servers.forEach(NanoKV::shutdown);
        if (client != null) {
            client.shutdown();
        }
    }

    @Test
    (timeout=300_000)public void when_leaderFails_then_kvCallsSucceedAfterNewLeaderElected() {
        NanoKV leader = waitUntilLeaderElected(servers);
        leader.shutdown();

        eventually(() -> {
            try {
                kv.put("key", "val");
            } catch (Throwable t) {
                fail(t.getMessage());
                sleepMillis(100);
            }
        });
    }

    @Test
    (timeout=300_000)public void when_clusterMovesToNewTermAndLeader_then_kvCallsSucceed() {
        NanoKV leader = waitUntilLeaderElected(servers);
        RaftNode leaderRaftNode = getRaftNode(leader);
        NanoKV follower = getAnyFollower(servers);

        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicBoolean transferDone = new AtomicBoolean(false);

        Thread thread = new Thread(() -> {
            try {
                startLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            try {
                leaderRaftNode.transferLeadership(follower.getLocalEndpoint()).join();
                transferDone.set(true);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        thread.start();

        while (!transferDone.get()) {
            try {
                kv.put("key", "val");
            } catch (StatusRuntimeException e) {
                assertThat(e.getStatus().getCode()).isEqualTo(Status.RESOURCE_EXHAUSTED.getCode());
            }

            sleepMillis(10);
            startLatch.countDown();
        }
    }

}
