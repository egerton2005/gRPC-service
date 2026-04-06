package ru.yanchukov;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.tarantool.client.box.TarantoolBoxClient;
import io.tarantool.client.factory.TarantoolFactory;
import io.tarantool.pool.InstanceConnectionGroup;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;
import ru.yanchukov.grpc.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class KvServicePerformanceTest {

    static final int SEED_COUNT = 1_000_000;
    static final int BATCH = 10_000;

    @Container
    static GenericContainer<?> tarantool = new GenericContainer<>("tarantool/tarantool:3.2")
            .withExposedPorts(3301)
            .withCopyFileToContainer(
                    MountableFile.forClasspathResource("init.lua"),
                    "/opt/tarantool/init.lua"
            )
            .withCommand("tarantool /opt/tarantool/init.lua")
            .waitingFor(Wait.forLogMessage(".*ready to accept requests.*", 1));

    static KvServiceGrpc.KvServiceBlockingStub stub;
    static Server grpcServer;
    static ManagedChannel channel;
    static TarantoolBoxClient tarantoolClient;

    @BeforeAll
    static void setup() throws Exception {
        String host = tarantool.getHost();
        int port = tarantool.getMappedPort(3301);

        InstanceConnectionGroup connectionGroup = InstanceConnectionGroup.builder()
                .withHost(host)
                .withPort(port)
                .withUser("user")
                .withPassword("password")
                .build();

        tarantoolClient = TarantoolFactory.box()
                .withGroups(Collections.singletonList(connectionGroup))
                .build();

        grpcServer = ServerBuilder.forPort(0)
                .addService(new KvServiceImpl(tarantoolClient))
                .build()
                .start();

        channel = ManagedChannelBuilder
                .forAddress("localhost", grpcServer.getPort())
                .usePlaintext()
                .build();

        stub = KvServiceGrpc.newBlockingStub(channel);

        seed();
    }

    static void seed() {
        System.out.printf("Seeding %,d records...%n", SEED_COUNT);
        long start = System.currentTimeMillis();

        String lua =
                "local data = ... " +
                        "for _, row in ipairs(data) do " +
                        "    box.space.KV:replace(row) " +
                        "end";

        for (int batchStart = 0; batchStart < SEED_COUNT; batchStart += BATCH) {
            int batchEnd = Math.min(batchStart + BATCH, SEED_COUNT);
            List<List<Object>> rows = new ArrayList<>();
            for (int i = batchStart; i < batchEnd; i++) {
                String key = String.format("key-%08d", i);
                byte[] value = String.format("value-%d", i).getBytes();
                rows.add(List.of(key, value));
            }
            tarantoolClient.eval(lua, List.of(rows)).join();

            if (batchStart % 100_000 == 0 && batchStart > 0) {
                long elapsed = System.currentTimeMillis() - start;
                System.out.printf("  %,d / %,d  (%.1f%%)  elapsed: %.1fs%n",
                        batchEnd, SEED_COUNT,
                        (double) batchEnd / SEED_COUNT * 100,
                        elapsed / 1000.0);
            }
        }

        long elapsed = System.currentTimeMillis() - start;
        System.out.printf("Seed done: %,d records in %.1fs%n", SEED_COUNT, elapsed / 1000.0);
    }

    @AfterAll
    static void teardown() throws Exception {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        grpcServer.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        tarantoolClient.close();
    }

    @Test
    @Order(1)
    void count_returns_seeded_amount() {
        CountResponse resp = stub.count(CountRequest.newBuilder().build());
        assertEquals(SEED_COUNT, resp.getCount());
    }

    @Test
    @Order(2)
    void get_performance_1000_requests() {
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            stub.get(GetRequest.newBuilder()
                    .setKey(String.format("key-%08d", i))
                    .build());
        }
        long elapsed = System.currentTimeMillis() - start;
        double rps = 1000.0 / elapsed * 1000;
        System.out.printf("Get: 1000 requests in %dms (%.0f rps)%n", elapsed, rps);
        assertTrue(elapsed < 10_000, "1000 get requests should complete in under 10s");
    }

    @Test
    @Order(3)
    void get_existing_key_correct_value() {
        GetResponse resp = stub.get(GetRequest.newBuilder()
                .setKey("key-00000042")
                .build());
        assertTrue(resp.hasValue());
        assertEquals("value-42", resp.getValue().toStringUtf8());
    }

    @Test
    @Order(4)
    void range_1000_records() {
        long start = System.currentTimeMillis();
        List<RangeResponse> results = new ArrayList<>();

        Iterator<RangeResponse> stream = stub.range(
                RangeRequest.newBuilder()
                        .setKeySince("key-00000000")
                        .setKeyTo("key-00000999")
                        .build()
        );

        while (stream.hasNext()) {
            results.add(stream.next());
        }

        long elapsed = System.currentTimeMillis() - start;
        System.out.printf("Range 1000: %d records in %dms%n", results.size(), elapsed);

        assertEquals(1000, results.size());
        assertTrue(elapsed < 3000, "Range of 1000 should complete in under 3s");
    }

    @Test
    @Order(5)
    void range_result_is_ordered() {
        List<RangeResponse> results = new ArrayList<>();
        stub.range(RangeRequest.newBuilder()
                        .setKeySince("key-00000000")
                        .setKeyTo("key-00000099")
                        .build())
                .forEachRemaining(results::add);

        List<String> keys = results.stream().map(RangeResponse::getKey).toList();
        assertEquals(keys.stream().sorted().toList(), keys);
    }

    @Test
    @Order(6)
    void put_performance_1000_requests() {
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            stub.put(PutRequest.newBuilder()
                    .setKey(String.format("perf-key-%08d", i))
                    .setValue(ByteString.copyFromUtf8("perf-value"))
                    .build());
        }
        long elapsed = System.currentTimeMillis() - start;
        double rps = 1000.0 / elapsed * 1000;
        System.out.printf("Put: 1000 requests in %dms (%.0f rps)%n", elapsed, rps);
        assertTrue(elapsed < 10_000, "1000 put requests should complete in under 10s");
    }
}