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
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class KvServiceTest {

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
    }

    @AfterAll
    static void teardown() throws Exception {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        grpcServer.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        tarantoolClient.close();
    }

    @AfterEach
    void cleanup() {
        for (String key : List.of("test-key", "test-null", "test-overwrite",
                "test-a", "test-b", "test-c", "test-bin")) {
            try {
                stub.delete(DeleteRequest.newBuilder().setKey(key).build());
            } catch (Exception ignored) {}
        }
    }

    @Test
    @Order(1)
    void put_withValue_succeeds() {
        PutResponse resp = stub.put(PutRequest.newBuilder()
                .setKey("test-key")
                .setValue(ByteString.copyFromUtf8("hello"))
                .build());
        assertNotNull(resp);
    }

    @Test
    @Order(2)
    void put_withoutValue_succeeds() {
        PutResponse resp = stub.put(PutRequest.newBuilder()
                .setKey("test-null")
                .build());
        assertNotNull(resp);
    }

    @Test
    @Order(3)
    void put_overwritesExistingKey() {
        stub.put(PutRequest.newBuilder().setKey("test-overwrite").setValue(ByteString.copyFromUtf8("first")).build());
        stub.put(PutRequest.newBuilder().setKey("test-overwrite").setValue(ByteString.copyFromUtf8("second")).build());

        GetResponse resp = stub.get(GetRequest.newBuilder().setKey("test-overwrite").build());
        assertEquals("second", resp.getValue().toStringUtf8());
    }

    @Test
    @Order(4)
    void get_existingKey_returnsValue() {
        stub.put(PutRequest.newBuilder().setKey("test-key").setValue(ByteString.copyFromUtf8("world")).build());

        GetResponse resp = stub.get(GetRequest.newBuilder().setKey("test-key").build());
        assertTrue(resp.hasValue());
        assertEquals("world", resp.getValue().toStringUtf8());
    }

    @Test
    @Order(5)
    void get_nonExistentKey_returnsEmpty() {
        GetResponse resp = stub.get(GetRequest.newBuilder().setKey("no-such-key-xyz").build());
        assertFalse(resp.hasValue());
    }

    @Test
    @Order(6)
    void get_nullValue_returnsEmpty() {
        stub.put(PutRequest.newBuilder().setKey("test-null").build());

        GetResponse resp = stub.get(GetRequest.newBuilder().setKey("test-null").build());
        assertFalse(resp.hasValue());
    }

    @Test
    @Order(7)
    void get_binaryValue_preserved() {
        byte[] binary = new byte[256];
        for (int i = 0; i < 256; i++) binary[i] = (byte) i;

        stub.put(PutRequest.newBuilder()
                .setKey("test-bin")
                .setValue(ByteString.copyFrom(binary))
                .build());

        GetResponse resp = stub.get(GetRequest.newBuilder().setKey("test-bin").build());
        assertArrayEquals(binary, resp.getValue().toByteArray());
    }

    @Test
    @Order(8)
    void delete_existingKey_removesIt() {
        stub.put(PutRequest.newBuilder().setKey("test-key").setValue(ByteString.copyFromUtf8("x")).build());
        stub.delete(DeleteRequest.newBuilder().setKey("test-key").build());

        GetResponse resp = stub.get(GetRequest.newBuilder().setKey("test-key").build());
        assertFalse(resp.hasValue());
    }

    @Test
    @Order(9)
    void delete_nonExistentKey_doesNotThrow() {
        assertDoesNotThrow(() ->
                stub.delete(DeleteRequest.newBuilder().setKey("no-such-key-xyz").build()));
    }

    @Test
    @Order(10)
    void count_returnsNonNegative() {
        CountResponse resp = stub.count(CountRequest.newBuilder().build());
        assertTrue(resp.getCount() >= 0);
    }

    @Test
    @Order(11)
    void count_increasesAfterPut() {
        long before = stub.count(CountRequest.newBuilder().build()).getCount();
        stub.put(PutRequest.newBuilder().setKey("test-key").setValue(ByteString.copyFromUtf8("x")).build());
        long after = stub.count(CountRequest.newBuilder().build()).getCount();
        assertEquals(before + 1, after);
    }

    @BeforeEach
    void setupRangeData() {
        stub.put(PutRequest.newBuilder().setKey("test-a").setValue(ByteString.copyFromUtf8("1")).build());
        stub.put(PutRequest.newBuilder().setKey("test-b").setValue(ByteString.copyFromUtf8("2")).build());
        stub.put(PutRequest.newBuilder().setKey("test-c").setValue(ByteString.copyFromUtf8("3")).build());
    }

    @Test
    @Order(12)
    void range_returnsAllInBounds() {
        List<RangeResponse> results = new ArrayList<>();
        stub.range(RangeRequest.newBuilder().setKeySince("test-a").setKeyTo("test-c").build())
                .forEachRemaining(results::add);

        List<String> keys = results.stream().map(RangeResponse::getKey).toList();
        assertTrue(keys.contains("test-a"));
        assertTrue(keys.contains("test-b"));
        assertTrue(keys.contains("test-c"));
    }

    @Test
    @Order(13)
    void range_respectsUpperBound() {
        List<RangeResponse> results = new ArrayList<>();
        stub.range(RangeRequest.newBuilder().setKeySince("test-a").setKeyTo("test-b").build())
                .forEachRemaining(results::add);

        List<String> keys = results.stream().map(RangeResponse::getKey).toList();
        assertTrue(keys.contains("test-a"));
        assertTrue(keys.contains("test-b"));
        assertFalse(keys.contains("test-c"));
    }

    @Test
    @Order(14)
    void range_emptyInterval_returnsNothing() {
        List<RangeResponse> results = new ArrayList<>();
        stub.range(RangeRequest.newBuilder().setKeySince("zzz-a").setKeyTo("zzz-z").build())
                .forEachRemaining(results::add);

        assertTrue(results.isEmpty());
    }

    @Test
    @Order(15)
    void range_singleKey_returnsOne() {
        List<RangeResponse> results = new ArrayList<>();
        stub.range(RangeRequest.newBuilder().setKeySince("test-a").setKeyTo("test-a").build())
                .forEachRemaining(results::add);

        assertEquals(1, results.size());
        assertEquals("test-a", results.getFirst().getKey());
        assertEquals("1", results.getFirst().getValue().toStringUtf8());
    }

    @Test
    @Order(16)
    void range_resultIsOrdered() {
        List<RangeResponse> results = new ArrayList<>();
        stub.range(RangeRequest.newBuilder().setKeySince("test-a").setKeyTo("test-c").build())
                .forEachRemaining(results::add);

        List<String> keys = results.stream().map(RangeResponse::getKey)
                .filter(k -> k.startsWith("test-")).toList();
        assertEquals(keys.stream().sorted().toList(), keys);
    }
}