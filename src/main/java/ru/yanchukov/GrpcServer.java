package ru.yanchukov;

import io.github.cdimascio.dotenv.Dotenv;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.tarantool.client.box.TarantoolBoxClient;
import io.tarantool.client.factory.TarantoolFactory;
import io.tarantool.pool.InstanceConnectionGroup;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class GrpcServer {
    private final Server server;
    private final TarantoolBoxClient tarantoolClient;

    public GrpcServer(String host, int tarantoolPort, String user, String password, int grpcPort) throws Exception {
        InstanceConnectionGroup connectionGroup = InstanceConnectionGroup.builder()
                .withHost(host)
                .withPort(tarantoolPort)
                .withUser(user)
                .withPassword(password)
                .build();

        tarantoolClient = TarantoolFactory.box()
                .withGroups(Collections.singletonList(connectionGroup))
                .build();

        KvServiceImpl kvService = new KvServiceImpl(tarantoolClient);

        server = ServerBuilder
                .forPort(grpcPort)
                .addService(kvService)
                .build()
                .start();

        System.out.println("gRPC server is started on the port " + grpcPort);
    }

    public void launch() throws InterruptedException {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
                tarantoolClient.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("Server is stopped");
        }));

        server.awaitTermination();
    }

    public static void main(String[] args) {
        Dotenv dotenv = Dotenv.configure()
                .ignoreIfMissing()
                .load();
        String host = dotenv.get("TARANTOOL_HOST", "localhost");
        int tarantoolPort = Integer.parseInt(dotenv.get("TARANTOOL_PORT", "3301"));
        String user = dotenv.get("TARANTOOL_USER", "user");
        String password = dotenv.get("TARANTOOL_PASSWORD", "password");
        int grpcPort = Integer.parseInt(dotenv.get("GRPC_PORT", "9090"));

        try {
            GrpcServer server = new GrpcServer(host, tarantoolPort, user, password, grpcPort);
            server.launch();
        } catch (Exception e) {
            System.err.println("Error start of service: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}