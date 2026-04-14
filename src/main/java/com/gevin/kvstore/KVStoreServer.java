package com.gevin.kvstore;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class KVStoreServer {
    private Server server;

    public void start() throws IOException {
        int port = 9999;
        server = ServerBuilder.forPort(port)
                .addService(new KVStoreService())
                .build().start();

        System.out.println("Server started, running on port: " + port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("!!! shutting down gRPC server since JVM is shutting down !!!");
            try {
                KVStoreServer.this.stop();
            }
            catch (Exception e) {
                e.printStackTrace(System.err);
            }
        }) );
    }

    private void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        final KVStoreServer server = new KVStoreServer();
        server.start();
        server.blockUntilShutdown();
    }

}
