package com.gevin.kvstore;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class KVStoreServer {
    private Server server;
    private final StorageEngine storage = new FileStorageDecorator(new InMemoryStorage());
    private final int port;
    private String serverState;

    public KVStoreServer(int portNumber, String serverState) {
        this.port = portNumber;
        this.serverState = serverState;
    }

    public void start() throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new KVStoreService(storage))
                .build().start();

        System.out.println("Server started, running on port: " + port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("!!! shutting down gRPC server since JVM is shutting down !!!");
            try {
                KVStoreServer.this.stop();
                storage.close();
                System.err.println("!!! WAL closed and server shut down successfully !!!");
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
        int portNumber = 9090;
        String serverState = "PRIMARY";
        ArrayList<String> backupServers = new ArrayList<>();
        if (args.length > 0)
            portNumber = Integer.parseInt(args[0]);
        if (args.length > 1) {
            serverState = args[1];
            if (serverState.equals("PRIMARY")) {
                for (int i = 2; i < args.length; i++) {
                    backupServers.add(args[i]);
                }
            }
        }
        final KVStoreServer server = new KVStoreServer(portNumber, serverState);
        server.start();
        server.blockUntilShutdown();
    }

}
