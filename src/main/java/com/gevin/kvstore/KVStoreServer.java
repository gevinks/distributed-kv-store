package com.gevin.kvstore;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class KVStoreServer {
    private Server server;
    private final StorageEngine storage = new FileStorageDecorator(new InMemoryStorage());
    private final int port;
    private String serverState;
    private static List<ManagedChannel> channels = new ArrayList<>();
    List<ReplicationServiceGrpc.ReplicationServiceFutureStub> stubs;

    public KVStoreServer(int portNumber, String serverState, List<ReplicationServiceGrpc.ReplicationServiceFutureStub> stubs) {
        this.port = portNumber;
        this.serverState = serverState;
        this.stubs = stubs;
    }

    public void start() throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new KVStoreService(storage, serverState, stubs))
                .addService(new ReplicationServiceImpl(storage))
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
            for (ManagedChannel channel: channels) {
                channel.shutdown().awaitTermination(10, TimeUnit.SECONDS);
            }
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
        List<ReplicationServiceGrpc.ReplicationServiceFutureStub> stubs = new ArrayList<>();
        if (args.length > 0)
            portNumber = Integer.parseInt(args[0]);
        if (args.length > 1) {
            serverState = args[1];
            if (serverState.equals("PRIMARY")) {
                for (int i = 2; i < args.length; i++) {
                    System.out.println("Servers - " + args[i]);
                    backupServers.add(args[i]);
                    ManagedChannel channel = ManagedChannelBuilder.forTarget(args[i])
                            .usePlaintext()
                            .build();
                    channel.getState(true);
                    channels.add(channel);
                    stubs.add(ReplicationServiceGrpc.newFutureStub(channel));
                }

            }
        }
        System.out.println("Starting server as " + serverState + " on port " + portNumber);
        final KVStoreServer server = new KVStoreServer(portNumber, serverState, stubs);
        server.start();
        server.blockUntilShutdown();
    }

}
