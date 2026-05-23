package com.gevin.kvstore;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.stub.StreamObserver;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class KVStoreService extends KVStoreGrpc.KVStoreImplBase {

    private final StorageEngine storage;
    List<ReplicationServiceGrpc.ReplicationServiceFutureStub> stubs;
    private final String serverType;
    private final java.util.concurrent.ExecutorService ioExecutor = java.util.concurrent.Executors.newFixedThreadPool(8);

    public KVStoreService(StorageEngine storage, String serverType, List<ReplicationServiceGrpc.ReplicationServiceFutureStub> stubs) {
        this.storage = storage;
        this.serverType = serverType;
        this.stubs = stubs;
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        String key = request.getKey();
        byte[] value = request.getValue().toByteArray();

        System.out.println("Received PUT for key : " + key);

        java.util.concurrent.CompletableFuture<Void> localWriteTask =
                java.util.concurrent.CompletableFuture.runAsync(() -> {
                    try {
                        storage.put(key, value);
                    }
                    catch (Exception e) {
                        System.err.println("Local write failed: " + e);
                        throw new RuntimeException(e);
                    }
                }, ioExecutor);

        java.util.concurrent.CompletableFuture<Void> replicationTask =
                replicateToBackupsAsync(request);

        localWriteTask.runAfterBothAsync(replicationTask, () -> {
            PutResponse response = PutResponse.newBuilder()
                    .setSuccess(true)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            System.out.println("Async PUT pipeline finished successfully for key: " + key);
        }, ioExecutor).exceptionally(ex -> {
            System.err.println("CRITICAL: Async PUT pipeline failed for key: " + key);
            responseObserver.onError(io.grpc.Status.INTERNAL
                    .withDescription("Distributed async write failed: " + ex.getMessage())
                    .asRuntimeException());
            return null;
        });

    }

    @Override
    public void get(GetRequest getRequest, StreamObserver<GetResponse> responseObserver) {
        Optional<byte[]> value = storage.get(getRequest.getKey());

        GetResponse.Builder getResponseBuilder = GetResponse.newBuilder();
        if (value.isPresent()) {
            getResponseBuilder.setFound(true);
            getResponseBuilder.setValue(com.google.protobuf.ByteString.copyFrom(value.get()));
        }
        else {
            getResponseBuilder.setFound(false);
        }

        responseObserver.onNext(getResponseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void delete(DeleteRequest deleteRequest, StreamObserver<DeleteResponse> responseObserver) {
        String key = deleteRequest.getKey();
        System.out.println("Deleting entry of key : " + key);
        try {
            storage.delete(key);
            DeleteResponse response = DeleteResponse.newBuilder()
                    .setSuccess(true)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            System.err.println("Couldn't add key, value ");
            e.printStackTrace(System.err);

            responseObserver.onError(io.grpc.Status.INTERNAL
                    .withDescription("Failed to write to storage: " + e.getMessage())
                    .asRuntimeException());
        }
    }

    private java.util.concurrent.CompletableFuture<Void> replicateToBackupsAsync(PutRequest request) {
        if (!serverType.equals("PRIMARY") || stubs.isEmpty())
            return java.util.concurrent.CompletableFuture.completedFuture(null);

        java.util.List<java.util.concurrent.CompletableFuture<PutResponse>> futures =
                stubs.stream().map(stub -> toCompletableFuture(stub.replicate(request))).toList();

        return java.util.concurrent.CompletableFuture.allOf(
                futures.toArray(new java.util.concurrent.CompletableFuture[0])
        );
    }


    // Add this method at the bottom of your KVStoreService class
    private <T> java.util.concurrent.CompletableFuture<T> toCompletableFuture(
            com.google.common.util.concurrent.ListenableFuture<T> listenableFuture
    ) {
        java.util.concurrent.CompletableFuture<T> completableFuture = new java.util.concurrent.CompletableFuture<>();

        com.google.common.util.concurrent.Futures.addCallback(
                listenableFuture,
                new com.google.common.util.concurrent.FutureCallback<T>() {
                    @Override
                    public void onSuccess(T result) {
                        // System network call succeeded! Complete our Java future
                        completableFuture.complete(result);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        // System network call failed! Pass the exception down the pipeline
                        completableFuture.completeExceptionally(t);
                    }
                },
                this.ioExecutor
        );

        return completableFuture;
    }
}