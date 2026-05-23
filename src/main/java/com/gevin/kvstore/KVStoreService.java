package com.gevin.kvstore;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.stub.StreamObserver;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class KVStoreService extends KVStoreGrpc.KVStoreImplBase {

    private final StorageEngine storage;
    List<ReplicationServiceGrpc.ReplicationServiceFutureStub> stubs;
    private final String serverType;

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
        try {
            storage.put(key, value);
            if (serverType.equals("PRIMARY")) {
                replicateToBackups(request);
            }
            PutResponse response = PutResponse.newBuilder()
                    .setSuccess(true)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
        catch (Exception e) {
            System.err.println("Couldn't add key, value ");
            e.printStackTrace(System.err);

            responseObserver.onError(io.grpc.Status.INTERNAL
                    .withDescription("Failed to write to storage: " + e.getMessage())
                    .asRuntimeException());
        }



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

    private void replicateToBackups(PutRequest request) {
        System.out.println("DEBUG: Primary fanning out to " + stubs.size() + " stubs");
        for (ReplicationServiceGrpc.ReplicationServiceFutureStub stub: stubs) {
            try {
                // .get() forces the code to stop and wait for the Backup to respond
                // This transforms the call from Async to Sync temporarily
                PutResponse resp = stub.replicate(request).get(5, TimeUnit.SECONDS);
                System.out.println("SUCCESS: Backup responded with: " + resp.getSuccess());
            } catch (Exception e) {
                System.err.println("FAILURE during replication call:");
                e.printStackTrace();
            }
        }
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
                com.google.common.util.concurrent.MoreExecutors.directExecutor()
        );

        return completableFuture;
    }
}