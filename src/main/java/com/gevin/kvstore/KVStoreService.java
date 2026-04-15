package com.gevin.kvstore;

import io.grpc.stub.StreamObserver;

import java.util.Optional;

public class KVStoreService extends KVStoreGrpc.KVStoreImplBase {

    private final StorageEngine storage;

    public KVStoreService(StorageEngine storage) {
        this.storage = storage;
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        String key = request.getKey();
        byte[] value = request.getValue().toByteArray();

        System.out.println("Received PUT for key : " + key);
        try {
            storage.put(key, value);
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
}