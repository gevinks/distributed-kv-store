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

        PutResponse response = PutResponse.newBuilder()
                .setSuccess(true)
                .build();
        storage.put(key, value);
        responseObserver.onNext(response);
        responseObserver.onCompleted();

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
}