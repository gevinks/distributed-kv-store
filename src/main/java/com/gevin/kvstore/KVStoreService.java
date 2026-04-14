package com.gevin.kvstore;

import io.grpc.stub.StreamObserver;

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
}