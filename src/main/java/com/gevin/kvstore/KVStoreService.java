package com.gevin.kvstore;

import io.grpc.stub.StreamObserver;

public class KVStoreService extends KVStoreGrpc.KVStoreImplBase {

    private final StorageEngine storage = new FileStorageDecorator(new InMemoryStorage());

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        String key = request.getKey();
        byte[] value = request.getValue().getBytes();

        System.out.println("Received PUT for key : " + key);

        PutResponse response = PutResponse.newBuilder()
                .setSuccess(true)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();

    }
}