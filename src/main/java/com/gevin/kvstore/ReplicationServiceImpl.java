package com.gevin.kvstore;

import io.grpc.stub.StreamObserver;

public class ReplicationServiceImpl extends ReplicationServiceGrpc.ReplicationServiceImplBase {
    private final StorageEngine storageEngine;

    public ReplicationServiceImpl(StorageEngine storage) {
        this.storageEngine = storage;
    }

    @Override
    public void replicate(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        storageEngine.put(request.getKey(), request.getValue().toByteArray());

        responseObserver.onNext(PutResponse.newBuilder().setSuccess(true).build());
        responseObserver.onCompleted();
    }

}
