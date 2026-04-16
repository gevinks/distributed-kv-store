package com.gevin.kvstore;

import io.grpc.stub.StreamObserver;

public class ReplicationServiceImpl extends ReplicationServiceGrpc.ReplicationServiceImplBase {
    private final StorageEngine storageEngine;

    public ReplicationServiceImpl(StorageEngine storage) {

        this.storageEngine = storage;
        System.out.println("DEBUG: ReplicationServiceImpl is initialized and ready!");
    }

    @Override
    public void replicate(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        storageEngine.put(request.getKey(), request.getValue().toByteArray());
        System.out.println("Received key in backup!!!");
        responseObserver.onNext(PutResponse.newBuilder().setSuccess(true).build());
        responseObserver.onCompleted();
    }

}
