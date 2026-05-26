package com.gevin.kvstore;

import io.grpc.Status;
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

    @Override
    public void replicateDelete(com.gevin.kvstore.DeleteRequest request,
                                io.grpc.stub.StreamObserver<com.gevin.kvstore.DeleteResponse> responseObserver) {
        try {
            String key = request.getKey();
            storageEngine.delete(key);
            System.out.println("Deleted key-value successfully for key: "+ key);

            responseObserver.onNext(DeleteResponse.newBuilder().setSuccess(true).build());
            responseObserver.onCompleted();
        }
        catch (Exception e) {
            System.err.println("Failed to delete key in backup node: " + e.getMessage());
            responseObserver.onError(io.grpc.Status.INTERNAL
                    .withDescription("Backup delete failed: " + e.getMessage())
                    .asRuntimeException());
        }
    }

}
