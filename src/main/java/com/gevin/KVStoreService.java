package com.gevin;

import com.gevin.kvstore.GetRequest;
import com.gevin.kvstore.GetResponse;
import com.gevin.kvstore.KVStoreGrpc;
import io.grpc.stub.StreamObserver;

public class KVStoreService extends KVStoreGrpc.KVStoreImplBase {
    
    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        // Implementation for GET
    }
}
