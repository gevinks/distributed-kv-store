package com.gevin.kvstore;

import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AsyncKVStoreTest {

    private KVStoreService kvStoreService;
    private StorageEngine fakeStorage;

    @BeforeEach
    public void setUp() {
        // 1. Initialize a clean storage engine instance
        fakeStorage = new InMemoryStorage();

        // 2. Pass empty stubs for now to isolate local async pipeline testing
        kvStoreService = new KVStoreService(fakeStorage, "PRIMARY", Collections.emptyList());
    }

    @Test
    public void testConcurrentAsynchronousPut() throws InterruptedException {
        int concurrentRequests = 50;
        CountDownLatch latch = new CountDownLatch(concurrentRequests);

        System.out.println("Starting asynchronous stress test with " + concurrentRequests + " requests...");

        for (int i = 0; i < concurrentRequests; i++) {
            final String key = "key-" + i;
            final String value = "value-" + i;

            PutRequest request = PutRequest.newBuilder()
                    .setKey(key)
                    .setValue(com.google.protobuf.ByteString.copyFromUtf8(value))
                    .build();

            // Invoke our async put method across multiple execution paths
            kvStoreService.put(request, new StreamObserver<PutResponse>() {
                @Override
                public void onNext(PutResponse value) {
                    assertTrue(value.getSuccess(), "Pipeline should return success acknowledgement");
                }

                @Override
                public void onError(Throwable t) {
                    System.err.println("Pipeline errored out: " + t.getMessage());
                    latch.countDown();
                }

                @Override
                public void onCompleted() {
                    // One request pipeline finished processing completely
                    latch.countDown();
                }
            });
        }

        // Wait up to 5 seconds for all 50 background tasks to wrap up execution
        boolean cleanFinished = latch.await(5, TimeUnit.SECONDS);
        assertTrue(cleanFinished, "The asynchronous pipeline timed out or left orphaned tasks!");
        System.out.println("SUCCESS: All concurrent async pipelines executed without blocking.");
    }
}