package com.gevin.kvstore;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryStorage implements StorageEngine {
    private final ConcurrentHashMap<String, byte[]> inMemoryStore;

    public InMemoryStorage() {
        inMemoryStore = new ConcurrentHashMap<>();
    }

    @Override
    public void put(String key, byte[] value) {
        inMemoryStore.put(key, value);
    }

    @Override
    public Optional<byte[]> get(String key) {
        return Optional.ofNullable(inMemoryStore.get(key));
    }

    @Override
    public void delete(String key) {
        inMemoryStore.remove(key);
    }

}
