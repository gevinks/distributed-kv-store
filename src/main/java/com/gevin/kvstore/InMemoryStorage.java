package com.gevin.kvstore;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryStorage implements StorageEngine {
    private final ConcurrentHashMap<String, String> inMemoryStore;

    public InMemoryStorage() {
        inMemoryStore = new ConcurrentHashMap<>();
    }

    @Override
    public void put(String key, String value) {
        inMemoryStore.put(key, value);
    }

    @Override
    public Optional<String> get(String key) {
        return Optional.ofNullable(inMemoryStore.get(key));
    }

    @Override
    public void delete(String key) {
        inMemoryStore.remove(key);
    }

}
