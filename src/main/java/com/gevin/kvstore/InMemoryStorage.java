package com.gevin.kvstore;

import java.util.concurrent.ConcurrentHashMap;

public class InMemoryStorage implements StorageEngine {
    private final ConcurrentHashMap<String, String> inMemoryStore;

    InMemoryStorage() {
        inMemoryStore = new ConcurrentHashMap<>();
    }

    @Override
    public void put(String key, String value) {
        inMemoryStore.put(key, value);
    }

    @Override
    public String get(String key) {
        return inMemoryStore.get(key);
    }

    @Override
    public void delete(String key) {
        inMemoryStore.remove(key);
    }

}
