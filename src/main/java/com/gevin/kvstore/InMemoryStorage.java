package com.gevin.kvstore;

import java.util.concurrent.ConcurrentHashMap;

public class InMemoryStorage implements StorageEngine {
    ConcurrentHashMap<String, String> inMemoryStore;

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
