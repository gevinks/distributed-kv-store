package com.gevin.kvstore;

import java.util.Optional;

public interface StorageEngine {
    void put(String key, byte[] value);
    Optional<byte[]> get(String key);
    void delete(String key);
    void close();
}
