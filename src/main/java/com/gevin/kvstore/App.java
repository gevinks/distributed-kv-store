package com.gevin.kvstore;

public class App {
    public static void main(String[] args) {
        StorageEngine engine = new InMemoryStorage();
        FileStorageDecorator mainCache = new FileStorageDecorator(engine);

    }
}
