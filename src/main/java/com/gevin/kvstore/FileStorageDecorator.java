package com.gevin.kvstore;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Optional;

public class FileStorageDecorator implements StorageEngine{

    private InMemoryStorage inMemoryStorage;

    private FileWriter writeAheadLog;
    
    public FileStorageDecorator() {
        try {
            writeAheadLog = new FileWriter("wal.log");
            inMemoryStorage = new InMemoryStorage();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void put(String key, String value) {
        try {
            writeAheadLog.write("put || " + key + " || " + value);
            inMemoryStorage.put(key, value);
        }
        catch(IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Optional<String> get(String key) {
        try {
            writeAheadLog.write("get || " + key);
            return inMemoryStorage.get(key);
        }
        catch(IOException e) {
            e.printStackTrace();
        }
        return Optional.empty();
    }

    @Override
    public void delete(String key) {
        try {
            writeAheadLog.write("delete || " + key);
            inMemoryStorage.delete(key);
        } 
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
