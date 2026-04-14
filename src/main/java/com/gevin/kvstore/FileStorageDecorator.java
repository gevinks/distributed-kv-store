package com.gevin.kvstore;

import java.io.*;
import java.util.Objects;
import java.util.Optional;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileStorageDecorator implements StorageEngine{

    private final StorageEngine inMemoryStorage;
    private final String walPathStr = "distributed-kv-store/storage/wal.log";
    private DataOutputStream walOutputStream;

    //constructor
    public FileStorageDecorator(StorageEngine inMemoryStorage) {
        this.inMemoryStorage = inMemoryStorage; //get the memory storage type from client
        try {
            Path storageDirectory = Paths.get("distributed-kv-store/storage");
            if (!Files.exists(storageDirectory)) {
                Files.createDirectories(storageDirectory);
            }
            recover();
            this.walOutputStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(walPathStr, true)));
        }
        catch (IOException e) {
            e.printStackTrace(System.err);
        }
    }

    private void recover() {
        File file = new File(walPathStr);
        if (!file.exists() || file.length() == 0)
            return;

        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))) {
            while (dis.available() > 0) {
                byte type = dis.readByte(); // 0 for PUT, 1 for DELETE
                String key = dis.readUTF();

                if (type == 0) { // PUT
                    int valueLength = dis.readInt();
                    byte[] value = new byte[valueLength];
                    dis.readFully(value);
                    inMemoryStorage.put(key, value);
                } else if (type == 1) { // DELETE
                    inMemoryStorage.delete(key);
                }
            }
        }
        catch (IOException e) {
            System.err.println("Recovery failed: " + e.getMessage());
        }
    }


    @Override
    public synchronized void put(String key, byte[] value) {
        try {
            walOutputStream.writeByte(0);
            walOutputStream.writeUTF(key);
            walOutputStream.writeInt(value.length);
            walOutputStream.write(value);
            walOutputStream.flush();
            inMemoryStorage.put(key, value);
        }
        catch(IOException e) {
            e.printStackTrace(System.err);
        }
    }

    @Override
    public Optional<byte[]> get(String key) {
        return inMemoryStorage.get(key);
    }

    @Override
    public synchronized void delete(String key) {
        try {
            walOutputStream.writeByte(1);
            walOutputStream.writeUTF(key);
            walOutputStream.flush();
            inMemoryStorage.delete(key);
        } 
        catch (IOException e) {
            e.printStackTrace(System.err);
        }
    }

    @Override
    public void close() {
        try {
            if (walOutputStream != null) {
                walOutputStream.close();
            }
        } catch (IOException e) {
            System.err.println("Cannot close WAL output stream: " + e.getMessage());
        }
    }
}
