package com.gevin.kvstore;

import java.io.*;
import java.util.Objects;
import java.util.Optional;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileStorageDecorator implements StorageEngine{

    private final StorageEngine inMemoryStorage;

    private BufferedWriter writeAheadLog;

    //constructor
    public FileStorageDecorator(StorageEngine inMemoryStorage) {
        this.inMemoryStorage = inMemoryStorage; //get the memory storage type from client
        try {

            Path walPath = Paths.get("distributed-kv-store/storage", "wal.log");
            Path storageDirectory = Paths.get("distributed-kv-store/storage");
            if (!Files.exists(storageDirectory)) {
                try {
                    Files.createDirectories(storageDirectory);
                }
                catch (IOException e) {
                    System.err.println("Couldn't create directory : " + e.getMessage());
                }
            }
            try {
                this.writeAheadLog = new BufferedWriter(new FileWriter("distributed-kv-store/storage/wal.log", true));
            }
            catch (IOException e) {
                System.err.println("Cannot initialize file writer : " + e.getMessage());
            }
            if (Files.exists(walPath) && !Files.isDirectory(walPath)) {
                //read from disk and write to RAM
                try {
                    BufferedReader reader = new BufferedReader(new FileReader("distributed-kv-store/storage/wal.log"));
                    String line;
                    while ((line = reader.readLine()) != null) {
                        callAssignedMethod(line);
                    }
                    reader.close();
                }
                catch (IOException e) {
                    System.err.println("Failed reading file : " + e.getMessage());
                }
            }
            else if (Files.isDirectory(walPath)) {
                throw new IOException("File is a directory");
            }
        }
        catch (IOException e) {
            e.printStackTrace(System.err);
        }
    }


    @Override
    public synchronized void put(String key, String value) {
        try {
            writeAheadLog.write("put || " + key + " || " + value);
            writeAheadLog.newLine();
            writeAheadLog.flush();
            inMemoryStorage.put(key, value);
        }
        catch(IOException e) {
            e.printStackTrace(System.err);
        }
    }

    @Override
    public Optional<String> get(String key) {
        return inMemoryStorage.get(key);
    }

    @Override
    public synchronized void delete(String key) {
        try {
            writeAheadLog.write("delete || " + key);
            writeAheadLog.newLine();
            writeAheadLog.flush();
            inMemoryStorage.delete(key);
        } 
        catch (IOException e) {
            e.printStackTrace(System.err);
        }
    }

    private void callAssignedMethod(String line) {
        if (line.isEmpty())
                return;
        String[] parts = line.split(" \\|\\| ");
        String command = parts[0];
        String key = parts[1];
        if (command.equals("put")) {
            String value = parts[2];
            inMemoryStorage.put(key, value);
        }
        else if (command.equals("delete")) {
            inMemoryStorage.delete(key);
        }
    }

    public void close() {
        try {
            if (writeAheadLog != null) {
                writeAheadLog.flush();
                writeAheadLog.close();
            }
        }
        catch (IOException e) {
            System.err.println("cannot close Buffered writer : " + e.getMessage());
        }
    }
}
