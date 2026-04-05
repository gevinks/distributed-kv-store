package com.gevin.kvstore;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Optional;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileStorageDecorator implements StorageEngine{

    private StorageEngine inMemoryStorage;

    private FileWriter writeAheadLog;
    
    public FileStorageDecorator(StorageEngine inMemoryStorage) {
        this.inMemoryStorage = inMemoryStorage;
        try {
            Path walPath = Paths.get("storage", "wal.log");
            Path storageDirectory = Paths.get("storage");
            if (!Files.exists(storageDirectory)) {
                try {
                    Files.createDirectories(storageDirectory);
                }
                catch (IOException e) {
                    System.err.println("Couldn't create directory : " + e.getMessage());
                }
            }
            if (Files.exists(walPath) && !Files.isDirectory(walPath)) {
                //read from disk and write to RAM
                try {
                    BufferedReader reader = new BufferedReader(new FileReader("storage/wal.log"));
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

    private void callAssignedMethod(String line) {
        if (line.isEmpty())
                return;
        String key = "", value = "";
        if (line.charAt(0) == 'p') {
            int ind = 7;
            while ((ind < line.length()) && !((ind < (line.length() - 1)) && (line.charAt(ind) != '|') && (line.charAt(ind + 1) != '|'))){
                key = key + line.charAt(ind);
                ind++;
            }
            ind++;
            while ((ind < line.length())){
                value = value + line.charAt(ind);
                ind++;
            }
            put(key, value);
        }
        else if (line.charAt(0) == 'g') {
            int ind = 7;
            while ((ind < line.length()) && !((ind < (line.length() - 1)) && (line.charAt(ind) != '|') && (line.charAt(ind + 1) != '|'))){
                key = key + line.charAt(ind);
                ind++;
            }
            ind++;
            while ((ind < line.length())){
                value = value + line.charAt(ind);
                ind++;
            }
            get(key);
        }
        else if (line.charAt(0) == 'd') {
            int ind = 10;
            while ((ind < line.length()) && !((ind < (line.length() - 1)) && (line.charAt(ind) != '|') && (line.charAt(ind + 1) != '|'))){
                key = key + line.charAt(ind);
                ind++;
            }
            ind++;
            while ((ind < line.length())){
                value = value + line.charAt(ind);
                ind++;
            }
            delete(key);
        }
    }
}
