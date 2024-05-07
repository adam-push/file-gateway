package com.diffusiondata.gateway.providers;

import com.diffusiondata.gateway.files.UpdateEvent;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class NextFileNextLine implements Provider {

    private final Queue<UpdateEvent> eventQueue;
    private final Path dir;
    private final boolean processOnce;
    private final boolean deleteFiles;

    private List<Path> paths = null;
    private Path path; // Current path being processed
    private Map<Path, BufferedReader> readerMap = new HashMap<>();

    public NextFileNextLine(Queue<UpdateEvent> eventQueue, Path dir, boolean processOnce, boolean deleteFiles) {
        this.eventQueue = eventQueue;
        this.dir = dir;
        this.processOnce = processOnce;
        this.deleteFiles = deleteFiles;
    }

    private BufferedReader getReader(Path path) {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(path.toFile()));
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        }
        if(reader != null) {
            readerMap.put(path, reader);
        }
        return reader;
    }

     public ProviderResult run() {
         if(paths == null) {
             try {
                 paths = Files.walk(dir).filter(Files::isRegularFile).sorted().collect(Collectors.toList());
                 if(paths.isEmpty()) { // Nothing to do
                     return processOnce ? ProviderResult.PROCESS_FINISHED : ProviderResult.PROCESS_OK;
                 }
             }
             catch(IOException ex) {
                 ex.printStackTrace();
                 return ProviderResult.PROCESS_ERROR;
             }
         }

         if(path == null) {
             if(! paths.isEmpty()) {
                 path = paths.remove(0); // Guaranteed not empty here
             }
             else {
                 if(processOnce) {
                     return ProviderResult.PROCESS_FINISHED;
                 }
                 else {
                     paths = null;
                     return run();
                 }
             }
         }

         BufferedReader reader = null;
         if(! readerMap.containsKey(path)) {
             reader = getReader(path);
         }
         else {
             reader = readerMap.get(path);
         }

         if(reader != null) {
             try {
                 String topicName = path.toString();
                 String line = reader.readLine();
                 if(line == null) {
                     paths.remove(path);
                     reader.close();
                     if(deleteFiles) {
                         Files.delete(path);
                     }
                     if(paths.isEmpty() && processOnce) {
                         return ProviderResult.PROCESS_FINISHED;
                     }
                     readerMap.remove(path);
                     path = null;
                 }
                 if(line != null) {
                     eventQueue.add(new UpdateEvent(topicName, line));
                 }
             }
             catch(IOException ex) {
                 ex.printStackTrace();
                 return ProviderResult.PROCESS_ERROR;
             }
         }

         return ProviderResult.PROCESS_OK;
    }
}