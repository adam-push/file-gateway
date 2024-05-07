package com.diffusiondata.gateway.providers;

import com.diffusiondata.gateway.files.UpdateEvent;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class AllFilesNextLine implements Provider {

    private final Queue<UpdateEvent> eventQueue;
    private final Path dir;
    private final boolean processOnce;

    private List<Path> paths = null;
    private Map<Path, BufferedReader> readerMap = new HashMap<>();

    public AllFilesNextLine(Queue<UpdateEvent> eventQueue, Path dir, boolean processOnce) {
        this.eventQueue = eventQueue;
        this.dir = dir;
        this.processOnce = processOnce;
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
            }
            catch(IOException ex) {
                ex.printStackTrace();
                return ProviderResult.PROCESS_ERROR;
            }
        }

        for(Path path : paths) {
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
                    if (line == null) {
                        reader.close();
                        readerMap.put(path, null);
                        if(! processOnce) {
                            reader = getReader(path);
                            line = reader.readLine();
                        }
                    }
                    if (line != null) {
                        eventQueue.add(new UpdateEvent(topicName, line));
                    }
                } catch (IOException ex) {
                    ex.printStackTrace();
                    return ProviderResult.PROCESS_ERROR;
                }
            }
        }

        if(processOnce) {
            if (readerMap.values().stream().allMatch(v -> v == null)) {
                return ProviderResult.PROCESS_FINISHED;
            }
            else {
                return ProviderResult.PROCESS_OK;
            }
        }
        else {
            return ProviderResult.PROCESS_OK;
        }
    }
}