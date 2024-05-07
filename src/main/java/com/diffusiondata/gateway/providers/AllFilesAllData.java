package com.diffusiondata.gateway.providers;

import com.diffusiondata.gateway.files.UpdateEvent;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

public class AllFilesAllData implements Provider {

    private final Queue<UpdateEvent> eventQueue;
    private final Path dir;
    private final boolean processOnce;
    private final boolean deleteFiles;

    public AllFilesAllData(Queue<UpdateEvent> eventQueue, Path dir, boolean processOnce, boolean deleteFiles) {
        this.eventQueue = eventQueue;
        this.dir = dir;
        this.processOnce = processOnce;
        this.deleteFiles = deleteFiles;
    }

    public ProviderResult run() {
        try {
            List<Path> paths = Files.walk(dir).filter(Files::isRegularFile).sorted().collect(Collectors.toList());
            for(Path path : paths) {
                String topicName = path.toString();
                String data;
                try {
                    data = new String(Files.readAllBytes(path));
                    eventQueue.add(new UpdateEvent(topicName, data));
                    Files.delete(path);
                } catch (IOException ex) {
                    ex.printStackTrace();
                    return ProviderResult.PROCESS_ERROR;
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
            return ProviderResult.PROCESS_ERROR;
        }

        return processOnce ? ProviderResult.PROCESS_FINISHED : ProviderResult.PROCESS_OK;
    }
}