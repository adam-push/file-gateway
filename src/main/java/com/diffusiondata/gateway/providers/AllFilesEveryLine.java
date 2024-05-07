package com.diffusiondata.gateway.providers;

import com.diffusiondata.gateway.files.UpdateEvent;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class AllFilesEveryLine implements Provider {

    private final Queue<UpdateEvent> eventQueue;
    private final Path dir;
    private final boolean processOnce;

    public AllFilesEveryLine(Queue<UpdateEvent> eventQueue, Path dir, boolean processOnce) {
        this.eventQueue = eventQueue;
        this.dir = dir;
        this.processOnce = processOnce;
    }

    public ProviderResult run() {
        try {
            List<Path> paths = Files.walk(dir).filter(Files::isRegularFile).sorted().collect(Collectors.toList());
            for(Path path : paths) {
                String topicName = path.toString();
                try {
                    Files.readAllLines(path).forEach(line -> {
                        eventQueue.add(new UpdateEvent(topicName, line));
                    });
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
