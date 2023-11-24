package com.diffusiondata.gateway;

import com.diffusiondata.gateway.files.DirectoryLoader;
import com.diffusiondata.gateway.files.UpdateEvent;
import com.diffusiondata.gateway.framework.PollingSourceHandler;
import com.diffusiondata.gateway.framework.Publisher;
import com.diffusiondata.gateway.framework.exceptions.PayloadConversionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

public class FilePollingSourceHandler implements PollingSourceHandler {

    private static final Logger LOG = LoggerFactory.getLogger(FilePollingSourceHandler.class);

    private final Publisher publisher;
    private final Path dir;

    public FilePollingSourceHandler(Publisher publisher, Path dir) {
        this.publisher = publisher;
        this.dir = dir;
    }

    @Override
    public CompletableFuture<?> poll() {
        CompletableFuture<?> future = new CompletableFuture<>();

        final DirectoryLoader loader = new DirectoryLoader(dir);
        final Stream<UpdateEvent> updateEventStream = loader.eventStream();

        LOG.info("poll...");
        ArrayList<CompletableFuture<?>> publishFutures = new ArrayList<>();
                updateEventStream.forEach(evt -> {
            try {
                publishFutures.add(publisher.publish(evt.getName(), evt.getPayload().getBytes()));

            } catch (PayloadConversionException ex) {
                ex.printStackTrace();
                LOG.error("Error converting payload for topic {}:", evt.getName(), ex);
                future.completeExceptionally(ex);
            }
        });

        publishFutures.forEach(CompletableFuture::join);
        future.complete(null);

        return future;
    }

    @Override
    public CompletableFuture<?> pause(PauseReason reason) {
        LOG.info("Pausing");
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<?> resume(ResumeReason reason) {
        LOG.info("Resuming");
        return CompletableFuture.completedFuture(null);
    }
}
