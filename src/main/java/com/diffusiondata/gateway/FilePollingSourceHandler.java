package com.diffusiondata.gateway;

import com.diffusiondata.gateway.files.DirectoryLoader;
import com.diffusiondata.gateway.files.UpdateEvent;
import com.diffusiondata.gateway.framework.DiffusionGatewayFramework;
import com.diffusiondata.gateway.framework.PollingSourceHandler;
import com.diffusiondata.gateway.framework.Publisher;
import com.diffusiondata.gateway.framework.exceptions.PayloadConversionException;
import com.pushtechnology.diffusion.topics.tree.TopicPathUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

public class FilePollingSourceHandler implements PollingSourceHandler {

    private static final Logger LOG = LoggerFactory.getLogger(FilePollingSourceHandler.class);

    private final Publisher publisher;
    private final Path dir;
    private final boolean stopAfterInitialLoad;
    private final Path topicRoot;

    public FilePollingSourceHandler(Publisher publisher, Map<String, Object> parameters) {
        this.publisher = publisher;

        this.dir = Path.of((String) parameters.getOrDefault("directory", "data"));
        LOG.info("Polling files in directory: {}", dir);

        String root = (String)parameters.get("topicRoot");
        if(root != null) {
            topicRoot = Path.of(root);
        }
        else {
            topicRoot = Path.of("/");
        }

        this.stopAfterInitialLoad = (boolean) parameters.getOrDefault("stopAfterInitialLoad", false);
        LOG.info("stopAfterInitialLoad:" + stopAfterInitialLoad);
    }

    @Override
    public CompletableFuture<?> poll() {
        CompletableFuture<?> future = new CompletableFuture<>();

        final DirectoryLoader loader = new DirectoryLoader(dir);
        final Stream<UpdateEvent> updateEventStream = loader.eventStream();

        LOG.debug("poll...");
        ArrayList<CompletableFuture<?>> publishFutures = new ArrayList<>();
                updateEventStream.forEach(evt -> {
            try {
                // Use Path to convert to a topic path, they are similar enough
                String topicPath = Path.of(topicRoot.toString(), evt.getName()).toString();
                publishFutures.add(publisher.publish(topicPath, evt.getPayload()));
            } catch (PayloadConversionException ex) {
                ex.printStackTrace();
                LOG.error("Error converting payload for topic {}:", evt.getName(), ex);
                future.completeExceptionally(ex);
            }
        });

        publishFutures.forEach(CompletableFuture::join);
        future.complete(null);

        if(stopAfterInitialLoad) {
            DiffusionGatewayFramework.shutdown();
        }

        return future;
    }

    @Override
    public CompletableFuture<?> pause(PauseReason reason) {
        LOG.info("Pausing: " + reason);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<?> resume(ResumeReason reason) {
        LOG.info("Resuming: " + reason);
        return CompletableFuture.completedFuture(null);
    }
}
