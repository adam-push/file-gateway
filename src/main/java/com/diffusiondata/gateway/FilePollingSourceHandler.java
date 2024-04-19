package com.diffusiondata.gateway;

import clojure.lang.APersistentMap;
import com.diffusiondata.gateway.files.DirectoryLoader;
import com.diffusiondata.gateway.files.UpdateEvent;
import com.diffusiondata.gateway.framework.DiffusionGatewayFramework;
import com.diffusiondata.gateway.framework.PollingSourceHandler;
import com.diffusiondata.gateway.framework.Publisher;
import com.diffusiondata.gateway.framework.exceptions.PayloadConversionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

public class FilePollingSourceHandler implements PollingSourceHandler {

    private static final Logger LOG = LoggerFactory.getLogger(FilePollingSourceHandler.class);

    private final Publisher publisher;
    private final Path dir;
    private final boolean stopAfterInitialLoad;
    private final Path topicRoot;
    private final boolean recordPerLine;
    private final Processor processor;

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

        this.recordPerLine = (boolean) parameters.getOrDefault("recordPerLine", false);

        Map<String, String> processorConfig = (Map<String, String>) parameters.get("processor");
        if(processorConfig != null) {
            this.processor = new Processor(processorConfig);
        }
        else {
            this.processor = null;
        }
    }

    private CompletableFuture<? extends Object> proxyPublish(String defaultTopic, Object value) {
        if(value instanceof String) {
            try {
                return publisher.publish(defaultTopic, (String) value);
            }
            catch(PayloadConversionException ex) {
                ex.printStackTrace();
                return CompletableFuture.failedFuture(ex);
            }
        }

        if(value instanceof clojure.lang.APersistentMap) {
            APersistentMap map = (APersistentMap)value;

            List<CompletableFuture> futures = new ArrayList<>();
            map.forEach((topicPath, topicValue) -> {
               CompletableFuture<?> pubFuture = null;
               try {
                   futures.add(publisher.publish((String)topicPath, topicValue));
               }
               catch(PayloadConversionException ex) {
                   ex.printStackTrace();
                   futures.add(CompletableFuture.failedFuture(ex));
               }
            });
            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        }

        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<?> poll() {
        CompletableFuture<?> future = new CompletableFuture<>();

        final DirectoryLoader loader = new DirectoryLoader(dir);
        final Stream<UpdateEvent> updateEventStream = loader.eventStream();

        LOG.debug("poll...");
        ArrayList<CompletableFuture<?>> publishFutures = new ArrayList<>();
                updateEventStream.forEach(evt -> {

            // Use Path to convert to a topic path, they are similar enough
            String topicPath = Path.of(topicRoot.toString(), evt.getName()).toString();

            if(recordPerLine) {
                evt.getPayload().lines().forEach(line -> {
                    Object value;
                    if(processor != null) {
                        value = processor.invoke(evt.getName(), line);
                    }
                    else {
                        value = line;
                    }
                    publishFutures.add(proxyPublish(topicPath, value));
                });
            }
            else {
                Object value;
                if(processor != null) {
                    value = processor.invoke(evt.getName(), evt.getPayload());
                }
                else {
                    value = evt.getPayload();
                }
                publishFutures.add(proxyPublish(topicPath, value));
            }

        });

        publishFutures.forEach(CompletableFuture::join);
        future.complete(null);

        if(stopAfterInitialLoad) {
            DiffusionGatewayFramework.shutdown();
            try {
                Thread.sleep(2000);
            }
            catch(InterruptedException ignore) {
            }
            finally {
                Runtime.getRuntime().exit(0);
            }
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
