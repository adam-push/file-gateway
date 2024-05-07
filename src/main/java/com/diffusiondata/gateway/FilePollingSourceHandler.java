package com.diffusiondata.gateway;

import clojure.lang.APersistentMap;
import com.diffusiondata.gateway.files.UpdateEvent;
import com.diffusiondata.gateway.framework.*;
import com.diffusiondata.gateway.framework.exceptions.PayloadConversionException;
import com.diffusiondata.gateway.providers.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

public class FilePollingSourceHandler implements PollingSourceHandler {

    private static final Logger LOG = LoggerFactory.getLogger(FilePollingSourceHandler.class);

    private final Publisher publisher;
    private final StateHandler stateHandler;

    private final Path dir;
    private final boolean stopAfterInitialLoad;
    private final Path topicRoot;
    private final boolean recordPerLine;
    private final Processor processor;

    private final BlockingQueue<UpdateEvent> eventQueue;
    private final Provider provider;

    public FilePollingSourceHandler(Publisher publisher, Map<String, Object> parameters, StateHandler stateHandler) {
        this.publisher = publisher;
        this.stateHandler = stateHandler;

        this.dir = Path.of((String) parameters.getOrDefault("directory", "data"));
        LOG.info("Polling files in directory: {}", dir);

        this.stopAfterInitialLoad = (boolean) parameters.getOrDefault("stopAfterInitialLoad", false);

        this.eventQueue = new LinkedBlockingQueue<>();
        switch((String) parameters.getOrDefault("provider", "AllFilesAllData")) {
            case "AllFilesAllData" :
                this.provider = new AllFilesAllData(eventQueue, dir, stopAfterInitialLoad);
                break;
            case "AllFilesEveryLine" :
                this.provider = new AllFilesEveryLine(eventQueue, dir, stopAfterInitialLoad);
                break;
            case "AllFilesNextLine":
                this.provider = new AllFilesNextLine(eventQueue, dir, stopAfterInitialLoad);
                break;
            case "NextFileAllData":
                this.provider = new NextFileAllData(eventQueue, dir, stopAfterInitialLoad);
                break;
            case "NextFileEveryLine":
                this.provider = new NextFileEveryLine(eventQueue, dir, stopAfterInitialLoad);
                break;
            case "NextFileNextLine":
                this.provider = new NextFileNextLine(eventQueue, dir, stopAfterInitialLoad);
                break;
            default:
                this.provider = new AllFilesAllData(eventQueue, dir, stopAfterInitialLoad);
                break;
        }

        String root = (String)parameters.get("topicRoot");
        if(root != null) {
            topicRoot = Path.of(root);
        }
        else {
            topicRoot = Path.of("/");
        }

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

    private Future<?> processTask = null;
    private EventQueueProcessor eventQueueProcessor = null;

    private class EventQueueProcessor implements Runnable {
        private volatile boolean canExit = false;

        public void canExit() {
            canExit = true;
        }

        @Override
        public void run() {
            List<CompletableFuture<?>> futures = new ArrayList<>();

            UpdateEvent updateEvent;
            while (true) {
                try {
                    updateEvent = eventQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (updateEvent == null) {
                        if (canExit) {
                            break;
                        } else {
                            continue;
                        }
                    }
                    futures.add(proxyPublish(updateEvent.getName(), updateEvent.getPayload()));
                } catch (InterruptedException intEx) {
                    continue;
                }
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        }
    }

    public void startProcessing() {
        if(processTask == null || processTask.isDone() || processTask.isCancelled()) {
            eventQueueProcessor = new EventQueueProcessor();
            processTask = Executors.newSingleThreadExecutor().submit(eventQueueProcessor);
        }
    }

    public void stopProcessing() {
        if(processTask != null) {
            processTask.cancel(false);
            processTask = null;
        }
    }

    public CompletableFuture<?> poll() {

        if(processTask == null || processTask.isDone()) {
            startProcessing();
        }

        ProviderResult result = provider.run();

        // Signal to processor that it should exit if the queue is empty
        eventQueueProcessor.canExit();

        // TODO: Wait for all events to be done
        while(! processTask.isDone()) {
            try {
                Thread.sleep(10);
            }
            catch(InterruptedException ignore) {}
        }

        switch(result) {
            case PROCESS_OK:
                return CompletableFuture.completedFuture(null);
            case PROCESS_FINISHED:
                stateHandler.reportStatus(StateHandler.Status.RED, "Finished processing", "All files are processed and service is configured to stop");
                return CompletableFuture.completedFuture(null);
            case PROCESS_ERROR:
                return CompletableFuture.failedFuture(new RuntimeException("Provider failed"));
            default:
                return CompletableFuture.failedFuture(new RuntimeException("Inknown ProviderResult: " + result));
        }
    }

    @Override
    public CompletableFuture<?> pause(PauseReason reason) {
        LOG.info("Pausing: " + reason);
        stopProcessing();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<?> resume(ResumeReason reason) {
        LOG.info("Resuming: " + reason);
        startProcessing();
        return CompletableFuture.completedFuture(null);
    }
}