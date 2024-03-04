package com.diffusiondata.gateway;

import com.diffusiondata.gateway.framework.SinkHandler;
import com.diffusiondata.gateway.framework.Subscriber;
import com.diffusiondata.gateway.framework.TopicProperties;
import com.diffusiondata.gateway.framework.exceptions.InvalidConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class FileStreamingSinkHandler implements SinkHandler<String> {

    private static final Logger LOG = LoggerFactory.getLogger(FileStreamingSinkHandler.class);

    private final Subscriber subscriber;
    private final Path dir;
    private final Path file;
    private final boolean overwrite;
    private final boolean cacheFileHandles;
    private final boolean flush;
    private final boolean newline;

    private final Map<String, BufferedWriter> fileWriterCache = new HashMap<>();

    public FileStreamingSinkHandler(Subscriber subscriber, Map<String, Object> parameters) throws InvalidConfigurationException {
        this.subscriber = subscriber;

        // Must provide a target directory
        String targetDir = (String)parameters.get("directory");
        if(targetDir == null) {
            throw new InvalidConfigurationException("Missing configuration parameter \"directory\"");
        }
        this.dir = Path.of(targetDir);

        // If a filename is given, we us it for all writes, else we will calculate a filename later
        // based on the topic name.
        String filename = (String)parameters.get("filename");
        if(filename != null) {
            this.file = Path.of(targetDir, filename);
        }
        else {
            this.file = null;
        }

        // We append the file, unless explicitly told to overwrite it.
        this.overwrite = (Boolean)parameters.getOrDefault("overwrite", false);

        // To avoid opening and closing files with each write, we cache the file handler (writer)
        // unless told otherwise.
        this.cacheFileHandles = (Boolean)parameters.getOrDefault("cache", true);

        // Flush data to file on every write, unless overridden
        this.flush = (Boolean)parameters.getOrDefault("flush", true);

        // Add a newline to each file write?
        // The default for this depends on whether we append to a file on each write (true)
        // or overwrite the file on each write (false)
        this.newline = (Boolean)parameters.getOrDefault("newline", !overwrite);
    }

    private BufferedWriter createWriter(Path path) throws FileNotFoundException {
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path.toFile(), !overwrite)));
        return writer;
    }

    @Override
    public CompletableFuture<?> update(String topicPath, String value, TopicProperties topicProperties) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("upate(" + topicPath + ", " + value + ")");
        }

        final CompletableFuture future = new CompletableFuture();

        Path path;
        if(file != null) {
            path = file;
        }
        else {
            path = Paths.get(dir.toString(), topicPath);
        }

        LOG.info("" + (overwrite ? "Overwrite " : "Append ") + path.toString());

        BufferedWriter writer = null;
        try {
            if (cacheFileHandles) {
                writer = fileWriterCache.get(path.toString());
                if (writer == null) {
                    writer = createWriter(path);
                }
                fileWriterCache.put(path.toString(), writer);
            } else {
                writer = createWriter(path);
            }

            writer.write(value);
            if(newline) {
                writer.newLine();
            }
            if(flush) {
                writer.flush();
            }

            if (!cacheFileHandles) {
                writer.close();
            }

            future.complete(null);
        }
        catch(Exception ex) {
            fileWriterCache.remove(path.toString());
            try {
                if(writer != null) {
                    writer.close();
                }
            }
            catch(Exception ignore) {}
            future.completeExceptionally(ex);
        }

        return future;
    }

    @Override
    public Class<String> valueType() {
        return String.class;
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
