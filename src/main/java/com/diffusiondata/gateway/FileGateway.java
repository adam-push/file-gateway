package com.diffusiondata.gateway;

import com.diffusiondata.gateway.framework.*;
import com.diffusiondata.gateway.framework.exceptions.ApplicationConfigurationException;
import com.diffusiondata.gateway.framework.exceptions.InvalidConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.diffusiondata.gateway.framework.DiffusionGatewayFramework.newApplicationDetailsBuilder;

public class FileGateway implements GatewayApplication {

    private static final Logger LOG = LoggerFactory.getLogger(FileGateway.class);

    public FileGateway() {
    }

    public static void main(String[] args) {
        DiffusionGatewayFramework.start(new FileGateway());
    }

    @Override
    public ApplicationDetails getApplicationDetails() throws ApplicationConfigurationException {
        return newApplicationDetailsBuilder()
                .addServiceType("POLLING_JSON_SOURCE",
                        ServiceMode.POLLING_SOURCE,
                        "A polling source that reads files from a directory and publishes their contents as topics",
                        null)
                .addServiceType("STREAMING_STRING_SINK",
                        ServiceMode.SINK,
                        "A sink that writes topic data to files",
                        null)
                .build("FILE_SOURCE", 1);
    }

    @Override
    public PollingSourceHandler addPollingSource(ServiceDefinition serviceDefinition, Publisher publisher, StateHandler stateHandler) {
        final Map<String, Object> parameters = serviceDefinition.getParameters();

        return new FilePollingSourceHandler(publisher, parameters);
    }

    @Override
    public SinkHandler<?> addSink(ServiceDefinition serviceDefinition, Subscriber subscriber, StateHandler stateHandler) throws InvalidConfigurationException {
        LOG.info("Added sink service");
        return new FileStreamingSinkHandler();
    }

    @Override
    public CompletableFuture<?> stop() {
        LOG.info("stop");
        return CompletableFuture.completedFuture(null);
    }
}