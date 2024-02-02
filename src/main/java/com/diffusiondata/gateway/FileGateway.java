package com.diffusiondata.gateway;

import com.diffusiondata.gateway.framework.*;
import com.diffusiondata.gateway.framework.exceptions.ApplicationConfigurationException;
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
        GatewayFramework framework = DiffusionGatewayFramework.initialize((new FileGateway()));
        framework.start();
    }

    @Override
    public ApplicationDetails getApplicationDetails() throws ApplicationConfigurationException {
        return newApplicationDetailsBuilder()
                .addServiceType("POLLING_BINARY_SOURCE",
                        ServiceMode.POLLING_SOURCE,
                        "A polling source that reads files from a directory and publishes their contents as topics",
                        null)
                .build("FILE_SOURCE", 1);
    }

    @Override
    public PollingSourceHandler addPollingSource(ServiceDefinition serviceDefinition, Publisher publisher, StateHandler stateHandler) {
        final Map<String, Object> parameters = serviceDefinition.getParameters();

        Path dir = Path.of((String) parameters.getOrDefault("directory", "data"));
        LOG.info("Polling files in directory: {}", dir);

        return new FilePollingSourceHandler(publisher, dir);
    }

    @Override
    public CompletableFuture<?> stop() {
        LOG.info("stop");
        return CompletableFuture.completedFuture(null);
    }
}