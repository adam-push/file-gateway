package com.diffusiondata.gateway;

import com.diffusiondata.gateway.framework.*;
import com.diffusiondata.gateway.framework.exceptions.ApplicationConfigurationException;
import com.diffusiondata.gateway.framework.exceptions.InvalidConfigurationException;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.diffusiondata.gateway.framework.DiffusionGatewayFramework.newApplicationDetailsBuilder;

public class FileGateway implements GatewayApplication {

    private static final Logger LOG = LoggerFactory.getLogger(FileGateway.class);
    private static final PrometheusMeterRegistry meterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

    private PrometheusServer prometheusServer;
    private ApplicationContext applicationContext;

    private String prometheusPath = "/metrics";
    private int prometheusPort = 8005;

    public FileGateway() {
    }

    public static void main(String[] args) {
        DiffusionGatewayFramework.start(new FileGateway());
    }

    @Override
    public void initialize(ApplicationContext applicationContext) throws ApplicationConfigurationException {
        this.applicationContext = applicationContext;
        if(applicationContext.getGlobalConfiguration().containsKey("prometheus")) {
            Map<String, Object> prometheusConfig = (Map<String, Object>)applicationContext.getGlobalConfiguration().get("prometheus");
            this.prometheusPath = (String)prometheusConfig.getOrDefault("path", this.prometheusPath);
            this.prometheusPort = (int)prometheusConfig.getOrDefault("port", this.prometheusPort);
        }
    }

    @Override
    public ApplicationDetails getApplicationDetails() throws ApplicationConfigurationException {
        return newApplicationDetailsBuilder()
                .addServiceType("POLLING_JSON_SOURCE",
                        ServiceMode.POLLING_SOURCE,
                        "A polling source that reads files from a directory and publishes their contents as topics",
                        null)
                .addServiceType("FILE_STRING_SINK",
                        ServiceMode.SINK,
                        "A sink that writes topic data to files",
                        null)
                .build("FILE_SOURCE", 1);
    }

    @Override
    public GatewayMeterRegistry getGatewayMeterRegistry() {
        return new GatewayMeterRegistry() {
            @Override
            public MeterRegistry getMeterRegistry() {
                return meterRegistry;
            }
        };
    }

    @Override
    public PollingSourceHandler addPollingSource(ServiceDefinition serviceDefinition, Publisher publisher, StateHandler stateHandler) {
        final Map<String, Object> parameters = serviceDefinition.getParameters();
        return new FilePollingSourceHandler(publisher, parameters);
    }

    @Override
    public SinkHandler<?> addSink(ServiceDefinition serviceDefinition, Subscriber subscriber, StateHandler stateHandler) throws InvalidConfigurationException {
        LOG.info("Added sink service");
        final Map<String, Object> parameters = serviceDefinition.getParameters();

        return new FileStreamingSinkHandler(subscriber, parameters);
    }

    @Override
    public CompletableFuture<?> start() {
        if(applicationContext.isMetricsEnabled()) {
            try {
                prometheusServer = new PrometheusServer(prometheusPath, prometheusPort, meterRegistry);
                applicationContext.getExecutorService().submit(prometheusServer);
            }
            catch(IOException ex) {
                LOG.error("Failed to start Prometheus server", ex);
                return CompletableFuture.completedFuture(ex);
            }
        }

        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<?> stop() {
        LOG.info("stop");
        if(prometheusServer != null) {
            prometheusServer.close();
        }

        return CompletableFuture.completedFuture(null);
    }
}