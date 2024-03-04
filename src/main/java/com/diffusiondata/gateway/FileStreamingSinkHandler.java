package com.diffusiondata.gateway;

import com.diffusiondata.gateway.framework.SinkHandler;
import com.diffusiondata.gateway.framework.TopicProperties;

import java.util.concurrent.CompletableFuture;

public class FileStreamingSinkHandler implements SinkHandler<String> {
    @Override
    public CompletableFuture<?> update(String path, String value, TopicProperties topicProperties) {
        System.out.println("++AST: update(" + path + ", " + value + ")");
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Class<String> valueType() {
        return String.class;
    }

    @Override
    public CompletableFuture<?> pause(PauseReason reason) {
        System.out.println("++AST: Service paused: " + reason);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<?> resume(ResumeReason reason) {
        System.out.println("++AST: Service resumed: " + reason);
        return CompletableFuture.completedFuture(null);
    }
}
