package com.diffusiondata.gateway.providers;

import java.util.concurrent.CompletableFuture;

public interface Provider {
    public ProviderResult run();
}
