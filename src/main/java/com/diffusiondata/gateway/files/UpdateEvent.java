package com.diffusiondata.gateway.files;

public class UpdateEvent {
    private final String name;
    private final String payload;

    public UpdateEvent(String topicPath, String payload) {
        this.name = topicPath;
        this.payload = payload;
    }

    public String getName() {
        return name;
    }

    public String getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "UpdateEvent{" +
                "name='" + name + '\'' +
                ", payload='" + payload + '\'' +
                '}';
    }
}
