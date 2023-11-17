package com.diffusiondata.gateway.files;

public class UpdateEvent {
    private final String topicPath;
    private final String payload;

    public UpdateEvent(String topicPath, String payload) {
        this.topicPath = topicPath;
        this.payload = payload;
    }

    public String getTopicPath() {
        return topicPath;
    }

    public String getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "UpdateEvent{" +
                "topicPath='" + topicPath + '\'' +
                ", payload='" + payload + '\'' +
                '}';
    }
}
