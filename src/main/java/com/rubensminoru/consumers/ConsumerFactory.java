package com.rubensminoru.consumers;

public class ConsumerFactory {
    public KafkaConsumer createInstance(String bootstrapServers, String schemaRegistryURL) {
        return new KafkaConsumer(bootstrapServers, schemaRegistryURL);
    }
}
