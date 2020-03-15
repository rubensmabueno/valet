package com.rubensminoru.consumers;

import com.rubensminoru.messages.MessageFactory;

public class ConsumerFactory {
    public KafkaConsumer createInstance(String bootstrapServers, String schemaRegistryURL) {
        return new KafkaConsumer(bootstrapServers, schemaRegistryURL, new MessageFactory());
    }
}
