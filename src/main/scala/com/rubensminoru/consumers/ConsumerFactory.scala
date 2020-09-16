package com.rubensminoru.consumers;

object ConsumerFactory {
    def apply(bootstrapServers:String, schemaRegistryURL:String) = new KafkaConsumer(bootstrapServers, schemaRegistryURL)
}
