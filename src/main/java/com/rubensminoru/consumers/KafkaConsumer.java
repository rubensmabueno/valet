package com.rubensminoru.consumers;

import com.rubensminoru.messages.KafkaMessage;

import com.rubensminoru.messages.MessageFactory;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class KafkaConsumer {
    private org.apache.kafka.clients.consumer.KafkaConsumer<Long, GenericRecord> consumer;
    private MessageFactory kafkaMessageFactory;

    public KafkaConsumer(String bootstrapServers, String schemaRegistryURL, MessageFactory messageFactory) {
        final Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-example-consumer-2");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.setProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);

        consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
        kafkaMessageFactory = messageFactory;
    }

    public void subscribe(String topic) { consumer.subscribe(Collections.singletonList(topic)); }

    public List<KafkaMessage> poll(int size) {
        return kafkaMessageFactory.createInstances(consumer.poll(size));
    }
}
