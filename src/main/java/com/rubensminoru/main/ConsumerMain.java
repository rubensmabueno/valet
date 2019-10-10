package com.rubensminoru.main;

import com.rubensminoru.partitioner.TimeBasedPartitioner;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;

import java.util.Collections;
import java.util.Properties;

public class ConsumerMain {
    private final static String TOPIC = "topic";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
    private final static String SCHEMA_REGISTRY_URL = "http://localhost:8081";

    public static void main( String[] args ) {
        final Consumer<Long, GenericRecord> consumer = createConsumer();

        TimeBasedPartitioner timeBasedPartitioner = new TimeBasedPartitioner(TOPIC);

        while (true) {
            System.out.println("BLA");

            final ConsumerRecords<Long, GenericRecord> records = consumer.poll(1000);

            timeBasedPartitioner.process(records);
        }
    }

    private static Consumer<Long, GenericRecord> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-example-consumer-2");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.setProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);

        // Create the consumer using props.
        final Consumer<Long, GenericRecord> consumer = new KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(TOPIC));

        return consumer;
    }
}
