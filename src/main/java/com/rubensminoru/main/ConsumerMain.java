package com.rubensminoru.main;

import com.rubensminoru.consumers.KafkaConsumer;
import com.rubensminoru.consumers.ConsumerFactory;

import com.rubensminoru.partitioners.PartitionerFactory;
import com.rubensminoru.partitioners.TimeBasedPartitioner;

public class ConsumerMain {
    private final static String TOPIC = "topic";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
    private final static String SCHEMA_REGISTRY_URL = "http://localhost:8081";

    public static void main( String[] args ) {
        ConsumerMain.process(new ConsumerFactory(), new PartitionerFactory());
    }

    public static void process(ConsumerFactory consumerFactory, PartitionerFactory partitionerFactory) {
        KafkaConsumer consumer = consumerFactory.createInstance(BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL);
        TimeBasedPartitioner timeBasedPartitioner = partitionerFactory.createInstance(TOPIC);

        boolean process = true;

        consumer.subscribe(TOPIC);

        while (process) {
            process = timeBasedPartitioner.process(consumer.poll(1000));
        }
    }
}
