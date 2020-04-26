package com.rubensminoru.main;

import com.rubensminoru.consumers.KafkaConsumer;
import com.rubensminoru.consumers.ConsumerFactory;

import com.rubensminoru.partitioners.PartitionerFactory;
import com.rubensminoru.partitioners.Partitioner;
import com.rubensminoru.writers.WriterFactory;

public class ConsumerMain {
    private final static String TOPIC = "topic";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
    private final static String SCHEMA_REGISTRY_URL = "http://localhost:8081";

    public static void main( String[] args ) {
        ConsumerMain.process(new ConsumerFactory(), new ProcessorFactory(), new PartitionerFactory(), new WriterFactory());
    }

    public static void process(ConsumerFactory consumerFactory, ProcessorFactory processorFactory, PartitionerFactory partitionerFactory, WriterFactory writerFactory) {
        KafkaConsumer consumer = consumerFactory.createInstance(BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL);
        Partitioner partitioner = partitionerFactory.createInstance();
        Processor processor = processorFactory.createInstance(TOPIC, writerFactory, partitioner);

        boolean process = true;

        consumer.subscribe(TOPIC);

        while (process) {
            process = processor.process(consumer.poll(1000));
        }
    }
}
