package com.rubensminoru.main;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;

import java.util.Properties;

public class BootstrapMain {
    private final static String TOPIC = "topic";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
    private final static String SCHEMA_REGISTRY_URL = "http://localhost:8081";

    public static void main( String[] args ) {
        final Producer<Long, GenericRecord> producer = createProducer();

        try {
            String userSchema = "{\"type\":\"record\"," +
                    "\"name\":\"myrecord\"," +
                    "\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}";
            Schema.Parser parser = new Schema.Parser();

            Schema schema = parser.parse(userSchema);

            for (long index = 0; index < 10000; index++) {
                GenericRecord avroRecord = new GenericData.Record(schema);
                avroRecord.put("name", "value" + index);

                final ProducerRecord<Long, GenericRecord> record = new ProducerRecord<>(TOPIC, index, avroRecord);

                producer.send(record);
            }
        } finally {
            producer.flush();
            producer.close();
            System.out.println("DONE");
        }
    }

    private static Producer<Long, GenericRecord> createProducer() {
        final Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.setProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);

        final Producer<Long, GenericRecord> producer = new KafkaProducer<>(props);

        return producer;
    }
}
