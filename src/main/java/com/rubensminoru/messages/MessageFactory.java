package com.rubensminoru.messages;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.ArrayList;
import java.util.List;

public class MessageFactory {
    public KafkaMessage createInstance(ConsumerRecord<Long, GenericRecord> record) {
        return new KafkaMessage(record);
    }

    public List<KafkaMessage> createInstances(ConsumerRecords<Long, GenericRecord> records) {
        List messages = new ArrayList<KafkaMessage>();

        for (ConsumerRecord<Long, GenericRecord> record : records) {
            messages.add(createInstance(record));
        }

        return messages;
    }
}
