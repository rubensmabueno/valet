package com.rubensminoru.messages;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaMessage {
    private ConsumerRecord<Long, GenericRecord> kafkaRecord;

    public KafkaMessage(ConsumerRecord<Long, GenericRecord> record) {
        kafkaRecord = record;
    }

    public int getPartition() { return kafkaRecord.partition(); }

    public long getTimestamp() { return kafkaRecord.timestamp(); }

    public long getOffset() { return kafkaRecord.offset(); }
}
