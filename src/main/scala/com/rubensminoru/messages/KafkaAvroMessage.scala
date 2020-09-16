package com.rubensminoru.messages

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord

class KafkaAvroMessage(record:ConsumerRecord[Long, GenericRecord]) {
    def value: GenericRecord = record.value()

    def schema: Schema = record.value().getSchema

    def partition: Int = record.partition()

    def timestamp: Long = record.timestamp()

    def offset: Long = record.offset()
}
