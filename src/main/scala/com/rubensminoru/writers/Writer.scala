package com.rubensminoru.writers;

import com.rubensminoru.messages.KafkaAvroMessage;
import org.apache.avro.Schema;

trait Writer {
    def write(message:KafkaAvroMessage)

    def close

    def getTimestamp: Long

    def isSchemaCompatible(schema:Schema): Boolean
}
