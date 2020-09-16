package com.rubensminoru.partitioners;

import com.rubensminoru.messages.KafkaAvroMessage;
import com.rubensminoru.writers.Writer;

class TimeBasedPartitioner extends Partitioner {
    def DURATION_MS = 3600000L

    def check(message:KafkaAvroMessage, writer:Writer): Boolean = {
        message.timestamp / DURATION_MS == (writer.getTimestamp / DURATION_MS) &&
          writer.isSchemaCompatible(message.schema)
    }
}
