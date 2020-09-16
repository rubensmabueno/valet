package com.rubensminoru.partitioners;

import com.rubensminoru.messages.KafkaAvroMessage;
import com.rubensminoru.writers.Writer;

trait Partitioner {
    def check(message:KafkaAvroMessage, writer:Writer): Boolean
}
