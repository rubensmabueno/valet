package com.rubensminoru.partitioners;

import com.rubensminoru.messages.KafkaMessage;
import com.rubensminoru.writers.Writer;

public interface Partitioner {
    boolean check(KafkaMessage message, Writer writer);
}
