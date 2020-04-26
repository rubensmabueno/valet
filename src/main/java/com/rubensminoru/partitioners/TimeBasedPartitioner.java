package com.rubensminoru.partitioners;

import com.rubensminoru.messages.KafkaMessage;
import com.rubensminoru.writers.Writer;

public class TimeBasedPartitioner implements Partitioner {
    public Long DURATION_MS = 3600000L;

    public boolean check(KafkaMessage message, Writer writer) {
        return ((int) (message.getTimestamp() / DURATION_MS)) == ((int) (writer.getTimestamp() / DURATION_MS));
    }
}
