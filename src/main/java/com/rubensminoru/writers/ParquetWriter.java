package com.rubensminoru.writers;

import com.rubensminoru.messages.KafkaMessage;

import java.util.ArrayList;
import java.util.List;

public class ParquetWriter {
    private List<KafkaMessage> records;

    public ParquetWriter() {
        this.records = new ArrayList();
    }

    public void write(KafkaMessage message) {
        this.records.add(message);
    }

    public long getTimestamp() {
        return this.records.get(0).getTimestamp();
    }
}
