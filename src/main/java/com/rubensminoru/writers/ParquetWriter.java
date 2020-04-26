package com.rubensminoru.writers;

import com.rubensminoru.messages.KafkaMessage;

import java.util.ArrayList;
import java.util.List;

public class ParquetWriter implements Writer {
    private List<KafkaMessage> records;

    public ParquetWriter() {
        this.records = new ArrayList();
    }

    @Override
    public void write(KafkaMessage message) {
        this.records.add(message);
    }

    @Override
    public long getTimestamp() {
        return this.records.get(0).getTimestamp();
    }
}
