package com.rubensminoru.writer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.ArrayList;
import java.util.List;

public class ParquetWriter {
    private List<ConsumerRecord> records;

    public ParquetWriter() {
        this.records = new ArrayList();
    }

    public void write(ConsumerRecord record) {
        this.records.add(record);
    }

    public long getTimestamp() {
        return this.records.get(0).timestamp();
    }
}
