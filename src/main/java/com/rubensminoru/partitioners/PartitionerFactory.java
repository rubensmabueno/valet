package com.rubensminoru.partitioners;

public class PartitionerFactory {
    public TimeBasedPartitioner createInstance(String topic) {
        return new TimeBasedPartitioner(topic);
    }
}
