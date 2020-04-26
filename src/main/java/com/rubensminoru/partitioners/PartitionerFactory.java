package com.rubensminoru.partitioners;

public class PartitionerFactory {
    public Partitioner createInstance() {
        return new TimeBasedPartitioner();
    }
}
