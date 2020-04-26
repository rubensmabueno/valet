package com.rubensminoru.main;

import com.rubensminoru.partitioners.Partitioner;
import com.rubensminoru.writers.WriterFactory;

public class ProcessorFactory {
    public Processor createInstance(String topic, WriterFactory writerFactory, Partitioner partitioner) {
        return new Processor(topic, writerFactory, partitioner);
    }
}
