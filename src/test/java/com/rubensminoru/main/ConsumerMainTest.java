package com.rubensminoru.main;

import com.rubensminoru.consumers.KafkaConsumer;
import com.rubensminoru.consumers.ConsumerFactory;
import com.rubensminoru.messages.KafkaAvroMessage;
import com.rubensminoru.partitioners.Partitioner;
import com.rubensminoru.partitioners.PartitionerFactory;
import com.rubensminoru.writers.WriterFactory;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.*;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

public class ConsumerMainTest {
    @Mock
    private ConsumerFactory consumerFactory;

    @Mock
    private ProcessorFactory processorFactory;

    @Mock
    private PartitionerFactory partitionerFactory;

    @Mock
    private WriterFactory writerFactory;

    @Mock
    private KafkaConsumer consumer;

    @Mock
    private Partitioner partitioner;

    @Mock
    private Processor processor;

    @BeforeEach
    public void initMocks() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void shouldPoll() {
        List<KafkaAvroMessage> messages = new ArrayList<>();
        String topic = "topic";
        Schema schema = SchemaBuilder.record("record").fields().name("id").type().intType().noDefault().endRecord();

        ConsumerRecord<Long, GenericRecord> record1 = new ConsumerRecord<>(topic, 0, 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, 1L, new GenericRecordBuilder(schema).set("id", 1).build());
        ConsumerRecord<Long, GenericRecord> record2 = new ConsumerRecord<>(topic, 0, 1, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, 2L, new GenericRecordBuilder(schema).set("id", 2).build());

        messages.add(new KafkaAvroMessage(record1));
        messages.add(new KafkaAvroMessage(record2));

        when(consumerFactory.createInstance("localhost:9092", "http://localhost:8081")).thenReturn(consumer);
        when(partitionerFactory.createInstance()).thenReturn(partitioner);
        when(processorFactory.createInstance("topic", writerFactory, partitioner)).thenReturn(processor);

        when(consumer.poll(anyInt())).thenReturn(messages);
        when(processor.process(messages)).thenReturn(false);

        ConsumerMain.process(consumerFactory, processorFactory, partitionerFactory, writerFactory);

        verify(consumer).subscribe("topic");
        verify(processor).process(messages);
    }
}
