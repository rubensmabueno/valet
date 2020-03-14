package com.rubensminoru.main;

import com.rubensminoru.consumers.KafkaConsumer;
import com.rubensminoru.consumers.ConsumerFactory;
import com.rubensminoru.partitioners.PartitionerFactory;
import com.rubensminoru.partitioners.TimeBasedPartitioner;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.*;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

public class ConsumerMainTest {
    @Mock
    private ConsumerFactory consumerFactory;

    @Mock
    private PartitionerFactory partitionerFactory;

    @Mock
    private KafkaConsumer consumer;

    @Mock
    private TimeBasedPartitioner partitioner;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void shouldPoll() {
        //TODO: Should move Record to an internal class to make it easier to interact
        Map<TopicPartition, List<ConsumerRecord<Long, GenericRecord>>> records = new LinkedHashMap<>();
        String topic = "topic";
        Schema schema = SchemaBuilder.record("record").fields().name("id").type().intType().noDefault().endRecord();

        ConsumerRecord<Long, GenericRecord> record1 = new ConsumerRecord<>(topic, 0, 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, 1L, new GenericRecordBuilder(schema).set("id", 1).build());
        ConsumerRecord<Long, GenericRecord> record2 = new ConsumerRecord<>(topic, 0, 1, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, 2L, new GenericRecordBuilder(schema).set("id", 2).build());
        records.put(new TopicPartition(topic, 0), Arrays.asList(record1, record2));

        ConsumerRecords<Long, GenericRecord> consumerRecords = new ConsumerRecords<>(records);

        ConsumerRecords<Long, GenericRecord> consumerRecords2 = new ConsumerRecords<>(records);

        when(consumerFactory.createInstance("localhost:9092", "http://localhost:8081")).thenReturn(consumer);
        when(partitionerFactory.createInstance("topic")).thenReturn(partitioner);

        when(consumer.poll(anyInt())).thenReturn(consumerRecords);
        when(partitioner.process(consumerRecords)).thenReturn(false);

        ConsumerMain.process(consumerFactory, partitionerFactory);

        verify(consumer).subscribe("topic");
        verify(partitioner).process(consumerRecords);
    }
}
