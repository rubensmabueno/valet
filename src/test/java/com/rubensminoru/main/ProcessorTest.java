package com.rubensminoru.main;

import com.rubensminoru.messages.KafkaMessage;
import com.rubensminoru.partitioners.TimeBasedPartitioner;
import com.rubensminoru.writers.ParquetWriter;
import com.rubensminoru.writers.Writer;
import com.rubensminoru.writers.WriterFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class ProcessorTest {
    @Nested
    public class ProcessEachTest {
        @Mock
        KafkaMessage message1;

        @Mock
        KafkaMessage message2;

        @Captor
        ArgumentCaptor<KafkaMessage> messageArgumentCaptor;

        @BeforeEach
        public void init() {
            MockitoAnnotations.initMocks(this);
        }

        @Test
        public void shouldCallProcessAndReturnTrue() {
            Processor processor = new Processor("topic", new WriterFactory(), new TimeBasedPartitioner());
            List<KafkaMessage> messages = new ArrayList<>();
            Processor processorSpy = spy(processor);

            messages.add(message1);
            messages.add(message2);

            doNothing().when(processorSpy).process(any(KafkaMessage.class));

            processorSpy.process(messages);

            verify(processorSpy, times(2)).process(messageArgumentCaptor.capture());

            List<KafkaMessage> captorMessages = messageArgumentCaptor.getAllValues();

            assertEquals(message1, captorMessages.get(0));
            assertEquals(message2, captorMessages.get(1));
        }
    }

    @Nested
    public class ProcessTest {
        @Mock
        KafkaMessage message;

        @Captor
        ArgumentCaptor<KafkaMessage> messageArgumentCaptor;

        @BeforeEach
        public void init() {
            MockitoAnnotations.initMocks(this);
        }

        @Test
        public void shouldAddOrUpdateWriterAndAddOrUpdatePartition() {
            Processor processor = new Processor("topic", new WriterFactory(), new TimeBasedPartitioner());
            Processor processorSpy = spy(processor);

            doReturn(null).when(processorSpy).addOrUpdateWriter(any(Map.class), any(KafkaMessage.class));
            doReturn(null).when(processorSpy).addOrUpdatePartitionInfo(any(List.class), any(KafkaMessage.class));

            processorSpy.process(message);

            verify(processorSpy).addOrUpdateWriter(any(Map.class), messageArgumentCaptor.capture());
            verify(processorSpy).addOrUpdatePartitionInfo(any(List.class), messageArgumentCaptor.capture());

            assertEquals(messageArgumentCaptor.getValue(), message);
        }
    }

    @Nested
    public class AddOrUpdateWriterTest {
        @Mock
        KafkaMessage message;

        @Mock
        WriterFactory writerFactory;

        @BeforeEach
        public void init() {
            MockitoAnnotations.initMocks(this);
        }

        @Test
        public void shouldAddNewWriterTopicInfoWithEmptyTopicInfo() {
            Processor processor = spy(new Processor("topic", writerFactory, new TimeBasedPartitioner()));

            Writer writer = new ParquetWriter();
            List<Processor.TopicInfo> topicInfos = new ArrayList<>();

            Map<Writer, List<Processor.TopicInfo>> localWriterTopicInfos = new HashMap<>();

            when(writerFactory.createInstance()).thenReturn(writer);

            doReturn(topicInfos).when(processor).addOrUpdatePartitionInfo(any(List.class), eq(message));

            Map<Writer, List<Processor.TopicInfo>> resultTopicInfos = processor.addOrUpdateWriter(localWriterTopicInfos, message);

            assertEquals(resultTopicInfos.size(), 1);
            assertEquals(resultTopicInfos.get(writer), topicInfos);
        }

        @Test
        public void shouldUpdateWriterTopicInfoWhenTopicInfoFound() {
            Processor processor = spy(new Processor("topic", writerFactory, new TimeBasedPartitioner()));

            Writer writer = new ParquetWriter();
            List<Processor.TopicInfo> topicInfos = new ArrayList<>();
            Map<Writer, List<Processor.TopicInfo>> localWriterTopicInfos = new HashMap<>();

            localWriterTopicInfos.put(writer, topicInfos);

            doReturn(true).when(processor).checkRecordPartition(message, writer);
            doReturn(topicInfos).when(processor).addOrUpdatePartitionInfo(any(List.class), eq(message));

            Map<Writer, List<Processor.TopicInfo>> resultTopicInfos = processor.addOrUpdateWriter(localWriterTopicInfos, message);

            assertEquals(resultTopicInfos.size(), 1);
            assertEquals(resultTopicInfos.get(writer), topicInfos);
        }

        @Test
        public void shouldAddNewWriterTopicInfoWhenNoTopicInfoFound() {
            Processor processor = spy(new Processor("topic", writerFactory, new TimeBasedPartitioner()));

            Writer oldWriter = new ParquetWriter();
            Writer newWriter = new ParquetWriter();

            List<Processor.TopicInfo> topicInfos = new ArrayList<>();
            Map<Writer, List<Processor.TopicInfo>> localWriterTopicInfos = new HashMap<>();

            localWriterTopicInfos.put(oldWriter, topicInfos);

            when(writerFactory.createInstance()).thenReturn(newWriter);

            doReturn(false).when(processor).checkRecordPartition(message, oldWriter);
            doReturn(topicInfos).when(processor).addOrUpdatePartitionInfo(any(List.class), eq(message));

            Map<Writer, List<Processor.TopicInfo>> resultTopicInfos = processor.addOrUpdateWriter(localWriterTopicInfos, message);

            assertEquals(resultTopicInfos.size(), 2);
            assertEquals(resultTopicInfos.get(newWriter), topicInfos);
        }
    }

    @Nested
    public class AddOrUpdatePartitionInfoTest {
        @Mock
        KafkaMessage message;

        @BeforeEach
        public void init() {
            MockitoAnnotations.initMocks(this);
        }

        @Test
        public void shouldAddNewTopicInfoWhenNoPartitionFound() {
            List<Processor.TopicInfo> localTopicInfos = new ArrayList<>();

            Processor processor = new Processor("topic", new WriterFactory(), new TimeBasedPartitioner());
            Processor processorSpy = spy(processor);

            doReturn(1).when(message).getPartition();
            doReturn(2L).when(message).getOffset();

            List<Processor.TopicInfo> resultTopicInfos = processor.addOrUpdatePartitionInfo(localTopicInfos, message);

            assertEquals(resultTopicInfos.size(), 1);
            assertEquals(resultTopicInfos.get(0).getTopic(), "topic");
            assertEquals(resultTopicInfos.get(0).getPartition(), 1);
            assertEquals(resultTopicInfos.get(0).getOffset(), 2L);
        }

        @Test
        public void shouldUpdateOffsetTopicInfoWhenPartitionFound() {
            List<Processor.TopicInfo> localTopicInfos = new ArrayList<>();
            Processor.TopicInfo topicInfo = new Processor.TopicInfo("topic", 1, 2L);

            localTopicInfos.add(topicInfo);

            Processor processor = new Processor("topic", new WriterFactory(), new TimeBasedPartitioner());
            Processor processorSpy = spy(processor);

            doReturn(1).when(message).getPartition();
            doReturn(3L).when(message).getOffset();

            List<Processor.TopicInfo> resultTopicInfos = processorSpy.addOrUpdatePartitionInfo(localTopicInfos, message);

            assertEquals(resultTopicInfos.size(), 1);
            assertEquals(resultTopicInfos.get(0).getTopic(), "topic");
            assertEquals(resultTopicInfos.get(0).getPartition(), 1);
            assertEquals(resultTopicInfos.get(0).getOffset(), 3L);
        }
    }
}
