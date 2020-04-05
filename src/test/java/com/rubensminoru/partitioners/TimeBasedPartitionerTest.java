package com.rubensminoru.partitioners;

import com.rubensminoru.messages.KafkaMessage;
import com.rubensminoru.writers.WriterFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class TimeBasedPartitionerTest {
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
            TimeBasedPartitioner timeBasedPartitioner = new TimeBasedPartitioner("topic", new WriterFactory());
            List<KafkaMessage> messages = new ArrayList<>();
            TimeBasedPartitioner timeBasedPartitionSpy = spy(timeBasedPartitioner);

            messages.add(message1);
            messages.add(message2);

            doNothing().when(timeBasedPartitionSpy).process(any(KafkaMessage.class));

            timeBasedPartitionSpy.process(messages);

            verify(timeBasedPartitionSpy, times(2)).process(messageArgumentCaptor.capture());

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
            TimeBasedPartitioner timeBasedPartitioner = new TimeBasedPartitioner("topic", new WriterFactory());
            TimeBasedPartitioner timeBasedPartitionSpy = spy(timeBasedPartitioner);

            doReturn(null).when(timeBasedPartitionSpy).addOrUpdateWriter(any(Map.class), any(KafkaMessage.class));
            doReturn(null).when(timeBasedPartitionSpy).addOrUpdatePartitionInfo(any(List.class), any(KafkaMessage.class));

            timeBasedPartitionSpy.process(message);

            verify(timeBasedPartitionSpy).addOrUpdateWriter(any(Map.class), messageArgumentCaptor.capture());
            verify(timeBasedPartitionSpy).addOrUpdatePartitionInfo(any(List.class), messageArgumentCaptor.capture());

            assertEquals(messageArgumentCaptor.getValue(), message);
        }
    }

    @Nested
    public class AddOrUpdateWriterTest {

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
            List<TimeBasedPartitioner.TopicInfo> localTopicInfos = new ArrayList<>();

            TimeBasedPartitioner timeBasedPartitioner = new TimeBasedPartitioner("topic", new WriterFactory());
            TimeBasedPartitioner timeBasedPartitionSpy = spy(timeBasedPartitioner);

            doReturn(1).when(message).getPartition();
            doReturn(2L).when(message).getOffset();

            List<TimeBasedPartitioner.TopicInfo> resultTopicInfos = timeBasedPartitionSpy.addOrUpdatePartitionInfo(localTopicInfos, message);

            assertEquals(resultTopicInfos.size(), 1);
            assertEquals(resultTopicInfos.get(0).getTopic(), "topic");
            assertEquals(resultTopicInfos.get(0).getPartition(), 1);
            assertEquals(resultTopicInfos.get(0).getOffset(), 2L);
        }

        @Test
        public void shouldUpdateOffsetTopicInfoWhenPartitionFound() {
            List<TimeBasedPartitioner.TopicInfo> localTopicInfos = new ArrayList<>();
            TimeBasedPartitioner.TopicInfo topicInfo = new TimeBasedPartitioner.TopicInfo("topic", 1, 2L);

            localTopicInfos.add(topicInfo);

            TimeBasedPartitioner timeBasedPartitioner = new TimeBasedPartitioner("topic", new WriterFactory());
            TimeBasedPartitioner timeBasedPartitionSpy = spy(timeBasedPartitioner);

            doReturn(1).when(message).getPartition();
            doReturn(3L).when(message).getOffset();

            List<TimeBasedPartitioner.TopicInfo> resultTopicInfos = timeBasedPartitionSpy.addOrUpdatePartitionInfo(localTopicInfos, message);

            assertEquals(resultTopicInfos.size(), 1);
            assertEquals(resultTopicInfos.get(0).getTopic(), "topic");
            assertEquals(resultTopicInfos.get(0).getPartition(), 1);
            assertEquals(resultTopicInfos.get(0).getOffset(), 3L);
        }
    }
}
