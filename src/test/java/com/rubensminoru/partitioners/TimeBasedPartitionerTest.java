package com.rubensminoru.partitioners;

import com.rubensminoru.messages.KafkaMessage;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class TimeBasedPartitionerTest {
    @Mock
    KafkaMessage message1;

    @Mock
    KafkaMessage message2;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void shouldProcessEachAndReturnTrue() {
        TimeBasedPartitioner timeBasedPartitioner = new TimeBasedPartitioner("topic");
        List<KafkaMessage> messages = new ArrayList<>();
        TimeBasedPartitioner timeBasedPartitionSpy;

        ArgumentCaptor<KafkaMessage> messageArgumentCaptor = ArgumentCaptor.forClass(KafkaMessage.class);

        messages.add(message1);
        messages.add(message2);

        timeBasedPartitionSpy = spy(timeBasedPartitioner);

        timeBasedPartitionSpy.process(messages);

        verify(timeBasedPartitionSpy, times(2)).process(messageArgumentCaptor.capture());

        List<KafkaMessage> captorMessages = messageArgumentCaptor.getAllValues();

        assertEquals(message1, captorMessages.get(0));
        assertEquals(message2, captorMessages.get(1));
    }
}
