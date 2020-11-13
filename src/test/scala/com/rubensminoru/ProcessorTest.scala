package com.rubensminoru

import com.rubensminoru.messages.KafkaAvroMessage
import com.rubensminoru.partitioners.TimeBasedPartitioner
import com.rubensminoru.writers.Writer
import org.mockito.captor.ArgCaptor
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.funspec.AnyFunSpec

class ProcessorTest extends AnyFunSpec with MockitoSugar with ArgumentMatchersSugar {
  describe("testProcess") {
    it("should call process and return true") {
      val message1 = mock[KafkaAvroMessage]
      val message2 = mock[KafkaAvroMessage]
      val messageArgumentCaptor = ArgCaptor[KafkaAvroMessage]

      val processor: Processor = new Processor("topic", "path", new TimeBasedPartitioner)
      var messages: List[KafkaAvroMessage] = List(message1, message2)
      val processorSpy: Processor = spy(processor)

      doNothing.when(processorSpy).processEach(any[KafkaAvroMessage])

      processorSpy.process(messages)

      verify(processorSpy, times(2)).processEach(messageArgumentCaptor.capture)

      val captorMessages: List[KafkaAvroMessage] = messageArgumentCaptor.values

      assert(message1 == captorMessages(0))
      assert(message2 == captorMessages(1))
    }
  }

  describe("testProcessEach") {
    val message = mock[KafkaAvroMessage]

    val processor: Processor = new Processor("topic", "path", new TimeBasedPartitioner)
    val processorSpy: Processor = spy(processor)

    // FIXME: we need to remove the type of any, since this is internal class information
    doReturn(null).when(processorSpy).addOrUpdateWriter(any[Map[Writer, Map[Int, TopicInfo]]], any[KafkaAvroMessage])
    doReturn(null).when(processorSpy).addOrUpdatePartitionInfo(any[Map[Int, TopicInfo]], any[KafkaAvroMessage])

    processorSpy.processEach(message)

    verify(processorSpy).addOrUpdateWriter(any[Map[Writer, Map[Int, TopicInfo]]], eqTo(message))
    verify(processorSpy).addOrUpdatePartitionInfo(any[Map[Int, TopicInfo]], eqTo(message))
  }

  describe("testAddOrUpdateWriter") {

  }

  describe("testAddOrUpdatePartitionInfo") {

  }
}
