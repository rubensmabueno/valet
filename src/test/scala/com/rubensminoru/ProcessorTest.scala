package com.rubensminoru

import com.rubensminoru.messages.KafkaAvroMessage
import com.rubensminoru.partitioners.TimeBasedPartitioner
import com.rubensminoru.writers.{FileWriterFactory, Writer}
import org.apache.avro.Schema
import org.mockito.captor.ArgCaptor
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.funspec.AnyFunSpec

class ProcessorTest extends AnyFunSpec with MockitoSugar with ArgumentMatchersSugar {
  describe("testProcess") {
    it("should call processEach") {
      val message1 = mock[KafkaAvroMessage]
      val message2 = mock[KafkaAvroMessage]
      val messageArgumentCaptor = ArgCaptor[KafkaAvroMessage]

      val processor: Processor = new Processor("topic", "path", new TimeBasedPartitioner)
      val messages: List[KafkaAvroMessage] = List(message1, message2)
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
    it("should call addOrUpdateWriter and addOrUpdatePartitionInfo") {
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
  }

  describe("testAddOrUpdateWriter") {
    describe("with one writer that returns true on checkRecordPartition") {
      it("should call addOrUpdatePartitionInfo with matched topicInfo") {
        val message = mock[KafkaAvroMessage]
        val writer1 = mock[Writer]
        val topicInfo1 = mock[TopicInfo]
        val partitionTopicInfo1 = Map(1 -> topicInfo1)
        val writerTopicInfos: Map[Writer, Map[Int, TopicInfo]] = Map(writer1 -> partitionTopicInfo1)

        val processor: Processor = new Processor("topic", "path", new TimeBasedPartitioner)
        val processorSpy: Processor = spy(processor)

        doReturn(true).when(processorSpy).checkRecordPartition(message, writer1)
        doReturn(partitionTopicInfo1).when(processorSpy).addOrUpdatePartitionInfo(partitionTopicInfo1, message)

        val result = processorSpy.addOrUpdateWriter(writerTopicInfos, message)

        assert(result.size == 1)
        assert(result.head._1 eq writer1)
        assert(result.head._2 eq partitionTopicInfo1)
      }
    }

    describe("with one writer that returns false on checkRecordPartition") {
      it("should create a new writer and call addOrUpdatePartitionInfo with new topicInfo") {
        val message = mock[KafkaAvroMessage]
        val schema = mock[Schema]
        val writer1 = mock[Writer]
        val writer2 = mock[Writer]
        val topicInfo1 = mock[TopicInfo]
        val partitionTopicInfo1 = Map(1 -> topicInfo1)
        val partitionTopicInfo2 = Map(2 -> topicInfo1)
        val writerTopicInfos: Map[Writer, Map[Int, TopicInfo]] = Map(writer1 -> partitionTopicInfo1)

        val processor: Processor = new Processor("topic", "path", new TimeBasedPartitioner)
        val processorSpy: Processor = spy(processor)

        when(message.schema).thenReturn(schema)
        doReturn(false).when(processorSpy).checkRecordPartition(message, writer1)
        doReturn(writer2).when(processorSpy).createWriter(schema, "path")
        doReturn(partitionTopicInfo2).when(processorSpy).addOrUpdatePartitionInfo(any[Map[Int, TopicInfo]], eqTo(message))

        val result = processorSpy.addOrUpdateWriter(writerTopicInfos, message)

        assert(result.size == 2)
        assert(result contains writer2)
        assert(result.get(writer2).head == partitionTopicInfo2)
      }
    }
  }

  describe("testAddOrUpdatePartitionInfo") {

  }
}
