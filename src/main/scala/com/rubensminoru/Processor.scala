package com.rubensminoru;

import com.rubensminoru.messages.KafkaAvroMessage
import com.rubensminoru.partitioners.Partitioner
import com.rubensminoru.writers.Writer
import com.rubensminoru.writers.FileWriterFactory
import java.io.IOException

import org.apache.kafka.clients.consumer.ConsumerRecord

class Processor(topic:String, path:String, partitioner:Partitioner) {
    // {writer => partition, topicinfo}    
    var writerTopicInfos:Map[Writer, Map[Int, TopicInfo]] = Map()
    var topicInfos:Map[Int, TopicInfo] = Map()

    @throws(classOf[IOException])
    def process(messages:Iterable[KafkaAvroMessage]):Boolean = {
        for (message <- messages) {
            processEach(message)
        }

        true
    }

    @throws(classOf[IOException])
    def processEach(message:KafkaAvroMessage) = {
        this.writerTopicInfos = addOrUpdateWriter(this.writerTopicInfos, message);

        this.topicInfos = addOrUpdatePartitionInfo(this.topicInfos, message);
    }

    def addOrUpdateWriter(localWriterTopicInfos:Map[Writer, Map[Int, TopicInfo]], message:KafkaAvroMessage):Map[Writer, Map[Int, TopicInfo]] = {
        val localWriterTopicInfo = localWriterTopicInfos
          .find(kv => checkRecordPartition(message, kv._1))
          .get

        if(localWriterTopicInfo == null) {
            val currentWriter = FileWriterFactory.apply(message.schema, this.path)
            localWriterTopicInfos + (currentWriter -> this.addOrUpdatePartitionInfo(Map[Int, TopicInfo](), message))
        } else {
            localWriterTopicInfos + (localWriterTopicInfo._1 -> this.addOrUpdatePartitionInfo(localWriterTopicInfo._2, message))
        }
    }

    def addOrUpdatePartitionInfo(localTopicInfos:Map[Int, TopicInfo], message:KafkaAvroMessage): Map[Int, TopicInfo] = {
        val topicInfo = localTopicInfos.getOrElse(message.partition, new TopicInfo(this.topic, message.partition, message.offset))
        topicInfo.offset = message.offset

        localTopicInfos + (message.partition -> topicInfo)
    }

    def checkRecordPartition(message:KafkaAvroMessage, writer:Writer): Boolean = partitioner.check(message, writer)
}

class TopicInfo(var topic:String, var partition:Int, var offset:Long)