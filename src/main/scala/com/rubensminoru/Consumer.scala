package com.rubensminoru

import java.io.IOException

import com.rubensminoru.consumers.ConsumerFactory
import com.rubensminoru.messages.KafkaAvroMessage
import com.rubensminoru.partitioners.PartitionerFactory
import org.apache.kafka.clients.consumer.ConsumerRecord

object Consumer {
  private val TOPIC = "topic"
  private val BOOTSTRAP_SERVERS = "localhost:9092"
  private val SCHEMA_REGISTRY_URL = "http://localhost:8081"

  @throws[IOException]
  def main(args: Array[String]): Unit = {
    val consumer = ConsumerFactory.apply(BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL)
    val partitioner = PartitionerFactory.apply
    val processor = ProcessorFactory.apply(TOPIC, TOPIC, partitioner)
    var process = true

    consumer.subscribe(TOPIC)

    while (process) process = processor.process(
      consumer.poll(1000).map(consumerRecord => consumerRecord.value())
    )
  }
}
