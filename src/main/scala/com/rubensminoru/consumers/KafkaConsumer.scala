package com.rubensminoru.consumers

import java.util.Properties

import com.rubensminoru.messages.KafkaAvroMessage
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroDeserializer}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.LongDeserializer

import scala.jdk.CollectionConverters.iterableAsScalaIterableConverter

class KafkaConsumer(bootstrapServers:String, schemaRegistryURL:String) {
    def props = new Properties()

    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-example-consumer")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[LongDeserializer])
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer])
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL)

    def consumer = new org.apache.kafka.clients.consumer.KafkaConsumer[Long, KafkaAvroMessage] (props)

    def subscribe(topic:String): Unit = {
        import scala.collection.JavaConverters._

        consumer.subscribe(List(topic).asJava)
    }

    def poll(size:Int): Iterable[ConsumerRecord[Long, KafkaAvroMessage]] = consumer.poll(size).asScala
}
