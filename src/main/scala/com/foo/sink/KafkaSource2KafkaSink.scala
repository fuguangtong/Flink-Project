package com.foo.sink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.api.scala._
object KafkaSource2KafkaSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties();
    properties.setProperty("bootstrap.servers", "hadoop3:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    //读取kafka中的数据
    val kafkaStream = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))

    val properties2 = new Properties();

    properties2.setProperty("bootstrap.servers", "hadoop3:9092")
    properties2.setProperty("group.id", "consumer-group")
    properties2.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties2.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties2.setProperty("auto.offset.reset", "latest")
    kafkaStream.addSink(new FlinkKafkaProducer011[String]("kafka2sink", new SimpleStringSchema(),properties2))

    kafkaStream.print()
    env.execute("Flink kafkaSource 2 Flink kafkaSink")

  }
}
