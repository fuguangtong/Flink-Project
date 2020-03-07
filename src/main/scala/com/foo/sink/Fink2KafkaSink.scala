package com.foo.sink

import java.util.Properties

import com.foo.source.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

object Fink2KafkaSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    import org.apache.flink.streaming.api.scala._
    //从文件读取数据打印到kafka中
    val stream2 = env.readTextFile("F:\\Flink-Project\\src\\main\\resources\\sensor.txt")
      .map(line=>{
        val splits = line.split(",")
        SensorReading(splits(0).trim,splits(1).trim.toLong,splits(2).trim.toDouble).toString
      })
    val properties = new Properties();

    properties.setProperty("bootstrap.servers", "hadoop3:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    stream2.addSink(new FlinkKafkaProducer011[String]("kafka2sink", new SimpleStringSchema(),properties))

    stream2.print()

    env.execute("Fink2KafkaSink and console")
  }
}
