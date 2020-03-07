package com.foo.sink


import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests



object Flink2EsSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    import org.apache.flink.streaming.api.scala._
    //从文件读取数据打印到kafka中
    val stream2 = env.readTextFile("F:\\Flink-Project\\src\\main\\resources\\sensor.txt")
      .map(line=>{
        val splits = line.split(",")
        SensorReading(splits(0).trim,splits(1).trim.toLong,splits(2).trim.toDouble)
      })

    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("hadoop3", 9200))
    httpHosts.add(new HttpHost("hadoop4", 9200))
    httpHosts.add(new HttpHost("hadoop5", 9200))

    val esSinkBuilder = new ElasticsearchSink.Builder[SensorReading]( httpHosts,
      new ElasticsearchSinkFunction[SensorReading] {
        override def process(t: SensorReading, runtimeContext: RuntimeContext,
                             requestIndexer: RequestIndexer): Unit = {
          println("saving data: " + t)
          val json = new util.HashMap[String, String]()
          json.put("sensor_id", t.id)
          json.put("temperature", t.temperature.toString)
          json.put("timestamp", t.timestamp.toString)
          val indexRequest =
            Requests.indexRequest().index("sensor2").`type`("TestData").source(json)
          requestIndexer.add(indexRequest)
          println("saved successfully")
        }
      } )
    stream2.addSink(esSinkBuilder.build())
    stream2.print()
    env.execute("Flink2EsSink")
  }
}
