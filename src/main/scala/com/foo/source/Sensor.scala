package com.foo.source

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//定义样例类
case class SensorReading(id:String, timestamp:Long, temperature:Double)
object Sensor {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    //从集合读取数据
    val stream1 = env
      .fromCollection(List(
        SensorReading("sensor_1", 1547718199, 35.80018327300259),
        SensorReading("sensor_6", 1547718201, 15.402984393403084),
        SensorReading("sensor_7", 1547718202, 6.720945201171228),
        SensorReading("sensor_10", 1547718205, 38.101067604893444)
      ))
    //从文件读取数据
    val stream2 = env.readTextFile("F:\\Flink-Project\\src\\main\\resources\\sensor.txt")
    //设置并行度为1，并将从集合中取出的数据打印
    stream1.print("stream1:").setParallelism(1)
    //设置并行度为1,并将从文件中取出的数据打印
    stream2.print("stream2:").setParallelism(1)
    env.execute()
  }
}
