package com.foo.transfrom

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
//定义样例类
case class SensorReading(id:String, timestamp:Long, temperature:Double)
object TransformationTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.readTextFile("F:\\Flink-Project\\src\\main\\resources\\sensor.txt")
      .map(line=>{
        val dataArray = line.split(",")
        SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).toDouble)
      }).keyBy("id").reduce((x,y)=>SensorReading(x.id,x.timestamp+1,y.temperature))
    //传感器数据按照温度高低（ 以 30 度为界）， 拆分成两个流。
   val splistStrem = stream.split(
    line=>{
      if (line.temperature > 30) Seq("high") else Seq("low")
  }
  )

    //高于三十度的所有信息
    val high = splistStrem.select("high")
    //低于三十度的所有信息
    val low = splistStrem.select("low")
    //所有信息
    val all = splistStrem.select("high", "low")
    /*//高于三十度的id和温度
    val warning = high.map( sensorData => (sensorData.id,sensorData.temperature) )
    //将高于三十度的id和温度和低于三十度的所有信息进行合并
    val connected = warning.connect(low)
    //将高于三十度的id和温度和低于三十度id进行打印，warning 危险，healthy 健康
    val coMap = connected.map(
      warningData => (warningData._1, warningData._2, "warning"),
      lowData => (lowData.id, "healthy")
    ).print()*/

    //使用union合并
    val unData = high.union(low)
    println(unData)

  env.execute("TransformationTest")
  }
}
