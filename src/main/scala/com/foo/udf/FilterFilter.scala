package com.foo.udf

import com.foo.source.SensorReading
import com.foo.transfrom.SensorReading
import org.apache.flink.api.common.functions.{FilterFunction, MapFunction, RichFilterFunction}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
case class SensorReading(id:String, timestamp:Long, temperature:Double)
object FilterFilter {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    import org.apache.flink.streaming.api.scala._
    val stream1 = env.readTextFile("F:\\Flink-Project\\src\\main\\resources\\sensor.txt").map(line=>{
      val dataArray = line.split(",")
      SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).toDouble)//.toString
    })
    //通过继承FilterFunction实现高于三十五度的温度报警
    /* stream1.filter(new MyFilterDemo).print()*/

    //通过继承FilterFunction实现高于三十五度的温度报警
    //stream1.map(new MyFilterMapFunction).print()

    //MyProcessFunction
    //stream1.filter(new MyProcessFunction).print()

    //实现匿名类
   /* stream1.filter(new RichFilterFunction[SensorReading] {
      override def filter(value: SensorReading): Boolean ={
        value.temperature>35
      }
    }).print()*/
   // 将温度传入进去当参数
   /* stream1.filter(new KeyWordFilter("temperature")).print()*/
    //stream1.map(new )
    env.execute("MyFilterDemo")
  }

}

//继承ProcessFunction
/*class MyProcessFunction extends ProcessFunction[SensorReading,Boolean] {
  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, Boolean]#Context, out: Collector[Boolean]): Unit = {

  }
}*/
//继承mapfunction
class MyFilterMapFunction extends MapFunction[SensorReading,String] {
  override def map(value: SensorReading): String = {
    if(value.temperature>35){
      value.toString
    }else{
      "正常"
    }
  }
}

class KeyWordFilter(keyWord: String) extends FilterFunction[String] {
  override def filter(value: String): Boolean = {
    value.contains("sensor_1")
  }
}
//继承FilterFunction
class MyFilterDemo extends FilterFunction[SensorReading] {
  override def filter(value: SensorReading): Boolean = {
    value.temperature>35
  }
}