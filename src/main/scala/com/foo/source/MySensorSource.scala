package com.foo.source

import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random

class MySensorSource extends SourceFunction[SensorReading]{
  // flag: 表示数据源是否还在正常运行
  var flag: Boolean = true

  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {

    // 初始化一个随机数发生器(该值大于等于0.0且小于1.0)
    val rand = new Random()
    println(rand)
    var curTemp = 1.to(10).map(
      i => ( "sensor_" + i, 65 + rand.nextGaussian() * 20 )
    )

    while(flag){
      // 更新温度值
      curTemp = curTemp.map(
        t => (t._1, t._2 + rand.nextGaussian() )
      )
      // 获取当前时间戳
      val curTime = System.currentTimeMillis()
      curTemp.foreach(
        t => ctx.collect(SensorReading(t._1, curTime, t._2))
      )
      Thread.sleep(100)
    }
  }

  //取消后传感器中断运行
  override def cancel(): Unit ={
    flag=false
  }




}
