package com.foo.sink

import java.net.InetSocketAddress
import java.util

import com.foo.source.SensorReading
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.{FlinkJedisClusterConfig, FlinkJedisPoolConfig}
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

case class SensorReading(id:String, timestamp:Long, temperature:Double)
object Fink2RedisSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    import org.apache.flink.streaming.api.scala._
    //从文件读取数据打印到redis中
    val stream2 = env.readTextFile("F:\\Flink-Project\\src\\main\\resources\\sensor.txt")
      .map(line=>{
        val splits = line.split(",")
        SensorReading(splits(0).trim,splits(1).trim.toLong,splits(2).trim.toDouble)
      })
    val add1 = new InetSocketAddress("192.168.64.143",6000)
    val add2 = new InetSocketAddress("192.168.64.143",6001)
    val add3 = new InetSocketAddress("192.168.64.144",6002)
     val add4 = new InetSocketAddress("192.168.64.144",6003)
    val add5 = new InetSocketAddress("192.168.64.145",6004)
    val add6 = new InetSocketAddress("192.168.64.145",6005)
    val redis  = new util.HashSet[InetSocketAddress]()
    redis.add(add1)
    redis.add(add2)
    redis.add(add3)
   redis.add(add4)
    redis.add(add5)
    redis.add(add6)
    //redis配置
    val conf = new  FlinkJedisClusterConfig.Builder().setNodes(redis).build()

    stream2.addSink(new RedisSink[SensorReading](conf, new MyRedisMapper()))
    stream2.print()
    env.execute("redis")
  }
}
class MyRedisMapper extends RedisMapper[SensorReading] {
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET, "sensor_temperature")
  }

  override def getKeyFromData(t: SensorReading): String = {t.temperature.toString}

  override def getValueFromData(t: SensorReading): String = t.id
}