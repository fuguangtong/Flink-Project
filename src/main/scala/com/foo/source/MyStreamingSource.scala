package com.foo.source

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object MyStreamingSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    val stream4 = env.addSource(new MySensorSource())
    stream4.print()
    env.execute("MyStreamingSource")
  }



}
