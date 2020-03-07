package com.foo.sink

import java.sql.{Connection, DriverManager, PreparedStatement}


import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

//读取本地文件通过自定义sink到mysql中
object Fink2MySink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    import org.apache.flink.streaming.api.scala._
    //从文件读取数据打印到redis中
    val stream2 = env.readTextFile("F:\\Flink-Project\\src\\main\\resources\\sensor.txt")
      .map(line=>{
        val splits = line.split(",")
        SensorReading(splits(0).trim,splits(1).trim.toLong,splits(2).trim.toDouble)
      })
    stream2.addSink(new MyJdbcSink())
    env.execute()
  }
}
class MyJdbcSink() extends RichSinkFunction[SensorReading]{
  var conn:Connection=_
  var insertStmt:PreparedStatement=_
  var updateStmt:PreparedStatement=_

  //open主要是创建连接
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root","root")
    //添加数据
    insertStmt = conn.prepareStatement("INSERT INTO temperatures (sensor, temp) VALUES (?, ?)")
    //更新数据
    updateStmt = conn.prepareStatement("UPDATE temperatures SET temp = ? WHERE  sensor = ?")
  }

  //调用连接，执行sql
  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {

    //更新数据
    updateStmt.setDouble(1,value.temperature)
    updateStmt.setString(2, value.id)
    updateStmt.execute()
    println(updateStmt.getUpdateCount)
    //成功更新了多少记录，如果返回值为0，就说明没有1条记录被更新
    if (updateStmt.getUpdateCount == 0){
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(2, value.temperature)
      insertStmt.execute()
    }
  }
  override def close(): Unit =  {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}

