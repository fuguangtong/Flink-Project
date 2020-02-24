import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object StreamingWordCount {
  def main(args: Array[String]): Unit = {
    //获取环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //连接socket获取输入数据
    val text = env.socketTextStream("hadoop3",9999);
    //导入隐示转换
    import org.apache.flink.streaming.api.scala._
    //对
    val wordCount  = text.flatMap(_.split(" ")).map((_,1)).keyBy(0).sum(1)

    wordCount.print()
    env.execute("StreamingWordCount")

  }
}
