package cn.lijie

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, Logging, SparkConf, SparkContext}

/**
  * User: lijie
  * Date: 2017/8/2
  * Time: 15:01
  */
object SparkStreamingWC {

  /**
    * 函数
    * Iterator[(String, Seq[Int], Option[Int])]
    * String:       表示key
    * Seq[Int]:     表示当前批次的value e.g:Seq(1,1,1,1,1,1)
    * Option[Int]:  表示之前的累加值
    */
  val updateFunc = (it: Iterator[(String, Seq[Int], Option[Int])]) => {
    it.map(x => {
      (x._1, x._2.sum + x._3.getOrElse(0))
    })
  }


  def main(args: Array[String]): Unit = {
    //设置日志级别
    myLog.setLogLeavel(Level.ERROR)
    val conf = new SparkConf().setAppName("st").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))
    //拉取socket的信息
    val dStream = ssc.socketTextStream("192.168.80.123", 10086)
    //当使用updateStateByKey这个算子必须设置setCheckpointDir
    sc.setCheckpointDir("C:\\Users\\Administrator\\Desktop\\checkpoint")
    //计算wordcount 累计
    val res = dStream.flatMap(_.split(" ")).map((_, 1)).updateStateByKey(updateFunc, new HashPartitioner(sc.defaultParallelism), true)
    //计算wordcount 每个批次
    //    val res = dStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)
    res.print()
    ssc.start()
    ssc.awaitTermination()
  }
}


object myLog extends Logging {
  /**
    * 设置日志级别
    *
    * @param level
    */
  def setLogLeavel(level: Level): Unit = {
    val flag = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!flag) {
      logInfo("set log level ->" + level)
      Logger.getRootLogger.setLevel(level)
    }
  }
}