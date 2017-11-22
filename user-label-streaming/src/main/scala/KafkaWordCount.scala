import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql.functions._

/**
  * Created by ethan on 2017/11/15.
  */
object KafkaWordCount {
  def main(args: Array[String]): Unit = {
    val Array(zkQuorum, group, topics, numThreads) = Array("localhost", "test", "test", "1")
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    sparkConf.setMaster("local[2]")
              .set("redis.host", "localhost")
              .set("redis.port", "6379")
              .set("redis.db", "5")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val addFunc = (currValues: Seq[Int], prevValueState: Option[Int]) => {
      val currentCount = currValues.sum
      val previousCount = prevValueState.getOrElse(0)
      Some(currentCount + previousCount)
    }

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    val actions = lines.map(_.split(","))
    val wordCounts = actions.map(x => x(0) + "_" + x(1))
                            .map(x => (x, 1))
                              .reduceByKey(_+_)

    val totalCounts = wordCounts.updateStateByKey(addFunc)

    val user_data = totalCounts.map {case(k, v) => (k.split("_")(0), (k.split("_")(1) + ":" + v.toString))}

    val fd = user_data.reduceByKey((x,y) => {
      x+","+y
    })

    fd.foreachRDD {
      rdd => {
        rdd.foreach(record => {
          println("Key:" + record._1 + "  Value:" + record._2)
        })
      }
    }

    /*
    totalCounts.foreachRDD {
      rdd => {
        val user_data = rdd.map {case(k, v) => (k.split("_")(0), (k.split("_")(1) + ":" + v.toString))}

        val fd = user_data.reduceByKey((x,y) => {
          x+","+y
        })


        //println(fd)
        fd.foreach(record => {
          println(record._1 + ":" + record._2)
        })
      }
    }

    */
    ssc.start()
    ssc.awaitTermination()
  }
}
