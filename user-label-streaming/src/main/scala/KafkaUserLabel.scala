import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions._

/**
  * Created by ethan on 2017/11/15.
  */
object KafkaUserLabel {

  def get_similary(itemId: String, redis: Jedis) {
    val SIMKEY = "p:smlr:" + itemId
    //println(SIMKEY)
    val similaryList = redis.zrangeWithScores(SIMKEY, 0, 10-1)
    //println(similaryList)
    val result = similaryList.map {x => (x.getElement(), x.getScore())}.toMap
    result
    println(result)
  }

  def map_merge(map1: Map[String, Double], map2: Map[String, Double]): Map[String, Double] ={
    map1 ++ map2.map{ case (k,v) => k -> (v + map1.getOrElse(k,0.0)) }
  }

  def map_mul_weight(mapInput: Map[String, Double], weight: Double): Map[String, Double] ={
    mapInput.map{ case(k, v) => k -> (v * weight)}
  }

  def main(args: Array[String]): Unit = {
    val Array(zkQuorum, group, topics, numThreads) = Array("localhost", "test", "test", "1")
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    sparkConf.setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.checkpoint("checkpoint")

    val addFunc = (currValues: Seq[Int], prevValueState: Option[Int]) => {
      val currentCount = currValues.sum
      val previousCount = prevValueState.getOrElse(0)
      Some(currentCount + previousCount)
    }

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    val actions = lines.map(_.split(","))
    val wordCounts = actions.map(x => (x(0) + "_" + x(1), x(2).toFloat))
                              .reduceByKey(_+_)

    //val totalCounts = wordCounts.updateStateByKey(addFunc)

    val user_data = wordCounts.map {case(k, v) => (k.split("_")(0), k.split("_")(1), v)}

    //val fd = user_data.reduceByKey((x,y) => {
    //  x+","+y
    //})

    val dbIndex = 0

    user_data.foreachRDD {
      rdd => {
        rdd.foreachPartition(partitionOfRecord => {
          //val redisHost = "localhost"
          //val redisPort = 6379
          //val jedis = new Jedis(redisHost, redisPort)
          val jedis = RedisClient.pool.getResource
          partitionOfRecord.foreach(pair => {
            val user = pair._1
            val item = pair._2
            val value = pair._3
            println("Key:" + user + "  item:" + item + "  value:" + value)
            jedis.select(dbIndex)
            jedis.hincrByFloat(user, item, value)

            val startTime = System.currentTimeMillis()
            val test = jedis.hgetAll(user)

            var mapList: List[Map[String, Double]] = List()
            for ((key, value) <- test) {
              println("key: " + key)
              println("value: " + value)
              //val map = get_similary(key, jedis)
              //println(get_similary(key, jedis))
              //mapList = mapList :+ map

              val SIMKEY = "p:smlr:" + key
              //println(SIMKEY)
              val similaryList = jedis.zrangeWithScores(SIMKEY, 0, 10-1)
              //println(similaryList)
              val map: Map[String, Double] = similaryList.map {x => (x.getElement(), x.getScore())}.toMap
              mapList = mapList :+ map_mul_weight(map, value.toDouble)
              println("maplist: " + mapList)
            }

            val ret = mapList.reduce(map_merge)
            val endtTime = System.currentTimeMillis()
            println("Time consumed : " + (endtTime-startTime) +"ms")
            println("ret: " + ret)
            RedisClient.pool.returnResource(jedis)
            //jedis.close()
          })

        })
      }
    }




    ssc.start()
    ssc.awaitTermination()
  }
}