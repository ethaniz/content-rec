import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.JavaConversions._
/**
  * Created by ethan on 2017/11/15.
  */
object FileWordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("FileWordCount")
                                    .setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val lines = ssc.textFileStream("/Users/ethan/MyCodes/taglib/02_sourcecode/user-label-streaming/temp/")

    val words = lines.flatMap(_.split(" "))
    println(words.count)
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_+_)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
