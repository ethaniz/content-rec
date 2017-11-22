import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime

/**
  * Created by ethan on 2017/11/16.
  */
object SS {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
        .builder()
        .appName("StructuredNetworkWordCount")
        .master("local[2]")
          .getOrCreate()

    //spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val lines = spark.readStream
                  .format("socket")
                  .option("host","localhost")
                    .option("port", 9999)
                     .load()


    val words = lines.as[String].flatMap(_.split(" "))

    val wordCounts = words.groupBy("value").count()

    import scala.concurrent.duration._
    val query = wordCounts.writeStream
        .outputMode("complete")
        .format("console")
        .trigger(ProcessingTime(2.seconds))
          .start()

    query.awaitTermination()
  }

}
