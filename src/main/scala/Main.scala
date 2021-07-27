import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import com.grafana.loki.LokiReceiver

object Main {

  def main(args: Array[String]) {
    //val spark = SparkSession.builder.appName("CI Analysis").getOrCreate()
    //import spark.implicits._

    // Create the context with a 1 second batch size
    val sparkConf = new SparkConf().setAppName("CustomReceiver")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    /*
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()
      */
    val endpoint = "https://logs-prod-us-central1.grafana.net"
    val query = """{job="ci"}"""
    val username = sparkConf.get("spark.driver.GRAFANA_USER")
    val password = sparkConf.get("spark.driver.GRAFANA_TOKEN")
    val lines = ssc.receiverStream(new LokiReceiver(endpoint, query, username, password))

    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
    /*
    val words = lines.as[String].flatMap(_.split(" "))
    val wordCounts = words.groupBy("value").count()

    val query = wordCounts.writeStream
    .outputMode(OutputMode.Complete())
    .format("console")
    .start()

    query.awaitTermination()
    spark.stop()*/
  }
}
