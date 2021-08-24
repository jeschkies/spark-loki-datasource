import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import com.grafana.loki.LokiReceiver
import org.apache.spark.sql.loki.stream.LokiSourceProvider
import org.apache.spark.sql.loki.stream.LokiSource
import org.apache.spark.sql.loki.stream.LokiEntry

object Main {

  def main(args: Array[String]) {

    /** Streaming
    // Create the context with a 1 second batch size
    val sparkConf = new SparkConf().setAppName("CustomReceiver")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val endpoint = sparkConf.get("spark.driver.GRAFANA_ENDPOINT")
    val query = """{job="ci"}"""
    val username = sparkConf.get("spark.driver.GRAFANA_USER")
    val password = sparkConf.get("spark.driver.GRAFANA_TOKEN")
    val lines = ssc.receiverStream(new LokiReceiver(endpoint, query, username, password))

    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
    */

    // Structured Streaming
    val spark = SparkSession.builder.appName("CI Analysis").getOrCreate()
    import spark.implicits._

    val logql = """{hostname="localhost.localdomain"}"""
    val lines = spark
      .readStream
      .format("org.apache.spark.sql.loki.stream.LokiSourceProvider")
      .option("loki.query", logql)
      .option("loki.start", "1629819869111910096")
      .schema(LokiSource.schema)
      .load()


    val words = lines.flatMap { d =>
      d.getAs[String]("value").split(" ")
    }
    val wordCounts = words.groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode(OutputMode.Complete())
      .format("console")
      .start()

    query.awaitTermination()
    spark.stop()
  }
}
