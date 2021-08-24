package org.apache.spark.sql.loki.stream

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.{Partition, TaskContext}
import scalaj.http.Http
import org.apache.spark.internal.Logging
import scala.util.Try

object LokiPartition extends Partition {
    // TODO: partition by batch.
    override def index: Int = 0
}

final case class ClientError(message: String, cause: Throwable = None.orNull) extends Exception(message, cause)

case class LokiEntry(ts: Long, value: String)

class LokiSourceRdd(sc: SparkContext, start: LokiSourceOffset, end: LokiSourceOffset, lokiConfig: LokiConfig, lokiSourceConfig: LokiSourceConfig) extends RDD[LokiEntry](sc, Nil) with Logging {

    override def compute(split: Partition, context: TaskContext): Iterator[LokiEntry] = {
        // TODO: actually use split
        val s: Long = start.ts
        val request = Http(lokiConfig.endpoint+"/loki/api/v1/query_range")
          .param("query", lokiSourceConfig.query)
          .auth(lokiConfig.username, lokiConfig.password)

        val response = request.param("start", s.toString()).asString
        if(response.is4xx) {
            logError(s"Failed to query logs from ${request.url}: ${response.body}")
            val cause = Try(response.throwError).failed.get
            throw ClientError(s"Failed to query logs from ${request.url}: ${response.body}", cause)
        }
        response.throwError

        val result = ujson.read(response.body)("data")("result").arr
        result.toIterator.flatMap { data =>
          data("values").arr.map { v =>
            LokiEntry(v(0).str.toLong, v(1).str)
          }.toIterator
        }
    }

    override protected def getPartitions: Array[Partition] = Array(LokiPartition)
}
