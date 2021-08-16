package org.apache.spark.sql.loki.stream

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.{Partition, TaskContext}
import scalaj.http.Http

object LokiPartition extends Partition {
    // TODO: partition by batch.
    override def index: Int = 0
}

case class LokiEntry(ts: Long, value: String)

class LokiSourceRdd(sc: SparkContext, start: LokiSourceOffset, end: LokiSourceOffset, lokiConfig: LokiConfig, lokiSourceConfig: LokiSourceConfig) extends RDD[LokiEntry](sc, Nil) {

    override def compute(split: Partition, context: TaskContext): Iterator[LokiEntry] = {
        // TODO: actually use split
        val s: Long = start.ts
        val request = Http(lokiConfig.endpoint+"/loki/api/v1/query_range")
          .param("query", lokiSourceConfig.query)
          .auth(lokiConfig.username, lokiConfig.password)

        val response = request.param("start", s.toString()).asString.throwError

        ujson.read(response.body)("data")("result")(0)("values").arr.map { v =>
            LokiEntry(v(0).str.toLong, v(1).str)
        }.toIterator
    }

    override protected def getPartitions: Array[Partition] = Array(LokiPartition)
}
