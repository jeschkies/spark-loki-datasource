package org.apache.spark.sql.loki.stream

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset, Source}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.catalyst.InternalRow
import java.time.Instant
import org.apache.spark.internal.Logging

import ujson._
import scala.concurrent.duration._

case class LokiSourceOffset(ts: Long) extends Offset {

  override def json(): String = write(Js.Obj("ts" -> ts))

  def +(add: Duration): LokiSourceOffset= LokiSourceOffset(ts + add.toNanos)
}

object LokiSourceOffset {
    def fromOffset(offset: Offset): LokiSourceOffset = {
        offset match {
          case o: LokiSourceOffset => o
          case so: SerializedOffset => fromJson(so.json)
          case _ =>
            throw new IllegalArgumentException(
              s"Invalid conversion from offset of ${offset.getClass} to LokiSourceOffset")
        }
    }

    def fromJson(json: String): LokiSourceOffset = LokiSourceOffset(read(json)("ts").str.toLong)

    def now(): LokiSourceOffset = {
        // This precision is fine for us since we control the offset.
        val now = Instant.now()
        val nanoseconds: Long = now.getEpochSecond() * 1000000000L
        LokiSourceOffset(nanoseconds)
    }
}

/**
  * The main implementation of a Spark [[org.apache.spark.sql.execution.streaming.Source]].
  * 
  * The implementation is inspired by the [[https://github.com/RedisLabs/spark-redis Redis Spark library]].
  */
class LokiSource(sqlContext: SQLContext, parameters: Map[String, String]) extends Source with Logging {

    private val sc = sqlContext.sparkContext
    private val lokiConfig = LokiConfig.fromSparkConf(sc.getConf) 
    private val lokiSourceConfig = LokiSourceConfig.fromMap(parameters) 

    override def schema: StructType = LokiSource.schema

    def stop(): Unit = ???
    
    /**
      * The maximum offset is always up until now. So we return the current time in nano seconds.
      *
      * @return The current time in nanoseconds.
      */
    override def getOffset: Option[Offset] = synchronized {
        Option(LokiSourceOffset.now())
    }

    /**
      * Commit has no effect since Spark is persisting the offsets for us.
      *
      * @param end the end offset that Spark persisted.
      */
    override def commit(end: Offset): Unit = {
        logInfo(
        s"""Committing offset..
            |  end: ${end.json()}
            |""".stripMargin)
    }
    
    def getBatch(start: Option[Offset], end: Offset): DataFrame = {

        // If no start is provided we are on our first run an use the configured start.
        val actualStart = start match {
            case Some(o) =>
                LokiSourceOffset.fromOffset(o) + 1.nanosecond
            case None => LokiSourceOffset(lokiSourceConfig.start)
        }
        logInfo(s"Getting new batch: start=$actualStart end=$end")

        // TODO: Validate that we actually get new offsets each time.
        val internalRdd = new LokiSourceRdd(
            sc,
            actualStart,
            LokiSourceOffset.fromOffset(end),
            lokiConfig,
            lokiSourceConfig
        )
            .map { entry =>
                InternalRow(entry.ts, UTF8String.fromString(entry.value))
            }

        sqlContext.internalCreateDataFrame(internalRdd, schema, isStreaming = true)
    }
}

object LokiSource {
    val schema: StructType =
        StructType(List(StructField("ts", LongType), StructField("value", StringType)))
}
