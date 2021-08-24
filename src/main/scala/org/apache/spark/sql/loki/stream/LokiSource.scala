package org.apache.spark.sql.loki.stream

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset, Source}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import ujson._

import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String
import java.time.Instant
import org.apache.spark.internal.Logging


case class LokiSourceOffset(ts: Long) extends Offset {

  override def json(): String = write(Js.Obj("ts" -> ts))
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

    override def commit(end: Offset): Unit = {
        logInfo(
        s"""Committing offset..
            |  end: ${end.json()}
            |""".stripMargin)
    }
    
    def getBatch(start: Option[Offset], end: Offset): DataFrame = {

        // If no start is provided we are on our first run an use the configured start.
        val actualStart = start.map(LokiSourceOffset.fromOffset).getOrElse(LokiSourceOffset(lokiSourceConfig.start))

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