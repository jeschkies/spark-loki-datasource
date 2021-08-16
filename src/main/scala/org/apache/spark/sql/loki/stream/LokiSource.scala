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
}

class LokiSource(sqlContext: SQLContext, parameters: Map[String, String]) extends Source {

    var offset: Offset = LokiSourceOffset(1627213800000000000L)

    private val sc = sqlContext.sparkContext
    private val lokiConfig = LokiConfig.fromSparkConf(sc.getConf) 
    private val lokiSourceConfig = LokiSourceConfig.fromMap(parameters) 

    override def schema: StructType = LokiSource.schema

    def stop(): Unit = ???
    
    override def getOffset: Option[Offset] = synchronized {
        Option(offset)
    }

    override def commit(end: Offset): Unit = offset = end
    
    def getBatch(start: Option[Offset], end: Offset): DataFrame = {

        // nanoTime might be slow.
        lazy val now = LokiSourceOffset(1627213800000000000L)//LokiSourceOffset(System.nanoTime())
        val internalRdd = new LokiSourceRdd(
            sc,
            start.map(LokiSourceOffset.fromOffset).getOrElse(now),
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
