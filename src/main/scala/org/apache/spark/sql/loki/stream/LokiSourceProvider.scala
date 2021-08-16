package org.apache.spark.sql.loki.stream

import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.execution.streaming.Source

class LokiSourceProvider extends StreamSourceProvider with DataSourceRegister {
    
    override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
          (shortName(), LokiSource.schema)
      }

    override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
          new LokiSource(sqlContext, parameters)
      }

    override def shortName(): String = "loki"
}
