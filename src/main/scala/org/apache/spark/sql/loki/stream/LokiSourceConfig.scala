package org.apache.spark.sql.loki.stream

import org.apache.spark.SparkConf

/** This configuration is per source but not per connection.
  *
  * @param query The query that should be performed for the stream.
  * @param start The start point for the query.
  */
case class LokiSourceConfig(query: String, start: Long)

object LokiSourceConfig {
  def fromMap(config: Map[String, String]): LokiSourceConfig = {
    val query = config("loki.query")
    val start = config("loki.start").toLong
    LokiSourceConfig(query, start)
  }
}

/** This configuration is for the connection to Loki.
  *
  * @param username The user or account number to use.
  * @param password The password or API key used for authentication.
  * @param endpoint The Loki API endpoint to use.
  */
case class LokiConfig(username: String, password: String, endpoint: String)

object LokiConfig {
  def fromSparkConf(sparkConf: SparkConf): LokiConfig = {
    val endpoint = sparkConf.get("spark.driver.GRAFANA_ENDPOINT")
    val username = sparkConf.get("spark.driver.GRAFANA_USER")
    val password = sparkConf.get("spark.driver.GRAFANA_TOKEN")

    LokiConfig(username, password, endpoint)
  }
}
