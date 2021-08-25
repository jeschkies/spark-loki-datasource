package org.apache.spark.sql.loki.stream

import org.apache.spark.SparkConf

case class LokiSourceConfig(query: String, start: Long)
object LokiSourceConfig {
    def fromMap(config: Map[String, String]): LokiSourceConfig = {
        val query = config("loki.query")
        val start = config("loki.start").toLong
        LokiSourceConfig(query, start)
    }
}

case class LokiConfig(username: String, password: String, endpoint: String)
object LokiConfig {
    def fromSparkConf(sparkConf: SparkConf): LokiConfig = {
      val endpoint = sparkConf.get("spark.driver.GRAFANA_ENDPOINT")
      val username = sparkConf.get("spark.driver.GRAFANA_USER")
      val password = sparkConf.get("spark.driver.GRAFANA_TOKEN")

      LokiConfig(username, password, endpoint)
    }
}
