package org.apache.spark.sql.loki.stream

import org.apache.spark.SparkConf

// TODO: add limits etc
case class LokiSourceConfig(query: String)
object LokiSourceConfig {
    def fromMap(config: Map[String, String]): LokiSourceConfig = {
        val query = config("loki.query")
        LokiSourceConfig(query)
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
