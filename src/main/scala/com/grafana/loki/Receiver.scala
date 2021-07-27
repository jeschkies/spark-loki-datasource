package com.grafana.loki

import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel
import org.apache.spark.internal.Logging
import scalaj.http.Http

// TODO: implement data source instead.
// See https://github.com/fdeantoni/spark-websocket-datasource/tree/master/src/main/scala/example
class LokiReceiver(endpoint: String, query: String, username: String, password: String)
    extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {
    
    def onStart() {
        new Thread("Loki Receiver") {
        override def run() { receive() }
        }.start()
    }

    def onStop() {

    }

    private def receive() {
        val start: Long = 1627213800000000000L // TODO: load start
        val request = Http(endpoint+"/loki/api/v1/query_range").param("query", query).auth(username, password)

        while(!isStopped()) {
            try {
                val response = request.param("start", start.toString()).asString.throwError

                val values = ujson.read(response.body)("data")("result")(0)("values").arr.map { v =>
                    v(1).str
                }
                store(values)

                // TODO: reset start
                stop("read query")
                return
            } catch {
                case ex: Exception => stop("stopping due to error", ex)
            }
        }
    }
}
