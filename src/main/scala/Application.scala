import org.apache.spark.sql.streaming.Trigger

import org.apache.spark.sql.{DataFrame, SparkSession}

object Application {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Test monitoring streaming application")
      .master("local[*]")
      .config("spark.metrics.conf.*.sink.prometheusServlet.class", "org.apache.spark.metrics.sink.PrometheusServlet")
      .config("spark.metrics.conf.*.sink.prometheusServlet.path", "/metrics/prometheus")
      .config("spark.metrics.conf.master.sink.prometheusServlet.path", "/metrics/master/prometheus")
      .config("spark.metrics.conf.applications.sink.prometheusServlet.path", "/metrics/applications/prometheus")
      .config("spark.sql.streaming.streamingQueryListeners", "listeners.StreamingApplicationMonitor")
      .getOrCreate()


    // spark.streams.addListener(new StreamingApplicationMonitor())

    val query = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "dummy-topic")
      .load()
      .writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        println(s"Processing batch $batchId with ${batchDF.count()} records.")
      }
      .option("checkpointLocation", "checkpoints")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    query.awaitTermination()

  }

}

 