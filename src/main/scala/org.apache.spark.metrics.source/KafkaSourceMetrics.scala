package org.apache.spark.metrics.source

import com.codahale.metrics.{Gauge, MetricRegistry}
import org.apache.spark.SparkEnv

import java.util.concurrent.atomic.AtomicLong

class KafkaSourceMetrics extends Source {
  override val sourceName: String = "KafkaSourceMetrics"
  override val metricRegistry: MetricRegistry = new MetricRegistry

  private val registerOffset = new AtomicLong(0L)

  def registerOffsetGauge(queryName: String, topic: String, partition: String, typeOffset: String, offset: Long): Gauge[_] = {
    val offsetLong = offset
    registerOffset.set(offsetLong)

    val metricName = MetricRegistry.name(queryName, topic, partition, typeOffset)

    metricRegistry.gauge(
      metricName,
      new MetricRegistry.MetricSupplier[Gauge[_]] {
        override def newMetric(): Gauge[_] =
          new Gauge[Long] {
            override def getValue: Long = registerOffset.get()
          }
      }
    )
  }

}

object KafkaSourceMetrics {

  val sourceName = "spark_streaming_kafka"
  val CONSUMER_END_OFFSET: String = "consumer.end.offset"

  def apply(): KafkaSourceMetrics = {

    SparkEnv.get.metricsSystem.getSourcesByName(sourceName).headOption.getOrElse{
      val source = new KafkaSourceMetrics
      SparkEnv.get.metricsSystem.registerSource(source)
      source
    }.asInstanceOf[KafkaSourceMetrics]
  }
}
