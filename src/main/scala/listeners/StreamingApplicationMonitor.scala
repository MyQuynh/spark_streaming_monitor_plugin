package listeners

import org.apache.spark.metrics.source.KafkaSourceMetrics
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

class StreamingApplicationMonitor() extends StreamingQueryListener {
  private val kafkaMetrics = KafkaSourceMetrics()
  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    print(s"Query started: ${event.id}, name: ${event.name}")
  }

  private implicit val formats: DefaultFormats.type = DefaultFormats
  private def extractOffset(eventName: String, offsetJson: String, typeOffset: String): Unit = {

    val offsets = parse(offsetJson).extract[Map[String, Map[String, Long]]]
    offsets.foreach {
      case (topic, partitionOffset) =>
        partitionOffset.foreach {
          case (partition, offset) =>
            kafkaMetrics.registerOffsetGauge(
              queryName = eventName,
              topic = topic,
              partition = partition,
              typeOffset = typeOffset,
              offset = offset
            )
        }
    }
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {

    print(s"Query started: ${event.progress}")

    event.progress.sources.foreach { source =>

      // endOffset
      extractOffset(
        eventName = event.progress.name,
        offsetJson = source.endOffset,
        typeOffset = KafkaSourceMetrics.CONSUMER_END_OFFSET
      )

    }
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    print(s"Query terminated: ${event.id}, exception: ${event.exception.getOrElse("No exception")}")
  }

}
