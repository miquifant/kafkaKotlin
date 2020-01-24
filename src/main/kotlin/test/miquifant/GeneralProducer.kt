package test.miquifant

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

import java.util.*

object GeneralProducer {

  fun main(args: Array<String>) {

    val props = Properties()
    props["bootstrap.servers"] = "quickstart.cloudera:9092"
    props["key.serializer"]    = "org.apache.kafka.common.serialization.StringSerializer"
    props["value.serializer"]  = "org.apache.kafka.common.serialization.StringSerializer"
    props["acks"]              = "1"

    val producer = KafkaProducer<String, String>(props)
    val topic = "miprima"

    try {
      (1..10).forEach { i ->
        val message = ProducerRecord<String, String>(
          topic,
          (i % 2).toString(),
          "message $i content"
        )
        val metadata = producer.send(message).get()
        println("sending message (key=%s, value=%s), meta (partition=%d, offset=%d, topic=%s)".format(
          message.key(),
          message.value(),
          metadata.partition(),
          metadata.offset(),
          metadata.topic()
        ))
      }
    }
    catch(e: Exception) {
      e.printStackTrace()
    }
    finally {
      producer.close()
    }
  }
}
