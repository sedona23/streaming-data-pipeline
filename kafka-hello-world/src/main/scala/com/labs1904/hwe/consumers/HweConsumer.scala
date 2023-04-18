package com.labs1904.hwe.consumers

import com.labs1904.hwe.util.Constants._
import com.labs1904.hwe.util.Util
import net.liftweb.json.DefaultFormats
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.KafkaProducer
import org.slf4j.LoggerFactory

import java.time.Duration
import java.util.Arrays

object HweConsumer {
  private val logger = LoggerFactory.getLogger(getClass)

  val consumerTopic: String = "question-1"
  val producerTopic: String = "question-1-output"

  implicit val formats: DefaultFormats.type = DefaultFormats

  def main(args: Array[String]): Unit = {

    // Create the KafkaConsumer
    val consumerProperties = Util.getConsumerProperties(BOOTSTRAP_SERVER)
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](consumerProperties)

    // Create the KafkaProducer
    val producerProperties = Util.getProperties(BOOTSTRAP_SERVER)
    val producer = new KafkaProducer[String, String](producerProperties)

    // Subscribe to the topic
    consumer.subscribe(Arrays.asList(consumerTopic))

    while ( {
      true
    }) {
      // poll for new data
      val duration: Duration = Duration.ofMillis(100)
      val records: ConsumerRecords[String, String] = consumer.poll(duration)

      records.forEach((record: ConsumerRecord[String, String]) => {
        // Retrieve the message from each record
        val message = record.value()
        logger.info(s"Message Received: $message")

        case class RawUser(senderKey: Int, senderId: String, senderName: String, senderEmail: String, senderDOB: String)
        val recvdArray = message.split("\\t")
        val parsedRawUser = RawUser(recvdArray(0).toInt,recvdArray(1),recvdArray(2),recvdArray(3),recvdArray(4))
        logger.info(s"Parsed Message Key: ${parsedRawUser.senderKey}" )

        case class EnrichedUser(rawUser: RawUser, numberAsWord: String, hweDeveloper: String)
        val enrichedUser = EnrichedUser(parsedRawUser, Util.mapNumberToWord(parsedRawUser.senderKey), "Nag N")
        logger.info(s"Parsed Message Key: ${enrichedUser.numberAsWord}" )


        //Convert your instance of `EnrichedUser` into a comma-separated string of values
        val commaSeparatedString = parsedRawUser.senderKey + "," +  parsedRawUser.senderId + ","
        + parsedRawUser.senderName + "," + parsedRawUser.senderEmail + "," + parsedRawUser.senderDOB + ',' + enrichedUser.numberAsWord + "," + enrichedUser.hweDeveloper

      })
    }
  }
}