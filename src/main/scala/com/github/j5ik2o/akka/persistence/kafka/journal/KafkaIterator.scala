package com.github.j5ik2o.akka.persistence.kafka.journal

import java.time.Duration

import org.apache.kafka.clients.consumer.{ Consumer, ConsumerRecord }
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._

class KafkaIterator(
    consumer: Consumer[String, Array[Byte]],
    topic: String,
    partition: Int,
    offset: Long,
    timeOut: Duration
) extends Iterator[ConsumerRecord[String, Array[Byte]]] {

  private val logger = LoggerFactory.getLogger(getClass)

  private val tp = new TopicPartition(topic, partition)
  consumer.assign(List(tp).asJava)
  consumer.seek(tp, offset)
  private val iter = consumer.poll(timeOut).iterator().asScala

  def next(): ConsumerRecord[String, Array[Byte]] =
    iter.next()

  final def hasNext: Boolean =
    if (iter.hasNext) {
      true
    } else {
      consumer.close()
      false
    }

}
