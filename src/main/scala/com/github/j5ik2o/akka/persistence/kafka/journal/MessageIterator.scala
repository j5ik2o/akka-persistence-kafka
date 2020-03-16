package com.github.j5ik2o.akka.persistence.kafka.journal

import java.time.Duration

import org.apache.kafka.clients.consumer.{ Consumer, ConsumerRecord, KafkaConsumer }
import org.apache.kafka.common.TopicPartition

import scala.jdk.CollectionConverters._

class MessageIterator(
    consumer: Consumer[String, Array[Byte]],
    topic: String,
    partition: Int,
    offset: Long,
    timeOut: Duration
) extends Iterator[ConsumerRecord[String, Array[Byte]]] {

  private var iter: Iterator[ConsumerRecord[String, Array[Byte]]] = iterator(offset)
  private var readMessages                                        = 0
  private var nextOffset: Long                                    = offset

  private def iterator(offset: Long): Iterator[ConsumerRecord[String, Array[Byte]]] = {
    val tp = new TopicPartition(topic, partition)
    consumer.assign(List(tp).asJava)
    consumer.seek(tp, offset)
    val it = consumer.poll(timeOut).iterator().asScala
    it
  }

  def next(): ConsumerRecord[String, Array[Byte]] = {
    val mo = iter.next()
    readMessages += 1
    nextOffset = mo.offset() + 1
    mo
  }

  @annotation.tailrec
  final def hasNext: Boolean =
    if (iter.hasNext) {
      true
    } else if (readMessages == 0) {
      close()
      false
    } else {
      iter = iterator(nextOffset)
      readMessages = 0
      hasNext
    }

  private def close(): Unit = {
    consumer.close()
  }
}
