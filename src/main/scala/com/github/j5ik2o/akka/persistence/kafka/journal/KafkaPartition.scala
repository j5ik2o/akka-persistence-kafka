package com.github.j5ik2o.akka.persistence.kafka.journal

case class KafkaPartition(value: Int) {
  require(value >= 0)
}
