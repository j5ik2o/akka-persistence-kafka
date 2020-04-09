package com.github.j5ik2o.akka.persistence.kafka.resolver

import com.github.j5ik2o.akka.persistence.kafka.journal.PersistenceId
import com.typesafe.config.Config

trait KafkaPartitionResolver {

  def resolve(persistenceId: PersistenceId): KafkaPartition

}

object KafkaPartitionResolver {

  class PartitionZero(config: Config) extends KafkaPartitionResolver {
    override def resolve(persistenceId: PersistenceId): KafkaPartition =
      KafkaPartition(0)
  }

  class PartitionOne(config: Config) extends KafkaPartitionResolver {
    override def resolve(persistenceId: PersistenceId): KafkaPartition =
      KafkaPartition(1)
  }

}
