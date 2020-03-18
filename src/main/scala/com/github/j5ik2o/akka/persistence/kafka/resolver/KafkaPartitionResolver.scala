package com.github.j5ik2o.akka.persistence.kafka.resolver

import com.github.j5ik2o.akka.persistence.kafka.journal.PersistenceId

trait KafkaPartitionResolver {

  def resolve(persistenceId: PersistenceId): KafkaPartition

}

object KafkaPartitionResolver {

  object PartitionZero extends KafkaPartitionResolver {
    override def resolve(persistenceId: PersistenceId): KafkaPartition =
      KafkaPartition(0)
  }

  object PartitionOne extends KafkaPartitionResolver {
    override def resolve(persistenceId: PersistenceId): KafkaPartition =
      KafkaPartition(1)
  }

}
