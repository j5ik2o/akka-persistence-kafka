package com.github.j5ik2o.akka.persistence.kafka.journal

trait KafkaPartitionResolver {
  def resolve(persistenceId: PersistenceId): KafkaPartition
}

object KafkaPartitionResolver {
  object Default extends KafkaPartitionResolver {
    override def resolve(persistenceId: PersistenceId): KafkaPartition =
      KafkaPartition(0)
  }
}
