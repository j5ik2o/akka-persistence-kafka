package com.github.j5ik2o.akka.persistence.kafka.resolver

import com.github.j5ik2o.akka.persistence.kafka.journal.PersistenceId
import com.typesafe.config.Config

trait KafkaPartitionResolver {

  def resolve(partitionSize: Int, persistenceId: PersistenceId): KafkaPartition

}

object KafkaPartitionResolver {

  private def abs(n: Int) = {
    if (n == Int.MinValue) 0 else math.abs(n)
  }

  class PartitionAuto(config: Config) extends KafkaPartitionResolver {
    override def resolve(partitionSize: Int, persistenceId: PersistenceId): KafkaPartition =
      KafkaPartition(abs(persistenceId.##) % partitionSize)
  }

  class PartitionZero(config: Config) extends KafkaPartitionResolver {
    override def resolve(partitionSize: Int, persistenceId: PersistenceId): KafkaPartition =
      KafkaPartition(0)
  }

  class PartitionOne(config: Config) extends KafkaPartitionResolver {
    override def resolve(partitionSize: Int, persistenceId: PersistenceId): KafkaPartition =
      KafkaPartition(1)
  }

}
