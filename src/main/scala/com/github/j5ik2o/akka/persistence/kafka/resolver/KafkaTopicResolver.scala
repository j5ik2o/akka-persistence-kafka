package com.github.j5ik2o.akka.persistence.kafka.resolver

import com.github.j5ik2o.akka.persistence.kafka.journal.PersistenceId

trait KafkaTopicResolver {

  def resolve(persistenceId: PersistenceId): KafkaTopic

}

object KafkaTopicResolver {

  object PersistenceId extends KafkaTopicResolver {
    override def resolve(persistenceId: PersistenceId): KafkaTopic = {
      KafkaTopic(persistenceId.asString)
    }
  }

}
