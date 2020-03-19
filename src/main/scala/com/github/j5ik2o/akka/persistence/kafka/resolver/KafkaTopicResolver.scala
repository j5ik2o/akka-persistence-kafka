package com.github.j5ik2o.akka.persistence.kafka.resolver

import com.github.j5ik2o.akka.persistence.kafka.journal.{ PersistenceId => PID }

trait KafkaTopicResolver {

  def resolve(persistenceId: PID): KafkaTopic

}

object KafkaTopicResolver {

  class PersistenceId extends KafkaTopicResolver {
    override def resolve(persistenceId: PID): KafkaTopic = {
      KafkaTopic(persistenceId.asString)
    }
  }

}
