package com.github.j5ik2o.akka.persistence.kafka.resolver

import com.github.j5ik2o.akka.persistence.kafka.journal.{ PersistenceId => PID }
import com.typesafe.config.Config

trait KafkaTopicResolver {

  def resolve(persistenceId: PID): KafkaTopic

}

object KafkaTopicResolver {

  class PersistenceId(config: Config) extends KafkaTopicResolver {
    override def resolve(persistenceId: PID): KafkaTopic = {
      KafkaTopic(persistenceId.asString)
    }
  }

}
