package com.github.j5ik2o.akka.persistence.kafka.journal

trait KafkaTopicResolver {
  def resolve(persistenceId: PersistenceId): KafkaTopic
}

object KafkaTopicResolver {

  object Default extends KafkaTopicResolver {
    override def resolve(persistenceId: PersistenceId): KafkaTopic = {
      KafkaTopic(persistenceId.asString)
    }
  }

}
