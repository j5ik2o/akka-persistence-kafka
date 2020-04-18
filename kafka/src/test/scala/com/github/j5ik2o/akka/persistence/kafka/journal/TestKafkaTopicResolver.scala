package com.github.j5ik2o.akka.persistence.kafka.journal

import com.github.j5ik2o.akka.persistence.kafka.resolver.{ KafkaTopic, KafkaTopicResolver }
import com.typesafe.config.Config

class TestKafkaTopicResolver(config: Config) extends KafkaTopicResolver {
  override def resolve(persistenceId: PersistenceId): KafkaTopic = {
    val id        = persistenceId.asString
    val modelName = id.split("-")(0)
    KafkaTopic(modelName)
  }

}
