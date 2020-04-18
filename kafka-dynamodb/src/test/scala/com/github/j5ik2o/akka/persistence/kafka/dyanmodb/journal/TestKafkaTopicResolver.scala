package com.github.j5ik2o.akka.persistence.kafka.dyanmodb.journal

import com.github.j5ik2o.akka.persistence.kafka.journal.PersistenceId
import com.github.j5ik2o.akka.persistence.kafka.resolver.{ KafkaTopic, KafkaTopicResolver }
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

class TestKafkaTopicResolver(config: Config) extends KafkaTopicResolver {
  val logger = LoggerFactory.getLogger(getClass)
  override def resolve(persistenceId: PersistenceId): KafkaTopic = {
    val topic = persistenceId.asString.split("-")(0)
    logger.debug(s"topic = $topic")
    KafkaTopic(topic)
  }
}
