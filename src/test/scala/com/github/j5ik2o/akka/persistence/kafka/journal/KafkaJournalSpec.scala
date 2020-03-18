package com.github.j5ik2o.akka.persistence.kafka.journal

import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalSpec
import com.typesafe.config.ConfigFactory
import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.scalatest.BeforeAndAfter

class KafkaJournalSpec
    extends JournalSpec(
      ConfigFactory.parseString("""
                                |akka.test.single-expect-default = 60s
      """.stripMargin).withFallback(ConfigFactory.load())
    )
    with BeforeAndAfter {

  implicit val kafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig()

  before {
    EmbeddedKafka.start()
  }

  after {
    EmbeddedKafka.stop()
  }

  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = CapabilityFlag.on()
}
