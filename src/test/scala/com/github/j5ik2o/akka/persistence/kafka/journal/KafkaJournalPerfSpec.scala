package com.github.j5ik2o.akka.persistence.kafka.journal

import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalPerfSpec
import com.typesafe.config.ConfigFactory
import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.scalatest.BeforeAndAfter

class KafkaJournalPerfSpec
    extends JournalPerfSpec(
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
  override def eventsCount: Int = 10 * 1

  /** Number of measurement iterations each test will be run. */
  override def measurementIterations: Int = 1

  override def awaitDurationMillis: Long = 20000

  override def supportsAtomicPersistAllOfSeveralEvents: Boolean = false

  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = CapabilityFlag.on()

  override protected def supportsSerialization: CapabilityFlag = CapabilityFlag.off()

}
