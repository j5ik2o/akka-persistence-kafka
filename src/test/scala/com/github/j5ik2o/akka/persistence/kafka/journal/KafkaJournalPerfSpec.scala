package com.github.j5ik2o.akka.persistence.kafka.journal

import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalPerfSpec
import com.typesafe.config.ConfigFactory
import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.scalatest.{ BeforeAndAfter, BeforeAndAfterAll }

class KafkaJournalPerfSpec
    extends JournalPerfSpec(
      ConfigFactory.parseString("""
                              |akka.test.single-expect-default = 60s
      """.stripMargin).withFallback(ConfigFactory.load())
    )
    with BeforeAndAfterAll {

  implicit val kafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
    customBrokerProperties = Map("num.partitions" -> "12")
  )

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedKafka.start()
  }

  protected override def afterAll(): Unit = {
    EmbeddedKafka.stop()
    super.afterAll()
  }

  override def eventsCount: Int = 500

  /** Number of measurement iterations each test will be run. */
  override def measurementIterations: Int = 1

  override def awaitDurationMillis: Long = 20000

  override def supportsAtomicPersistAllOfSeveralEvents: Boolean = false

  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = CapabilityFlag.on()

  override protected def supportsSerialization: CapabilityFlag = CapabilityFlag.off()

}
