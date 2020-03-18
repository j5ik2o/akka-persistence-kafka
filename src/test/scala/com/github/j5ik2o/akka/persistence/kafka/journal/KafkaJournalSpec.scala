package com.github.j5ik2o.akka.persistence.kafka.journal

import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalSpec
import com.typesafe.config.ConfigFactory
import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.scalatest.BeforeAndAfterAll

class KafkaJournalSpec
    extends JournalSpec(
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

  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = CapabilityFlag.on()
}
