package com.github.j5ik2o.akka.persistence.kafka.journal

import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalSpec
import com.typesafe.config.ConfigFactory
import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.{ Eventually, ScalaFutures }

import scala.concurrent.duration._

class KafkaJournalSpec
    extends JournalSpec(
      ConfigFactory.parseString("""
                                |akka.test.single-expect-default = 60s
      """.stripMargin).withFallback(ConfigFactory.load())
    )
    with BeforeAndAfter
    with ScalaFutures
    with Eventually {
  implicit val pc: PatienceConfig = PatienceConfig(30 seconds, 1 seconds)
  val kafkaPort                   = 6667

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
