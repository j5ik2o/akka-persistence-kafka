package com.github.j5ik2o.akka.persistence.kafka.snapshot

import akka.persistence.CapabilityFlag
import akka.persistence.snapshot.SnapshotStoreSpec
import com.typesafe.config.ConfigFactory
import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.scalatest.BeforeAndAfterAll

class KafkaSnapshotStoreSpec extends SnapshotStoreSpec(config = ConfigFactory.parseString(s"""
      |akka.test.single-expect-default = 60s
    """.stripMargin).withFallback(ConfigFactory.load())) with BeforeAndAfterAll {

  protected override def supportsSerialization: CapabilityFlag = CapabilityFlag.off()

  implicit val kafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
    customBrokerProperties = Map("num.partitions" -> "128")
  )

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedKafka.start()
  }

  protected override def afterAll(): Unit = {
    EmbeddedKafka.stop()
    super.afterAll()
  }

}
