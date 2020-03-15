package com.github.j5ik2o.akka.persistence.kafka.serialization

import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import akka.serialization.SerializationExtension
import akka.testkit.TestKit
import com.github.j5ik2o.akka.persistence.kafka.journal.{ Journal, PersistenceId, SequenceNumber }
import com.typesafe.config.ConfigFactory
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers

class KafkaJournalSerializerSpec
    extends TestKit(
      ActorSystem(
        "KafkaJournalSerializerSpec",
        ConfigFactory.parseString(
          """
    |akka {
    |  actor {
    |    serializers {
    |      kafka-journal = "com.github.j5ik2o.akka.persistence.kafka.serialization.KafkaJournalSerializer"
    |    }
    |    serialization-bindings {
    |      "com.github.j5ik2o.akka.persistence.kafka.journal.Journal" = kafka-journal
    |    }
    |  }
    |}
    |""".stripMargin
        )
      )
    )
    with AnyFreeSpecLike
    with Matchers {
  "KafkaJournalSerializer" - {
    "serialization" in {
      val serialization = SerializationExtension(system)
      val journal = Journal(
        persistenceId = PersistenceId("aaaa"),
        sequenceNumber = SequenceNumber(1),
        deleted = false,
        manifest = "",
        writerUuid = UUID.randomUUID().toString,
        timestamp = Instant.now().toEpochMilli,
        payload = "test",
        tags = Seq.empty
      )
      val serializer = serialization.findSerializerFor(journal)
      val binary     = serializer.toBinary(journal)
      val reversed   = serialization.deserialize(binary, classOf[Journal]).get
      reversed shouldBe journal
    }
  }
}
