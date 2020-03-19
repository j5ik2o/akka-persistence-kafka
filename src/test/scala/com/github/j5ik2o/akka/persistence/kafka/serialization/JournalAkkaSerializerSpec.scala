package com.github.j5ik2o.akka.persistence.kafka.serialization

import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import akka.persistence.PersistentRepr
import akka.serialization.SerializationExtension
import akka.testkit.TestKit
import com.github.j5ik2o.akka.persistence.kafka.journal.{ JournalRow, PersistenceId, SequenceNumber }
import com.typesafe.config.ConfigFactory
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers

class JournalAkkaSerializerSpec
    extends TestKit(
      ActorSystem(
        "JournalAkkaSerializerSpec",
        ConfigFactory.parseString(
          """
        |akka {
        |  actor {
        |    serializers {
        |      kafka-journal = "com.github.j5ik2o.akka.persistence.kafka.serialization.JournalAkkaSerializer"
        |    }
        |    serialization-bindings {
        |      "com.github.j5ik2o.akka.persistence.kafka.journal.JournalRow" = kafka-journal
        |    }
        |  }
        |}
        |""".stripMargin
        )
      )
    )
    with AnyFreeSpecLike
    with Matchers {
  "JournalAkkaSerializer" - {
    "serialization" in {
      val serialization = SerializationExtension(system)
      val pid           = PersistenceId("aaaa")
      val seq           = SequenceNumber(1)
      val wUuid         = UUID.randomUUID().toString
      val now           = Instant.now().toEpochMilli
      val journal = JournalRow(
        persistentRepr = PersistentRepr(
          payload = "test",
          persistenceId = pid.asString,
          sequenceNr = seq.value,
          writerUuid = wUuid
        ).withTimestamp(now),
        tags = Seq.empty
      )
      val serializer = serialization.findSerializerFor(journal)
      val binary     = serializer.toBinary(journal)
      val reversed   = serialization.deserialize(binary, classOf[JournalRow]).get
      reversed shouldBe journal
    }
  }
}
