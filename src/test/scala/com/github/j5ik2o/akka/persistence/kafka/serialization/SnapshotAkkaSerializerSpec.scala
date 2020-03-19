package com.github.j5ik2o.akka.persistence.kafka.serialization

import akka.actor.ActorSystem
import akka.persistence.SnapshotMetadata
import akka.serialization.SerializationExtension
import akka.testkit.TestKit
import com.github.j5ik2o.akka.persistence.kafka.snapshot.SnapshotRow
import com.typesafe.config.ConfigFactory
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers

class SnapshotAkkaSerializerSpec
    extends TestKit(
      ActorSystem(
        "JournalAkkaSerializerSpec",
        ConfigFactory.parseString(
          """
        |akka {
        |  actor {
        |    serializers {
        |      kafka-snapshot = "com.github.j5ik2o.akka.persistence.kafka.serialization.SnapshotAkkaSerializer"
        |    }
        |    serialization-bindings {
        |      "com.github.j5ik2o.akka.persistence.kafka.snapshot.SnapshotRow" = kafka-snapshot
        |    }
        |  }
        |}
        |""".stripMargin
        )
      )
    )
    with AnyFreeSpecLike
    with Matchers {

  "SnapshotAkkaSerializerSpec" - {
    "serialization" in {
      val serialization = SerializationExtension(system)
      val snapshot = SnapshotRow(
        SnapshotMetadata(
          persistenceId = "aaaa",
          sequenceNr = 1
        ),
        payload = "test"
      )
      val serializer = serialization.findSerializerFor(snapshot)
      val binary     = serializer.toBinary(snapshot)
      val reversed   = serialization.deserialize(binary, classOf[SnapshotRow]).get
      reversed shouldBe snapshot
    }
  }

}
