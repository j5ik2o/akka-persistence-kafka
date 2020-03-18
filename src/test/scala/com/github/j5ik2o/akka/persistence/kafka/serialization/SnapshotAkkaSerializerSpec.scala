package com.github.j5ik2o.akka.persistence.kafka.serialization

import akka.actor.ActorSystem
import akka.persistence.SnapshotMetadata
import akka.serialization.SerializationExtension
import akka.testkit.TestKit
import com.github.j5ik2o.akka.persistence.kafka.snapshot.Snapshot
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers

class SnapshotAkkaSerializerSpec
    extends TestKit(ActorSystem("JournalAkkaSerializerSpec"))
    with AnyFreeSpecLike
    with Matchers {

  "SnapshotAkkaSerializerSpec" - {
    "serialization" in {
      val serialization = SerializationExtension(system)
      val snapshot = Snapshot(
        SnapshotMetadata(
          persistenceId = "aaaa",
          sequenceNr = 1
        ),
        payload = "test"
      )
      val serializer = serialization.findSerializerFor(snapshot)
      val binary     = serializer.toBinary(snapshot)
      val reversed   = serialization.deserialize(binary, classOf[Snapshot]).get
      reversed shouldBe snapshot
    }
  }

}
