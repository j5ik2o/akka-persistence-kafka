package com.github.j5ik2o.akka.persistence.kafka.serialization

import akka.actor.ExtendedActorSystem
import akka.persistence.SnapshotMetadata
import akka.serialization.{ SerializationExtension, Serializer }
import com.github.j5ik2o.akka.persistence.kafka.journal.protocol.{
  JournalFormat,
  PayloadFormat,
  SnapshotFormat,
  SnapshotMetadataFormat
}
import com.github.j5ik2o.akka.persistence.kafka.snapshot.Snapshot
import com.google.protobuf.ByteString

class SnapshotAkkaSerializer(system: ExtendedActorSystem) extends Serializer {
  override def identifier: Int          = 19720204
  override def includeManifest: Boolean = false

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    case s: Snapshot =>
      val data       = s.payload.asInstanceOf[AnyRef]
      val serializer = SerializationExtension(system).findSerializerFor(data)
      SnapshotFormat(
        metadata = Some(
          SnapshotMetadataFormat(
            persistenceId = s.metadata.persistenceId,
            sequenceNumber = s.metadata.sequenceNr,
            timestamp = s.metadata.timestamp
          )
        ),
        payload = Some(
          PayloadFormat(
            serializerId = serializer.identifier,
            data = ByteString.copyFrom(serializer.toBinary(data)),
            hasDataManifest = serializer.includeManifest,
            dataManifest =
              if (serializer.includeManifest) ByteString.copyFromUtf8(data.getClass.getName) else ByteString.EMPTY
          )
        )
      ).toByteArray
    case _ => throw new IllegalArgumentException(s"Can't serialize object of type ${o.getClass}")
  }

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    val snapshotFormat = SnapshotFormat.parseFrom(bytes)
    require(snapshotFormat.metadata.isDefined)
    require(snapshotFormat.payload.isDefined)
    val metadata = snapshotFormat.metadata.get
    val payload  = snapshotFormat.payload.get
    Snapshot(
      metadata = SnapshotMetadata(
        persistenceId = metadata.persistenceId,
        sequenceNr = metadata.sequenceNumber,
        timestamp = metadata.timestamp
      ),
      payload = SerializationExtension(system)
        .deserialize(
          payload.data.toByteArray,
          payload.serializerId,
          payload.dataManifest.toStringUtf8
        )
        .get
    )

  }
}
