package com.github.j5ik2o.akka.persistence.kafka.serialization

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream }

import akka.actor.ExtendedActorSystem
import akka.persistence.SnapshotMetadata
import akka.persistence.serialization.{ Snapshot => AkkaSnapshot }
import akka.serialization.{ SerializationExtension, Serializer }
import com.github.j5ik2o.akka.persistence.kafka.protocol.SnapshotMetadataFormat
import com.github.j5ik2o.akka.persistence.kafka.snapshot.Snapshot

object SnapshotAkkaSerializer {
  val Identifier = 15442
}

class SnapshotAkkaSerializer(system: ExtendedActorSystem) extends Serializer {
  override def identifier: Int          = SnapshotAkkaSerializer.Identifier
  override def includeManifest: Boolean = false

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    case ks: Snapshot =>
      val extension          = SerializationExtension(system)
      val snapshot           = AkkaSnapshot(ks.payload)
      val snapshotSerializer = extension.findSerializerFor(snapshot)

      val snapshotBytes = snapshotSerializer.toBinary(snapshot)
      val metadataBytes = snapshotMetadataToBinary(ks)

      val out = new ByteArrayOutputStream
      writeInt(out, metadataBytes.length)
      out.write(metadataBytes)
      out.write(snapshotBytes)
      out.toByteArray
    case _ => throw new IllegalArgumentException(s"Can't serialize object of type ${o.getClass}")
  }

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    val extension      = SerializationExtension(system)
    val metadataLength = readInt(new ByteArrayInputStream(bytes))
    val metadataBytes  = bytes.slice(4, metadataLength + 4)
    val snapshotBytes  = bytes.drop(metadataLength + 4)

    val metadata = snapshotMetadataFromBinary(metadataBytes)
    val snapshot = extension.deserialize(snapshotBytes, classOf[AkkaSnapshot]).get
    Snapshot(metadata, snapshot.data)
  }

  private def snapshotMetadataToBinary(ks: Snapshot): Array[Byte] = {
    SnapshotMetadataFormat(
      persistenceId = ks.metadata.persistenceId,
      sequenceNumber = ks.metadata.sequenceNr,
      timestamp = ks.metadata.timestamp
    ).toByteArray
  }

  private def snapshotMetadataFromBinary(metadataBytes: Array[Byte]): SnapshotMetadata = {
    val metadataFormat = SnapshotMetadataFormat.parseFrom(metadataBytes)
    val metadata =
      SnapshotMetadata(metadataFormat.persistenceId, metadataFormat.sequenceNumber, metadataFormat.timestamp)
    metadata
  }

  private def writeInt(outputStream: OutputStream, i: Int): Unit =
    0 to 24 by 8 foreach { shift => outputStream.write(i >> shift) }

  private def readInt(inputStream: InputStream) =
    (0 to 24 by 8).foldLeft(0) { (id, shift) => id | (inputStream.read() << shift) }
}
