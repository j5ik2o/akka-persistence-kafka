package com.github.j5ik2o.akka.persistence.kafka.serialization

import akka.actor.ExtendedActorSystem
import akka.serialization.{ SerializationExtension, Serializer => AkkaSerializer }
import com.github.j5ik2o.akka.persistence.kafka.journal.protocol.{ JournalFormat, PayloadFormat }
import com.github.j5ik2o.akka.persistence.kafka.journal.{ Journal, PersistenceId, SequenceNumber }
import com.google.protobuf.ByteString
import org.slf4j.LoggerFactory

object JournalAkkaSerializer {
  val Identifier = 19720203
}

class JournalAkkaSerializer(system: ExtendedActorSystem) extends AkkaSerializer {
  private val logger = LoggerFactory.getLogger(getClass)

  override def identifier: Int          = JournalAkkaSerializer.Identifier
  override def includeManifest: Boolean = false

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    case journal: Journal =>
      logger.debug("journal = {}", journal)
      val data       = journal.payload.asInstanceOf[AnyRef]
      val serializer = SerializationExtension(system).findSerializerFor(data)
      JournalFormat(
        persistenceId = journal.persistenceId.asString,
        sequenceNumber = journal.sequenceNumber.value,
        payload = Some(
          PayloadFormat(
            serializerId = serializer.identifier,
            data = ByteString.copyFrom(serializer.toBinary(data)),
            hasDataManifest = serializer.includeManifest,
            dataManifest =
              if (serializer.includeManifest) ByteString.copyFromUtf8(data.getClass.getName) else ByteString.EMPTY
          )
        ),
        deleted = journal.deleted,
        manifest = journal.manifest,
        timestamp = journal.timestamp,
        writerUuid = journal.writerUuid,
        tags = journal.tags
      ).toByteArray
    case _ => throw new IllegalArgumentException(s"Can't serialize object of type ${o.getClass}")
  }

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    val journalFormat = JournalFormat.parseFrom(bytes)
    require(journalFormat.payload.isDefined)
    val payload = journalFormat.payload.get
    val result = Journal(
      persistenceId = PersistenceId(journalFormat.persistenceId),
      sequenceNumber = SequenceNumber(journalFormat.sequenceNumber),
      payload = SerializationExtension(system)
        .deserialize(
          payload.data.toByteArray,
          payload.serializerId,
          payload.dataManifest.toStringUtf8
        )
        .get,
      deleted = journalFormat.deleted,
      manifest = journalFormat.manifest,
      timestamp = journalFormat.timestamp,
      writerUuid = journalFormat.writerUuid,
      tags = journalFormat.tags.toList
    )
    logger.debug("journal = {}", result)
    result
  }
}
