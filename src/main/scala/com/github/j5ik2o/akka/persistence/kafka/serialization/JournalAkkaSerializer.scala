package com.github.j5ik2o.akka.persistence.kafka.serialization

import akka.actor.ExtendedActorSystem
import akka.persistence.PersistentRepr
import akka.serialization.{ SerializationExtension, Serializer => AkkaSerializer }
import com.github.j5ik2o.akka.persistence.kafka.journal.{ JournalRow, PersistenceId, SequenceNumber }
import com.github.j5ik2o.akka.persistence.kafka.protocol.JournalFormat
import com.google.protobuf.ByteString
import org.slf4j.LoggerFactory

object JournalAkkaSerializer {
  val Identifier = 15443
}

class JournalAkkaSerializer(system: ExtendedActorSystem) extends AkkaSerializer {
  private val logger = LoggerFactory.getLogger(getClass)

  override def identifier: Int          = JournalAkkaSerializer.Identifier
  override def includeManifest: Boolean = false

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    case journal: JournalRow =>
      logger.debug("toBinary:journal = {}", journal)
      val serializer = SerializationExtension(system).findSerializerFor(journal.persistentRepr)
      JournalFormat(
        persistenceId = journal.persistenceId.asString,
        sequenceNumber = journal.sequenceNumber.value,
        persistentRepr = ByteString.copyFrom(serializer.toBinary(journal.persistentRepr)),
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
    val result = JournalRow(
      persistentRepr = SerializationExtension(system)
        .deserialize(
          journalFormat.persistentRepr.toByteArray,
          classOf[PersistentRepr]
        )
        .get,
      tags = journalFormat.tags.toList
    )
    logger.debug("fromBinary:journal = {}", result)
    result
  }
}
