package com.github.j5ik2o.akka.persistence.kafka.serialization

import akka.actor.ActorSystem
import akka.persistence.journal.Tagged
import akka.persistence.{ AtomicWrite, PersistentRepr }
import akka.serialization.{ Serialization, SerializationExtension }
import com.github.j5ik2o.akka.persistence.kafka.journal.{ Journal, PersistenceId, SequenceNumber }
import com.github.j5ik2o.akka.persistence.kafka.utils.EitherSeq

object PersistentReprSerializer {
  type JournalWithByteArray = (Journal, Array[Byte])

}

class PersistentReprSerializer(system: ActorSystem) {
  import PersistentReprSerializer._

  private val serialization: Serialization = SerializationExtension(system)

  def serialize(persistentRepr: PersistentRepr): Either[Throwable, JournalWithByteArray] =
    serialize(persistentRepr, None)

  def serialize(
      persistentRepr: PersistentRepr,
      tags: Set[String],
      index: Option[Int]
  ): Either[Throwable, JournalWithByteArray] = {
    val journal = Journal(
      persistenceId = PersistenceId(persistentRepr.persistenceId),
      sequenceNumber = SequenceNumber(persistentRepr.sequenceNr),
      payload = persistentRepr.payload,
      deleted = persistentRepr.deleted,
      manifest = persistentRepr.manifest,
      timestamp = persistentRepr.timestamp,
      writerUuid = persistentRepr.writerUuid,
      tags = tags.toSeq,
      ordering = index
    )
    serialization.serialize(journal).map((journal, _)).toEither
  }

  def serialize(persistentRepr: PersistentRepr, index: Option[Int]): Either[Throwable, JournalWithByteArray] = {
    persistentRepr.payload match {
      case Tagged(payload, tags) =>
        serialize(persistentRepr.withPayload(payload), tags, index)
      case _ =>
        serialize(persistentRepr, Set.empty[String], index)
    }
  }

  def serialize(atomicWrites: Seq[AtomicWrite]): Seq[Either[Throwable, Seq[JournalWithByteArray]]] = {
    atomicWrites.map { atomicWrite =>
      val serialized = atomicWrite.payload.zipWithIndex.map {
        case (v, index) => serialize(v, Some(index))
      }
      EitherSeq.sequence(serialized)
    }
  }
}
