package com.github.j5ik2o.akka.persistence.kafka.serialization

import akka.persistence.journal.Tagged
import akka.persistence.{ AtomicWrite, PersistentRepr }
import akka.serialization.Serialization
import com.github.j5ik2o.akka.persistence.kafka.journal.{ JournalRow, PersistenceId, SequenceNumber }
import com.github.j5ik2o.akka.persistence.kafka.utils.EitherSeq

import scala.util.{ Failure, Success }

object PersistentReprSerializer {
  type JournalWithByteArray = (JournalRow, Array[Byte])

}

class PersistentReprSerializer(serialization: Serialization) {
  import PersistentReprSerializer._

  def serialize(persistentRepr: PersistentRepr): Either[Throwable, JournalWithByteArray] =
    serialize(persistentRepr, None)

  def serialize(
      persistentRepr: PersistentRepr,
      tags: Set[String],
      index: Option[Int]
  ): Either[Throwable, JournalWithByteArray] = {
    val journal = JournalRow(
      persistentRepr = persistentRepr,
      tags = tags.toSeq,
      ordering = index
    )
    serialization.serialize(journal).map((journal, _)) match {
      case Failure(ex)    => Left(ex)
      case Success(value) => Right(value)
    }
  }

  def serialize(persistentRepr: PersistentRepr, index: Option[Int]): Either[Throwable, JournalWithByteArray] = {
    persistentRepr.payload match {
      case Tagged(payload, tags) =>
        serialize(persistentRepr.withPayload(payload), tags, index)
      case _ =>
        serialize(persistentRepr, Set.empty[String], index)
    }
  }

  def serialize(
      atomicWrites: Seq[AtomicWrite]
  ): Seq[Either[Throwable, Seq[JournalWithByteArray]]] = {
    atomicWrites.map { atomicWrite =>
      val serialized = atomicWrite.payload.zipWithIndex.map {
        case (v, index) => serialize(v, Some(index))
      }
      EitherSeq.sequence(serialized)
    }
  }
}
