package com.github.j5ik2o.akka.persistence.kafka.serialization

import akka.persistence.journal.Tagged
import akka.persistence.{ AtomicWrite, PersistentRepr }
import akka.serialization.Serialization
import com.github.j5ik2o.akka.persistence.kafka.journal.JournalRow

import scala.concurrent.{ ExecutionContext, Future }

object PersistentReprSerializer {
  type JournalWithByteArray = (JournalRow, Array[Byte])
}

class PersistentReprSerializer(serialization: Serialization) {
  import PersistentReprSerializer._

  private val className = classOf[JournalAkkaSerializer].getName
  private val serializer: JournalAkkaSerializer =
    serialization
      .serializerOf(className)
      .map(_.asInstanceOf[JournalAkkaSerializer])
      .getOrElse(throw new ClassNotFoundException(className))

  def serialize(
      persistentRepr: PersistentRepr
  )(implicit ec: ExecutionContext): Future[JournalWithByteArray] =
    serialize(persistentRepr, None)

  def serialize(
      persistentRepr: PersistentRepr,
      tags: Set[String],
      index: Option[Int]
  )(implicit ec: ExecutionContext): Future[JournalWithByteArray] = {
    val journal = JournalRow(
      persistentRepr = persistentRepr,
      tags = tags.toSeq,
      ordering = index
    )
    serializer.toBinaryAsync(journal).map((journal, _))
  }

  def serialize(persistentRepr: PersistentRepr, index: Option[Int])(implicit
      ec: ExecutionContext
  ): Future[JournalWithByteArray] = {
    persistentRepr.payload match {
      case Tagged(payload, tags) =>
        serialize(persistentRepr.withPayload(payload), tags, index)
      case _ =>
        serialize(persistentRepr, Set.empty[String], index)
    }
  }

  def serialize(
      atomicWrites: Seq[AtomicWrite]
  )(implicit ec: ExecutionContext): Seq[Future[Seq[JournalWithByteArray]]] = {
    atomicWrites.map { atomicWrite =>
      val serialized = atomicWrite.payload.zipWithIndex.map { case (v, index) =>
        serialize(v, Some(index))
      }
      Future.sequence(serialized)
    }
  }
}
