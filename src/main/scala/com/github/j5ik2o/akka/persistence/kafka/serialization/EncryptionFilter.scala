package com.github.j5ik2o.akka.persistence.kafka.serialization

import com.github.j5ik2o.akka.persistence.kafka.journal.PersistenceId

import scala.concurrent.{ ExecutionContext, Future }

trait EncryptionFilter {

  def encrypt(
      persistenceId: PersistenceId,
      bytes: Array[Byte],
      context: Map[String, AnyRef]
  )(implicit ec: ExecutionContext): Future[Array[Byte]]

  def decrypt(
      bytes: Array[Byte]
  )(implicit ec: ExecutionContext): Future[Array[Byte]]

}

object EncryptionFilter {

  class NonEncryption extends EncryptionFilter {
    override def encrypt(persistenceId: PersistenceId, bytes: Array[Byte], context: Map[String, AnyRef])(
        implicit ec: ExecutionContext
    ): Future[Array[Byte]] = Future.successful(bytes)

    override def decrypt(bytes: Array[Byte])(
        implicit ec: ExecutionContext
    ): Future[Array[Byte]] = Future.successful(bytes)
  }

}
