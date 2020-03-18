package com.github.j5ik2o.akka.persistence.kafka.journal

import akka.actor.ActorRef

object KafkaJournalProtocol {

  case class ReadHighestSequenceNr(fromSequenceNr: Long = 1L, persistenceId: String, persistentActor: ActorRef)

  case class ReadHighestSequenceNrSuccess(highestSequenceNr: Long)

  case class ReadHighestSequenceNrFailure(cause: Throwable)

}
