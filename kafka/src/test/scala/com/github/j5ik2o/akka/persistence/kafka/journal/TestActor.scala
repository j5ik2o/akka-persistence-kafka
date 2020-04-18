package com.github.j5ik2o.akka.persistence.kafka.journal

import java.util.UUID

import akka.actor.{ ActorLogging, ActorRef }
import akka.persistence.{ PersistentActor, SnapshotOffer }
import com.github.j5ik2o.akka.persistence.kafka.journal.TestActor._

object TestActor {
  sealed trait Command
  sealed trait CommandReply
  trait HasReply extends Command {
    def replyTo: ActorRef
  }
  case class DeleteJournal(id: UUID, toSeqNr: Long, replyTo: ActorRef)           extends HasReply
  case class DeleteJournalReply(id: UUID)                                        extends CommandReply
  case class CreateState(id: UUID, name: String, amount: Int, replyTo: ActorRef) extends HasReply
  case class CreateStateReply(id: UUID)                                          extends CommandReply
  case class AddAmount(id: UUID, amount: Int, replyTo: ActorRef)                 extends HasReply
  case class AddAmountReply(id: UUID)                                            extends CommandReply

  case class GetState(id: UUID, replyTo: ActorRef) extends HasReply
  case class GetStateReply(state: State)           extends CommandReply

  sealed trait Event
  case class StateCreated(id: UUID, name: String, amount: Int) extends Event
  case class StateAmountUpdated(id: UUID, amount: Int)         extends Event
  case class State(id: UUID, name: String, amount: Int) {
    def withAmount(value: Int): State = copy(amount = value)
    def addAmount(value: Int): State  = withAmount(amount + value)
  }
}

class TestActor(modelName: String, id: UUID, snapShotInterval: Long, saveSnapshot: Boolean = true)
    extends PersistentActor
    with ActorLogging {
  override def persistenceId: String = modelName + "-" + id.toString
  private var state: State           = _

  override def receiveRecover: Receive = {
    case o @ SnapshotOffer(_, state: State) =>
      require(state.id == id, s"Invalid id: ${state.id} != $id")
      log.info(
        s"SnapshotOffer: pid = ${o.metadata.persistenceId} seqNr = ${o.metadata.sequenceNr}, ts = ${o.metadata.timestamp} state = $state"
      )
      this.state = state
    case e @ StateCreated(_id, name, amount) =>
      require(_id == id, s"${_id} != $id")
      log.info(s"event = $e")
      state = State(_id, name, amount)
    case e @ StateAmountUpdated(_id, amount) =>
      require(_id == id, s"${_id} != $id")
      log.info(s"event = $e, state = $state")
      state = state.addAmount(amount)

  }

  override def receiveCommand: Receive = {
    case DeleteJournal(_id, toSeqNr, replyTo) if _id == id =>
      deleteMessages(toSeqNr)
      replyTo ! DeleteJournalReply(_id)
    case CreateState(_id, name, amount, replyTo) if _id == id =>
      state = State(id, name, amount)
      persist(StateCreated(id, name, amount)) { event =>
        log.info(s"persist: seqNr = $lastSequenceNr, event = $event")
        replyTo ! CreateStateReply(_id)
      }
    case AddAmount(_id, amount, replyTo) if _id == id =>
      state = state.addAmount(amount)
      persist(StateAmountUpdated(_id, amount)) { event =>
        log.info(s"persist: seqNr = $lastSequenceNr, event = $event")
        if (saveSnapshot && lastSequenceNr % snapShotInterval == 0 && lastSequenceNr != 0) {
          log.info(s"saveSnapshot: seqNr = $lastSequenceNr, state = $state")
          saveSnapshot(state)
        }
        replyTo ! AddAmountReply(id)
      }
    case GetState(_id, replyTo) if _id == id =>
      replyTo ! GetStateReply(state)
  }

}
