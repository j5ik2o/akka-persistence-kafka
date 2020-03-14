package com.github.j5ik2o.akka.persistence.kafka.journal

import akka.Done
import akka.actor.{ ActorLogging, ActorSystem }
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{ Committer, Consumer, Producer }
import akka.kafka._
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{ AtomicWrite, PersistentRepr }
import akka.serialization.{ Serialization, SerializationExtension }
import akka.stream.scaladsl.{ Keep, Sink, Source }
import com.github.j5ik2o.akka.persistence.kafka.serialization.{
  ByteArrayJournalSerializer,
  FlowPersistentReprSerializer
}
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import org.apache.kafka.clients.consumer.{ Consumer => KafkaConsumer }
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{
  ByteArrayDeserializer,
  ByteArraySerializer,
  StringDeserializer,
  StringSerializer
}

import scala.collection.mutable
import scala.concurrent.{ ExecutionContext, Future }
import scala.jdk.CollectionConverters._
import scala.util.{ Failure, Success, Try }

object KafkaJournal {

  final case class InPlaceUpdateEvent(persistenceId: String, sequenceNumber: Long, message: AnyRef)

  private case class WriteFinished(pid: String, f: Future[_])

}

class KafkaJournal(config: Config) extends AsyncWriteJournal with ActorLogging {
  import KafkaJournal._
  type Deletions = Map[String, (Long, Boolean)]

  implicit val ec: ExecutionContext = context.dispatcher
  implicit val system: ActorSystem  = context.system

  private val pluginConfig     = JournalPluginConfig.fromConfig(config)
  private val bootstrapServers = config.as[List[String]]("bootstrap-servers").mkString(",")

  private val producerConfig = config.getConfig("producer")
  private val consumerConfig = config.getConfig("consumer")

  private val producerSettings =
    ProducerSettings(producerConfig, new StringSerializer, new ByteArraySerializer)
      .withBootstrapServers(bootstrapServers)

  private val consumerSettings = ConsumerSettings(consumerConfig, new StringDeserializer, new ByteArrayDeserializer)
    .withBootstrapServers(bootstrapServers)

  // private val producer: KafkaProducer[String, Array[Byte]] = producerSettings.createKafkaProducer()
  private val consumer: KafkaConsumer[String, Array[Byte]] = consumerSettings.createKafkaConsumer()

  protected val serialization: Serialization = SerializationExtension(system)
  protected val serializer: FlowPersistentReprSerializer[JournalRow] =
    new ByteArrayJournalSerializer(serialization, pluginConfig.tagSeparator)

  protected val writeInProgress: mutable.Map[String, Future[_]] = mutable.Map.empty

  // Transient deletions only to pass TCK (persistent not supported)
  private var deletions: Deletions = Map.empty

  override def postStop(): Unit = {
    writeInProgress.clear()
    consumer.close()
    super.postStop()
  }

  private def getPartition(pid: PersistenceId): Int = Math.abs(pid.asString.split(":")(1).##) % 256

  private def getTopic(pid: PersistenceId): String = pid.asString.split(":")(0)

  private val attempts = 1

  private def nextOffsetFor(topic: String, partition: Int): Long = {
    val tp = new TopicPartition(topic, partition)
    consumer.assign(List(tp).asJava)
    consumer.endOffsets(List(tp).asJava).get(tp)
  }

  override def asyncWriteMessages(atomicWrites: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
    log.debug(s"asyncWriteMessages($atomicWrites): start")

    val serializedTries: Seq[Either[Throwable, Seq[JournalRow]]] = serializer.serialize(atomicWrites)
    val rowsToWrite: Seq[JournalRow] = for {
      serializeTry <- serializedTries
      row          <- serializeTry.right.getOrElse(Seq.empty)
    } yield row

    def resultWhenWriteComplete: Seq[Either[Throwable, Unit]] =
      if (serializedTries.forall(_.isRight)) Nil
      else
        serializedTries
          .map(_.right.map(_ => ()))

    val future: Future[Seq[Try[Unit]]] = Source
      .single(ProducerMessage.multi(rowsToWrite.map { row =>
        new ProducerRecord(
          getTopic(row.persistenceId),
          getPartition(row.persistenceId),
          row.persistenceId.asString,
          row.message
        )
      }))
      .via(Producer.flexiFlow(producerSettings))
      .recoverWithRetries(attempts, {
        case ex =>
          log.error("occurred error")
          Source.failed(ex)
      })
      .mapConcat {
        case ProducerMessage.MultiResult(parts, passThrough) =>
          resultWhenWriteComplete.map {
            case Right(value) => Success(value)
            case Left(ex)     => Failure(ex)
          }.toVector
      }
      .runWith(Sink.seq)

    val persistenceId = atomicWrites.head.persistenceId
    writeInProgress.put(persistenceId, future)

    future.onComplete { result =>
      self ! WriteFinished(persistenceId, future)
      log.debug(s"asyncWriteMessages($atomicWrites): finished")
    }
    future

  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] =
    Future.successful(deleteMessagesTo(persistenceId, toSequenceNr, permanent = false))

  private def deleteMessagesTo(persistenceId: String, toSequenceNr: Long, permanent: Boolean): Unit =
    deletions = deletions + (persistenceId -> (toSequenceNr, permanent))

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      replayCallback: PersistentRepr => Unit
  ): Future[Unit] = {
    replayMessages(PersistenceId(persistenceId), fromSequenceNr, toSequenceNr, max, deletions, replayCallback)
  }

  private def replayMessages(
      persistenceId: PersistenceId,
      fromSequenceNr: Long,
      toSequenceNr: Long,
      max: Long,
      deletions: Deletions,
      callback: PersistentRepr => Unit
  ): Future[Unit] = {
    val (deletedTo, permanent) = deletions.getOrElse(persistenceId.asString, (0L, false))

    val adjustedFrom = if (permanent) math.max(deletedTo + 1L, fromSequenceNr) else fromSequenceNr
    val adjustedNum  = toSequenceNr - adjustedFrom + 1L
    val adjustedTo   = if (max < adjustedNum) adjustedFrom + max - 1L else toSequenceNr

    val committerSettings = CommitterSettings(system)

    val control: DrainingControl[Done] = Consumer
      .committableSource(
        consumerSettings,
        Subscriptions.assignmentWithOffset(
          new TopicPartition(getTopic(persistenceId), getPartition(persistenceId)) -> (adjustedFrom - 1L)
        )
      )
      .flatMapConcat { p =>
        Source
          .single(
            JournalRow(
              PersistenceId(p.record.key()),
              SequenceNumber(p.record.offset()),
              deleted = false,
              message = p.record.value(),
              ordering = 0,
              None
            )
          )
          .via(serializer.deserializeFlowWithoutTags)
          .map(repr => (p, repr))
      }
      .map {
        case (p, journalRow) =>
          (p, if (!permanent && journalRow.sequenceNr <= deletedTo) journalRow.update(deleted = true) else journalRow)
      }
      .filter { case (_, p) => p.sequenceNr >= adjustedFrom && p.sequenceNr <= adjustedTo }
      .map {
        case (p, repr) =>
          callback(repr)
          p.committableOffset
      }
      .toMat(Committer.sink(committerSettings))(Keep.both)
      .mapMaterializedValue(DrainingControl.apply)
      .run()

    control.streamCompletion.map(_ => ())

  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] =
    Future(readHighestSequenceNr(persistenceId, fromSequenceNr))

  private def readHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Long = {
    val topic = getTopic(PersistenceId(persistenceId))
    Math.max(nextOffsetFor(topic, getPartition(PersistenceId(persistenceId))) - 1, 0)
  }

  override def receivePluginInternal: Receive = {
    case msg @ WriteFinished(persistenceId, _) =>
      writeInProgress.remove(persistenceId)
  }
}
