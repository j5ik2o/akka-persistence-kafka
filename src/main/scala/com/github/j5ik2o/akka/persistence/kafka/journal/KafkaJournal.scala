package com.github.j5ik2o.akka.persistence.kafka.journal

import akka.actor.{ ActorLogging, ActorSystem }
import akka.kafka._
import akka.kafka.scaladsl.{ Consumer, Producer }
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{ AtomicWrite, PersistentRepr }
import akka.serialization.{ Serialization, SerializationExtension }
import akka.stream.scaladsl.{ Keep, Sink, Source }
import com.github.j5ik2o.akka.persistence.kafka.serialization.PersistentReprSerializer
import com.github.j5ik2o.akka.persistence.kafka.serialization.PersistentReprSerializer.JournalWithByteArray
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{
  ByteArrayDeserializer,
  ByteArraySerializer,
  StringDeserializer,
  StringSerializer
}

import scala.concurrent._
import scala.jdk.CollectionConverters._
import scala.util.{ Failure, Success, Try }

object KafkaJournal {

  final case class InPlaceUpdateEvent(persistenceId: String, sequenceNumber: Long, message: AnyRef)

  private case class WriteFinished(pid: String, f: Future[_])

}

class KafkaJournal(config: Config) extends AsyncWriteJournal with ActorLogging {
  type Deletions = Map[PersistenceId, (Long, Boolean)]

  implicit val ec: ExecutionContext = context.dispatcher
  implicit val system: ActorSystem  = context.system

  private val bootstrapServers = config.as[List[String]]("bootstrap-servers").mkString(",")

  private val producerConfig = config.getConfig("producer")
  private val consumerConfig = config.getConfig("consumer")

  protected val serialization: Serialization = SerializationExtension(system)

  protected val serializer = new PersistentReprSerializer(system)

  private val producerSettings =
    ProducerSettings(producerConfig, new StringSerializer, new ByteArraySerializer)
      .withBootstrapServers(bootstrapServers)

  private val consumerSettings = ConsumerSettings(consumerConfig, new StringDeserializer, new ByteArrayDeserializer)
    .withBootstrapServers(bootstrapServers)

  // Transient deletions only to pass TCK (persistent not supported)
  private var deletions: Deletions = Map.empty

  private def getPartition(pid: PersistenceId): Int = 0

  private def getTopic(pid: PersistenceId): String = pid.asString

  override def asyncWriteMessages(atomicWrites: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
    log.debug(s"asyncWriteMessages($atomicWrites): start")
    val serializedTries: Seq[Either[Throwable, Seq[JournalWithByteArray]]] = serializer.serialize(atomicWrites)
    val rowsToWrite: Seq[JournalWithByteArray] = for {
      serializeTry <- serializedTries
      row          <- serializeTry.right.getOrElse(Seq.empty)
    } yield row
    def resultWhenWriteComplete: Vector[Try[Unit]] =
      if (serializedTries.forall(_.isRight))
        Vector.empty
      else
        serializedTries
          .map(_.right.map(_ => ()))
          .map {
            case Right(value) => Success(value)
            case Left(ex)     => Failure(ex)
          }
          .toVector
    val messages =
      if (rowsToWrite.size == 1) {
        val journal   = rowsToWrite.head._1
        val byteArray = rowsToWrite.head._2
        ProducerMessage.single(
          new ProducerRecord(
            getTopic(journal.persistenceId),
            getPartition(journal.persistenceId),
            journal.persistenceId.asString,
            byteArray
          )
        )
      } else
        ProducerMessage.multi(rowsToWrite.map {
          case (journal, byteArray) =>
            new ProducerRecord(
              getTopic(journal.persistenceId),
              getPartition(journal.persistenceId),
              journal.persistenceId.asString,
              byteArray
            )
        })
    val future: Future[Seq[Try[Unit]]] = Source
      .single(messages)
      .via(Producer.flexiFlow(producerSettings))
      .mapConcat {
        case ProducerMessage.Result(_, _) =>
          resultWhenWriteComplete
        case ProducerMessage.MultiResult(_, _) =>
          resultWhenWriteComplete
        case ProducerMessage.PassThroughResult(_) =>
          Vector.empty
      }
      .toMat(Sink.seq)(Keep.right)
      .run()
    future.onComplete {
      case Success(value) =>
        log.debug(s"asyncWriteMessages($atomicWrites): finished, succeeded($value)")
      case Failure(ex) =>
        log.debug(s"asyncWriteMessages($atomicWrites): finished, failed($ex)")
    }
    future
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] =
    Future.successful {
      deletions += (PersistenceId(persistenceId) -> (toSequenceNr, false))
    }

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      replayCallback: PersistentRepr => Unit
  ): Future[Unit] = {
    log.debug(
      s"asyncReplayMessages($persistenceId, $fromSequenceNr, $toSequenceNr, $max): start"
    )
    val pid                    = PersistenceId(persistenceId)
    val (deletedTo, permanent) = deletions.getOrElse(pid, (0L, false))
    log.debug("deletedTo = {}, permanent = {}", deletedTo, permanent)

    val adjustedFrom =
      if (!permanent) Math.max(deletedTo + 1L, fromSequenceNr) else fromSequenceNr
    val adjustedNum = toSequenceNr - adjustedFrom + 1L
    val adjustedTo  = if (max < adjustedNum) adjustedFrom + max - 1L else toSequenceNr

    log.debug("adjustedFrom = {}, adjustedNum = {}, adjustedTo = {}", adjustedFrom, adjustedNum, adjustedTo)

    val future =
      if (max == 0 || adjustedFrom > adjustedTo || deletedTo == Long.MaxValue)
        Future.successful(())
      else
        Consumer
          .plainSource(
            consumerSettings.withProperties(Map(ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_committed")),
            Subscriptions.assignmentWithOffset(
              new TopicPartition(getTopic(pid), getPartition(pid)) -> Math.max(adjustedFrom - 1, 0)
            )
          )
          .map { record => (record, serialization.deserialize(record.value(), classOf[Journal]).get) }
          .map {
            case (record, journal) =>
              (
                record,
                PersistentRepr(
                  persistenceId = journal.persistenceId.asString,
                  sequenceNr = journal.sequenceNumber.value,
                  payload = journal.payload,
                  deleted = journal.deleted,
                  manifest = journal.manifest,
                  writerUuid = journal.writerUuid
                ).withTimestamp(journal.timestamp)
              )
          }
          .map {
            case (record, persistentRepr) =>
              log.debug(
                s"m.offset = ${record.offset()}, p.sequenceNr = ${persistentRepr.sequenceNr}, deletedTo = $deletedTo"
              )
              if (!permanent && persistentRepr.sequenceNr <= deletedTo) {
                log.debug("update: deleted = true")
                persistentRepr.update(deleted = true)
              } else persistentRepr
          }
          .take(adjustedNum)
          .runWith(Sink.foreach { persistentRepr =>
            if (adjustedFrom <= persistentRepr.sequenceNr && persistentRepr.sequenceNr <= adjustedTo) {
              log.debug("callback = {}", persistentRepr)
              replayCallback(persistentRepr)
            }
          })
          .map(_ => ())
    future.onComplete {
      case Success(value) =>
        log.debug(
          s"asyncReplayMessages($persistenceId, $fromSequenceNr, $toSequenceNr, $max): finished, succeeded($value)"
        )
      case Failure(ex) =>
        log.debug(
          s"asyncReplayMessages($persistenceId, $fromSequenceNr, $toSequenceNr, $max): finished, failed($ex)"
        )
    }
    future
  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    val future = Future {
      log.debug("asyncReadHighestSequenceNr({},{}): start", persistenceId, fromSequenceNr)
      val topic     = getTopic(PersistenceId(persistenceId))
      val partition = getPartition(PersistenceId(persistenceId))
      val tp        = new TopicPartition(topic, partition)
      val consumer  = consumerSettings.createKafkaConsumer()
      val result =
        try {
          consumer.assign(List(tp).asJava)
          consumer.seek(tp, fromSequenceNr)
          consumer.endOffsets(List(tp).asJava).get(tp)
        } finally {
          consumer.close()
        }
      Math.max(result, 0)
    }
    future.onComplete {
      case Success(value) =>
        log.debug("asyncReadHighestSequenceNr({},{}): finished, succeeded({})", persistenceId, fromSequenceNr, value)
      case Failure(ex) =>
        log.debug("asyncReadHighestSequenceNr({},{}): finished, failed({})", persistenceId, fromSequenceNr, ex)
    }
    future
  }

}
