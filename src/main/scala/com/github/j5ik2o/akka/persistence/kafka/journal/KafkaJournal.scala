package com.github.j5ik2o.akka.persistence.kafka.journal

import java.time.Duration

import akka.actor.{ ActorLogging, ActorSystem }
import akka.kafka._
import akka.kafka.scaladsl.{ Consumer, Producer }
import akka.persistence.journal.{ AsyncWriteJournal, Tagged }
import akka.persistence.{ AtomicWrite, PersistentRepr }
import akka.serialization.{ Serialization, SerializationExtension }
import akka.stream.scaladsl.{ Keep, Sink, Source }
import com.github.j5ik2o.akka.persistence.kafka.utils.EitherSeq
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import org.apache.kafka.clients.consumer.{ ConsumerConfig, Consumer => KafkaConsumer }
import org.apache.kafka.clients.producer.{ ProducerRecord, Producer => KafkaProducer }
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

  private val producerSettings =
    ProducerSettings(producerConfig, new StringSerializer, new ByteArraySerializer)
      .withBootstrapServers(bootstrapServers)

  private val consumerSettings = ConsumerSettings(consumerConfig, new StringDeserializer, new ByteArrayDeserializer)
    .withBootstrapServers(bootstrapServers)

  protected val serialization: Serialization = SerializationExtension(system)

  // Transient deletions only to pass TCK (persistent not supported)
  private var deletions: Deletions = Map.empty

  override def postStop(): Unit = {
    super.postStop()
  }

  private def getPartition(pid: PersistenceId): Int = 0

  private def getTopic(pid: PersistenceId): String = pid.asString

  private val attempts = 1

  type JournalWithByteArray = (Journal, Array[Byte])

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
      tags = tags.toSeq
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

  override def asyncWriteMessages(atomicWrites: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
    log.debug(s"asyncWriteMessages($atomicWrites): start")
    val serializedTries: Seq[Either[Throwable, Seq[JournalWithByteArray]]] = serialize(atomicWrites)
    val rowsToWrite: Seq[JournalWithByteArray] = for {
      serializeTry <- serializedTries
      row          <- serializeTry.right.getOrElse(Seq.empty)
    } yield row
    def resultWhenWriteComplete: Seq[Either[Throwable, Unit]] =
      if (serializedTries.forall(_.isRight)) Nil
      else
        serializedTries
          .map(_.right.map(_ => ()))
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
        case ProducerMessage.Result(metadata, ProducerMessage.Message(record, passThrough)) =>
          resultWhenWriteComplete.map {
            case Right(value) => Success(value)
            case Left(ex)     => Failure(ex)
          }.toVector
        case ProducerMessage.MultiResult(parts, passThrough) =>
          resultWhenWriteComplete.map {
            case Right(value) => Success(value)
            case Left(ex)     => Failure(ex)
          }.toVector
        case ProducerMessage.PassThroughResult(passThrough) =>
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
    Future.successful(deleteMessagesTo(persistenceId, toSequenceNr, permanent = false))

  private def deleteMessagesTo(persistenceId: String, toSequenceNr: Long, permanent: Boolean): Unit = {
    log.info(s"deleteMessagesTo($persistenceId, $toSequenceNr, $permanent)")
    deletions += (PersistenceId(persistenceId) -> (toSequenceNr, permanent))
    log.info(s"deletions = $deletions")
    if (toSequenceNr == Long.MaxValue) {
      log.info("toSequenceNr = Long.MaxValue")
    }
  }

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      replayCallback: PersistentRepr => Unit
  ): Future[Unit] = {
    val pid = PersistenceId(persistenceId)
    log.info(
      s"persistenceId = $persistenceId, fromSequenceNr = $fromSequenceNr, toSequenceNr = $toSequenceNr, max = $max, deletions = $deletions"
    )

    akkaStreamImpl(pid, fromSequenceNr, toSequenceNr, max)(replayCallback)

    // iteratorImpl(pid, adjustedFrom, adjustedTo, permanent, deletedTo)(replayCallback)
  }

  private def akkaStreamImpl(
      pid: PersistenceId,
      fromSequenceNr: Long,
      toSequenceNr: Long,
      max: Long
  )(
      replayCallback: PersistentRepr => Unit
  ): Future[Unit] = {
    val (deletedTo, permanent) = deletions.getOrElse(pid, (0L, false))
    log.debug("deletedTo = {}, permanent = {}", deletedTo, permanent)

    val adjustedFrom =
      if (!permanent) Math.max(deletedTo + 1L, fromSequenceNr) else fromSequenceNr
    val adjustedNum = toSequenceNr - adjustedFrom + 1L
    val adjustedTo  = if (max < adjustedNum) adjustedFrom + max - 1L else toSequenceNr

    log.debug("adjustedFrom = {}, adjustedNum = {}, adjustedTo = {}", adjustedFrom, adjustedNum, adjustedTo)

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
  }

  private def iteratorImpl(
      pid: PersistenceId,
      fromSequenceNr: Long,
      toSequenceNr: Long,
      max: Long
  )(
      replayCallback: PersistentRepr => Unit
  ): Future[Unit] = {
    val (deletedTo, permanent) = deletions.getOrElse(pid, (0L, false))
    log.info("deletedTo = {}, permanent = {}", deletedTo, permanent)

    val adjustedFrom = if (permanent) Math.max(deletedTo + 1L, fromSequenceNr) else fromSequenceNr
    val adjustedNum  = toSequenceNr - adjustedFrom + 1L
    val adjustedTo   = if (max < adjustedNum) adjustedFrom + max - 1L else toSequenceNr

    log.info("adjustedFrom = {}, adjustedNum = {}, adjustedTo = {}", adjustedFrom, adjustedNum, adjustedTo)
    Future {
      new KafkaIterator(
        consumerSettings
          .withProperties(Map(ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_committed"))
          .createKafkaConsumer(),
        getTopic(pid),
        getPartition(pid),
        Math.max(adjustedFrom - 1L, 0),
        Duration.ofMillis(500)
      ).map { m => (m, serialization.deserialize(m.value(), classOf[Journal]).get) }
        .map {
          case (m, journal) =>
            (
              m,
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
          case (m, p) =>
            log.info(s"m.offset = ${m.offset()}, p.sequenceNr = ${p.sequenceNr}, deletedTo = $deletedTo")
            if (!permanent && p.sequenceNr <= deletedTo) {
              log.info("update: deleted = true")
              p.update(deleted = true)
            } else p
        }
        .foldLeft(adjustedFrom) {
          case (_, p) =>
            if (p.sequenceNr >= adjustedFrom && p.sequenceNr <= adjustedTo) {
              log.info("callback = {}", p)
              replayCallback(p)
            }
            p.sequenceNr
        }
    }
  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] =
    Future {
      val topic     = getTopic(PersistenceId(persistenceId))
      val partition = getPartition(PersistenceId(persistenceId))
      val tp        = new TopicPartition(topic, partition)
      val consumer  = consumerSettings.createKafkaConsumer()
      val result =
        try {
          consumer.assign(List(tp).asJava)
          // consumer.seek(tp, fromSequenceNr)
          consumer.endOffsets(List(tp).asJava).get(tp)
        } finally {
          consumer.close()
        }
      log.info("asyncReadHighestSequenceNr = {}", result)
      Math.max(result, 0)
    }

}
