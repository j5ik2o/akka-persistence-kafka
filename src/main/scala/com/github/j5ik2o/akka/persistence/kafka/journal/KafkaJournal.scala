package com.github.j5ik2o.akka.persistence.kafka.journal

import java.time.Duration

import akka.actor.{ ActorLogging, ActorSystem }
import akka.kafka._
import akka.kafka.scaladsl.Producer
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
  type Deletions = Map[String, (Long, Boolean)]

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

  private val producer: KafkaProducer[String, Array[Byte]] = producerSettings.createKafkaProducer()
  private val consumer: KafkaConsumer[String, Array[Byte]] = consumerSettings.createKafkaConsumer()

  protected val serialization: Serialization = SerializationExtension(system)

  // Transient deletions only to pass TCK (persistent not supported)
  private var deletions: Deletions = Map.empty

  override def postStop(): Unit = {
    producer.close()
    consumer.close()
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
    log.info(s"asyncWriteMessages($atomicWrites): start")
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
    val future = Source
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
    future.onComplete { _ => log.debug(s"asyncWriteMessages($atomicWrites): finished") }
    future
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] =
    Future.successful(deleteMessagesTo(persistenceId, toSequenceNr, permanent = false))

  private def deleteMessagesTo(persistenceId: String, toSequenceNr: Long, permanent: Boolean): Unit =
    deletions = deletions + (persistenceId -> (toSequenceNr, permanent))

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      replayCallback: PersistentRepr => Unit
  ): Future[Unit] = {
    Future(replayMessages(PersistenceId(persistenceId), fromSequenceNr, toSequenceNr, max, deletions, replayCallback))
  }

  private def replayMessages(
      persistenceId: PersistenceId,
      fromSequenceNr: Long,
      toSequenceNr: Long,
      max: Long,
      deletions: Deletions,
      callback: PersistentRepr => Unit
  ): Unit = {
    log.info(
      s"persistenceId = $persistenceId, fromSequenceNr = $fromSequenceNr, toSequenceNr = $toSequenceNr, max = $max, deletions = $deletions"
    )
    val (deletedTo, permanent) = deletions.getOrElse(persistenceId.asString, (0L, false))
    log.info("deletedTo = {}, permanent = {}", deletedTo, permanent)

    val adjustedFrom = if (permanent) math.max(deletedTo + 1L, fromSequenceNr) else fromSequenceNr
    val adjustedNum  = toSequenceNr - adjustedFrom + 1L
    val adjustedTo   = if (max < adjustedNum) adjustedFrom + max - 1L else toSequenceNr

    log.info("adjustedFrom = {}, adjustedNum = {}, adjustedTo = {}", adjustedFrom, adjustedNum, adjustedTo)

    val iter = persistentIterator(persistenceId, adjustedFrom - 1L)
    iter
      .map { journal =>
        PersistentRepr(
          persistenceId = journal.persistenceId.asString,
          sequenceNr = journal.sequenceNumber.value,
          payload = journal.payload,
          deleted = journal.deleted,
          manifest = journal.manifest,
          writerUuid = journal.writerUuid
        ).withTimestamp(journal.timestamp)
      }
      .map(p => if (!permanent && p.sequenceNr <= deletedTo) p.update(deleted = true) else p)
      .foldLeft(adjustedFrom) {
        case (_, p) =>
          if (p.sequenceNr >= adjustedFrom && p.sequenceNr <= adjustedTo) {
            log.info("callback = {}", p)
            callback(p)
          }
          p.sequenceNr
      }

  }

  private def persistentIterator(persistenceId: PersistenceId, offset: Long): Iterator[Journal] = {
    new MessageIterator(
      consumerSettings
        .withProperties(Map(ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_committed"))
        .createKafkaConsumer(),
      getTopic(persistenceId),
      getPartition(persistenceId),
      Math.max(offset, 0),
      Duration.ofMillis(500)
    ).map { m => serialization.deserialize(m.value(), classOf[Journal]).get }
  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] =
    Future {
      val topic     = getTopic(PersistenceId(persistenceId))
      val partition = getPartition(PersistenceId(persistenceId))
      val tp        = new TopicPartition(topic, partition)
      consumer.assign(List(tp).asJava)
      consumer.seek(tp, fromSequenceNr)
      val result = consumer.endOffsets(List(tp).asJava).get(tp)
      Math.max(result, 0)
    }

}
