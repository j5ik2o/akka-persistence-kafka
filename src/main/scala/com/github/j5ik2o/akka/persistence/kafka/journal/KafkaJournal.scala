package com.github.j5ik2o.akka.persistence.kafka.journal

import akka.actor.{ ActorLogging, ActorSystem }
import akka.kafka._
import akka.kafka.scaladsl.{ Consumer, Producer }
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{ AtomicWrite, PersistentRepr }
import akka.serialization.{ Serialization, SerializationExtension }
import akka.stream.scaladsl.{ Keep, Sink, Source }
import com.github.j5ik2o.akka.persistence.kafka.journal.KafkaJournalProtocol.{
  ReadHighestSequenceNr,
  ReadHighestSequenceNrFailure,
  ReadHighestSequenceNrSuccess
}
import com.github.j5ik2o.akka.persistence.kafka.resolver.{ KafkaPartitionResolver, KafkaTopicResolver }
import com.github.j5ik2o.akka.persistence.kafka.serialization.PersistentReprSerializer
import com.github.j5ik2o.akka.persistence.kafka.serialization.PersistentReprSerializer.JournalWithByteArray
import com.github.j5ik2o.akka.persistence.kafka.utils.ClassUtil
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import org.apache.kafka.clients.consumer.{ ConsumerConfig, Consumer => KafkaConsumer }
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{
  ByteArrayDeserializer,
  ByteArraySerializer,
  StringDeserializer,
  StringSerializer
}

import scala.collection.immutable
import scala.concurrent._
import scala.jdk.CollectionConverters._
import scala.util.{ Failure, Success, Try }

object KafkaJournal {
  type Deletions = Map[PersistenceId, (Long, Boolean)]
}

class KafkaJournal(config: Config) extends AsyncWriteJournal with ActorLogging {
  import KafkaJournal._
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val system: ActorSystem  = context.system

  private val bootstrapServers = config.as[List[String]]("bootstrap-servers").mkString(",")

  private val producerConfig = config.getConfig("producer")
  private val consumerConfig = config.getConfig("consumer")

  protected val serialization: Serialization = SerializationExtension(system)

  protected val serializer = new PersistentReprSerializer(serialization)

  protected val producerSettings: ProducerSettings[String, Array[Byte]] =
    ProducerSettings(producerConfig, new StringSerializer, new ByteArraySerializer)
      .withBootstrapServers(bootstrapServers)

  protected val consumerSettings: ConsumerSettings[String, Array[Byte]] =
    ConsumerSettings(consumerConfig, new StringDeserializer, new ByteArrayDeserializer)
      .withBootstrapServers(bootstrapServers)

  protected val journalTopicResolver: KafkaTopicResolver =
    config
      .getAs[String]("journal.topic-resolver-class-name")
      .map { name => ClassUtil.create(classOf[KafkaTopicResolver], name) }
      .getOrElse(KafkaTopicResolver.PersistenceId)

  protected val journalPartitionResolver: KafkaPartitionResolver =
    config
      .getAs[String]("journal.partition-resolver-class-name")
      .map { name => ClassUtil.create(classOf[KafkaPartitionResolver], name) }
      .getOrElse(KafkaPartitionResolver.PartitionZero)

  // Transient deletions only to pass TCK (persistent not supported)
  private var deletions: Deletions = Map.empty

  private def resolveTopic(persistenceId: PersistenceId): String =
    journalTopicResolver.resolve(persistenceId).asString

  private def resolvePartition(persistenceId: PersistenceId): Int =
    journalPartitionResolver.resolve(persistenceId).value

  override def receivePluginInternal: Receive = localReceive.orElse(super.receivePluginInternal)

  private def localReceive: Receive = {
    case ReadHighestSequenceNr(fromSequenceNr, persistenceId, _) =>
//      try {
//        val highest = readHighestSequenceNr(persistenceId, fromSequenceNr)
//        sender() ! ReadHighestSequenceNrSuccess(highest)
//      } catch {
//        case e: Exception => sender ! ReadHighestSequenceNrFailure(e)
//      }
  }

  override def asyncWriteMessages(atomicWrites: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = {
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
            resolveTopic(journal.persistenceId),
            resolvePartition(journal.persistenceId),
            journal.persistenceId.asString,
            byteArray
          )
        )
      } else
        ProducerMessage.multi(rowsToWrite.map {
          case (journal, byteArray) =>
            new ProducerRecord(
              resolveTopic(journal.persistenceId),
              resolvePartition(journal.persistenceId),
              journal.persistenceId.asString,
              byteArray
            )
        }.asJava) // asJava method, Must not modify for 2.12
    val future: Future[immutable.Seq[Try[Unit]]] = Source
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
              new TopicPartition(resolveTopic(pid), resolvePartition(pid)) -> Math.max(adjustedFrom - 1, 0)
            )
          )
          .flatMapConcat { record =>
            serialization
              .deserialize(record.value(), classOf[Journal])
              .fold(Source.failed, journal => Source.single((record, journal)))
          }
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
                s"record.offset = ${record.offset()}, persistentRepr.sequenceNr = ${persistentRepr.sequenceNr}, deletedTo = $deletedTo"
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
      val journalSequence = new JournalSequence(consumerSettings, journalTopicResolver, journalPartitionResolver)
      try {
        journalSequence.readHighestSequenceNr(PersistenceId(persistenceId), Some(fromSequenceNr))
      } finally {
        journalSequence.close()
      }
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

class JournalSequence(
    consumerSettings: ConsumerSettings[String, Array[Byte]],
    journalTopicResolver: KafkaTopicResolver,
    journalPartitionResolver: KafkaPartitionResolver
) {
  private val consumer: KafkaConsumer[String, Array[Byte]] = consumerSettings.createKafkaConsumer()

  def close(): Unit = consumer.close()

  def readHighestSequenceNr(
      persistenceId: PersistenceId,
      fromSequenceNr: Option[Long] = None
  ): Long = {
    val tp =
      new TopicPartition(
        journalTopicResolver.resolve(persistenceId).asString,
        journalPartitionResolver.resolve(persistenceId).value
      )
    consumer.assign(List(tp).asJava)
    fromSequenceNr.foreach(consumer.seek(tp, _))
    val result =
      consumer.endOffsets(List(tp).asJava).get(tp)
    Math.max(result, 0)
  }
}
