package com.github.j5ik2o.akka.persistence.kafka.journal

import java.nio.charset.StandardCharsets
import java.util

import akka.actor.{ ActorLogging, ActorSystem, DynamicAccess, ExtendedActorSystem }
import akka.kafka._
import akka.kafka.scaladsl.{ Consumer, Producer }
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{ AtomicWrite, PersistentRepr }
import akka.serialization.{ Serialization, SerializationExtension }
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Keep, Sink, Source }
import com.github.j5ik2o.akka.persistence.kafka.resolver.{ KafkaPartitionResolver, KafkaTopicResolver }
import com.github.j5ik2o.akka.persistence.kafka.serialization.{ JournalAkkaSerializer, PersistentReprSerializer }
import com.github.j5ik2o.akka.persistence.kafka.serialization.PersistentReprSerializer.JournalWithByteArray
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.apache.kafka.clients.admin.{ AdminClient, RecordsToDelete }
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.{ RecordHeader, RecordHeaders }
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
  final val PersistenceIdHeaderKey = "persistenceId"
}

class KafkaJournal(config: Config) extends AsyncWriteJournal with ActorLogging {
  import KafkaJournal._

  implicit val ec: ExecutionContext   = context.dispatcher
  implicit val system: ActorSystem    = context.system
  implicit val mat: ActorMaterializer = ActorMaterializer()

  private val producerConfig = config.getConfig("producer")
  private val consumerConfig = config.getConfig("consumer")

  protected val serialization: Serialization = SerializationExtension(system)

  protected val serializer = new PersistentReprSerializer(serialization)

  protected val producerSettings: ProducerSettings[String, Array[Byte]] =
    ProducerSettings(producerConfig, new StringSerializer, new ByteArraySerializer)
  protected val producer = producerSettings.createKafkaProducer()

  protected val consumerSettings: ConsumerSettings[String, Array[Byte]] =
    ConsumerSettings(consumerConfig, new StringDeserializer, new ByteArrayDeserializer)

  protected val adminClient: AdminClient = AdminClient.create(producerSettings.getProperties)

  private val dynamicAccess: DynamicAccess = system.asInstanceOf[ExtendedActorSystem].dynamicAccess

  protected val journalTopicResolver: KafkaTopicResolver = {
    val className = config
      .as[String]("topic-resolver-class-name")
    dynamicAccess
      .createInstanceFor[KafkaTopicResolver](
        className,
        immutable.Seq(classOf[Config] -> config)
      )
      .getOrElse(throw new ClassNotFoundException(className))
  }

  protected val journalPartitionResolver: KafkaPartitionResolver = {
    val className = config
      .as[String]("partition-resolver-class-name")
    dynamicAccess
      .createInstanceFor[KafkaPartitionResolver](
        className,
        immutable.Seq(classOf[Config] -> config)
      )
      .getOrElse(throw new ClassNotFoundException(className))
  }

  protected def resolveTopic(persistenceId: PersistenceId): String =
    config.as[String]("topic-prefix") + journalTopicResolver.resolve(persistenceId).asString

  protected def resolvePartitionSize(persistenceId: PersistenceId): Int = {
    val topic = resolveTopic(persistenceId)
    producer.partitionsFor(topic).asScala.size
  }

  protected def resolvePartition(persistenceId: PersistenceId): Int = {
    journalPartitionResolver.resolve(resolvePartitionSize(persistenceId), persistenceId).value
  }

  protected val journalSequence = new JournalSequence(
    consumerSettings,
    config.as[String]("topic-prefix"),
    journalTopicResolver,
    journalPartitionResolver
  )

  override def postStop(): Unit = {
    producer.close()
    adminClient.close()
    super.postStop()
  }

  override def asyncWriteMessages(atomicWrites: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = {
    log.debug(s"asyncWriteMessages($atomicWrites): start")
    val serializedTries: Seq[Future[Seq[(JournalRow, Array[Byte])]]] = serializer.serialize(atomicWrites)
    val rowsToWriteFuture: Future[Seq[(JournalRow, Array[Byte])]] =
      serializedTries.foldLeft(Future.successful(Seq.empty[JournalWithByteArray])) { (result, element) =>
        for {
          r <- result
          e <- element.recover { case _ => Seq.empty }
        } yield r ++ e
      }
    def resultWhenWriteComplete: Future[Vector[Try[Unit]]] = {
      val result = serializedTries
        .foldLeft(Future.successful(Seq.empty[Boolean])) { (result, element) =>
          for {
            r <- result
            e <- element.map(_ => true).recover { case _ => false }
          } yield r :+ e
        }
        .map(_.forall(identity))
        .flatMap { result =>
          if (result)
            Future.successful(Vector.empty)
          else {
            val result = serializedTries
              .foldLeft(Future.successful(Vector.empty[Try[Unit]])) { (result, element) =>
                for {
                  r <- result
                  e <- element.map(_ => Success(())).recover { case ex => Failure(ex) }
                } yield r :+ e
              }
            result
          }

        }
      result
    }
    val future = rowsToWriteFuture.flatMap { rowsToWrite: Seq[JournalWithByteArray] =>
      val messages =
        if (rowsToWrite.size == 1) {
          val journal   = rowsToWrite.head._1
          val byteArray = rowsToWrite.head._2
          ProducerMessage.single(
            new ProducerRecord(
              resolveTopic(journal.persistenceId),
              resolvePartition(journal.persistenceId),
              journal.persistenceId.asString,
              byteArray,
              createHeaders(journal)
            )
          )
        } else
          ProducerMessage.multi(rowsToWrite.map {
            case (journal, byteArray) =>
              new ProducerRecord(
                resolveTopic(journal.persistenceId),
                resolvePartition(journal.persistenceId),
                journal.persistenceId.asString,
                byteArray,
                createHeaders(journal)
              )
          }.asJava) // asJava method, Must not modify for 2.12
      val future: Future[immutable.Seq[Try[Unit]]] = Source
        .single(messages)
        .via(Producer.flexiFlow(producerSettings))
        .mapAsync(1) {
          case ProducerMessage.Result(_, _) =>
            resultWhenWriteComplete
          case ProducerMessage.MultiResult(_, _) =>
            resultWhenWriteComplete
          case ProducerMessage.PassThroughResult(_) =>
            Future.successful(Vector.empty)
        }
        .mapConcat(identity)
        .toMat(Sink.seq)(Keep.right)
        .run()
      future
    }
    future.onComplete {
      case Success(value) =>
        log.debug(s"asyncWriteMessages($atomicWrites): finished, succeeded($value)")
      case Failure(ex) =>
        log.error(ex, s"asyncWriteMessages($atomicWrites): finished, failed($ex)")
    }
    future
  }

  private def createHeaders(journal: JournalRow): util.List[Header] = {
    val header: Header =
      new RecordHeader(PersistenceIdHeaderKey, journal.persistenceId.asString.getBytes(StandardCharsets.UTF_8))
    val headers: util.List[Header] = List(header).asJava
    headers
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    log.debug(s"asyncDeleteMessagesTo($persistenceId, $toSequenceNr): start")
    val future = for {
      to <- if (toSequenceNr == Long.MaxValue)
        journalSequence.readHighestSequenceNrAsync(PersistenceId(persistenceId))
      else
        Future.successful(toSequenceNr)
      _ <- Future {
        adminClient
          .deleteRecords(
            Map(
              new TopicPartition(
                resolveTopic(PersistenceId(persistenceId)),
                resolvePartition(PersistenceId(persistenceId))
              ) -> RecordsToDelete
                .beforeOffset(to)
            ).asJava
          )
          .all()
          .get()
      }
    } yield ()
    future.onComplete {
      case Success(value) =>
        log.debug(s"asyncDeleteMessagesTo($persistenceId, $toSequenceNr): finished, succeeded($value)")
      case Failure(ex) =>
        log.error(ex, s"asyncDeleteMessagesTo($persistenceId, $toSequenceNr): finished, failed($ex)")
    }
    future
  }

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      replayCallback: PersistentRepr => Unit
  ): Future[Unit] = {
    log.debug(
      s"asyncReplayMessages($persistenceId, $fromSequenceNr, $toSequenceNr, $max): start"
    )
    val pid = PersistenceId(persistenceId)
    val future = journalSequence.readLowestSequenceNrAsync(pid).flatMap { deletedTo =>
      log.debug(s"deletedTo = $deletedTo")

      val adjustedFrom = Math.max(deletedTo + 1L, fromSequenceNr)
      val adjustedNum  = toSequenceNr - adjustedFrom + 1L
      val adjustedTo   = if (max < adjustedNum) adjustedFrom + max - 1L else toSequenceNr

      log.debug("adjustedFrom = {}, adjustedNum = {}, adjustedTo = {}", adjustedFrom, adjustedNum, adjustedTo)

      if (max == 0 || adjustedFrom > adjustedTo || deletedTo == Long.MaxValue)
        Future.successful(())
      else {
        val className = classOf[JournalAkkaSerializer].getName
        val serializer =
          serialization
            .serializerOf(className)
            .map(_.asInstanceOf[JournalAkkaSerializer])
            .getOrElse(throw new ClassNotFoundException(className))
        Consumer
          .plainSource(
            consumerSettings.withProperties(Map(ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_committed")),
            Subscriptions.assignmentWithOffset(
              new TopicPartition(resolveTopic(pid), resolvePartition(pid)) -> Math.max(adjustedFrom - 1, 0)
            )
          )
          .take(adjustedNum)
          .filter { record =>
            val recordedPid =
              new String(record.headers().lastHeader(PersistenceIdHeaderKey).value(), StandardCharsets.UTF_8)
            val result = recordedPid == persistenceId
            log.debug(s"[same = $result], recordedPid = $recordedPid, journalRow.pid = $persistenceId")
            result
          }
          .mapAsync(1) { record =>
            serializer
              .fromBinaryAsync(record.value(), classOf[JournalRow].getName)
              .map(journal => (record, journal.asInstanceOf[JournalRow]))
          }
          .map {
            case (record, journal) =>
              log.debug(s"record = $record, journal = $journal")
              (
                record,
                journal.persistentRepr
              )
          }
          .map {
            case (record, persistentRepr) =>
              log.debug(
                s"record.offset = ${record.offset()}, persistentRepr.sequenceNr = ${persistentRepr.sequenceNr}, deletedTo = $deletedTo"
              )
              if (persistentRepr.sequenceNr <= deletedTo) {
                log.debug("update: deleted = true")
                persistentRepr.update(deleted = true)
              } else persistentRepr
          }
          .runWith(Sink.foreach { persistentRepr =>
            if (adjustedFrom <= persistentRepr.sequenceNr && persistentRepr.sequenceNr <= adjustedTo) {
              log.debug("callback = {}", persistentRepr)
              replayCallback(persistentRepr)
            }
          })
          .map(_ => ())
      }
    }
    future.onComplete {
      case Success(value) =>
        log.debug(
          s"asyncReplayMessages($persistenceId, $fromSequenceNr, $toSequenceNr, $max): finished, succeeded($value)"
        )
      case Failure(ex) =>
        log.error(
          ex,
          s"asyncReplayMessages($persistenceId, $fromSequenceNr, $toSequenceNr, $max): finished, failed($ex)"
        )
    }
    future
  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    log.debug("asyncReadHighestSequenceNr({},{}): start", persistenceId, fromSequenceNr)
    val future =
      journalSequence.readHighestSequenceNrAsync(PersistenceId(persistenceId), Some(fromSequenceNr))
    future.onComplete {
      case Success(value) =>
        log.debug("asyncReadHighestSequenceNr({},{}): finished, succeeded({})", persistenceId, fromSequenceNr, value)
      case Failure(ex) =>
        log.error(ex, "asyncReadHighestSequenceNr({},{}): finished, failed({})", persistenceId, fromSequenceNr, ex)
    }
    future
  }

}
