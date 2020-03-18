package com.github.j5ik2o.akka.persistence.kafka.snapshot

import akka.actor.ActorSystem
import akka.kafka.scaladsl.{ Consumer, Producer }
import akka.kafka.{ ConsumerSettings, ProducerSettings, Subscriptions }
import akka.persistence.snapshot.SnapshotStore
import akka.persistence.{ SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria }
import akka.serialization.SerializationExtension
import akka.stream.scaladsl.{ Sink, Source }
import com.github.j5ik2o.akka.persistence.kafka.journal.{ JournalSequence, PersistenceId }
import com.github.j5ik2o.akka.persistence.kafka.resolver.{ KafkaPartitionResolver, KafkaTopicResolver }
import com.github.j5ik2o.akka.persistence.kafka.utils.ClassUtil
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{
  ByteArrayDeserializer,
  ByteArraySerializer,
  StringDeserializer,
  StringSerializer
}

import scala.concurrent.Future

object KafkaSnapshotStore {

  type RangeDeletions  = Map[String, SnapshotSelectionCriteria]
  type SingleDeletions = Map[String, List[SnapshotMetadata]]

}

class KafkaSnapshotStore(config: Config) extends SnapshotStore {
  import KafkaSnapshotStore._

  import context.dispatcher
  implicit val system: ActorSystem = context.system

  private val bootstrapServers = config.as[List[String]]("bootstrap-servers").mkString(",")

  private val producerConfig = config.getConfig("producer")
  private val consumerConfig = config.getConfig("consumer")

  val ignoreOrphan = false

  protected val producerSettings: ProducerSettings[String, Array[Byte]] =
    ProducerSettings(producerConfig, new StringSerializer, new ByteArraySerializer)
      .withBootstrapServers(bootstrapServers)

  protected val consumerSettings: ConsumerSettings[String, Array[Byte]] =
    ConsumerSettings(consumerConfig, new StringDeserializer, new ByteArrayDeserializer)
      .withBootstrapServers(bootstrapServers)

  private var rangeDeletions: RangeDeletions   = Map.empty.withDefaultValue(SnapshotSelectionCriteria.None)
  private var singleDeletions: SingleDeletions = Map.empty.withDefaultValue(Nil)

  private val serialization = SerializationExtension(context.system)

  protected val journalTopicResolver: KafkaTopicResolver =
    config
      .getAs[String]("snapshot.topic-resolver-class-name")
      .map { name => ClassUtil.create(classOf[KafkaTopicResolver], name) }
      .getOrElse(KafkaTopicResolver.PersistenceId)

  protected val journalPartitionResolver: KafkaPartitionResolver =
    config
      .getAs[String]("snapshot.partition-resolver-class-name")
      .map { name => ClassUtil.create(classOf[KafkaPartitionResolver], name) }
      .getOrElse(KafkaPartitionResolver.PartitionOne)

  override def postStop(): Unit = {
    journalSequence.close()
    super.postStop()
  }

  protected def resolveTopic(persistenceId: PersistenceId): String =
    journalTopicResolver.resolve(persistenceId).asString

  protected def resolvePartition(persistenceId: PersistenceId): Int =
    journalPartitionResolver.resolve(persistenceId).value

  protected val journalSequence = new JournalSequence(consumerSettings, journalTopicResolver, journalPartitionResolver)

  override def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    log.debug(s"saveAsync($metadata, $snapshot): start")
    Source
      .single(Snapshot(metadata, snapshot))
      .flatMapConcat { snapshot =>
        serialization.serialize(snapshot).fold(Source.failed, bytes => Source.single(bytes))
      }
      .map { snapshotBytes =>
        new ProducerRecord[String, Array[Byte]](
          resolveTopic(PersistenceId(metadata.persistenceId)),
          resolvePartition(PersistenceId(metadata.persistenceId)),
          metadata.persistenceId,
          snapshotBytes
        )
      }
      .runWith(Producer.plainSink(producerSettings))
      .map(_ => ())
  }

  override def loadAsync(
      persistenceId: String,
      criteria: SnapshotSelectionCriteria
  ): Future[Option[SelectedSnapshot]] = {
    val singleDeletions = this.singleDeletions
    val rangeDeletions  = this.rangeDeletions

    for {
      highest <- if (ignoreOrphan) Future {
        journalSequence.readHighestSequenceNr(PersistenceId(persistenceId))
      }
      else Future.successful(Long.MaxValue)
      adjusted = if (ignoreOrphan &&
                     highest < criteria.maxSequenceNr &&
                     highest > 0L) criteria.copy(maxSequenceNr = highest)
      else criteria
      // highest  <- Future.successful(Long.MaxValue)
      // adjusted = criteria
      snapshot <- {
        // if timestamp was unset on delete, matches only on same sequence nr
        def matcher(snapshot: Snapshot): Boolean =
          snapshot.matches(adjusted) &&
          !snapshot.matches(rangeDeletions(persistenceId)) &&
          !singleDeletions(persistenceId).contains(snapshot.metadata) &&
          !singleDeletions(persistenceId)
            .filter(_.timestamp == 0L)
            .map(_.sequenceNr)
            .contains(snapshot.metadata.sequenceNr)
        load(PersistenceId(persistenceId), matcher).map(sOpt =>
          sOpt.map { s => SelectedSnapshot(s.metadata, s.payload) }
        )
      }
    } yield snapshot
  }

  private def load(persistenceId: PersistenceId, matcher: Snapshot => Boolean): Future[Option[Snapshot]] = {
    val offset = journalSequence.readHighestSequenceNr(persistenceId)
    def load0(persistenceId: PersistenceId, offset: Long): Future[Option[Snapshot]] =
      if (offset < 0) Future.successful(None)
      else {
        snapshot(persistenceId, offset).flatMap { s =>
          if (matcher(s)) Future.successful(Some(s)) else load0(persistenceId, offset - 1)
        }
      }
    load0(persistenceId, offset - 1)
  }

  private def snapshot(persistenceId: PersistenceId, offset: Long): Future[Snapshot] = {
    Consumer
      .plainSource(
        consumerSettings,
        Subscriptions.assignmentWithOffset(
          new TopicPartition(
            journalTopicResolver.resolve(persistenceId).asString,
            journalPartitionResolver.resolve(persistenceId).value
          ) -> offset
        )
      )
      .take(1)
      .map { record => serialization.deserialize(record.value(), classOf[Snapshot]).get }
      .runWith(Sink.head)
  }

  override def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = Future.successful {
    singleDeletions.get(metadata.persistenceId) match {
      case Some(dels) => singleDeletions += (metadata.persistenceId -> (metadata :: dels))
      case None       => singleDeletions += (metadata.persistenceId -> List(metadata))
    }
  }

  override def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] =
    Future.successful {
      rangeDeletions += (persistenceId -> criteria)
    }
}
