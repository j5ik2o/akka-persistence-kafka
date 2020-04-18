package com.github.j5ik2o.akka.persistence.kafka.dyanmodb.journal

import akka.Done
import akka.pattern.pipe
import akka.persistence.PersistentRepr
import akka.stream.scaladsl.Sink
import com.github.j5ik2o.akka.persistence.dynamodb.config.JournalPluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.journal.DynamoDBJournal.InPlaceUpdateEvent
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.{ WriteJournalDao, WriteJournalDaoImpl }
import com.github.j5ik2o.akka.persistence.dynamodb.journal.{ JournalRow, PersistenceId, SequenceNumber }
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.serialization.{
  ByteArrayJournalSerializer,
  FlowPersistentReprSerializer
}
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ DynamoDbClientBuilderUtils, HttpClientBuilderUtils }
import com.github.j5ik2o.akka.persistence.kafka.journal.KafkaJournal
import com.github.j5ik2o.reactive.aws.dynamodb.DynamoDbAsyncClient
import com.typesafe.config.Config
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.dynamodb.{
  DynamoDbAsyncClient => JavaDynamoDbAsyncClient,
  DynamoDbAsyncClientBuilder => JavaDynamoDbAsyncClientBuilder
}

import scala.concurrent.Future

class KafkaDynamoDBJournal(config: Config) extends KafkaJournal(config) {
  protected val pluginConfig: JournalPluginConfig = JournalPluginConfig.fromConfig(config)

  protected val httpClientBuilder: NettyNioAsyncHttpClient.Builder =
    HttpClientBuilderUtils.setup(pluginConfig.clientConfig)

  protected val dynamoDbAsyncClientBuilder: JavaDynamoDbAsyncClientBuilder =
    DynamoDbClientBuilderUtils.setup(pluginConfig.clientConfig, httpClientBuilder.build())

  protected val javaClient: JavaDynamoDbAsyncClient = dynamoDbAsyncClientBuilder.build()
  protected val asyncClient: DynamoDbAsyncClient    = DynamoDbAsyncClient(javaClient)

  protected val metricsReporter: MetricsReporter = MetricsReporter.create(pluginConfig.metricsReporterClassName)

  private val dynamodbJournalSerializer
      : FlowPersistentReprSerializer[com.github.j5ik2o.akka.persistence.dynamodb.journal.JournalRow] =
    new ByteArrayJournalSerializer(serialization, pluginConfig.tagSeparator)

  protected val journalDao: WriteJournalDaoImpl =
    new WriteJournalDaoImpl(asyncClient, serialization, pluginConfig, dynamodbJournalSerializer, metricsReporter)

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      recoveryCallback: PersistentRepr => Unit
  ): Future[Unit] = {
    log.debug(s"asyncReplayMessages($persistenceId, $fromSequenceNr, $toSequenceNr, $max): start")
    val startTime = System.nanoTime()
    val future = journalDao
      .getMessagesWithBatch(persistenceId, fromSequenceNr, toSequenceNr, pluginConfig.replayBatchSize, None)
      .take(max)
      .mapAsync(1)(deserializedRepr => Future.fromTry(deserializedRepr))
      .runForeach(recoveryCallback)
      .map(_ => ())
    future.onComplete { result =>
      metricsReporter.setAsyncReplayMessagesCallDuration(System.nanoTime() - startTime)
      if (result.isSuccess)
        metricsReporter.incrementAsyncReplayMessagesCallCounter()
      else
        metricsReporter.incrementAsyncReplayMessagesCallErrorCounter()
      log.debug(s"asyncReplayMessages($persistenceId, $fromSequenceNr, $toSequenceNr, $max): finished")
    }
    future
  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    log.debug(s"asyncReadHighestSequenceNr($persistenceId, $fromSequenceNr): start")
    val startTime = System.nanoTime()
    val result =
      journalDao.highestSequenceNr(PersistenceId(persistenceId), SequenceNumber(fromSequenceNr)).runWith(Sink.head)
    result.onComplete { result =>
      metricsReporter.setAsyncReadHighestSequenceNrCallDuration(System.nanoTime() - startTime)
      if (result.isSuccess)
        metricsReporter.incrementAsyncReadHighestSequenceNrCallCounter()
      else
        metricsReporter.incrementAsyncReadHighestSequenceNrCallErrorCounter()
      log.debug(s"asyncReadHighestSequenceNr($persistenceId, $fromSequenceNr): finished($result)")
    }
    result
  }

  override def receivePluginInternal: Receive = {
    case msg @ InPlaceUpdateEvent(pid, seq, message) =>
      log.debug(s"receivePluginInternal:$msg: start")
      asyncUpdateEvent(pid, seq, message).pipeTo(sender())
      log.debug(s"receivePluginInternal:$msg: finished")
  }

  private def asyncUpdateEvent(persistenceId: String, sequenceNumber: Long, message: AnyRef): Future[Done] = {
    log.debug(s"asyncUpdateEvent($persistenceId, $sequenceNumber, $message): start")
    val write = PersistentRepr(message, sequenceNumber, persistenceId)
    val serializedRow: JournalRow = dynamodbJournalSerializer.serialize(write) match {
      case Right(row) => row
      case Left(_) =>
        throw new IllegalArgumentException(
          s"Failed to serialize ${write.getClass} for update of [$persistenceId] @ [$sequenceNumber]"
        )
    }
    val future = journalDao.updateMessage(serializedRow).runWith(Sink.ignore)
    future.onComplete { _ => log.debug(s"asyncUpdateEvent($persistenceId, $sequenceNumber, $message): finished") }
    future
  }
}
