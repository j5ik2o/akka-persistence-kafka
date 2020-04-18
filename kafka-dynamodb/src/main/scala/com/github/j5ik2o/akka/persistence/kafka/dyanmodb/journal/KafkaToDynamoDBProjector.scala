package com.github.j5ik2o.akka.persistence.kafka.dyanmodb.journal

import akka.actor.{ Actor, ActorLogging, ExtendedActorSystem, Props }
import akka.kafka.ConsumerMessage.CommittableOffset
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{ Committer, Consumer }
import akka.kafka.{ CommitterSettings, ConsumerMessage, ConsumerSettings, Subscriptions }
import akka.pattern.pipe
import akka.serialization.{ Serialization, SerializationExtension }
import akka.stream.{ ActorMaterializer, Attributes }
import akka.stream.scaladsl.{ Flow, Keep, Sink, Source }
import akka.{ Done, NotUsed }
import com.github.j5ik2o.akka.persistence.dynamodb.config.JournalPluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.{ WriteJournalDao, WriteJournalDaoImpl }
import com.github.j5ik2o.akka.persistence.dynamodb.journal.{ JournalRow => DynamoDBJournalRow }
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.serialization.{
  ByteArrayJournalSerializer,
  FlowPersistentReprSerializer
}
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ DynamoDbClientBuilderUtils, HttpClientBuilderUtils }
import com.github.j5ik2o.akka.persistence.kafka.dyanmodb.journal.KafkaToDynamoDBProjector.{
  Start,
  Started,
  Stop,
  Stopped,
  Tick
}
import com.github.j5ik2o.akka.persistence.kafka.journal.JournalRow
import com.github.j5ik2o.akka.persistence.kafka.serialization.JournalAkkaSerializer
import com.github.j5ik2o.reactive.aws.dynamodb.DynamoDbAsyncClient
import com.typesafe.config.Config
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ ByteArrayDeserializer, StringDeserializer }
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.dynamodb.{
  DynamoDbAsyncClient => JavaDynamoDbAsyncClient,
  DynamoDbAsyncClientBuilder => JavaDynamoDbAsyncClientBuilder
}

import scala.concurrent.ExecutionContext

object KafkaToDynamoDBProjector {
  def props(config: Config): Props = Props(new KafkaToDynamoDBProjector(config))
  case class Start(topics: Seq[String])
  case object Started
  case object Stop
  case object Stopped
  case object Tick
}

class KafkaToDynamoDBProjector(config: Config) extends Actor with ActorLogging {
  implicit val system = context.system
  import system.dispatcher
  private val consumerConfig = config.getConfig("consumer")

  protected val consumerSettings: ConsumerSettings[String, Array[Byte]] =
    ConsumerSettings(consumerConfig, new StringDeserializer, new ByteArrayDeserializer)

  val parallelism: Int = config.getInt("parallelism")

  protected val pluginConfig: JournalPluginConfig = JournalPluginConfig.fromConfig(config)

  protected val httpClientBuilder: NettyNioAsyncHttpClient.Builder =
    HttpClientBuilderUtils.setup(pluginConfig.clientConfig)

  protected val dynamoDbAsyncClientBuilder: JavaDynamoDbAsyncClientBuilder =
    DynamoDbClientBuilderUtils.setup(pluginConfig.clientConfig, httpClientBuilder.build())

  protected val javaClient: JavaDynamoDbAsyncClient = dynamoDbAsyncClientBuilder.build()
  protected val asyncClient: DynamoDbAsyncClient    = DynamoDbAsyncClient(javaClient)

  protected val metricsReporter: MetricsReporter = MetricsReporter.create(pluginConfig.metricsReporterClassName)

  protected val serialization: Serialization = SerializationExtension(system)

  protected val serializer: FlowPersistentReprSerializer[DynamoDBJournalRow] =
    new ByteArrayJournalSerializer(serialization, pluginConfig.tagSeparator)

  protected val journalDao: WriteJournalDao =
    new WriteJournalDaoImpl(asyncClient, serialization, pluginConfig, serializer, metricsReporter)

  private val committerSettings: CommitterSettings = CommitterSettings(config.getConfig("committer-settings"))
  private var control: DrainingControl[Done]       = _

  private val journalAkkaSerializer: JournalAkkaSerializer = new JournalAkkaSerializer(
    system.asInstanceOf[ExtendedActorSystem]
  )

  private def projectionFlow(topicPartition: TopicPartition)(
      implicit ec: ExecutionContext
  ): Flow[ConsumerMessage.CommittableMessage[String, Array[Byte]], CommittableOffset, NotUsed] = {
    log.debug(s"topicPartition = $topicPartition")
    Flow[ConsumerMessage.CommittableMessage[String, Array[Byte]]]
      .mapAsync(1) { message =>
        log.debug(s"message = $message")
        journalAkkaSerializer
          .fromBinaryAsync(message.record.value(), "")
          .map(_.asInstanceOf[JournalRow])
          .map { journalRow =>
            log.debug(s"journalRow = $journalRow")
            (journalRow, message)
          }
      }
      .map {
        case (journalRow, message) =>
          val event = journalRow.persistentRepr
          log.debug(s"event =  $event")
          (serializer.serialize(journalRow.persistentRepr).getOrElse(throw new Exception()), message)
      }
      .batch(100, Seq(_))((v, x) => v :+ x)
      .flatMapConcat { seq =>
        val batch = seq.map(_._1)
        journalDao.putMessages(batch).map { _ => seq.map(_._2).last.committableOffset }
      }
  }

  private def stream(topics: String*): Unit = {
    control = Consumer
      .committablePartitionedSource(consumerSettings, Subscriptions.topics(topics: _*))
      .log("kafka source")
      .mapAsyncUnordered(parallelism) {
        case (topicPartition, source) =>
          system.log.debug(s">>> topicPartition = $topicPartition, source = $source")
          source
            .log("sub source")
            .via(projectionFlow(topicPartition))
            .log("projectionFlow")
            .via(Committer.flow(committerSettings))
            .log("committerFlow")
            .addAttributes(
              Attributes.logLevels(
                onElement = Attributes.LogLevels.Info,
                onFinish = Attributes.LogLevels.Info,
                onFailure = Attributes.LogLevels.Error
              )
            )
            .runWith(Sink.ignore)
      }
      .log("mapAsyncUnordered")
      .toMat(Sink.ignore)(Keep.both)
      .mapMaterializedValue(DrainingControl.apply)
      .addAttributes(
        Attributes.logLevels(
          onElement = Attributes.LogLevels.Info,
          onFinish = Attributes.LogLevels.Info,
          onFailure = Attributes.LogLevels.Error
        )
      )
      .run()
  }

  override def receive: Receive = {
    case Done =>
      context.stop(self)
    case Stop =>
      control.drainAndShutdown().pipeTo(self)
      sender() ! Stopped
    case Start(topics) =>
      log.info(">>>>>>>>>>")
      stream(topics: _*)
      sender() ! Started
  }

}
