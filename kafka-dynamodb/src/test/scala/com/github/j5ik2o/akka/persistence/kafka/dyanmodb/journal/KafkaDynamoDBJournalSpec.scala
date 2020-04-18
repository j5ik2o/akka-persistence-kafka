package com.github.j5ik2o.akka.persistence.kafka.dyanmodb.journal

import java.net.URI

import akka.pattern.ask
import akka.actor.ActorRef
import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalSpec
import akka.util.Timeout
import com.github.j5ik2o.akka.persistence.kafka.dyanmodb.journal.KafkaToDynamoDBProjector.{ Start, Stop }
import com.github.j5ik2o.akka.persistence.kafka.dyanmodb.utils.DynamoDBSpecSupport
import com.github.j5ik2o.reactive.aws.dynamodb.DynamoDbAsyncClient
import com.typesafe.config.ConfigFactory
import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.scalatest.BeforeAndAfterAll
import org.scalatest.time.{ Seconds, Span }
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.services.dynamodb.{ DynamoDbAsyncClient => JavaDynamoDbAsyncClient }

import scala.concurrent.duration._

class KafkaDynamoDBJournalSpec
    extends JournalSpec(
      ConfigFactory
        .parseString(
          s"""
             |akka.test.single-expect-default = 60s
             |j5ik2o.kafka-dynamodb-journal {
             |  query-batch-size = 1
             |  dynamo-db-client {
             |    endpoint = "http://127.0.0.1:${KafkaDynamoDBReplaySpec.dynamoDBPort}/"
             |  }
             |}
           """.stripMargin
        )
        .withFallback(ConfigFactory.load())
    )
    with BeforeAndAfterAll
    with DynamoDBSpecSupport {

  implicit val pc = PatienceConfig(timeout = Span(20, Seconds), interval = Span(1, Seconds))

  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = CapabilityFlag.on()

  override protected def supportsSerialization: CapabilityFlag = CapabilityFlag.on()

  override protected lazy val dynamoDBPort: Int = KafkaDynamoDBReplaySpec.dynamoDBPort

  val underlying: JavaDynamoDbAsyncClient = JavaDynamoDbAsyncClient
    .builder()
    .credentialsProvider(
      StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey))
    )
    .endpointOverride(URI.create(dynamoDBEndpoint))
    .build()

  override def pid = super.pid.replace("p", "test")

  override def asyncClient: DynamoDbAsyncClient = DynamoDbAsyncClient(underlying)

  var projectorRef: ActorRef = _

  implicit val kafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
    customBrokerProperties = Map(
      "num.partitions" -> "2"
    )
  )

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    createTable()
    EmbeddedKafka.start()
    projectorRef =
      system.actorOf(KafkaToDynamoDBProjector.props(system.settings.config.getConfig("j5ik2o.kafka-dynamodb-journal")))
    implicit val to = Timeout(5 seconds)
    (projectorRef ? Start(Seq("test"))).futureValue
    Thread.sleep(5 * 1000)
  }

  protected override def afterAll(): Unit = {
    projectorRef ! Stop
    EmbeddedKafka.stop()
    deleteTable()
    super.afterAll()
  }

}
