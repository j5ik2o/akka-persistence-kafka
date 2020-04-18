package com.github.j5ik2o.akka.persistence.kafka.dyanmodb.journal

import java.net.URI
import java.util.UUID

import akka.actor.{ ActorRef, ActorSystem, Props }
import akka.pattern.ask
import akka.testkit.{ ImplicitSender, TestKit }
import akka.util.Timeout
import com.github.j5ik2o.akka.persistence.kafka.dyanmodb.journal.KafkaToDynamoDBProjector.{ Start, Stop }
import com.github.j5ik2o.akka.persistence.kafka.dyanmodb.utils.DynamoDBSpecSupport
import com.github.j5ik2o.akka.persistence.kafka.journal.TestActor
import com.github.j5ik2o.akka.persistence.kafka.journal.TestActor._
import com.github.j5ik2o.akka.persistence.kafka.utils.RandomPortUtil
import com.github.j5ik2o.reactive.aws.dynamodb.DynamoDbAsyncClient
import com.typesafe.config.ConfigFactory
import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Seconds, Span }
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.services.dynamodb.{ DynamoDbAsyncClient => JavaDynamoDbAsyncClient }

import scala.concurrent.duration._

object KafkaDynamoDBReplaySpec {
  val dynamoDBPort = RandomPortUtil.temporaryServerPort()
}

class KafkaDynamoDBReplaySpec
    extends TestKit(
      ActorSystem(
        "KafkaDynamoDBJournalSpec",
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
    )
    with AnyFreeSpecLike
    with BeforeAndAfterAll
    with ImplicitSender
    with Matchers
    with DynamoDBSpecSupport {

  implicit val pc = PatienceConfig(timeout = Span(20, Seconds), interval = Span(1, Seconds))

  implicit val kafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
    customBrokerProperties = Map(
      "num.partitions" -> "1"
    )
  )

  override protected lazy val dynamoDBPort: Int = KafkaDynamoDBReplaySpec.dynamoDBPort

  val underlying: JavaDynamoDbAsyncClient = JavaDynamoDbAsyncClient
    .builder()
    .credentialsProvider(
      StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey))
    )
    .endpointOverride(URI.create(dynamoDBEndpoint))
    .build()

  override def asyncClient: DynamoDbAsyncClient = DynamoDbAsyncClient(underlying)

  var projectorRef: ActorRef = _

  def getState(actorRef: ActorRef, id: UUID): State = {
    system.log.debug(s"actorRef = $actorRef")
    actorRef ! GetState(id, self)
    val reply = expectMsgType[GetStateReply]((5 * sys.env.getOrElse("SBT_TEST_TIME_FACTOR", "1").toInt) seconds)
    system.log.debug(s"reply = $reply")
    reply.state
  }

  "KafkaDynamoDBJournal" - {
    "create" in {
      val snapShotInterval = 5
      val maxActors        = 5
      val idWithNames = for { idx <- 1 to maxActors } yield (
        UUID.randomUUID(),
        "test-" + UUID.randomUUID().toString,
        idx
      )
      val modelName = "test"
      val actorRefs = idWithNames.map {
        case (id, name, idx) =>
          (system.actorOf(Props(new TestActor(modelName, id, snapShotInterval, false)), name), id, name, idx)
      }

      actorRefs.foreach {
        case (actorRef, id, name, idx) =>
          actorRef ! CreateState(id, name, idx, self)
          expectMsg((60 * sys.env.getOrElse("SBT_TEST_TIME_FACTOR", "1").toInt) seconds, CreateStateReply(id))
      }

      actorRefs.foreach {
        case (actorRef, _, _, _) =>
          watch(actorRef)
          system.stop(actorRef)
          expectTerminated(actorRef)
      }

      actorRefs.foreach {
        case (_, id, name, idx) =>
          val rebootRef1 = system.actorOf(Props(new TestActor(modelName, id, snapShotInterval)), name)
          val state      = getState(rebootRef1, id)
          state.amount shouldBe idx
      }
    }
  }

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    createTable()
    EmbeddedKafka.start()
    Thread.sleep(1000 * 3)
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
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

}
