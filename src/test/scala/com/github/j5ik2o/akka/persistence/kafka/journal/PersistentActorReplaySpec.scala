package com.github.j5ik2o.akka.persistence.kafka.journal

import java.util.UUID

import akka.actor.{ ActorRef, ActorSystem, DynamicAccess, ExtendedActorSystem, Props }
import akka.kafka.ConsumerSettings
import akka.testkit.{ ImplicitSender, TestKit }
import com.github.j5ik2o.akka.persistence.kafka.journal.TestActor._
import com.github.j5ik2o.akka.persistence.kafka.resolver.{ KafkaPartitionResolver, KafkaTopicResolver }
import com.typesafe.config.{ Config, ConfigFactory }
import net.ceedubs.ficus.Ficus._
import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.common.serialization.{ ByteArrayDeserializer, StringDeserializer }
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable
import scala.concurrent.duration._

class PersistentActorReplaySpec
    extends TestKit(
      ActorSystem(
        "KafkaTestActorSpec",
        ConfigFactory
          .parseString(
            """
          |akka.test.single-expect-default = 60s
          """.stripMargin
          )
          .withFallback(ConfigFactory.load())
      )
    )
    with AnyFreeSpecLike
    with ImplicitSender
    with BeforeAndAfterAll
    with Matchers {

  implicit val kafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
    customBrokerProperties = Map(
      "num.partitions" -> "128"
    )
  )

  var producer: Producer[String, Array[Byte]] = _
  val config: Config                          = system.settings.config.getConfig("j5ik2o.kafka-journal")
  val consumerConfig: Config                  = config.getConfig("consumer")
  val dynamicAccess: DynamicAccess            = system.asInstanceOf[ExtendedActorSystem].dynamicAccess
  var journalSequence: JournalSequence        = _

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

  "KafkaTestActor" - {
    "Will actors replay correctly after deleting journals older than the latest snapshot?" in {
      val snapShotInterval = 5
      val id               = UUID.randomUUID()
      val actorRef         = system.actorOf(Props(new TestActor(id, snapShotInterval)), "test")
      val name             = "test-1"
      val minAmount        = 1
      val maxAmount        = 10
      actorRef ! CreateState(id, name, 0, self)
      expectMsg((5 * sys.env.getOrElse("SBT_TEST_TIME_FACTOR", "1").toInt) seconds, CreateStateReply(id))

      for (index <- minAmount to maxAmount) {
        val updateAmount = index
        actorRef ! AddAmount(id, updateAmount, self)
        expectMsg((5 * sys.env.getOrElse("SBT_TEST_TIME_FACTOR", "1").toInt) seconds, AddAmountReply(id))
      }

      // Deleting journals older than the last snapshot
      val highestSeqNr = journalSequence.readHighestSequenceNr(PersistenceId(id.toString))
      val n            = highestSeqNr % snapShotInterval
      actorRef ! DeleteJournal(id, highestSeqNr - n, self)
      expectMsg(DeleteJournalReply(id))

      // Stop the current persistent actor.
      watch(actorRef)
      system.stop(actorRef)
      expectTerminated(actorRef)

      def getState(actorRef: ActorRef): State = {
        actorRef ! GetState(id, self)
        val reply = expectMsgType[GetStateReply]((5 * sys.env.getOrElse("SBT_TEST_TIME_FACTOR", "1").toInt) seconds)
        reply.state
      }
      // Restart the persistent actor with the same id
      val rebootRef = system.actorOf(Props(new TestActor(id, snapShotInterval)), "test")
      getState(rebootRef).amount shouldBe 55
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedKafka.start()
    val consumerSettings: ConsumerSettings[String, Array[Byte]] =
      ConsumerSettings(consumerConfig, new StringDeserializer, new ByteArrayDeserializer)
    journalSequence = new JournalSequence(
      consumerSettings,
      config.as[String]("topic-prefix"),
      journalTopicResolver,
      journalPartitionResolver
    )
  }

  override def afterAll(): Unit = {
    EmbeddedKafka.stop()
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

}
