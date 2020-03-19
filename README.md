# akka-persistence-kafka

STATUS: WIP

akka-persistence-kafka writes journal and snapshot entries to Kafka.

## Features

### Supported versions:

- Scala: `2.12.x` or `2.13.x` 
- Akka: `2.6.x+`
- Java: `1.8+`
- [Alpakka-Kafka](https://github.com/akka/alpakka-kafka) 2.0.x+.
- Kafka 2.4.x+

## Installation

Add the following to your sbt build (2.12.x, 2.13.x):

```scala
resolvers += "Sonatype OSS Release Repository" at "https://oss.sonatype.org/content/repositories/releases/"

val version = "..."

libraryDependencies += Seq(
  "com.github.j5ik2o" %% "akka-persistence-kafka" % version
)
```

## Configration

The minimum necessary settings are as follows.

```hocon
# if use journal plugin
akka.persistence.journal.plugin = "j5ik2o.kafka-journal"
# if use snapshot plugin
akka.persistence.snapshot-store.plugin = "j5ik2o.kafka-snapshot-store"

j5ik2o {
  kafka-journal {
    bootstrap-servers = ["localhost:6001"]
    consumer {
      # Properties defined by org.apache.kafka.clients.consumer.ConsumerConfig
      # can be defined in this configuration section.
      kafka-clients {
        group.id = "test"
      }
    }
    # if need customize, default is persistence-id
    topic-resolver-class-name = "my.MyJournalTopicResolver"
    # if need customize, default is partion 0
    partition-resolver-class-name = "my.MyJournalPartitionResolver"
  }

  kafka-snapshot-store {
    bootstrap-servers = ["localhost:6001"]
    consumer {
      # Properties defined by org.apache.kafka.clients.consumer.ConsumerConfig
      # can be defined in this configuration section.
      kafka-clients {
        group.id = "test"
      }
    }
    # if need customize, default is persistence-id
    topic-resolver-class-name = "my.MySnapshotTopicResolver"
    # if need customize, default is partition 1
    partition-resolver-class-name = "my.MySnapshotlPartitionResolver"
  }
}
```


