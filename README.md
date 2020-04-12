# akka-persistence-kafka

[![CircleCI](https://circleci.com/gh/j5ik2o/akka-persistence-kafka/tree/master.svg?style=shield&circle-token=c809688daf71f6ae582dd2d58cb5518401498373)](https://circleci.com/gh/j5ik2o/akka-persistence-kafka/tree/master)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.j5ik2o/akka-persistence-kafka_2.12/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.j5ik2o/akka-persistence-kafka_2.12)
[![Scaladoc](http://javadoc-badge.appspot.com/com.github.j5ik2o/akka-persistence-kafka_2.12.svg?label=scaladoc)](http://javadoc-badge.appspot.com/com.github.j5ik2o/akka-persistence-kafka_2.12/com/github/j5ik2o/akka/persistence/kafka/index.html?javadocio=true)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

akka-persistence-kafka writes journal and snapshot entries to Kafka.

## Features

### Supported versions:

- Java: `1.8+`
- Scala: `2.11.x` or `2.12.x` or `2.13.x` 
- Akka: `2.5.x`(Scala 2.11 only), `2.6.x`(Scala 2.12, 2.13)
- Kafka `2.4.x+`
- [Alpakka-Kafka](https://github.com/akka/alpakka-kafka) `2.0.x+.`

## Installation

Add the following to your sbt build (2.11.x, 2.12.x, 2.13.x):

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
    topic-prefix = "journal-"
    # if need customize, default is persistence-id
    topic-resolver-class-name = "com.github.j5ik2o.akka.persistence.kafka.resolver.KafkaTopicResolver$PersistenceId"
    # if need customize, default is partion 0
    partition-resolver-class-name = "com.github.j5ik2o.akka.persistence.kafka.resolver.KafkaPartitionResolver$PartitionZero"
   
    producer {
      kafka-clients {
        bootstrap.servers = "localhost:6001"
      }
    } 
    consumer {
      kafka-clients {
        bootstrap.servers = "localhost:6001"
        group.id = "akka-persistence-journal"
      }
    }
  }

  kafka-snapshot-store {
    topic-prefix = "snapshot-"
    # if need customize, default is persistence-id
    topic-resolver-class-name = "com.github.j5ik2o.akka.persistence.kafka.resolver.KafkaTopicResolver$PersistenceId"
    # if need customize, default is partition 0
    partition-resolver-class-name = "com.github.j5ik2o.akka.persistence.kafka.resolver.KafkaPartitionResolver$PartitionZero"

    producer {
      kafka-clients {
        bootstrap.servers = "localhost:6001"
      }
    } 
    consumer {
      kafka-clients {
        bootstrap.servers = "localhost:6001"
        group.id = "akka-persistence-snapshot"
      }
    }
  }
}
```


