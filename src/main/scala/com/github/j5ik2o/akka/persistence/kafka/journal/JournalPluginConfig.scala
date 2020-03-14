package com.github.j5ik2o.akka.persistence.kafka.journal

import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

object JournalPluginConfig {
  def fromConfig(config: Config): JournalPluginConfig = JournalPluginConfig(
    tagSeparator = config.as[String]("tag-separator")
  )
}

case class JournalPluginConfig(tagSeparator: String)
