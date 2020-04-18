package com.github.j5ik2o.akka.persistence.kafka.resolver

case class KafkaTopic(private val value: String) {

  private val MAX_NAME_LENGTH: Int = 249

  require(value.nonEmpty, "Topic name is illegal, it can't be empty")
  require(value != "." || value != "src/main", "Topic name cannot be \".\" or \"..\"")
  require(
    value.length <= MAX_NAME_LENGTH,
    "Topic name is illegal, it can't be longer than " + MAX_NAME_LENGTH + " characters, topic name: " + value
  )
  require(
    containsValidPattern(value),
    "Topic name \"" + value + "\" is illegal, it contains a character other than " + "ASCII alphanumerics, '.', '_' and '-'"
  )

  private def containsValidPattern(topic: String): Boolean = {
    for (i <- 0 until topic.length) {
      val c: Char = topic.charAt(i)
      // We don't use Character.isLetterOrDigit(c) because it's slower
      val validChar: Boolean =
        (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || c == '.' || c == '_' || c == '-'
      if (!validChar) return false
    }
    true
  }

  def asString: String = value
}
