/*
 * Copyright 2019 Junichi Kato
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.j5ik2o.akka.persistence.kafka.journal

import akka.persistence.PersistentRepr

final case class JournalRow(
    persistentRepr: PersistentRepr,
    tags: Seq[String] = Seq.empty,
    ordering: Option[Int] = None
) {
  val persistenceId: PersistenceId   = PersistenceId(persistentRepr.persistenceId)
  val sequenceNumber: SequenceNumber = SequenceNumber(persistentRepr.sequenceNr)
  val deleted: Boolean               = persistentRepr.deleted
  val manifest: String               = persistentRepr.manifest
  val timestamp: Long                = persistentRepr.timestamp
  val writerUuid: String             = persistentRepr.writerUuid

  def withDeleted: JournalRow                   = copy(persistentRepr = persistentRepr.update(deleted = true))
  def withManifest(value: String): JournalRow   = copy(persistentRepr = persistentRepr.withManifest(manifest))
  def withTimestamp(value: Long): JournalRow    = copy(persistentRepr = persistentRepr.withTimestamp(timestamp))
  def withWriterUuid(value: String): JournalRow = copy(persistentRepr = persistentRepr.update(writerUuid = value))
  def withTags(values: Seq[String]): JournalRow = copy(tags = values)
}
