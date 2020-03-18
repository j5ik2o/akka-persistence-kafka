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

final case class Journal(
    persistenceId: PersistenceId,
    sequenceNumber: SequenceNumber,
    payload: Any,
    deleted: Boolean = false,
    manifest: String = "",
    timestamp: Long = 0,
    writerUuid: String = "",
    tags: Seq[String] = Seq.empty,
    ordering: Option[Int] = None
) {
  def withDeleted: Journal                   = copy(deleted = true)
  def withManifest(value: String): Journal   = copy(manifest = value)
  def withTimestamp(value: Long): Journal    = copy(timestamp = value)
  def withWriterUuid(value: String): Journal = copy(writerUuid = value)
  def withTags(values: Seq[String]): Journal = copy(tags = values)
}
