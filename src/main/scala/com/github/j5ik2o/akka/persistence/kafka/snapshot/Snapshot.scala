package com.github.j5ik2o.akka.persistence.kafka.snapshot

import akka.persistence.{ SnapshotMetadata, SnapshotSelectionCriteria }

final case class Snapshot(metadata: SnapshotMetadata, payload: Any) {
  def matches(criteria: SnapshotSelectionCriteria): Boolean =
    metadata.sequenceNr <= criteria.maxSequenceNr &&
    metadata.timestamp <= criteria.maxTimestamp
}
