/*
 * Copyright © 2019 camunda services GmbH (info@camunda.com)
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
package io.zeebe.exporter.proto;

import io.zeebe.exporter.record.Record;
import java.util.Objects;

/**
 * Wraps around an existing record to provide an interface which easily identifies a unique ID for
 * the given record.
 */
public class RecordId {
  private final int partitionId;
  private final long position;

  public RecordId(int partitionId, long position) {
    this.partitionId = partitionId;
    this.position = position;
  }

  public static RecordId ofRecord(Record record) {
    return new RecordId(record.getMetadata().getPartitionId(), record.getPosition());
  }

  public static RecordId ofSchema(Schema.RecordId recordId) {
    return new RecordId(recordId.getPartitionId(), recordId.getPosition());
  }

  public int getPartitionId() {
    return partitionId;
  }

  public long getPosition() {
    return position;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof RecordId)) {
      return false;
    }

    final RecordId recordId = (RecordId) o;
    return partitionId == recordId.partitionId && position == recordId.position;
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitionId, position);
  }

  @Override
  public String toString() {
    return "RecordId{" + "partitionId=" + partitionId + ", position=" + position + '}';
  }
}
