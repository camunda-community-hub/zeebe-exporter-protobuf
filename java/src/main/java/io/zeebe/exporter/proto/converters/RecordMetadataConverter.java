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
package io.zeebe.exporter.proto.converters;

import io.zeebe.exporter.proto.MessageConverter;
import io.zeebe.exporter.proto.Schema;
import io.zeebe.exporter.record.Record;
import io.zeebe.exporter.record.RecordMetadata;
import io.zeebe.protocol.clientapi.RejectionType;

public class RecordMetadataConverter
    implements MessageConverter<Record, Schema.RecordMetadata.Builder> {
  private final TimestampConverter timestampConverter;

  public RecordMetadataConverter() {
    this(new TimestampConverter());
  }

  public RecordMetadataConverter(TimestampConverter timestampConverter) {
    this.timestampConverter = timestampConverter;
  }

  @Override
  public Schema.RecordMetadata.Builder convert(Record value) {
    final RecordMetadata metadata = value.getMetadata();
    final Schema.RecordMetadata.Builder builder =
        Schema.RecordMetadata.newBuilder()
            .setIntent(metadata.getIntent().name())
            .setKey(value.getKey())
            .setProducerId(value.getProducerId())
            .setRaftTerm(value.getRaftTerm())
            .setRecordType(metadata.getRecordType().name())
            .setSourceRecordPosition(value.getSourceRecordPosition())
            .setPosition(value.getPosition())
            .setTimestamp(timestampConverter.convert(value.getTimestamp()));

    if (metadata.getRejectionType() != RejectionType.NULL_VAL) {
      builder.setRejectionType(metadata.getRejectionType().name());
      builder.setRejectionReason(metadata.getRejectionReason());
    }

    return builder;
  }
}
