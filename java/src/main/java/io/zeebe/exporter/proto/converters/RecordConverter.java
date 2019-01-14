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

import com.google.protobuf.Message;
import io.zeebe.exporter.proto.MessageConverter;
import io.zeebe.exporter.proto.Schema;
import io.zeebe.exporter.record.Record;
import io.zeebe.exporter.record.RecordValue;
import io.zeebe.exporter.record.value.JobBatchRecordValue;
import io.zeebe.exporter.record.value.JobRecordValue;

public class RecordConverter implements MessageConverter<Record<?>, Message.Builder> {
  private final RecordMetadataConverter metadataFactory;
  private final JobConverter jobConverter;
  private final JobBatchConverter jobBatchConverter;

  public RecordConverter(
      RecordMetadataConverter metadataFactory,
      JobConverter jobConverter,
      JobBatchConverter jobBatchConverter) {
    this.metadataFactory = metadataFactory;
    this.jobConverter = jobConverter;
    this.jobBatchConverter = jobBatchConverter;
  }

  @Override
  public Message.Builder convert(Record<?> record) {
    final RecordValue value = record.getValue();
    final Schema.RecordMetadata.Builder metadata = metadataFactory.convert(record);

    if (value instanceof JobRecordValue) {
      return jobConverter.convert((JobRecordValue) value).setMetadata(metadata);
    }

    if (value instanceof JobBatchRecordValue) {
      return jobBatchConverter.convert((JobBatchRecordValue) value).setMetadata(metadata);
    }

    return null;
  }
}
