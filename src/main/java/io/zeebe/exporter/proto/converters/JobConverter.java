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
import io.zeebe.exporter.proto.converters.job.HeadersConverter;
import io.zeebe.exporter.record.value.JobRecordValue;

public class JobConverter implements MessageConverter<JobRecordValue, Schema.JobRecord.Builder> {
  private final TimestampConverter timestampConverter;
  private final StructConverter structConverter;
  private final HeadersConverter headersConverter;

  public JobConverter() {
    this(new TimestampConverter(), new StructConverter(), new HeadersConverter());
  }

  public JobConverter(
      TimestampConverter timestampConverter,
      StructConverter structConverter,
      HeadersConverter headersConverter) {
    this.timestampConverter = timestampConverter;
    this.structConverter = structConverter;
    this.headersConverter = headersConverter;
  }

  @Override
  public Schema.JobRecord.Builder convert(JobRecordValue value) {
    return Schema.JobRecord.newBuilder()
        .setDeadline(timestampConverter.convert(value.getDeadline()))
        .setErrorMessage(value.getErrorMessage())
        .setRetries(value.getRetries())
        .setType(value.getType())
        .setWorker(value.getWorker())
        .setPayload(structConverter.convert(value.getPayloadAsMap()))
        .setCustomHeaders(structConverter.convert(value.getCustomHeaders()))
        .setHeaders(headersConverter.convert(value.getHeaders()));
  }
}
