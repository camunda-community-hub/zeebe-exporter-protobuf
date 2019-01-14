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
import io.zeebe.exporter.record.value.JobBatchRecordValue;
import io.zeebe.exporter.record.value.JobRecordValue;

public class JobBatchConverter
    implements MessageConverter<JobBatchRecordValue, Schema.JobBatchRecord.Builder> {
  private final JobConverter jobConverter;

  public JobBatchConverter() {
    this(new JobConverter());
  }

  public JobBatchConverter(JobConverter jobConverter) {
    this.jobConverter = jobConverter;
  }

  @Override
  public Schema.JobBatchRecord.Builder convert(JobBatchRecordValue value) {
    final Schema.JobBatchRecord.Builder builder = Schema.JobBatchRecord.newBuilder();

    if (!value.getJobs().isEmpty()) {
      for (final JobRecordValue job : value.getJobs()) {
        builder.addJobs(jobConverter.convert(job));
      }
    }

    if (!value.getJobKeys().isEmpty()) {
      builder.addAllJobKeys(value.getJobKeys());
    }

    return builder
        .setAmount(value.getAmount())
        .setTimeout(value.getTimeout().toMillis())
        .setType(value.getType())
        .setWorker(value.getWorker());
  }
}
