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
package io.zeebe.exporter.proto.converters.job;

import io.zeebe.exporter.proto.MessageConverter;
import io.zeebe.exporter.proto.Schema;
import io.zeebe.exporter.record.value.job.Headers;

public class HeadersConverter
    implements MessageConverter<Headers, Schema.JobRecord.Headers.Builder> {
  @Override
  public Schema.JobRecord.Headers.Builder convert(Headers value) {
    return Schema.JobRecord.Headers.newBuilder()
        .setBpmnProcessId(value.getBpmnProcessId())
        .setElementId(value.getElementId())
        .setElementInstanceKey(value.getElementInstanceKey())
        .setWorkflowDefinitionVersion(value.getWorkflowDefinitionVersion())
        .setWorkflowInstanceKey(value.getWorkflowInstanceKey())
        .setWorkflowKey(value.getWorkflowKey());
  }
}
