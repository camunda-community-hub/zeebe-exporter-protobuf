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
