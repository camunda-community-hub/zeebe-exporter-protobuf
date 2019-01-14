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

import com.google.protobuf.*;
import io.zeebe.exporter.record.Record;
import io.zeebe.exporter.record.RecordMetadata;
import io.zeebe.exporter.record.value.*;
import io.zeebe.exporter.record.value.deployment.DeployedWorkflow;
import io.zeebe.exporter.record.value.deployment.DeploymentResource;
import io.zeebe.exporter.record.value.job.Headers;
import io.zeebe.exporter.record.value.raft.RaftMember;
import io.zeebe.protocol.clientapi.RejectionType;
import io.zeebe.protocol.clientapi.ValueType;
import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * As a one class god factory...not great but keeping it around since it has all the code necessary
 * to create any of the protocol values, and I don't want to rewrite that.
 */
public class RecordTransformer {
  private static final EnumMap<ValueType, Function<Record, GeneratedMessageV3>> TRANSFORMERS =
      new EnumMap<>(ValueType.class);

  static {
    TRANSFORMERS.put(ValueType.DEPLOYMENT, v -> toDeploymentRecord(v));
    TRANSFORMERS.put(ValueType.WORKFLOW_INSTANCE, v -> toWorkflowInstanceRecord(v));
    TRANSFORMERS.put(ValueType.JOB_BATCH, v -> toJobBatchRecord(v));
    TRANSFORMERS.put(ValueType.JOB, v -> toJobRecord(v));
    TRANSFORMERS.put(ValueType.INCIDENT, v -> toIncidentRecord(v));
    TRANSFORMERS.put(ValueType.MESSAGE, v -> toMessageRecord(v));
    TRANSFORMERS.put(
        ValueType.MESSAGE_START_EVENT_SUBSCRIPTION, v -> toMessageStartEventSubscriptionRecord(v));
    TRANSFORMERS.put(ValueType.MESSAGE_SUBSCRIPTION, v -> toMessageSubscriptionRecord(v));
    TRANSFORMERS.put(
        ValueType.WORKFLOW_INSTANCE_SUBSCRIPTION, v -> toWorkflowInstanceSubscriptionRecord(v));
    TRANSFORMERS.put(ValueType.TIMER, v -> toTimerRecord(v));
    TRANSFORMERS.put(ValueType.RAFT, v -> toRaftRecord(v));
  }

  public static GeneratedMessageV3 toProtobufMessage(Record record) {
    return TRANSFORMERS.get(record.getMetadata().getValueType()).apply(record);
  }

  public static Schema.RecordId toRecordId(Record record) {
    return Schema.RecordId.newBuilder()
        .setPartitionId(record.getMetadata().getPartitionId())
        .setPosition(record.getPosition())
        .build();
  }

  public static Schema.RecordId toRecordId(RecordId recordId) {
    return Schema.RecordId.newBuilder()
        .setPosition(recordId.getPosition())
        .setPartitionId(recordId.getPartitionId())
        .build();
  }

  public static Schema.RecordMetadata toMetadata(Record record) {
    final RecordMetadata metadata = record.getMetadata();
    final Schema.RecordMetadata.Builder builder =
        Schema.RecordMetadata.newBuilder()
            .setIntent(metadata.getIntent().name())
            .setKey(record.getKey())
            .setProducerId(record.getProducerId())
            .setRaftTerm(record.getRaftTerm())
            .setRecordType(metadata.getRecordType().name())
            .setSourceRecordPosition(record.getSourceRecordPosition())
            .setPosition(record.getPosition())
            .setTimestamp(toTimestamp(record.getTimestamp()));

    if (metadata.getRejectionType() != RejectionType.NULL_VAL) {
      builder.setRejectionType(metadata.getRejectionType().name());
      builder.setRejectionReason(metadata.getRejectionReason());
    }

    return builder.build();
  }

  public static Schema.DeploymentRecord toDeploymentRecord(Record<DeploymentRecordValue> record) {
    final Schema.DeploymentRecord.Builder builder =
        Schema.DeploymentRecord.newBuilder().setMetadata(toMetadata(record));

    for (final DeploymentResource resource : record.getValue().getResources()) {
      builder.addResources(toDeploymentRecordResource(resource));
    }

    for (final DeployedWorkflow workflow : record.getValue().getDeployedWorkflows()) {
      builder.addWorkflows(toDeploymentRecordWorkflow(workflow));
    }

    return builder.build();
  }

  public static Schema.DeploymentRecord.Resource toDeploymentRecordResource(
      DeploymentResource resource) {
    return Schema.DeploymentRecord.Resource.newBuilder()
        .setResource(ByteString.copyFrom(resource.getResource()))
        .setResourceName(resource.getResourceName())
        .setResourceType(resource.getResourceType().name())
        .build();
  }

  public static Schema.DeploymentRecord.Workflow toDeploymentRecordWorkflow(
      DeployedWorkflow workflow) {
    return Schema.DeploymentRecord.Workflow.newBuilder()
        .setBpmnProcessId(workflow.getBpmnProcessId())
        .setResourceName(workflow.getResourceName())
        .setVersion(workflow.getVersion())
        .setWorkflowKey(workflow.getWorkflowKey())
        .build();
  }

  public static Schema.IncidentRecord toIncidentRecord(Record<IncidentRecordValue> record) {
    final IncidentRecordValue value = record.getValue();

    return Schema.IncidentRecord.newBuilder()
        .setBpmnProcessId(value.getBpmnProcessId())
        .setElementId(value.getElementId())
        .setElementInstanceKey(value.getElementInstanceKey())
        .setErrorMessage(value.getErrorMessage())
        .setErrorType(value.getErrorType())
        .setJobKey(value.getJobKey())
        .setWorkflowInstanceKey(value.getWorkflowInstanceKey())
        .setMetadata(toMetadata(record))
        .build();
  }

  public static Schema.JobRecord toJobRecord(Record<JobRecordValue> record) {
    final Schema.JobRecord.Builder builder = toJobRecord(record.getValue());
    return builder.setMetadata(toMetadata(record)).build();
  }

  public static Schema.JobRecord.Builder toJobRecord(JobRecordValue value) {
    return Schema.JobRecord.newBuilder()
        .setDeadline(toTimestamp(value.getDeadline()))
        .setErrorMessage(value.getErrorMessage())
        .setRetries(value.getRetries())
        .setType(value.getType())
        .setWorker(value.getWorker())
        .setPayload(toStruct(value.getPayloadAsMap()))
        .setCustomHeaders(toStruct(value.getCustomHeaders()))
        .setHeaders(toJobRecordHeaders(value.getHeaders()));
  }

  public static Schema.JobBatchRecord toJobBatchRecord(Record<JobBatchRecordValue> record) {
    final JobBatchRecordValue value = record.getValue();
    final Schema.JobBatchRecord.Builder builder = Schema.JobBatchRecord.newBuilder();

    if (!value.getJobs().isEmpty()) {
      for (final JobRecordValue job : value.getJobs()) {
        builder.addJobs(toJobRecord(job));
      }
    }

    if (!value.getJobKeys().isEmpty()) {
      builder.addAllJobKeys(value.getJobKeys());
    }

    return builder
        .setAmount(value.getAmount())
        .setTimeout(value.getTimeout().toMillis())
        .setType(value.getType())
        .setWorker(value.getWorker())
        .setMetadata(toMetadata(record))
        .build();
  }

  public static Schema.MessageRecord toMessageRecord(Record<MessageRecordValue> record) {
    final MessageRecordValue value = record.getValue();

    return Schema.MessageRecord.newBuilder()
        .setCorrelationKey(value.getCorrelationKey())
        .setMessageId(value.getMessageId())
        .setName(value.getName())
        .setTimeToLive(value.getTimeToLive())
        .setPayload(toStruct(value.getPayloadAsMap()))
        .setMetadata(toMetadata(record))
        .build();
  }

  public static Schema.MessageSubscriptionRecord toMessageSubscriptionRecord(
      Record<MessageSubscriptionRecordValue> record) {
    final MessageSubscriptionRecordValue value = record.getValue();

    return Schema.MessageSubscriptionRecord.newBuilder()
        .setCorrelationKey(value.getCorrelationKey())
        .setElementInstanceKey(value.getElementInstanceKey())
        .setMessageName(value.getMessageName())
        .setWorkflowInstanceKey(value.getWorkflowInstanceKey())
        .setMetadata(toMetadata(record))
        .build();
  }

  public static Schema.MessageStartEventSubscriptionRecord toMessageStartEventSubscriptionRecord(
      Record<MessageStartEventSubscriptionRecordValue> record) {
    final MessageStartEventSubscriptionRecordValue value = record.getValue();

    return Schema.MessageStartEventSubscriptionRecord.newBuilder()
        .setMessageName(value.getMessageName())
        .setStartEventId(value.getStartEventId())
        .setWorkflowKey(value.getWorkflowKey())
        .setMetadata(toMetadata(record))
        .build();
  }

  public static Schema.RaftRecord toRaftRecord(Record<RaftRecordValue> record) {
    final RaftRecordValue value = record.getValue();
    final Schema.RaftRecord.Builder builder = Schema.RaftRecord.newBuilder();

    for (final RaftMember member : value.getMembers()) {
      builder.addMembers(toRaftRecordMember(member));
    }

    return builder.setMetadata(toMetadata(record)).build();
  }

  public static Schema.TimerRecord toTimerRecord(Record<TimerRecordValue> record) {
    final TimerRecordValue value = record.getValue();

    return Schema.TimerRecord.newBuilder()
        .setDueDate(value.getDueDate())
        .setElementInstanceKey(value.getElementInstanceKey())
        .setHandlerFlowNodeId(value.getHandlerFlowNodeId())
        .setMetadata(toMetadata(record))
        .build();
  }

  public static Schema.WorkflowInstanceRecord toWorkflowInstanceRecord(
      Record<WorkflowInstanceRecordValue> record) {
    final WorkflowInstanceRecordValue value = record.getValue();

    return Schema.WorkflowInstanceRecord.newBuilder()
        .setBpmnProcessId(value.getBpmnProcessId())
        .setElementId(value.getElementId())
        .setScopeInstanceKey(value.getScopeInstanceKey())
        .setVersion(value.getVersion())
        .setWorkflowInstanceKey(value.getWorkflowInstanceKey())
        .setWorkflowKey(value.getWorkflowKey())
        .setPayload(toStruct(value.getPayloadAsMap()))
        .setMetadata(toMetadata(record))
        .build();
  }

  public static Schema.WorkflowInstanceSubscriptionRecord toWorkflowInstanceSubscriptionRecord(
      Record<WorkflowInstanceSubscriptionRecordValue> record) {
    final WorkflowInstanceSubscriptionRecordValue value = record.getValue();

    return Schema.WorkflowInstanceSubscriptionRecord.newBuilder()
        .setElementInstanceKey(value.getElementInstanceKey())
        .setWorkflowInstanceKey(value.getWorkflowInstanceKey())
        .setMessageName(value.getMessageName())
        .setPayload(toStruct(value.getPayloadAsMap()))
        .build();
  }

  public static Schema.RaftRecord.Member toRaftRecordMember(RaftMember member) {
    return Schema.RaftRecord.Member.newBuilder().setNodeId(member.getNodeId()).build();
  }

  public static Schema.JobRecord.Headers toJobRecordHeaders(Headers headers) {
    return Schema.JobRecord.Headers.newBuilder()
        .setBpmnProcessId(headers.getBpmnProcessId())
        .setElementId(headers.getElementId())
        .setElementInstanceKey(headers.getElementInstanceKey())
        .setWorkflowDefinitionVersion(headers.getWorkflowDefinitionVersion())
        .setWorkflowInstanceKey(headers.getWorkflowInstanceKey())
        .setWorkflowKey(headers.getWorkflowKey())
        .build();
  }

  public static Timestamp toTimestamp(Instant instant) {
    return Timestamp.newBuilder()
        .setSeconds(instant.getEpochSecond())
        .setNanos(instant.getNano())
        .build();
  }

  public static Struct toStruct(Map<?, ?> map) {
    final Struct.Builder builder = Struct.newBuilder();

    for (final Map.Entry<?, ?> entry : map.entrySet()) {
      builder.putFields(entry.getKey().toString(), toValue(entry.getValue()));
    }

    return builder.build();
  }

  public static Value toValue(Object object) {
    final Value.Builder builder = Value.newBuilder();

    if (object == null) {
      builder.setNullValue(NullValue.NULL_VALUE);
    } else if (object instanceof Number) {
      builder.setNumberValue(((Number) object).doubleValue());
    } else if (object instanceof Boolean) {
      builder.setBoolValue((Boolean) object);
    } else if (object instanceof List) {
      final List list = (List) object;
      final ListValue.Builder listBuilder = ListValue.newBuilder();

      for (final Object item : list) {
        listBuilder.addValues(toValue(item));
      }

      builder.setListValue(listBuilder.build());
    } else if (object instanceof Map) {
      builder.setStructValue(toStruct((Map) object));
    } else if (object instanceof String) {
      builder.setStringValue((String) object);
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Unexpected struct value of type %s, should be one of: null, Number, Boolean, List, Map, String",
              object.getClass().getCanonicalName()));
    }

    return builder.build();
  }
}
