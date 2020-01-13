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

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.zeebe.exporter.proto.Schema.RecordMetadata;
import io.zeebe.exporter.proto.Schema.VariableDocumentRecord;
import io.zeebe.exporter.proto.Schema.VariableDocumentRecord.UpdateSemantics;
import io.zeebe.exporter.proto.Schema.WorkflowInstanceRecord;
import io.zeebe.protocol.record.Record;
import io.zeebe.protocol.record.RecordType;
import io.zeebe.protocol.record.ValueType;
import io.zeebe.protocol.record.value.BpmnElementType;
import io.zeebe.protocol.record.value.DeploymentRecordValue;
import io.zeebe.protocol.record.value.ErrorRecordValue;
import io.zeebe.protocol.record.value.IncidentRecordValue;
import io.zeebe.protocol.record.value.JobBatchRecordValue;
import io.zeebe.protocol.record.value.JobRecordValue;
import io.zeebe.protocol.record.value.MessageRecordValue;
import io.zeebe.protocol.record.value.MessageStartEventSubscriptionRecordValue;
import io.zeebe.protocol.record.value.MessageSubscriptionRecordValue;
import io.zeebe.protocol.record.value.TimerRecordValue;
import io.zeebe.protocol.record.value.VariableDocumentRecordValue;
import io.zeebe.protocol.record.value.VariableDocumentUpdateSemantic;
import io.zeebe.protocol.record.value.VariableRecordValue;
import io.zeebe.protocol.record.value.WorkflowInstanceCreationRecordValue;
import io.zeebe.protocol.record.value.WorkflowInstanceRecordValue;
import io.zeebe.protocol.record.value.WorkflowInstanceResultRecordValue;
import io.zeebe.protocol.record.value.WorkflowInstanceSubscriptionRecordValue;
import io.zeebe.protocol.record.value.deployment.DeployedWorkflow;
import io.zeebe.protocol.record.value.deployment.DeploymentResource;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * As a one class god factory...not great but keeping it around since it has all the code necessary
 * to create any of the protocol values, and I don't want to rewrite that.
 */
public final class RecordTransformer {

  private static final EnumMap<ValueType, Function<Record, GeneratedMessageV3>> TRANSFORMERS =
      new EnumMap<>(ValueType.class);

  private static final EnumMap<ValueType, RecordMetadata.ValueType> VALUE_TYPE_MAPPING =
      new EnumMap<>(ValueType.class);

  private static final EnumMap<RecordType, RecordMetadata.RecordType> RECORD_TYPE_MAPPING =
      new EnumMap<>(
          Map.of(
              RecordType.COMMAND,
              RecordMetadata.RecordType.COMMAND,
              RecordType.COMMAND_REJECTION,
              RecordMetadata.RecordType.COMMAND_REJECTION,
              RecordType.EVENT,
              RecordMetadata.RecordType.EVENT));

  private static final EnumMap<BpmnElementType, WorkflowInstanceRecord.BpmnElementType>
      BPMN_ELEMENT_TYPE_MAPPING = new EnumMap<>(BpmnElementType.class);

  private static final EnumMap<
          VariableDocumentUpdateSemantic, VariableDocumentRecord.UpdateSemantics>
      UPDATE_SEMANTICS_MAPPING =
          new EnumMap<>(
              Map.of(
                  VariableDocumentUpdateSemantic.LOCAL,
                  UpdateSemantics.LOCAL,
                  VariableDocumentUpdateSemantic.PROPAGATE,
                  UpdateSemantics.PROPAGATE));

  static {
    TRANSFORMERS.put(ValueType.DEPLOYMENT, RecordTransformer::toDeploymentRecord);
    TRANSFORMERS.put(ValueType.WORKFLOW_INSTANCE, RecordTransformer::toWorkflowInstanceRecord);
    TRANSFORMERS.put(ValueType.JOB_BATCH, RecordTransformer::toJobBatchRecord);
    TRANSFORMERS.put(ValueType.JOB, RecordTransformer::toJobRecord);
    TRANSFORMERS.put(ValueType.INCIDENT, RecordTransformer::toIncidentRecord);
    TRANSFORMERS.put(ValueType.MESSAGE, RecordTransformer::toMessageRecord);
    TRANSFORMERS.put(
        ValueType.MESSAGE_SUBSCRIPTION, RecordTransformer::toMessageSubscriptionRecord);
    TRANSFORMERS.put(
        ValueType.WORKFLOW_INSTANCE_SUBSCRIPTION,
        RecordTransformer::toWorkflowInstanceSubscriptionRecord);
    TRANSFORMERS.put(ValueType.TIMER, RecordTransformer::toTimerRecord);
    TRANSFORMERS.put(ValueType.VARIABLE, RecordTransformer::toVariableRecord);
    TRANSFORMERS.put(
        ValueType.MESSAGE_START_EVENT_SUBSCRIPTION,
        RecordTransformer::toMessageStartEventSubscriptionRecord);
    TRANSFORMERS.put(
        ValueType.WORKFLOW_INSTANCE_CREATION, RecordTransformer::toWorkflowInstanceCreationRecord);
    TRANSFORMERS.put(ValueType.VARIABLE_DOCUMENT, RecordTransformer::toVariableDocumentRecord);
    TRANSFORMERS.put(ValueType.ERROR, RecordTransformer::toErrorRecord);
    TRANSFORMERS.put(
        ValueType.WORKFLOW_INSTANCE_RESULT, RecordTransformer::toWorkflowInstanceResultRecord);

    VALUE_TYPE_MAPPING.put(ValueType.DEPLOYMENT, RecordMetadata.ValueType.DEPLOYMENT);
    VALUE_TYPE_MAPPING.put(ValueType.ERROR, RecordMetadata.ValueType.ERROR);
    VALUE_TYPE_MAPPING.put(ValueType.INCIDENT, RecordMetadata.ValueType.INCIDENT);
    VALUE_TYPE_MAPPING.put(ValueType.JOB, RecordMetadata.ValueType.JOB);
    VALUE_TYPE_MAPPING.put(ValueType.JOB_BATCH, RecordMetadata.ValueType.JOB_BATCH);
    VALUE_TYPE_MAPPING.put(ValueType.MESSAGE, RecordMetadata.ValueType.MESSAGE);
    VALUE_TYPE_MAPPING.put(
        ValueType.MESSAGE_START_EVENT_SUBSCRIPTION,
        RecordMetadata.ValueType.MESSAGE_START_EVENT_SUBSCRIPTION);
    VALUE_TYPE_MAPPING.put(
        ValueType.MESSAGE_SUBSCRIPTION, RecordMetadata.ValueType.MESSAGE_SUBSCRIPTION);
    VALUE_TYPE_MAPPING.put(ValueType.TIMER, RecordMetadata.ValueType.TIMER);
    VALUE_TYPE_MAPPING.put(ValueType.VARIABLE, RecordMetadata.ValueType.VARIABLE);
    VALUE_TYPE_MAPPING.put(ValueType.VARIABLE_DOCUMENT, RecordMetadata.ValueType.VARIABLE_DOCUMENT);
    VALUE_TYPE_MAPPING.put(ValueType.WORKFLOW_INSTANCE, RecordMetadata.ValueType.WORKFLOW_INSTANCE);
    VALUE_TYPE_MAPPING.put(
        ValueType.WORKFLOW_INSTANCE_CREATION, RecordMetadata.ValueType.WORKFLOW_INSTANCE_CREATION);
    VALUE_TYPE_MAPPING.put(
        ValueType.WORKFLOW_INSTANCE_RESULT, RecordMetadata.ValueType.WORKFLOW_INSTANCE_RESULT);
    VALUE_TYPE_MAPPING.put(
        ValueType.WORKFLOW_INSTANCE_SUBSCRIPTION,
        RecordMetadata.ValueType.WORKFLOW_INSTANCE_SUBSCRIPTION);

    BPMN_ELEMENT_TYPE_MAPPING.put(
        BpmnElementType.BOUNDARY_EVENT, WorkflowInstanceRecord.BpmnElementType.BOUNDARY_EVENT);
    BPMN_ELEMENT_TYPE_MAPPING.put(
        BpmnElementType.CALL_ACTIVITY, WorkflowInstanceRecord.BpmnElementType.CALL_ACTIVITY);
    BPMN_ELEMENT_TYPE_MAPPING.put(
        BpmnElementType.END_EVENT, WorkflowInstanceRecord.BpmnElementType.END_EVENT);
    BPMN_ELEMENT_TYPE_MAPPING.put(
        BpmnElementType.EVENT_BASED_GATEWAY,
        WorkflowInstanceRecord.BpmnElementType.EVENT_BASED_GATEWAY);
    BPMN_ELEMENT_TYPE_MAPPING.put(
        BpmnElementType.EXCLUSIVE_GATEWAY,
        WorkflowInstanceRecord.BpmnElementType.EXCLUSIVE_GATEWAY);
    BPMN_ELEMENT_TYPE_MAPPING.put(
        BpmnElementType.INTERMEDIATE_CATCH_EVENT,
        WorkflowInstanceRecord.BpmnElementType.INTERMEDIATE_CATCH_EVENT);
    BPMN_ELEMENT_TYPE_MAPPING.put(
        BpmnElementType.MULTI_INSTANCE_BODY,
        WorkflowInstanceRecord.BpmnElementType.MULTI_INSTANCE_BODY);
    BPMN_ELEMENT_TYPE_MAPPING.put(
        BpmnElementType.PARALLEL_GATEWAY, WorkflowInstanceRecord.BpmnElementType.PARALLEL_GATEWAY);
    BPMN_ELEMENT_TYPE_MAPPING.put(
        BpmnElementType.PROCESS, WorkflowInstanceRecord.BpmnElementType.PROCESS);
    BPMN_ELEMENT_TYPE_MAPPING.put(
        BpmnElementType.RECEIVE_TASK, WorkflowInstanceRecord.BpmnElementType.RECEIVE_TASK);
    BPMN_ELEMENT_TYPE_MAPPING.put(
        BpmnElementType.SEQUENCE_FLOW, WorkflowInstanceRecord.BpmnElementType.SEQUENCE_FLOW);
    BPMN_ELEMENT_TYPE_MAPPING.put(
        BpmnElementType.SERVICE_TASK, WorkflowInstanceRecord.BpmnElementType.SERVICE_TASK);
    BPMN_ELEMENT_TYPE_MAPPING.put(
        BpmnElementType.START_EVENT, WorkflowInstanceRecord.BpmnElementType.START_EVENT);
    BPMN_ELEMENT_TYPE_MAPPING.put(
        BpmnElementType.SUB_PROCESS, WorkflowInstanceRecord.BpmnElementType.SUB_PROCESS);
  }

  private RecordTransformer() {}

  public static GeneratedMessageV3 toProtobufMessage(Record record) {
    final ValueType valueType = record.getValueType();
    final Function<Record, GeneratedMessageV3> toRecordFunc = TRANSFORMERS.get(valueType);
    return toRecordFunc != null ? toRecordFunc.apply(record) : Empty.getDefaultInstance();
  }

  private static Schema.RecordId toRecordId(Record record) {
    return Schema.RecordId.newBuilder()
        .setPartitionId(record.getPartitionId())
        .setPosition(record.getPosition())
        .build();
  }

  private static Schema.RecordMetadata toMetadata(Record record) {
    final Schema.RecordMetadata.Builder builder =
        Schema.RecordMetadata.newBuilder()
            .setIntent(record.getIntent().name())
            .setValueType(toValueType(record.getValueType()))
            .setKey(record.getKey())
            .setRecordType(toRecordType(record.getRecordType()))
            .setSourceRecordPosition(record.getSourceRecordPosition())
            .setPosition(record.getPosition())
            .setTimestamp(record.getTimestamp())
            .setPartitionId(record.getPartitionId());

    if (record.getRejectionType() != null) {
      builder.setRejectionType(record.getRejectionType().name());
      builder.setRejectionReason(record.getRejectionReason());
    }

    return builder.build();
  }

  private static Schema.RecordMetadata.ValueType toValueType(ValueType valueType) {
    return VALUE_TYPE_MAPPING.getOrDefault(valueType, RecordMetadata.ValueType.UNKNOWN_VALUE_TYPE);
  }

  private static Schema.RecordMetadata.RecordType toRecordType(RecordType recordType) {
    return RECORD_TYPE_MAPPING.getOrDefault(
        recordType, RecordMetadata.RecordType.UNKNOWN_RECORD_TYPE);
  }

  private static Schema.DeploymentRecord toDeploymentRecord(Record<DeploymentRecordValue> record) {
    final Schema.DeploymentRecord.Builder builder =
        Schema.DeploymentRecord.newBuilder().setMetadata(toMetadata(record));

    for (final DeploymentResource resource : record.getValue().getResources()) {
      builder.addResources(toDeploymentRecordResource(resource));
    }

    for (final DeployedWorkflow workflow : record.getValue().getDeployedWorkflows()) {
      builder.addDeployedWorkflows(toDeploymentRecordWorkflow(workflow));
    }

    return builder.build();
  }

  private static Schema.DeploymentRecord.Resource toDeploymentRecordResource(
      DeploymentResource resource) {
    return Schema.DeploymentRecord.Resource.newBuilder()
        .setResource(ByteString.copyFrom(resource.getResource()))
        .setResourceName(resource.getResourceName())
        .setResourceType(resource.getResourceType().name())
        .build();
  }

  private static Schema.DeploymentRecord.Workflow toDeploymentRecordWorkflow(
      DeployedWorkflow workflow) {
    return Schema.DeploymentRecord.Workflow.newBuilder()
        .setBpmnProcessId(workflow.getBpmnProcessId())
        .setResourceName(workflow.getResourceName())
        .setVersion(workflow.getVersion())
        .setWorkflowKey(workflow.getWorkflowKey())
        .build();
  }

  private static Schema.IncidentRecord toIncidentRecord(Record<IncidentRecordValue> record) {
    final IncidentRecordValue value = record.getValue();

    return Schema.IncidentRecord.newBuilder()
        .setBpmnProcessId(value.getBpmnProcessId())
        .setElementId(value.getElementId())
        .setElementInstanceKey(value.getElementInstanceKey())
        .setErrorMessage(value.getErrorMessage())
        .setErrorType(value.getErrorType().name())
        .setJobKey(value.getJobKey())
        .setWorkflowInstanceKey(value.getWorkflowInstanceKey())
        .setWorkflowKey(value.getWorkflowKey())
        .setVariableScopeKey(value.getVariableScopeKey())
        .setMetadata(toMetadata(record))
        .build();
  }

  private static Schema.JobRecord toJobRecord(Record<JobRecordValue> record) {
    final Schema.JobRecord.Builder builder = toJobRecord(record.getValue());
    return builder.setMetadata(toMetadata(record)).build();
  }

  private static Schema.JobRecord.Builder toJobRecord(JobRecordValue value) {
    return Schema.JobRecord.newBuilder()
        .setDeadline(value.getDeadline())
        .setErrorMessage(value.getErrorMessage())
        .setRetries(value.getRetries())
        .setType(value.getType())
        .setWorker(value.getWorker())
        .setVariables(toStruct(value.getVariables()))
        .setCustomHeaders(toStruct(value.getCustomHeaders()))
        .setBpmnProcessId(value.getBpmnProcessId())
        .setElementId(value.getElementId())
        .setElementInstanceKey(value.getElementInstanceKey())
        .setWorkflowDefinitionVersion(value.getWorkflowDefinitionVersion())
        .setWorkflowInstanceKey(value.getWorkflowInstanceKey())
        .setWorkflowKey(value.getWorkflowKey());
  }

  private static Schema.JobBatchRecord toJobBatchRecord(Record<JobBatchRecordValue> record) {
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
        .setMaxJobsToActivate(value.getMaxJobsToActivate())
        .setTimeout(value.getTimeout())
        .setType(value.getType())
        .setWorker(value.getWorker())
        .setTruncated(value.isTruncated())
        .setMetadata(toMetadata(record))
        .build();
  }

  private static Schema.MessageRecord toMessageRecord(Record<MessageRecordValue> record) {
    final MessageRecordValue value = record.getValue();

    return Schema.MessageRecord.newBuilder()
        .setCorrelationKey(value.getCorrelationKey())
        .setMessageId(value.getMessageId())
        .setName(value.getName())
        .setTimeToLive(value.getTimeToLive())
        .setVariables(toStruct(value.getVariables()))
        .setMetadata(toMetadata(record))
        .build();
  }

  private static Schema.MessageSubscriptionRecord toMessageSubscriptionRecord(
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

  private static Schema.MessageStartEventSubscriptionRecord toMessageStartEventSubscriptionRecord(
      Record<MessageStartEventSubscriptionRecordValue> record) {
    final MessageStartEventSubscriptionRecordValue value = record.getValue();
    final Schema.MessageStartEventSubscriptionRecord.Builder builder =
        Schema.MessageStartEventSubscriptionRecord.newBuilder();

    builder
        .setWorkflowKey(value.getWorkflowKey())
        .setMessageName(value.getMessageName())
        .setStartEventId(value.getStartEventId());

    return builder.setMetadata(toMetadata(record)).build();
  }

  private static Schema.VariableRecord toVariableRecord(Record<VariableRecordValue> record) {
    final VariableRecordValue value = record.getValue();
    final Schema.VariableRecord.Builder builder = Schema.VariableRecord.newBuilder();

    builder
        .setScopeKey(value.getScopeKey())
        .setWorkflowInstanceKey(value.getWorkflowInstanceKey())
        .setWorkflowKey(value.getWorkflowKey())
        .setName(value.getName())
        .setValue(value.getValue());

    return builder.setMetadata(toMetadata(record)).build();
  }

  private static Schema.TimerRecord toTimerRecord(Record<TimerRecordValue> record) {
    final TimerRecordValue value = record.getValue();

    return Schema.TimerRecord.newBuilder()
        .setDueDate(value.getDueDate())
        .setRepetitions(value.getRepetitions())
        .setElementInstanceKey(value.getElementInstanceKey())
        .setTargetFlowNodeId(value.getTargetElementId())
        .setWorkflowInstanceKey(value.getWorkflowInstanceKey())
        .setWorkflowKey(value.getWorkflowKey())
        .setMetadata(toMetadata(record))
        .build();
  }

  private static Schema.WorkflowInstanceRecord toWorkflowInstanceRecord(
      Record<WorkflowInstanceRecordValue> record) {
    final WorkflowInstanceRecordValue value = record.getValue();

    return Schema.WorkflowInstanceRecord.newBuilder()
        .setBpmnProcessId(value.getBpmnProcessId())
        .setElementId(value.getElementId())
        .setFlowScopeKey(value.getFlowScopeKey())
        .setVersion(value.getVersion())
        .setWorkflowInstanceKey(value.getWorkflowInstanceKey())
        .setWorkflowKey(value.getWorkflowKey())
        .setBpmnElementType(toBpmnElementType(value.getBpmnElementType()))
        .setParentWorkflowInstanceKey(value.getParentWorkflowInstanceKey())
        .setParentElementInstanceKey(value.getParentElementInstanceKey())
        .setMetadata(toMetadata(record))
        .build();
  }

  private static Schema.WorkflowInstanceRecord.BpmnElementType toBpmnElementType(
      BpmnElementType type) {
    return BPMN_ELEMENT_TYPE_MAPPING.getOrDefault(
        type, WorkflowInstanceRecord.BpmnElementType.UNKNOWN_BPMN_ELEMENT_TYPE);
  }

  private static Schema.WorkflowInstanceSubscriptionRecord toWorkflowInstanceSubscriptionRecord(
      Record<WorkflowInstanceSubscriptionRecordValue> record) {
    final WorkflowInstanceSubscriptionRecordValue value = record.getValue();

    return Schema.WorkflowInstanceSubscriptionRecord.newBuilder()
        .setElementInstanceKey(value.getElementInstanceKey())
        .setWorkflowInstanceKey(value.getWorkflowInstanceKey())
        .setMessageName(value.getMessageName())
        .setVariables(toStruct(value.getVariables()))
        .setMetadata(toMetadata(record))
        .build();
  }

  private static Schema.WorkflowInstanceCreationRecord toWorkflowInstanceCreationRecord(
      Record<WorkflowInstanceCreationRecordValue> record) {
    final WorkflowInstanceCreationRecordValue value = record.getValue();

    return Schema.WorkflowInstanceCreationRecord.newBuilder()
        .setBpmnProcessId(value.getBpmnProcessId())
        .setVersion(value.getVersion())
        .setWorkflowKey(value.getWorkflowKey())
        .setWorkflowInstanceKey(value.getWorkflowInstanceKey())
        .setVariables(toStruct(value.getVariables()))
        .setMetadata(toMetadata(record))
        .build();
  }

  private static Schema.WorkflowInstanceResultRecord toWorkflowInstanceResultRecord(
      Record<WorkflowInstanceResultRecordValue> record) {
    final WorkflowInstanceResultRecordValue value = record.getValue();

    return Schema.WorkflowInstanceResultRecord.newBuilder()
        .setBpmnProcessId(value.getBpmnProcessId())
        .setVersion(value.getVersion())
        .setWorkflowKey(value.getWorkflowKey())
        .setWorkflowInstanceKey(value.getWorkflowInstanceKey())
        .setVariables(toStruct(value.getVariables()))
        .setMetadata(toMetadata(record))
        .build();
  }

  private static Schema.VariableDocumentRecord toVariableDocumentRecord(
      Record<VariableDocumentRecordValue> record) {
    final VariableDocumentRecordValue value = record.getValue();

    return Schema.VariableDocumentRecord.newBuilder()
        .setScopeKey(value.getScopeKey())
        .setUpdateSemantics(toUpdateSemantics(value.getUpdateSemantics()))
        .setVariables(toStruct(value.getVariables()))
        .setMetadata(toMetadata(record))
        .build();
  }

  private static Schema.VariableDocumentRecord.UpdateSemantics toUpdateSemantics(
      VariableDocumentUpdateSemantic updateSemantics) {
    return UPDATE_SEMANTICS_MAPPING.getOrDefault(
        updateSemantics, UpdateSemantics.UNKNOWN_UPDATE_SEMANTICS);
  }

  private static Schema.ErrorRecord toErrorRecord(Record<ErrorRecordValue> record) {
    final ErrorRecordValue value = record.getValue();

    return Schema.ErrorRecord.newBuilder()
        .setExceptionMessage(value.getExceptionMessage())
        .setStacktrace(value.getStacktrace())
        .setErrorEventPosition(value.getErrorEventPosition())
        .setWorkflowInstanceKey(value.getWorkflowInstanceKey())
        .setMetadata(toMetadata(record))
        .build();
  }

  private static Struct toStruct(Map<?, ?> map) {
    final Struct.Builder builder = Struct.newBuilder();

    for (final Map.Entry<?, ?> entry : map.entrySet()) {
      builder.putFields(entry.getKey().toString(), toValue(entry.getValue()));
    }

    return builder.build();
  }

  private static Value toValue(Object object) {
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
