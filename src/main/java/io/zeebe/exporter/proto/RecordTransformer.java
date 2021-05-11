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

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.value.DeploymentDistributionRecordValue;
import io.camunda.zeebe.protocol.record.value.DeploymentRecordValue;
import io.camunda.zeebe.protocol.record.value.ErrorRecordValue;
import io.camunda.zeebe.protocol.record.value.IncidentRecordValue;
import io.camunda.zeebe.protocol.record.value.JobBatchRecordValue;
import io.camunda.zeebe.protocol.record.value.JobRecordValue;
import io.camunda.zeebe.protocol.record.value.MessageRecordValue;
import io.camunda.zeebe.protocol.record.value.MessageStartEventSubscriptionRecordValue;
import io.camunda.zeebe.protocol.record.value.MessageSubscriptionRecordValue;
import io.camunda.zeebe.protocol.record.value.ProcessEventRecordValue;
import io.camunda.zeebe.protocol.record.value.ProcessInstanceCreationRecordValue;
import io.camunda.zeebe.protocol.record.value.ProcessInstanceRecordValue;
import io.camunda.zeebe.protocol.record.value.ProcessMessageSubscriptionRecordValue;
import io.camunda.zeebe.protocol.record.value.TimerRecordValue;
import io.camunda.zeebe.protocol.record.value.VariableDocumentRecordValue;
import io.camunda.zeebe.protocol.record.value.VariableDocumentUpdateSemantic;
import io.camunda.zeebe.protocol.record.value.VariableRecordValue;
import io.camunda.zeebe.protocol.record.value.deployment.DeploymentResource;
import io.camunda.zeebe.protocol.record.value.deployment.Process;
import io.camunda.zeebe.protocol.record.value.deployment.ProcessMetadataValue;
import io.zeebe.exporter.proto.Schema.RecordMetadata;
import io.zeebe.exporter.proto.Schema.VariableDocumentRecord;
import io.zeebe.exporter.proto.Schema.VariableDocumentRecord.UpdateSemantics;
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
    TRANSFORMERS.put(
        ValueType.DEPLOYMENT_DISTRIBUTION, RecordTransformer::toDeploymentDistributionRecord);
    TRANSFORMERS.put(ValueType.PROCESS_INSTANCE, RecordTransformer::toProcessInstanceRecord);
    TRANSFORMERS.put(ValueType.JOB_BATCH, RecordTransformer::toJobBatchRecord);
    TRANSFORMERS.put(ValueType.JOB, RecordTransformer::toJobRecord);
    TRANSFORMERS.put(ValueType.INCIDENT, RecordTransformer::toIncidentRecord);
    TRANSFORMERS.put(ValueType.MESSAGE, RecordTransformer::toMessageRecord);
    TRANSFORMERS.put(
        ValueType.MESSAGE_SUBSCRIPTION, RecordTransformer::toMessageSubscriptionRecord);
    TRANSFORMERS.put(ValueType.PROCESS, RecordTransformer::toProcessRecord);
    TRANSFORMERS.put(ValueType.PROCESS_EVENT, RecordTransformer::toProcessEventRecord);
    TRANSFORMERS.put(
        ValueType.PROCESS_MESSAGE_SUBSCRIPTION,
        RecordTransformer::toProcessMessageSubscriptionRecord);
    TRANSFORMERS.put(ValueType.TIMER, RecordTransformer::toTimerRecord);
    TRANSFORMERS.put(ValueType.VARIABLE, RecordTransformer::toVariableRecord);
    TRANSFORMERS.put(
        ValueType.MESSAGE_START_EVENT_SUBSCRIPTION,
        RecordTransformer::toMessageStartEventSubscriptionRecord);
    TRANSFORMERS.put(
        ValueType.PROCESS_INSTANCE_CREATION, RecordTransformer::toProcessInstanceCreationRecord);
    TRANSFORMERS.put(ValueType.VARIABLE_DOCUMENT, RecordTransformer::toVariableDocumentRecord);
    TRANSFORMERS.put(ValueType.ERROR, RecordTransformer::toErrorRecord);

    VALUE_TYPE_MAPPING.put(ValueType.DEPLOYMENT, RecordMetadata.ValueType.DEPLOYMENT);
    VALUE_TYPE_MAPPING.put(
        ValueType.DEPLOYMENT_DISTRIBUTION, RecordMetadata.ValueType.DEPLOYMENT_DISTRIBUTION);
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
    VALUE_TYPE_MAPPING.put(ValueType.PROCESS, RecordMetadata.ValueType.PROCESS);
    VALUE_TYPE_MAPPING.put(ValueType.PROCESS_EVENT, RecordMetadata.ValueType.PROCESS_EVENT);
    VALUE_TYPE_MAPPING.put(ValueType.PROCESS_INSTANCE, RecordMetadata.ValueType.PROCESS_INSTANCE);
    VALUE_TYPE_MAPPING.put(
        ValueType.PROCESS_INSTANCE_CREATION, RecordMetadata.ValueType.PROCESS_INSTANCE_CREATION);
    VALUE_TYPE_MAPPING.put(
        ValueType.PROCESS_MESSAGE_SUBSCRIPTION,
        RecordMetadata.ValueType.PROCESS_MESSAGE_SUBSCRIPTION);
  }

  private RecordTransformer() {}

  public static GeneratedMessageV3 toProtobufMessage(Record record) {
    final ValueType valueType = record.getValueType();
    final Function<Record, GeneratedMessageV3> toRecordFunc = TRANSFORMERS.get(valueType);
    return toRecordFunc != null ? toRecordFunc.apply(record) : Empty.getDefaultInstance();
  }

  public static Schema.RecordId toRecordId(Record record) {
    return Schema.RecordId.newBuilder()
        .setPartitionId(record.getPartitionId())
        .setPosition(record.getPosition())
        .build();
  }

  public static Schema.Record toGenericRecord(Record record) {
    final var protobufRecord = toProtobufMessage(record);
    final var anyRecord = Any.pack(protobufRecord);

    return Schema.Record.newBuilder().setRecord(anyRecord).build();
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

    for (final var processMetadata : record.getValue().getProcessesMetadata()) {
      builder.addProcessMetadata(toProcessMetadata(processMetadata));
    }

    return builder.build();
  }

  private static Schema.DeploymentRecord.Resource toDeploymentRecordResource(
      DeploymentResource resource) {
    return Schema.DeploymentRecord.Resource.newBuilder()
        .setResource(ByteString.copyFrom(resource.getResource()))
        .setResourceName(resource.getResourceName())
        .build();
  }

  private static Schema.DeploymentRecord.ProcessMetadata toProcessMetadata(
      ProcessMetadataValue processMetadata) {

    return Schema.DeploymentRecord.ProcessMetadata.newBuilder()
        .setBpmnProcessId(processMetadata.getBpmnProcessId())
        .setResourceName(processMetadata.getResourceName())
        .setVersion(processMetadata.getVersion())
        .setProcessDefinitionKey(processMetadata.getProcessDefinitionKey())
        .setChecksum(new String(processMetadata.getChecksum()))
        .build();
  }

  private static Schema.DeploymentDistributionRecord toDeploymentDistributionRecord(
      Record<DeploymentDistributionRecordValue> record) {
    return Schema.DeploymentDistributionRecord.newBuilder()
        .setPartitionId(record.getValue().getPartitionId())
        .setMetadata(toMetadata(record))
        .build();
  }

  private static Schema.ProcessRecord toProcessRecord(Record<Process> record) {
    final Process value = record.getValue();

    return Schema.ProcessRecord.newBuilder()
        .setBpmnProcessId(value.getBpmnProcessId())
        .setResourceName(value.getResourceName())
        .setResource(ByteString.copyFrom(value.getResource()))
        .setVersion(value.getVersion())
        .setProcessDefinitionKey(value.getProcessDefinitionKey())
        .setChecksum(new String(value.getChecksum()))
        .setMetadata(toMetadata(record))
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
        .setProcessInstanceKey(value.getProcessInstanceKey())
        .setProcessDefinitionKey(value.getProcessDefinitionKey())
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
        .setWorkflowDefinitionVersion(value.getProcessDefinitionVersion())
        .setProcessInstanceKey(value.getProcessInstanceKey())
        .setProcessDefinitionKey(value.getProcessDefinitionKey());
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
        .setProcessInstanceKey(value.getProcessInstanceKey())
        .setBpmnProcessId(value.getBpmnProcessId())
        .setMessageKey(value.getMessageKey())
        .setVariables(toStruct(value.getVariables()))
        .setMetadata(toMetadata(record))
        .build();
  }

  private static Schema.MessageStartEventSubscriptionRecord toMessageStartEventSubscriptionRecord(
      Record<MessageStartEventSubscriptionRecordValue> record) {
    final MessageStartEventSubscriptionRecordValue value = record.getValue();
    final Schema.MessageStartEventSubscriptionRecord.Builder builder =
        Schema.MessageStartEventSubscriptionRecord.newBuilder();

    builder
        .setProcessDefinitionKey(value.getProcessDefinitionKey())
        .setMessageName(value.getMessageName())
        .setStartEventId(value.getStartEventId())
        .setBpmnProcessId(value.getBpmnProcessId())
        .setCorrelationKey(value.getCorrelationKey())
        .setMessageKey(value.getMessageKey())
        .setProcessInstanceKey(value.getProcessInstanceKey())
        .setVariables(toStruct(value.getVariables()));

    return builder.setMetadata(toMetadata(record)).build();
  }

  private static Schema.VariableRecord toVariableRecord(Record<VariableRecordValue> record) {
    final VariableRecordValue value = record.getValue();
    final Schema.VariableRecord.Builder builder = Schema.VariableRecord.newBuilder();

    builder
        .setScopeKey(value.getScopeKey())
        .setProcessInstanceKey(value.getProcessInstanceKey())
        .setProcessDefinitionKey(value.getProcessDefinitionKey())
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
        .setTargetElementId(value.getTargetElementId())
        .setProcessInstanceKey(value.getProcessInstanceKey())
        .setProcessDefinitionKey(value.getProcessDefinitionKey())
        .setMetadata(toMetadata(record))
        .build();
  }

  private static Schema.ProcessInstanceRecord toProcessInstanceRecord(
      Record<ProcessInstanceRecordValue> record) {
    final ProcessInstanceRecordValue value = record.getValue();

    return Schema.ProcessInstanceRecord.newBuilder()
        .setBpmnProcessId(value.getBpmnProcessId())
        .setElementId(value.getElementId())
        .setFlowScopeKey(value.getFlowScopeKey())
        .setVersion(value.getVersion())
        .setProcessInstanceKey(value.getProcessInstanceKey())
        .setProcessDefinitionKey(value.getProcessDefinitionKey())
        .setBpmnElementType(value.getBpmnElementType().name())
        .setParentProcessInstanceKey(value.getParentProcessInstanceKey())
        .setParentElementInstanceKey(value.getParentElementInstanceKey())
        .setMetadata(toMetadata(record))
        .build();
  }

  private static Schema.ProcessMessageSubscriptionRecord toProcessMessageSubscriptionRecord(
      Record<ProcessMessageSubscriptionRecordValue> record) {
    final ProcessMessageSubscriptionRecordValue value = record.getValue();

    return Schema.ProcessMessageSubscriptionRecord.newBuilder()
        .setElementInstanceKey(value.getElementInstanceKey())
        .setProcessInstanceKey(value.getProcessInstanceKey())
        .setMessageName(value.getMessageName())
        .setVariables(toStruct(value.getVariables()))
        .setBpmnProcessId(value.getBpmnProcessId())
        .setMessageKey(value.getMessageKey())
        .setCorrelationKey(value.getCorrelationKey())
        .setElementId(value.getElementId())
        .setMetadata(toMetadata(record))
        .build();
  }

  private static Schema.ProcessInstanceCreationRecord toProcessInstanceCreationRecord(
      Record<ProcessInstanceCreationRecordValue> record) {
    final ProcessInstanceCreationRecordValue value = record.getValue();

    return Schema.ProcessInstanceCreationRecord.newBuilder()
        .setBpmnProcessId(value.getBpmnProcessId())
        .setVersion(value.getVersion())
        .setProcessInstanceKey(value.getProcessInstanceKey())
        .setProcessDefinitionKey(value.getProcessDefinitionKey())
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
        .setProcessInstanceKey(value.getProcessInstanceKey())
        .setMetadata(toMetadata(record))
        .build();
  }

  private static Schema.ProcessEventRecord toProcessEventRecord(
      Record<ProcessEventRecordValue> record) {
    final ProcessEventRecordValue value = record.getValue();

    return Schema.ProcessEventRecord.newBuilder()
        .setProcessDefinitionKey(value.getProcessDefinitionKey())
        .setScopeKey(value.getScopeKey())
        .setTargetElementId(value.getTargetElementId())
        .setVariables(toStruct(value.getVariables()))
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
