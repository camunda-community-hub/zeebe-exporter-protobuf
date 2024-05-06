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
import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.value.*;
import io.camunda.zeebe.protocol.record.value.deployment.*;
import io.camunda.zeebe.protocol.record.value.deployment.Process;
import io.camunda.zeebe.protocol.record.value.management.CheckpointRecordValue;
import io.zeebe.exporter.proto.Schema.RecordMetadata;
import io.zeebe.exporter.proto.Schema.VariableDocumentRecord;
import io.zeebe.exporter.proto.Schema.VariableDocumentRecord.UpdateSemantics;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * As a one class god factory...not great but keeping it around since it has all the code necessary
 * to create any of the protocol values, and I don't want to rewrite that.
 */
public final class RecordTransformer {

  private static final EnumMap<ValueType, Function<Record, GeneratedMessage>> TRANSFORMERS =
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
    TRANSFORMERS.put(ValueType.DECISION, RecordTransformer::toDecisionRecord);
    TRANSFORMERS.put(
        ValueType.DECISION_REQUIREMENTS, RecordTransformer::toDecisionRequirementsRecord);
    TRANSFORMERS.put(ValueType.DECISION_EVALUATION, RecordTransformer::toDecisionEvaluationRecord);
    TRANSFORMERS.put(
        ValueType.PROCESS_INSTANCE_MODIFICATION,
        RecordTransformer::toProcessInstanceModificationRecord);
    TRANSFORMERS.put(ValueType.CHECKPOINT, RecordTransformer::toCheckpointRecord);
    TRANSFORMERS.put(ValueType.SIGNAL, RecordTransformer::toSignalRecord);
    TRANSFORMERS.put(ValueType.SIGNAL_SUBSCRIPTION, RecordTransformer::toSignalSubscriptionRecord);
    TRANSFORMERS.put(ValueType.FORM, RecordTransformer::toFormRecord);
    TRANSFORMERS.put(ValueType.RESOURCE_DELETION, RecordTransformer::toResourceDeletionRecord);
    TRANSFORMERS.put(ValueType.USER_TASK, RecordTransformer::toUserTaskRecord);

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
    VALUE_TYPE_MAPPING.put(ValueType.DECISION, RecordMetadata.ValueType.DECISION);
    VALUE_TYPE_MAPPING.put(
        ValueType.DECISION_REQUIREMENTS, RecordMetadata.ValueType.DECISION_REQUIREMENTS);
    VALUE_TYPE_MAPPING.put(
        ValueType.DECISION_EVALUATION, RecordMetadata.ValueType.DECISION_EVALUATION);
    VALUE_TYPE_MAPPING.put(
        ValueType.PROCESS_INSTANCE_MODIFICATION,
        RecordMetadata.ValueType.PROCESS_INSTANCE_MODIFICATION);
    VALUE_TYPE_MAPPING.put(ValueType.CHECKPOINT, RecordMetadata.ValueType.CHECKPOINT);
    VALUE_TYPE_MAPPING.put(ValueType.ESCALATION, RecordMetadata.ValueType.ESCALATION);
    VALUE_TYPE_MAPPING.put(
        ValueType.SIGNAL_SUBSCRIPTION, RecordMetadata.ValueType.SIGNAL_SUBSCRIPTION);
    VALUE_TYPE_MAPPING.put(ValueType.SIGNAL, RecordMetadata.ValueType.SIGNAL);
    VALUE_TYPE_MAPPING.put(ValueType.RESOURCE_DELETION, RecordMetadata.ValueType.RESOURCE_DELETION);
    VALUE_TYPE_MAPPING.put(
        ValueType.COMMAND_DISTRIBUTION, RecordMetadata.ValueType.COMMAND_DISTRIBUTION);
    VALUE_TYPE_MAPPING.put(ValueType.FORM, RecordMetadata.ValueType.FORM);
    VALUE_TYPE_MAPPING.put(ValueType.USER_TASK, RecordMetadata.ValueType.USER_TASK);
  }

  private RecordTransformer() {}

  public static GeneratedMessage toProtobufMessage(Record record) {
    final ValueType valueType = record.getValueType();
    final Function<Record, GeneratedMessage> toRecordFunc = TRANSFORMERS.get(valueType);
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

    for (DecisionRequirementsMetadataValue decisionRequirementsMetadata :
        record.getValue().getDecisionRequirementsMetadata()) {
      builder.addDecisionRequirementsMetadata(
          toDecisionRequirementsMetadata(decisionRequirementsMetadata));
    }

    for (DecisionRecordValue decisionMetadata : record.getValue().getDecisionsMetadata()) {
      builder.addDecisionMetadata(toDecisionMetadata(decisionMetadata));
    }

    for (FormMetadataValue formMetadata : record.getValue().getFormMetadata()) {
      builder.addFormMetadata(toFormMetadata(formMetadata));
    }

    builder.setTenantId(toTenantId(record.getValue()));

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
        .setChecksum(ByteString.copyFrom(processMetadata.getChecksum()))
        .setIsDuplicate(processMetadata.isDuplicate())
        .setTenantId(toTenantId(processMetadata))
        .build();
  }

  private static Schema.DeploymentRecord.DecisionMetadata toDecisionMetadata(
      DecisionRecordValue decision) {
    return Schema.DeploymentRecord.DecisionMetadata.newBuilder()
        .setDecisionId(decision.getDecisionId())
        .setDecisionKey(decision.getDecisionKey())
        .setVersion(decision.getVersion())
        .setDecisionName(decision.getDecisionName())
        .setDecisionRequirementsId(decision.getDecisionRequirementsId())
        .setDecisionRequirementsKey(decision.getDecisionRequirementsKey())
        .setIsDuplicate(decision.isDuplicate())
        .setTenantId(toTenantId(decision))
        .build();
  }

  private static Schema.DeploymentRecord.FormMetadata toFormMetadata(FormMetadataValue form) {
    return Schema.DeploymentRecord.FormMetadata.newBuilder()
        .setFormId(form.getFormId())
        .setVersion(form.getVersion())
        .setFormKey(form.getFormKey())
        .setIsDuplicate(form.isDuplicate())
        .setResourceName(form.getResourceName())
        .setChecksum(ByteString.copyFrom(form.getChecksum()))
        .setTenantId(toTenantId(form))
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
        .setChecksum(ByteString.copyFrom(value.getChecksum()))
        .setMetadata(toMetadata(record))
        .setTenantId(toTenantId(value))
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
        .setTenantId(toTenantId(value))
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
        .setProcessDefinitionKey(value.getProcessDefinitionKey())
        .setTenantId(toTenantId(value));
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

    if (!value.getTenantIds().isEmpty()) {
      builder.addAllTenantIds(value.getTenantIds());
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
        .setTenantId(toTenantId(value))
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
        .setIsInterrupting(value.isInterrupting())
        .setMetadata(toMetadata(record))
        .setTenantId(toTenantId(value))
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
        .setVariables(toStruct(value.getVariables()))
        .setTenantId(toTenantId(value));

    return builder.setMetadata(toMetadata(record)).build();
  }

  private static Schema.VariableRecord toVariableRecord(Record<VariableRecordValue> record) {
    final VariableRecordValue value = record.getValue();
    final Schema.VariableRecord.Builder builder = Schema.VariableRecord.newBuilder();

    builder
        .setScopeKey(value.getScopeKey())
        .setProcessInstanceKey(value.getProcessInstanceKey())
        .setProcessDefinitionKey(value.getProcessDefinitionKey())
        .setBpmnProcessId(value.getBpmnProcessId())
        .setName(value.getName())
        .setValue(value.getValue())
        .setTenantId(toTenantId(value));

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
        .setTenantId(toTenantId(value))
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
        .setBpmnEventType(value.getBpmnEventType().name())
        .setParentProcessInstanceKey(value.getParentProcessInstanceKey())
        .setParentElementInstanceKey(value.getParentElementInstanceKey())
        .setMetadata(toMetadata(record))
        .setTenantId(toTenantId(value))
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
        .setIsInterrupting(value.isInterrupting())
        .setMetadata(toMetadata(record))
        .setTenantId(toTenantId(value))
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
        .setTenantId(toTenantId(value))
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
        .setTenantId(toTenantId(value))
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
        .setTenantId(toTenantId(value))
        .build();
  }

  private static Schema.DecisionRecord toDecisionRecord(Record<DecisionRecordValue> record) {
    final DecisionRecordValue value = record.getValue();

    return Schema.DecisionRecord.newBuilder()
        .setDecisionId(value.getDecisionId())
        .setDecisionKey(value.getDecisionKey())
        .setVersion(value.getVersion())
        .setDecisionName(value.getDecisionName())
        .setDecisionRequirementsId(value.getDecisionRequirementsId())
        .setDecisionRequirementsKey(value.getDecisionRequirementsKey())
        .setIsDuplicate(value.isDuplicate())
        .setMetadata(toMetadata(record))
        .setTenantId(toTenantId(value))
        .build();
  }

  private static Schema.DecisionRequirementsRecord toDecisionRequirementsRecord(
      Record<DecisionRequirementsRecordValue> record) {
    final DecisionRequirementsRecordValue value = record.getValue();

    return Schema.DecisionRequirementsRecord.newBuilder()
        .setResource(ByteString.copyFrom(value.getResource()))
        .setDecisionRequirementsMetadata(toDecisionRequirementsMetadata(value))
        .setMetadata(toMetadata(record))
        .setTenantId(toTenantId(value))
        .build();
  }

  private static Schema.DecisionRequirementsMetadata toDecisionRequirementsMetadata(
      DecisionRequirementsMetadataValue metadataValue) {
    return Schema.DecisionRequirementsMetadata.newBuilder()
        .setDecisionRequirementsId(metadataValue.getDecisionRequirementsId())
        .setDecisionRequirementsName(metadataValue.getDecisionRequirementsName())
        .setDecisionRequirementsVersion(metadataValue.getDecisionRequirementsVersion())
        .setDecisionRequirementsKey(metadataValue.getDecisionRequirementsKey())
        .setNamespace(metadataValue.getNamespace())
        .setResourceName(metadataValue.getResourceName())
        .setChecksum(ByteString.copyFrom(metadataValue.getChecksum()))
        .setIsDuplicate(metadataValue.isDuplicate())
        .setTenantId(toTenantId(metadataValue))
        .build();
  }

  private static Schema.DecisionEvaluationRecord toDecisionEvaluationRecord(
      Record<DecisionEvaluationRecordValue> record) {
    final DecisionEvaluationRecordValue value = record.getValue();

    final Schema.DecisionEvaluationRecord.Builder builder =
        Schema.DecisionEvaluationRecord.newBuilder();

    if (!value.getEvaluatedDecisions().isEmpty()) {
      for (EvaluatedDecisionValue item : value.getEvaluatedDecisions()) {
        builder.addEvaluatedDecisions(toEvaluatedDecision(item));
      }
    }

    return builder
        .setDecisionKey(value.getDecisionKey())
        .setDecisionId(value.getDecisionId())
        .setDecisionName(value.getDecisionName())
        .setDecisionVersion(value.getDecisionVersion())
        .setDecisionRequirementsId(value.getDecisionRequirementsId())
        .setDecisionRequirementsKey(value.getDecisionRequirementsKey())
        .setDecisionOutput(value.getDecisionOutput())
        .setBpmnProcessId(value.getBpmnProcessId())
        .setProcessDefinitionKey(value.getProcessDefinitionKey())
        .setProcessInstanceKey(value.getProcessInstanceKey())
        .setElementId(value.getElementId())
        .setElementInstanceKey(value.getElementInstanceKey())
        .setEvaluationFailureMessage(value.getEvaluationFailureMessage())
        .setFailedDecisionId(value.getFailedDecisionId())
        .setMetadata(toMetadata(record))
        .setTenantId(toTenantId(value))
        .build();
  }

  private static Schema.DecisionEvaluationRecord.EvaluatedDecision toEvaluatedDecision(
      EvaluatedDecisionValue value) {
    final Schema.DecisionEvaluationRecord.EvaluatedDecision.Builder builder =
        Schema.DecisionEvaluationRecord.EvaluatedDecision.newBuilder();
    if (!value.getEvaluatedInputs().isEmpty()) {
      for (EvaluatedInputValue item : value.getEvaluatedInputs()) {
        builder.addEvaluatedInputs(toEvaluatedInput(item));
      }
    }
    if (!value.getMatchedRules().isEmpty()) {
      for (MatchedRuleValue item : value.getMatchedRules()) {
        builder.addMatchedRules(toMatchedRule(item));
      }
    }
    return builder
        .setDecisionId(value.getDecisionId())
        .setDecisionName(value.getDecisionName())
        .setDecisionKey(value.getDecisionKey())
        .setDecisionVersion(value.getDecisionVersion())
        .setDecisionType(value.getDecisionType())
        .setDecisionOutput(value.getDecisionOutput())
        .setTenantId(toTenantId(value))
        .build();
  }

  private static Schema.DecisionEvaluationRecord.MatchedRule toMatchedRule(MatchedRuleValue value) {
    final Schema.DecisionEvaluationRecord.MatchedRule.Builder builder =
        Schema.DecisionEvaluationRecord.MatchedRule.newBuilder();
    if (!value.getEvaluatedOutputs().isEmpty()) {
      for (EvaluatedOutputValue item : value.getEvaluatedOutputs()) {
        builder.addEvaluatedOutputs(toEvaluatedOutput(item));
      }
    }
    return builder.setRuleId(value.getRuleId()).setRuleIndex(value.getRuleIndex()).build();
  }

  private static Schema.DecisionEvaluationRecord.EvaluatedOutput toEvaluatedOutput(
      EvaluatedOutputValue value) {
    return Schema.DecisionEvaluationRecord.EvaluatedOutput.newBuilder()
        .setOutputId(value.getOutputId())
        .setOutputName(value.getOutputName())
        .setOutputValue(value.getOutputValue())
        .build();
  }

  private static Schema.DecisionEvaluationRecord.EvaluatedInput toEvaluatedInput(
      EvaluatedInputValue value) {
    return Schema.DecisionEvaluationRecord.EvaluatedInput.newBuilder()
        .setInputId(value.getInputId())
        .setInputValue(value.getInputValue())
        .setInputName(value.getInputName())
        .build();
  }

  private static Schema.ProcessInstanceModificationRecord toProcessInstanceModificationRecord(
      Record<ProcessInstanceModificationRecordValue> record) {
    final ProcessInstanceModificationRecordValue value = record.getValue();

    final var activateInstructions =
        value.getActivateInstructions().stream()
            .map(
                activateInstruction ->
                    Schema.ProcessInstanceModificationRecord
                        .ProcessInstanceModificationActivateInstruction.newBuilder()
                        .setElementId(activateInstruction.getElementId())
                        .setAncestorScopeKey(activateInstruction.getAncestorScopeKey())
                        .addAllAncestorScopeKeys(activateInstruction.getAncestorScopeKeys())
                        .addAllVariableInstructions(
                            activateInstruction.getVariableInstructions().stream()
                                .map(
                                    variableInstruction ->
                                        Schema.ProcessInstanceModificationRecord
                                            .ProcessInstanceModificationVariableInstruction
                                            .newBuilder()
                                            .setElementId(variableInstruction.getElementId())
                                            .setVariables(
                                                toStruct(variableInstruction.getVariables()))
                                            .build())
                                .collect(Collectors.toList()))
                        .build())
            .collect(Collectors.toList());

    final var terminateInstructions =
        value.getTerminateInstructions().stream()
            .map(
                terminateInstruction ->
                    Schema.ProcessInstanceModificationRecord
                        .ProcessInstanceModificationTerminateInstruction.newBuilder()
                        .setElementInstanceKey(terminateInstruction.getElementInstanceKey())
                        .build())
            .collect(Collectors.toList());

    return Schema.ProcessInstanceModificationRecord.newBuilder()
        .setProcessInstanceKey(value.getProcessInstanceKey())
        .addAllActivateInstructions(activateInstructions)
        .addAllTerminateInstructions(terminateInstructions)
        .setMetadata(toMetadata(record))
        .setTenantId(toTenantId(value))
        .build();
  }

  private static Schema.CheckpointRecord toCheckpointRecord(Record<CheckpointRecordValue> record) {
    final CheckpointRecordValue value = record.getValue();
    return Schema.CheckpointRecord.newBuilder()
        .setId(value.getCheckpointId())
        .setPosition(value.getCheckpointPosition())
        .setMetadata(toMetadata(record))
        .build();
  }

  private static Schema.SignalRecord toSignalRecord(Record<SignalRecordValue> record) {
    final SignalRecordValue value = record.getValue();
    return Schema.SignalRecord.newBuilder()
        .setSignalName(value.getSignalName())
        .setVariables(toStruct(value.getVariables()))
        .setMetadata(toMetadata(record))
        .setTenantId(toTenantId(value))
        .build();
  }

  private static Schema.SignalSubscriptionRecord toSignalSubscriptionRecord(
      Record<SignalSubscriptionRecordValue> record) {
    final var value = record.getValue();
    return Schema.SignalSubscriptionRecord.newBuilder()
        .setSignalName(value.getSignalName())
        .setBpmnProcessId(value.getBpmnProcessId())
        .setProcessDefinitionKey(value.getProcessDefinitionKey())
        .setCatchEventId(value.getCatchEventId())
        .setCatchEventInstanceKey(value.getCatchEventInstanceKey())
        .setMetadata(toMetadata(record))
        .setTenantId(toTenantId(value))
        .build();
  }

  private static Schema.FormRecord toFormRecord(Record<Form> record) {
    final Form value = record.getValue();

    return Schema.FormRecord.newBuilder()
        .setFormId(value.getFormId())
        .setVersion(value.getVersion())
        .setFormKey(value.getFormKey())
        .setResourceName(value.getResourceName())
        .setChecksum(ByteString.copyFrom(value.getChecksum()))
        .setIsDuplicate(value.isDuplicate())
        .setResource(ByteString.copyFrom(value.getResource()))
        .setMetadata(toMetadata(record))
        .setTenantId(toTenantId(value))
        .build();
  }

  private static Schema.ResourceDeletionRecord toResourceDeletionRecord(
      Record<ResourceDeletionRecordValue> record) {
    final var value = record.getValue();

    return Schema.ResourceDeletionRecord.newBuilder()
        .setResourceKey(value.getResourceKey())
        .setMetadata(toMetadata(record))
        .setTenantId(toTenantId(value))
        .build();
  }

  private static Schema.UserTaskRecord toUserTaskRecord(Record<UserTaskRecordValue> record) {
    final var value = record.getValue();

    return Schema.UserTaskRecord.newBuilder()
        .setUserTaskKey(value.getUserTaskKey())
        .setAssignee(value.getAssignee())
        .setDueDate(value.getDueDate())
        .setFollowUpDate(value.getFollowUpDate())
        .setFormKey(value.getFormKey())
        .setVariables(toStruct(value.getVariables()))
        .setBpmnProcessId(value.getBpmnProcessId())
        .setProcessDefinitionVersion(value.getProcessDefinitionVersion())
        .setProcessDefinitionKey(value.getProcessDefinitionKey())
        .setProcessInstanceKey(value.getProcessInstanceKey())
        .setElementId(value.getElementId())
        .setElementInstanceKey(value.getElementInstanceKey())
        .setMetadata(toMetadata(record))
        .setTenantId(toTenantId(value))
        .addAllCandidateGroup(value.getCandidateGroupsList())
        .addAllCandidateUser(value.getCandidateUsersList())
        .setExternalFormReference(value.getExternalFormReference())
        .setCustomHeaders(toStruct(value.getCustomHeaders()))
        .addAllChangedAttribute(value.getChangedAttributes())
        .setAction(value.getAction())
        .setCreationTimestamp(value.getCreationTimestamp())
        // === deprecated properties
        .setCandidateGroups(String.join(",", value.getCandidateGroupsList()))
        .setCandidateUsers(String.join(",", value.getCandidateUsersList()))
        // ===
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

  private static String toTenantId(TenantOwned value) {
    return Optional.ofNullable(value.getTenantId()).orElse(TenantOwned.DEFAULT_TENANT_IDENTIFIER);
  }
}
