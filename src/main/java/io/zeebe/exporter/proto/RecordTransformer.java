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
    TRANSFORMERS.put(ValueType.MESSAGE_BATCH, RecordTransformer::toMessageBatchRecord);
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
    TRANSFORMERS.put(
        ValueType.COMPENSATION_SUBSCRIPTION, RecordTransformer::toCompensationSubscriptionRecord);
    TRANSFORMERS.put(ValueType.ESCALATION, RecordTransformer::toEscalationRecord);
    TRANSFORMERS.put(
        ValueType.PROCESS_INSTANCE_MIGRATION, RecordTransformer::toProcessInstanceMigrationRecord);
    TRANSFORMERS.put(ValueType.CLOCK, RecordTransformer::toClockRecord);
    TRANSFORMERS.put(ValueType.MESSAGE_CORRELATION, RecordTransformer::toMessageCorrelationRecord);
    TRANSFORMERS.put(
        ValueType.PROCESS_INSTANCE_BATCH, RecordTransformer::toProcessInstanceBatchRecord);
    TRANSFORMERS.put(
        ValueType.PROCESS_INSTANCE_RESULT, RecordTransformer::toProcessInstanceResultRecord);
    TRANSFORMERS.put(ValueType.RESOURCE, RecordTransformer::toResourceRecord);
    TRANSFORMERS.put(ValueType.USER, RecordTransformer::toUserRecord);
    TRANSFORMERS.put(ValueType.AUTHORIZATION, RecordTransformer::toAuthorizationRecord);

    VALUE_TYPE_MAPPING.put(ValueType.DEPLOYMENT, RecordMetadata.ValueType.DEPLOYMENT);
    VALUE_TYPE_MAPPING.put(
        ValueType.DEPLOYMENT_DISTRIBUTION, RecordMetadata.ValueType.DEPLOYMENT_DISTRIBUTION);
    VALUE_TYPE_MAPPING.put(ValueType.ERROR, RecordMetadata.ValueType.ERROR);
    VALUE_TYPE_MAPPING.put(ValueType.INCIDENT, RecordMetadata.ValueType.INCIDENT);
    VALUE_TYPE_MAPPING.put(ValueType.JOB, RecordMetadata.ValueType.JOB);
    VALUE_TYPE_MAPPING.put(ValueType.JOB_BATCH, RecordMetadata.ValueType.JOB_BATCH);
    VALUE_TYPE_MAPPING.put(ValueType.MESSAGE, RecordMetadata.ValueType.MESSAGE);
    VALUE_TYPE_MAPPING.put(ValueType.MESSAGE_BATCH, RecordMetadata.ValueType.MESSAGE_BATCH);
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
    VALUE_TYPE_MAPPING.put(
        ValueType.COMPENSATION_SUBSCRIPTION, RecordMetadata.ValueType.COMPENSATION_SUBSCRIPTION);
    VALUE_TYPE_MAPPING.put(
        ValueType.PROCESS_INSTANCE_MIGRATION, RecordMetadata.ValueType.PROCESS_INSTANCE_MIGRATION);
    VALUE_TYPE_MAPPING.put(ValueType.CLOCK, RecordMetadata.ValueType.CLOCK);
    VALUE_TYPE_MAPPING.put(
        ValueType.MESSAGE_CORRELATION, RecordMetadata.ValueType.MESSAGE_CORRELATION);
    VALUE_TYPE_MAPPING.put(
        ValueType.PROCESS_INSTANCE_BATCH, RecordMetadata.ValueType.PROCESS_INSTANCE_BATCH);
    VALUE_TYPE_MAPPING.put(
        ValueType.PROCESS_INSTANCE_RESULT, RecordMetadata.ValueType.PROCESS_INSTANCE_RESULT);
    VALUE_TYPE_MAPPING.put(ValueType.RESOURCE, RecordMetadata.ValueType.RESOURCE);
    VALUE_TYPE_MAPPING.put(ValueType.USER, RecordMetadata.ValueType.USER);
    VALUE_TYPE_MAPPING.put(ValueType.AUTHORIZATION, RecordMetadata.ValueType.AUTHORIZATION);
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
      builder.addProcessesMetadata(toProcessesMetadata(processMetadata));
    }

    for (DecisionRequirementsMetadataValue decisionRequirementsMetadata :
        record.getValue().getDecisionRequirementsMetadata()) {
      builder.addDecisionRequirementsMetadata(
          toDecisionRequirementsMetadata(decisionRequirementsMetadata));
    }

    for (DecisionRecordValue decisionMetadata : record.getValue().getDecisionsMetadata()) {
      builder.addDecisionsMetadata(toDecisionsMetadata(decisionMetadata));
    }

    for (FormMetadataValue formMetadata : record.getValue().getFormMetadata()) {
      builder.addFormMetadata(toFormMetadata(formMetadata));
    }

    for (ResourceMetadataValue resourceMetadata : record.getValue().getResourceMetadata()) {
      builder.addResourceMetadata(toResourceMetadata(resourceMetadata));
    }

    builder.setTenantId(toTenantId(record.getValue()));
    builder.setDeploymentKey(record.getValue().getDeploymentKey());

    return builder.build();
  }

  private static Schema.DeploymentRecord.DeploymentResource toDeploymentRecordResource(
      DeploymentResource resource) {
    return Schema.DeploymentRecord.DeploymentResource.newBuilder()
        .setResource(ByteString.copyFrom(resource.getResource()))
        .setResourceName(resource.getResourceName())
        .build();
  }

  private static Schema.DeploymentRecord.ProcessMetadata toProcessesMetadata(
      ProcessMetadataValue processMetadata) {

    return Schema.DeploymentRecord.ProcessMetadata.newBuilder()
        .setBpmnProcessId(processMetadata.getBpmnProcessId())
        .setResourceName(processMetadata.getResourceName())
        .setVersion(processMetadata.getVersion())
        .setProcessDefinitionKey(processMetadata.getProcessDefinitionKey())
        .setChecksum(ByteString.copyFrom(processMetadata.getChecksum()))
        .setIsDuplicate(processMetadata.isDuplicate())
        .setTenantId(toTenantId(processMetadata))
        .setOrClearVersionTag(processMetadata.getVersionTag())
        .setDeploymentKey(processMetadata.getDeploymentKey())
        .build();
  }

  private static Schema.DeploymentRecord.DecisionMetadata toDecisionsMetadata(
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
        .setOrClearVersionTag(decision.getVersionTag())
        .setDeploymentKey(decision.getDeploymentKey())
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
        .setOrClearVersionTag(form.getVersionTag())
        .setDeploymentKey(form.getDeploymentKey())
        .build();
  }

  private static Schema.DeploymentRecord.ResourceMetadata toResourceMetadata(
      ResourceMetadataValue resourceMetadata) {
    return Schema.DeploymentRecord.ResourceMetadata.newBuilder()
        .setResourceId(resourceMetadata.getResourceId())
        .setVersion(resourceMetadata.getVersion())
        .setVersionTag(resourceMetadata.getVersionTag())
        .setResourceKey(resourceMetadata.getResourceKey())
        .setChecksum(ByteString.copyFrom(resourceMetadata.getChecksum()))
        .setResourceName(resourceMetadata.getResourceName())
        .setIsDuplicate(resourceMetadata.isDuplicate())
        .setDeploymentKey(resourceMetadata.getDeploymentKey())
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

  private static final EnumMap<JobKind, Schema.JobRecord.JobKind> JOB_KIND_MAPPING =
      new EnumMap<>(
          Map.of(
              JobKind.BPMN_ELEMENT,
              Schema.JobRecord.JobKind.BPMN_ELEMENT,
              JobKind.EXECUTION_LISTENER,
              Schema.JobRecord.JobKind.EXECUTION_LISTENER,
              JobKind.TASK_LISTENER,
              Schema.JobRecord.JobKind.TASK_LISTENER));

  private static Schema.JobRecord.JobKind toJobKind(JobKind jobKind) {
    return JOB_KIND_MAPPING.getOrDefault(jobKind, Schema.JobRecord.JobKind.UNKNOWN_JOB_KIND);
  }

  private static final EnumMap<JobListenerEventType, Schema.JobRecord.JobListenerEventType>
      JOB_LISTENER_EVENT_TYPE_MAPPING =
          new EnumMap<>(
              Map.of(
                  JobListenerEventType.START,
                  Schema.JobRecord.JobListenerEventType.START,
                  JobListenerEventType.END,
                  Schema.JobRecord.JobListenerEventType.END));

  private static Schema.JobRecord.JobListenerEventType toJobListenerEventType(
      JobListenerEventType jobListenerEventType) {
    return JOB_LISTENER_EVENT_TYPE_MAPPING.getOrDefault(
        jobListenerEventType, Schema.JobRecord.JobListenerEventType.UNSPECIFIED);
  }

  private static Schema.JobRecord.Builder toJobRecord(JobRecordValue value) {
    return Schema.JobRecord.newBuilder()
        .setDeadline(value.getDeadline())
        .setOrClearErrorMessage(value.getErrorMessage())
        .setRetries(value.getRetries())
        .setType(value.getType())
        .setWorker(value.getWorker())
        .setVariables(toStruct(value.getVariables()))
        .setCustomHeaders(toStruct(value.getCustomHeaders()))
        .setBpmnProcessId(value.getBpmnProcessId())
        .setElementId(value.getElementId())
        .setElementInstanceKey(value.getElementInstanceKey())
        .setProcessDefinitionVersion(value.getProcessDefinitionVersion())
        .setProcessInstanceKey(value.getProcessInstanceKey())
        .setProcessDefinitionKey(value.getProcessDefinitionKey())
        .setTenantId(toTenantId(value))
        .setOrClearErrorCode(value.getErrorCode())
        .setRecurringTime(value.getRecurringTime())
        .setRetryBackoff(value.getRetryBackoff())
        .setTimeout(value.getTimeout())
        .addAllChangedAttributes(value.getChangedAttributes())
        .setJobKind(toJobKind(value.getJobKind()))
        .setJobListenerEventType(toJobListenerEventType(value.getJobListenerEventType()));
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
        .setDeadline(value.getDeadline())
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

    final var startInstructions =
        value.getStartInstructions().stream()
            .map(
                startInstruction ->
                    Schema.ProcessInstanceCreationRecord.ProcessInstanceCreationStartInstruction
                        .newBuilder()
                        .setElementId(startInstruction.getElementId())
                        .build())
            .collect(Collectors.toList());

    return Schema.ProcessInstanceCreationRecord.newBuilder()
        .setBpmnProcessId(value.getBpmnProcessId())
        .setVersion(value.getVersion())
        .setProcessInstanceKey(value.getProcessInstanceKey())
        .setProcessDefinitionKey(value.getProcessDefinitionKey())
        .setVariables(toStruct(value.getVariables()))
        .setMetadata(toMetadata(record))
        .setTenantId(toTenantId(value))
        .addAllStartInstructions(startInstructions)
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
        .setDeploymentKey(value.getDeploymentKey())
        .setOrClearVersionTag(value.getVersionTag())
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
        .setCheckpointId(value.getCheckpointId())
        .setCheckpointPosition(value.getCheckpointPosition())
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
        .addAllCandidateGroups(value.getCandidateGroupsList())
        .addAllCandidateUsers(value.getCandidateUsersList())
        .setExternalFormReference(value.getExternalFormReference())
        .setCustomHeaders(toStruct(value.getCustomHeaders()))
        .addAllChangedAttributes(value.getChangedAttributes())
        .setAction(value.getAction())
        .setCreationTimestamp(value.getCreationTimestamp())
        .setPriority(value.getPriority())
        .build();
  }

  private static Schema.CompensationSubscriptionRecord toCompensationSubscriptionRecord(
      Record<CompensationSubscriptionRecordValue> record) {
    final var value = record.getValue();

    return Schema.CompensationSubscriptionRecord.newBuilder()
        .setMetadata(toMetadata(record))
        .setTenantId(value.getTenantId())
        .setProcessInstanceKey(value.getProcessInstanceKey())
        .setProcessDefinitionKey(value.getProcessDefinitionKey())
        .setCompensableActivityId(value.getCompensableActivityId())
        .setThrowEventId(value.getThrowEventId())
        .setThrowEventInstanceKey(value.getThrowEventInstanceKey())
        .setCompensationHandlerId(value.getCompensationHandlerId())
        .setCompensationHandlerInstanceKey(value.getCompensationHandlerInstanceKey())
        .setCompensableActivityScopeKey(value.getCompensableActivityScopeKey())
        .setCompensableActivityInstanceKey(value.getCompensableActivityInstanceKey())
        .setVariables(toStruct(value.getVariables()))
        .build();
  }

  private static Schema.EscalationRecord toEscalationRecord(Record<EscalationRecordValue> record) {
    final var value = record.getValue();

    return Schema.EscalationRecord.newBuilder()
        .setMetadata(toMetadata(record))
        .setProcessInstanceKey(value.getProcessInstanceKey())
        .setEscalationCode(value.getEscalationCode())
        .setThrowElementId(value.getThrowElementId())
        .setCatchElementId(value.getCatchElementId())
        .build();
  }

  private static Schema.ProcessInstanceMigrationRecord toProcessInstanceMigrationRecord(
      Record<ProcessInstanceMigrationRecordValue> record) {
    final var value = record.getValue();

    var builder = Schema.ProcessInstanceMigrationRecord.newBuilder();

    value
        .getMappingInstructions()
        .forEach(
            instruction -> {
              builder.addMappingInstructions(
                  toProcessInstanceMigrationMappingInstructionRecord(instruction));
            });

    return builder
        .setMetadata(toMetadata(record))
        .setProcessInstanceKey(value.getProcessInstanceKey())
        .setTargetProcessDefinitionKey(value.getTargetProcessDefinitionKey())
        .build();
  }

  private static Schema.ProcessInstanceMigrationRecord.ProcessInstanceMigrationMappingInstruction
      toProcessInstanceMigrationMappingInstructionRecord(
          ProcessInstanceMigrationRecordValue.ProcessInstanceMigrationMappingInstructionValue
              mappingInstructionValue) {
    return Schema.ProcessInstanceMigrationRecord.ProcessInstanceMigrationMappingInstruction
        .newBuilder()
        .setSourceElementId(mappingInstructionValue.getSourceElementId())
        .setTargetElementId(mappingInstructionValue.getTargetElementId())
        .build();
  }

  private static Schema.MessageBatchRecord toMessageBatchRecord(
      Record<MessageBatchRecordValue> record) {
    final var value = record.getValue();

    var builder = Schema.MessageBatchRecord.newBuilder();

    value.getMessageKeys().forEach(builder::addMessageKeys);
    return builder.setMetadata(toMetadata(record)).build();
  }

  private static Schema.ClockRecord toClockRecord(Record<ClockRecordValue> record) {
    final var value = record.getValue();

    return Schema.ClockRecord.newBuilder()
        .setMetadata(toMetadata(record))
        .setTime(value.getTime())
        .build();
  }

  private static Schema.MessageCorrelationRecord toMessageCorrelationRecord(
      Record<MessageCorrelationRecordValue> record) {
    final var value = record.getValue();

    return Schema.MessageCorrelationRecord.newBuilder()
        .setMetadata(toMetadata(record))
        .setName(value.getName())
        .setCorrelationKey(value.getCorrelationKey())
        .setMessageKey(value.getMessageKey())
        .setRequestId(value.getRequestId())
        .setRequestStreamId(value.getRequestStreamId())
        .build();
  }

  private static Schema.ProcessInstanceResultRecord toProcessInstanceResultRecord(
      Record<ProcessInstanceResultRecordValue> record) {
    final ProcessInstanceResultRecordValue value = record.getValue();

    return Schema.ProcessInstanceResultRecord.newBuilder()
        .setBpmnProcessId(value.getBpmnProcessId())
        .setVersion(value.getVersion())
        .setProcessInstanceKey(value.getProcessInstanceKey())
        .setProcessDefinitionKey(value.getProcessDefinitionKey())
        .setVariables(toStruct(value.getVariables()))
        .setMetadata(toMetadata(record))
        .setTenantId(toTenantId(value))
        .build();
  }

  private static Schema.ProcessInstanceBatchRecord toProcessInstanceBatchRecord(
      Record<ProcessInstanceBatchRecordValue> record) {
    final ProcessInstanceBatchRecordValue value = record.getValue();

    return Schema.ProcessInstanceBatchRecord.newBuilder()
        .setProcessInstanceKey(value.getProcessInstanceKey())
        .setBatchElementInstanceKey(value.getBatchElementInstanceKey())
        .setIndex(value.getIndex())
        .setMetadata(toMetadata(record))
        .setTenantId(toTenantId(value))
        .build();
  }

  private static Schema.ResourceRecord toResourceRecord(Record<Resource> record) {
    final var value = record.getValue();

    return Schema.ResourceRecord.newBuilder()
        .setMetadata(toMetadata(record))
        .setResourceId(value.getResourceId())
        .setVersion(value.getVersion())
        .setVersionTag(value.getVersionTag())
        .setResourceKey(value.getResourceKey())
        .setChecksum(ByteString.copyFrom(value.getChecksum()))
        .setResourceName(value.getResourceName())
        .setIsDuplicate(value.isDuplicate())
        .setDeploymentKey(value.getDeploymentKey())
        .setTenantId(toTenantId(value))
        .setResourceProp(value.getResourceProp())
        .build();
  }

  private static final EnumMap<UserType, Schema.UserRecord.UserType> USER_TYPE_MAPPING =
      new EnumMap<>(
          Map.of(
              UserType.DEFAULT,
              Schema.UserRecord.UserType.DEFAULT,
              UserType.REGULAR,
              Schema.UserRecord.UserType.REGULAR));

  private static Schema.UserRecord.UserType toUserType(UserType userType) {
    return USER_TYPE_MAPPING.getOrDefault(userType, Schema.UserRecord.UserType.UNKNOWN_USER_TYPE);
  }

  private static Schema.UserRecord toUserRecord(Record<UserRecordValue> record) {
    final var value = record.getValue();
    return Schema.UserRecord.newBuilder()
        .setMetadata(toMetadata(record))
        .setUserKey(value.getUserKey())
        .setUsername(value.getUsername())
        .setName(value.getName())
        .setEmail(value.getEmail())
        .setPassword(value.getPassword())
        .setUserType(toUserType(value.getUserType()))
        .build();
  }

  private static final EnumMap<PermissionAction, Schema.AuthorizationRecord.PermissionAction>
      PERMISSION_ACTION_MAPPING =
          new EnumMap<>(
              Map.of(
                  PermissionAction.ADD,
                  Schema.AuthorizationRecord.PermissionAction.ADD,
                  PermissionAction.REMOVE,
                  Schema.AuthorizationRecord.PermissionAction.REMOVE));

  private static Schema.AuthorizationRecord.PermissionAction toPermissionAction(
      PermissionAction action) {
    return PERMISSION_ACTION_MAPPING.getOrDefault(
        action, Schema.AuthorizationRecord.PermissionAction.UNKNOWN_ACTION);
  }

  private static final EnumMap<
          AuthorizationOwnerType, Schema.AuthorizationRecord.AuthorizationOwnerType>
      AUTHORIZATION_OWNER_TYPE_MAPPING =
          new EnumMap<>(
              Map.of(
                  AuthorizationOwnerType.GROUP,
                  Schema.AuthorizationRecord.AuthorizationOwnerType._GROUP,
                  AuthorizationOwnerType.USER,
                  Schema.AuthorizationRecord.AuthorizationOwnerType._USER,
                  AuthorizationOwnerType.ROLE,
                  Schema.AuthorizationRecord.AuthorizationOwnerType._ROLE));

  private static Schema.AuthorizationRecord.AuthorizationOwnerType toAuthorizationOwnerType(
      AuthorizationOwnerType authorizationOwnerType) {
    return AUTHORIZATION_OWNER_TYPE_MAPPING.getOrDefault(
        authorizationOwnerType, Schema.AuthorizationRecord.AuthorizationOwnerType._UNSPECIFIED);
  }

  private static final EnumMap<
          AuthorizationResourceType, Schema.AuthorizationRecord.AuthorizationResourceType>
      AUTHORIZATION_RESOURCE_TYPE_MAPPING =
          new EnumMap<>(
              Map.ofEntries(
                  Map.entry(
                      AuthorizationResourceType.AUTHORIZATION,
                      Schema.AuthorizationRecord.AuthorizationResourceType.AUTHORIZATION),
                  Map.entry(
                      AuthorizationResourceType.MESSAGE,
                      Schema.AuthorizationRecord.AuthorizationResourceType.MESSAGE),
                  Map.entry(
                      AuthorizationResourceType.JOB,
                      Schema.AuthorizationRecord.AuthorizationResourceType.JOB),
                  Map.entry(
                      AuthorizationResourceType.APPLICATION,
                      Schema.AuthorizationRecord.AuthorizationResourceType.APPLICATION),
                  Map.entry(
                      AuthorizationResourceType.TENANT,
                      Schema.AuthorizationRecord.AuthorizationResourceType.TENANT),
                  Map.entry(
                      AuthorizationResourceType.DEPLOYMENT,
                      Schema.AuthorizationRecord.AuthorizationResourceType.DEPLOYMENT),
                  Map.entry(
                      AuthorizationResourceType.PROCESS_DEFINITION,
                      Schema.AuthorizationRecord.AuthorizationResourceType.PROCESS_DEFINITION),
                  Map.entry(
                      AuthorizationResourceType.USER_TASK,
                      Schema.AuthorizationRecord.AuthorizationResourceType.USER_TASK),
                  Map.entry(
                      AuthorizationResourceType.DECISION_REQUIREMENTS_DEFINITION,
                      Schema.AuthorizationRecord.AuthorizationResourceType
                          .DECISION_REQUIREMENTS_DEFINITION),
                  Map.entry(
                      AuthorizationResourceType.DECISION_DEFINITION,
                      Schema.AuthorizationRecord.AuthorizationResourceType.DECISION_DEFINITION),
                  Map.entry(
                      AuthorizationResourceType.USER_GROUP,
                      Schema.AuthorizationRecord.AuthorizationResourceType.USER_GROUP),
                  Map.entry(
                      AuthorizationResourceType.USER,
                      Schema.AuthorizationRecord.AuthorizationResourceType.USER),
                  Map.entry(
                      AuthorizationResourceType.ROLE,
                      Schema.AuthorizationRecord.AuthorizationResourceType.ROLE)));

  private static Schema.AuthorizationRecord.AuthorizationResourceType toAuthorizationResourceType(
      AuthorizationResourceType authorizationResourceType) {
    return AUTHORIZATION_RESOURCE_TYPE_MAPPING.getOrDefault(
        authorizationResourceType,
        Schema.AuthorizationRecord.AuthorizationResourceType.UNSPECIFIED);
  }

  private static final EnumMap<
          PermissionType, Schema.AuthorizationRecord.PermissionValue.PermissionType>
      PERMISSION_TYPE_MAPPING =
          new EnumMap<>(
              Map.of(
                  PermissionType.CREATE,
                  Schema.AuthorizationRecord.PermissionValue.PermissionType.CREATE,
                  PermissionType.READ,
                  Schema.AuthorizationRecord.PermissionValue.PermissionType.READ,
                  PermissionType.UPDATE,
                  Schema.AuthorizationRecord.PermissionValue.PermissionType.UPDATE,
                  PermissionType.DELETE,
                  Schema.AuthorizationRecord.PermissionValue.PermissionType.DELETE));

  private static Schema.AuthorizationRecord.PermissionValue.PermissionType toPermissionType(
      PermissionType permissionType) {
    return PERMISSION_TYPE_MAPPING.getOrDefault(
        permissionType, Schema.AuthorizationRecord.PermissionValue.PermissionType.UNSPECIFIED);
  }

  private static Schema.AuthorizationRecord toAuthorizationRecord(
      Record<AuthorizationRecordValue> record) {
    final var value = record.getValue();

    var builder = Schema.AuthorizationRecord.newBuilder().setMetadata(toMetadata(record));

    for (AuthorizationRecordValue.PermissionValue permission : value.getPermissions()) {
      builder.addPermissions(toPermissionValue(permission));
    }

    return builder
        .setAction(toPermissionAction(value.getAction()))
        .setOwnerKey(value.getOwnerKey())
        .setOwnerType(toAuthorizationOwnerType(value.getOwnerType()))
        .setResourceType(toAuthorizationResourceType(value.getResourceType()))
        .build();
  }

  private static Schema.AuthorizationRecord.PermissionValue toPermissionValue(
      AuthorizationRecordValue.PermissionValue permissionValue) {
    return Schema.AuthorizationRecord.PermissionValue.newBuilder()
        .setPermissionType(toPermissionType(permissionValue.getPermissionType()))
        .addAllResourceIds(permissionValue.getResourceIds())
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
