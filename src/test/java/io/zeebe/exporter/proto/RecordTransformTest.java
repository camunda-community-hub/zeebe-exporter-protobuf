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

import static java.util.function.Predicate.not;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.RecordValue;
import io.camunda.zeebe.protocol.record.RejectionType;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.intent.*;
import io.camunda.zeebe.protocol.record.intent.management.CheckpointIntent;
import io.camunda.zeebe.protocol.record.value.*;
import io.camunda.zeebe.protocol.record.value.deployment.*;
import io.camunda.zeebe.protocol.record.value.deployment.Process;
import io.camunda.zeebe.protocol.record.value.management.CheckpointRecordValue;
import io.zeebe.exporter.proto.Schema.JobRecord;
import io.zeebe.exporter.proto.Schema.RecordMetadata;
import io.zeebe.exporter.proto.Schema.VariableDocumentRecord.UpdateSemantics;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Test;

public class RecordTransformTest {

  public static final long KEY = 200;
  public static final int PARTITION_ID = 1;
  public static final long POSITION = 300L;
  public static final long TIMESTAMP = 1000L;
  public static final long SOURCE_POSITION = 100L;
  public static final Map<String, Object> VARIABLES = Map.of("var", "v1");
  public static final String TENANT_ID = "tenant-42";

  @Test
  public void shouldTransformDeployment() {
    // given
    final DeploymentRecordValue deploymentRecordValue = mockDeploymentRecordValue();
    final Record<DeploymentRecordValue> mockedRecord =
        mockRecord(deploymentRecordValue, ValueType.DEPLOYMENT, DeploymentIntent.CREATE);

    // when
    final Schema.DeploymentRecord deployment =
        (Schema.DeploymentRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(deployment.getMetadata(), "DEPLOYMENT", "CREATE");
    assertThat(deployment.getTenantId()).isEqualTo(TENANT_ID);
    assertThat(deployment.getDeploymentKey()).isEqualTo(12L);

    final List<Schema.DeploymentRecord.DeploymentResource> resourcesList =
        deployment.getResourcesList();
    assertThat(resourcesList).hasSize(1);

    final Schema.DeploymentRecord.DeploymentResource resource = resourcesList.get(0);
    assertThat(resource.getResource().toStringUtf8()).isEqualTo("resourceContent");
    assertThat(resource.getResourceName()).isEqualTo("process.bpmn");

    final List<Schema.DeploymentRecord.ProcessMetadata> processMetadataList =
        deployment.getProcessesMetadataList();
    assertThat(processMetadataList).hasSize(1);

    final Schema.DeploymentRecord.ProcessMetadata processMetadata = processMetadataList.get(0);
    assertThat(processMetadata.getBpmnProcessId()).isEqualTo("process");
    assertThat(processMetadata.getResourceName()).isEqualTo("process.bpmn");
    assertThat(processMetadata.getProcessDefinitionKey()).isEqualTo(4L);
    assertThat(processMetadata.getVersion()).isEqualTo(1);
    assertThat(processMetadata.getChecksum().toByteArray()).isEqualTo("checksum".getBytes());
    assertThat(processMetadata.getIsDuplicate()).isFalse();
    assertThat(processMetadata.getTenantId()).isEqualTo(TENANT_ID);
    assertThat(processMetadata.getDeploymentKey()).isEqualTo(7L);
    assertThat(processMetadata.getVersionTag()).isEqualTo("v7");

    final var decisionRequirementsMetadataList = deployment.getDecisionRequirementsMetadataList();
    assertThat(decisionRequirementsMetadataList).hasSize(1);

    final var transformedDecisionRequirementsMetadata = decisionRequirementsMetadataList.get(0);
    final var decisionRequirementsMetadata =
        deploymentRecordValue.getDecisionRequirementsMetadata().get(0);

    assertThat(transformedDecisionRequirementsMetadata.getDecisionRequirementsKey())
        .isEqualTo(decisionRequirementsMetadata.getDecisionRequirementsKey());
    assertThat(transformedDecisionRequirementsMetadata.getDecisionRequirementsId())
        .isEqualTo(decisionRequirementsMetadata.getDecisionRequirementsId());
    assertThat(transformedDecisionRequirementsMetadata.getDecisionRequirementsName())
        .isEqualTo(decisionRequirementsMetadata.getDecisionRequirementsName());
    assertThat(transformedDecisionRequirementsMetadata.getDecisionRequirementsVersion())
        .isEqualTo(decisionRequirementsMetadata.getDecisionRequirementsVersion());
    assertThat(transformedDecisionRequirementsMetadata.getNamespace())
        .isEqualTo(decisionRequirementsMetadata.getNamespace());
    assertThat(transformedDecisionRequirementsMetadata.getResourceName())
        .isEqualTo(decisionRequirementsMetadata.getResourceName());
    assertThat(transformedDecisionRequirementsMetadata.getChecksum().toByteArray())
        .isEqualTo(decisionRequirementsMetadata.getChecksum());
    assertThat(transformedDecisionRequirementsMetadata.getIsDuplicate())
        .isEqualTo(decisionRequirementsMetadata.isDuplicate());
    assertThat(transformedDecisionRequirementsMetadata.getTenantId())
        .isEqualTo(decisionRequirementsMetadata.getTenantId());

    final var decisionMetadataList = deployment.getDecisionsMetadataList();
    assertThat(decisionMetadataList).hasSize(1);

    final var transformedDecisionMetadata = decisionMetadataList.get(0);
    final var decisionMetadata = deploymentRecordValue.getDecisionsMetadata().get(0);

    assertThat(transformedDecisionMetadata.getDecisionRequirementsKey())
        .isEqualTo(decisionMetadata.getDecisionRequirementsKey());
    assertThat(transformedDecisionMetadata.getDecisionRequirementsId())
        .isEqualTo(decisionMetadata.getDecisionRequirementsId());
    assertThat(transformedDecisionMetadata.getVersion()).isEqualTo(decisionMetadata.getVersion());
    assertThat(transformedDecisionMetadata.getDecisionId())
        .isEqualTo(decisionMetadata.getDecisionId());
    assertThat(transformedDecisionMetadata.getDecisionName())
        .isEqualTo(decisionMetadata.getDecisionName());
    assertThat(transformedDecisionMetadata.getDecisionKey())
        .isEqualTo(decisionMetadata.getDecisionKey());
    assertThat(transformedDecisionMetadata.getIsDuplicate())
        .isEqualTo(decisionMetadata.isDuplicate());
    assertThat(transformedDecisionMetadata.getTenantId()).isEqualTo(decisionMetadata.getTenantId());
    assertThat(transformedDecisionMetadata.getDeploymentKey()).isEqualTo(7L);
    assertThat(transformedDecisionMetadata.getVersionTag()).isEqualTo("v7");

    final var formMetadataList = deployment.getFormMetadataList();
    assertThat(formMetadataList).hasSize(1);

    final var transformedFormMetadata = formMetadataList.get(0);
    final var formMetadata = deploymentRecordValue.getFormMetadata().get(0);

    assertThat(transformedFormMetadata.getFormId()).isEqualTo(formMetadata.getFormId());
    assertThat(transformedFormMetadata.getVersion()).isEqualTo(formMetadata.getVersion());
    assertThat(transformedFormMetadata.getFormKey()).isEqualTo(formMetadata.getFormKey());
    assertThat(transformedFormMetadata.getResourceName()).isEqualTo(formMetadata.getResourceName());
    assertThat(transformedFormMetadata.getChecksum().toByteArray())
        .isEqualTo(formMetadata.getChecksum());
    assertThat(transformedFormMetadata.getIsDuplicate()).isEqualTo(formMetadata.isDuplicate());
    assertThat(transformedFormMetadata.getTenantId()).isEqualTo(formMetadata.getTenantId());
    assertThat(transformedFormMetadata.getDeploymentKey()).isEqualTo(7L);
    assertThat(transformedFormMetadata.getVersionTag()).isEqualTo("v7");

    assertThat(deployment.getResourceMetadataList()).hasSize(1);
    final var transfomredResourceMetadata = deployment.getResourceMetadata(0);
    assertThat(transfomredResourceMetadata.getResourceId()).isEqualTo("resourceId");
    assertThat(transfomredResourceMetadata.getResourceName()).isEqualTo("resourceName");
    assertThat(transfomredResourceMetadata.getResourceKey()).isEqualTo(1L);
    assertThat(transfomredResourceMetadata.getVersion()).isEqualTo(42);
    assertThat(transfomredResourceMetadata.getVersionTag()).isEqualTo("v7");
    assertThat(transfomredResourceMetadata.getDeploymentKey()).isEqualTo(7L);
    assertThat(transfomredResourceMetadata.getChecksum().toByteArray())
        .isEqualTo("checksum".getBytes());
    assertThat(transfomredResourceMetadata.getIsDuplicate()).isFalse();
  }

  @Test
  public void shouldTransformDeploymentDistribution() {
    // given
    final DeploymentDistributionRecordValue deploymentDistributionRecordValue =
        mockDeploymentDistributionRecordValue();
    final Record<DeploymentDistributionRecordValue> mockedRecord =
        mockRecord(
            deploymentDistributionRecordValue,
            ValueType.DEPLOYMENT_DISTRIBUTION,
            DeploymentDistributionIntent.DISTRIBUTING);

    // when
    final Schema.DeploymentDistributionRecord deploymentDistribution =
        (Schema.DeploymentDistributionRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(deploymentDistribution.getMetadata(), "DEPLOYMENT_DISTRIBUTION", "DISTRIBUTING");

    assertThat(deploymentDistribution.getPartitionId())
        .isEqualTo(deploymentDistributionRecordValue.getPartitionId());
  }

  @Test
  public void shouldTransformProcess() {
    // given
    final var recordValue = mockProcessRecordValue();
    final Record<Process> mockedRecord =
        mockRecord(recordValue, ValueType.PROCESS, ProcessIntent.CREATED);

    // when
    final var transformedRecord =
        (Schema.ProcessRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(transformedRecord.getMetadata(), "PROCESS", "CREATED");

    assertThat(transformedRecord.getResource().toStringUtf8()).isEqualTo("resourceContent");
    assertThat(transformedRecord.getResourceName()).isEqualTo(recordValue.getResourceName());
    assertThat(transformedRecord.getBpmnProcessId()).isEqualTo(recordValue.getBpmnProcessId());
    assertThat(transformedRecord.getChecksum().toByteArray()).isEqualTo("checksum".getBytes());
    assertThat(transformedRecord.getProcessDefinitionKey())
        .isEqualTo(recordValue.getProcessDefinitionKey());
    assertThat(transformedRecord.getVersion()).isEqualTo(recordValue.getVersion());
  }

  @Test
  public void shouldTransformWorkflowInstance() {
    // given
    final ProcessInstanceRecordValue workflowInstanceRecordValue = mockProcessInstanceRecordValue();
    final Record<ProcessInstanceRecordValue> mockedRecord =
        mockRecord(
            workflowInstanceRecordValue,
            ValueType.PROCESS_INSTANCE,
            ProcessInstanceIntent.ELEMENT_ACTIVATED);

    // when
    final Schema.ProcessInstanceRecord workflowInstance =
        (Schema.ProcessInstanceRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(workflowInstance.getMetadata(), "PROCESS_INSTANCE", "ELEMENT_ACTIVATED");

    assertThat(workflowInstance.getBpmnProcessId()).isEqualTo("process");
    assertThat(workflowInstance.getElementId()).isEqualTo("startEvent");
    assertThat(workflowInstance.getProcessDefinitionKey()).isEqualTo(4L);
    assertThat(workflowInstance.getVersion()).isEqualTo(1);
    assertThat(workflowInstance.getProcessInstanceKey()).isEqualTo(1L);
    assertThat(workflowInstance.getFlowScopeKey()).isEqualTo(-1L);
    assertThat(workflowInstance.getBpmnElementType()).isEqualTo("START_EVENT");
    assertThat(workflowInstance.getBpmnEventType()).isEqualTo("NONE");
    assertThat(workflowInstance.getParentProcessInstanceKey()).isEqualTo(-1L);
    assertThat(workflowInstance.getParentElementInstanceKey()).isEqualTo(-1L);
    assertThat(workflowInstance.getTenantId()).isEqualTo(TENANT_ID);
  }

  @Test
  public void shouldTransformWorkflowInstanceCreation() {
    // given
    final ProcessInstanceCreationRecordValue workflowInstanceCreationRecordValue =
        mockProcessInstanceCreationRecordValue();
    final Record<ProcessInstanceCreationRecordValue> mockedRecord =
        mockRecord(
            workflowInstanceCreationRecordValue,
            ValueType.PROCESS_INSTANCE_CREATION,
            ProcessInstanceCreationIntent.CREATED);

    // when
    final Schema.ProcessInstanceCreationRecord workflowInstanceCreation =
        (Schema.ProcessInstanceCreationRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(workflowInstanceCreation.getMetadata(), "PROCESS_INSTANCE_CREATION", "CREATED");

    assertThat(workflowInstanceCreation.getBpmnProcessId()).isEqualTo("process");
    assertThat(workflowInstanceCreation.getProcessDefinitionKey())
        .isEqualTo(workflowInstanceCreationRecordValue.getProcessDefinitionKey());
    assertThat(workflowInstanceCreation.getVersion()).isEqualTo(1);
    assertThat(workflowInstanceCreation.getProcessInstanceKey()).isEqualTo(1L);
    assertThat(workflowInstanceCreation.getTenantId()).isEqualTo(TENANT_ID);
    assertStruct(
        workflowInstanceCreation.getVariables(),
        workflowInstanceCreationRecordValue.getVariables());
    assertThat(workflowInstanceCreation.getStartInstructionsList()).hasSize(1);
    assertThat(workflowInstanceCreation.getStartInstructionsList().get(0).getElementId())
        .isEqualTo("startId");
  }

  @Test
  public void shouldTransformProcessInstanceResult() {
    // given
    final ProcessInstanceResultRecordValue processInstanceCreationRecordValue =
        mockProcessInstanceResultRecordValue();
    final Record<ProcessInstanceResultRecordValue> mockedRecord =
        mockRecord(
            processInstanceCreationRecordValue,
            ValueType.PROCESS_INSTANCE_RESULT,
            ProcessInstanceResultIntent.COMPLETED);

    // when
    final Schema.ProcessInstanceResultRecord processInstanceResult =
        (Schema.ProcessInstanceResultRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(processInstanceResult.getMetadata(), "PROCESS_INSTANCE_RESULT", "COMPLETED");

    assertThat(processInstanceResult.getBpmnProcessId()).isEqualTo("process");
    assertThat(processInstanceResult.getProcessDefinitionKey())
        .isEqualTo(processInstanceCreationRecordValue.getProcessDefinitionKey());
    assertThat(processInstanceResult.getVersion()).isEqualTo(1);
    assertThat(processInstanceResult.getProcessInstanceKey()).isEqualTo(1L);
    assertThat(processInstanceResult.getTenantId()).isEqualTo(TENANT_ID);
    assertStruct(
        processInstanceResult.getVariables(), processInstanceCreationRecordValue.getVariables());
  }

  @Test
  public void shouldTransformProcessInstanceBatch() {
    // given
    final ProcessInstanceBatchRecordValue processInstanceBatchRecordValue =
        mockProcessInstanceBatchRecordValue();
    final Record<ProcessInstanceBatchRecordValue> mockedRecord =
        mockRecord(
            processInstanceBatchRecordValue,
            ValueType.PROCESS_INSTANCE_BATCH,
            ProcessInstanceBatchIntent.TERMINATED);

    // when
    final Schema.ProcessInstanceBatchRecord processInstanceBatch =
        (Schema.ProcessInstanceBatchRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(processInstanceBatch.getMetadata(), "PROCESS_INSTANCE_BATCH", "TERMINATED");
    assertThat(processInstanceBatch.getProcessInstanceKey()).isEqualTo(1L);
    assertThat(processInstanceBatch.getTenantId()).isEqualTo(TENANT_ID);
    assertThat(processInstanceBatch.getBatchElementInstanceKey())
        .isEqualTo(processInstanceBatchRecordValue.getBatchElementInstanceKey());
    assertThat(processInstanceBatch.getIndex())
        .isEqualTo(processInstanceBatchRecordValue.getIndex());
  }

  @Test
  public void shouldTransformJob() {
    // given
    final JobRecordValue jobRecordValue = mockJobRecordValue();
    final Record<JobRecordValue> mockedRecord =
        mockRecord(jobRecordValue, ValueType.JOB, JobIntent.CREATED);

    // when
    final Schema.JobRecord jobRecord =
        (Schema.JobRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(jobRecord.getMetadata(), "JOB", "CREATED");
    assertJobRecord(jobRecord);
  }

  @Test
  public void shouldTransformJobBatch() {
    // given
    final JobBatchRecordValue jobBatchRecordValue = mockJobBatchRecordValue();
    final Record<JobBatchRecordValue> mockedRecord =
        mockRecord(jobBatchRecordValue, ValueType.JOB_BATCH, JobBatchIntent.ACTIVATED);

    // when
    final Schema.JobBatchRecord jobBatchRecord =
        (Schema.JobBatchRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(jobBatchRecord.getMetadata(), "JOB_BATCH", "ACTIVATED");

    assertThat(jobBatchRecord.getJobKeysList()).containsExactly(5L);
    assertThat(jobBatchRecord.getMaxJobsToActivate()).isEqualTo(1);
    assertThat(jobBatchRecord.getTimeout()).isEqualTo(1_000L);
    assertThat(jobBatchRecord.getType()).isEqualTo("jobType");
    assertThat(jobBatchRecord.getWorker()).isEqualTo("myveryownworker");
    assertThat(jobBatchRecord.getTruncated()).isTrue();

    assertThat(jobBatchRecord.getJobsList()).hasSize(1);
    final JobRecord jobRecord = jobBatchRecord.getJobsList().get(0);
    assertJobRecord(jobRecord);
  }

  @Test
  public void shouldTransformIncident() {
    // given
    final IncidentRecordValue incidentRecordValue = mockIncidentRecordValue();
    final Record<IncidentRecordValue> mockedRecord =
        mockRecord(incidentRecordValue, ValueType.INCIDENT, IncidentIntent.CREATED);

    // when
    final Schema.IncidentRecord incidentRecord =
        (Schema.IncidentRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(incidentRecord.getMetadata(), "INCIDENT", "CREATED");

    assertThat(incidentRecord.getBpmnProcessId()).isEqualTo("process");
    assertThat(incidentRecord.getElementId()).isEqualTo("gateway");
    assertThat(incidentRecord.getElementInstanceKey()).isEqualTo(1L);
    assertThat(incidentRecord.getProcessInstanceKey()).isEqualTo(1L);
    assertThat(incidentRecord.getProcessDefinitionKey()).isEqualTo(32L);
    assertThat(incidentRecord.getVariableScopeKey()).isEqualTo(1L);

    assertThat(incidentRecord.getErrorMessage()).isEqualTo("failed");
    assertThat(incidentRecord.getErrorType()).isEqualTo(ErrorType.JOB_NO_RETRIES.name());

    assertThat(incidentRecord.getJobKey()).isEqualTo(12L);
    assertThat(incidentRecord.getTenantId()).isEqualTo(TENANT_ID);
  }

  @Test
  public void shouldTransformMessage() {
    // given
    final MessageRecordValue messageRecordValue = mockMessageRecordValue();
    final Record<MessageRecordValue> mockedRecord =
        mockRecord(messageRecordValue, ValueType.MESSAGE, MessageIntent.PUBLISHED);

    // when
    final Schema.MessageRecord messageRecord =
        (Schema.MessageRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(messageRecord.getMetadata(), "MESSAGE", "PUBLISHED");

    assertThat(messageRecord.getCorrelationKey()).isEqualTo("key");
    assertThat(messageRecord.getMessageId()).isEqualTo("msgId");
    assertThat(messageRecord.getName()).isEqualTo("message");
    assertThat(messageRecord.getTimeToLive()).isEqualTo(1000L);
    assertThat(messageRecord.getTenantId()).isEqualTo(TENANT_ID);
    assertThat(messageRecord.getDeadline()).isEqualTo(424242L);
    assertStruct(messageRecord.getVariables(), messageRecordValue.getVariables());
  }

  @Test
  public void shouldTransformTimer() {
    // given
    final TimerRecordValue timerRecordValue = mockTimerRecordValue();
    final Record<TimerRecordValue> mockedRecord =
        mockRecord(timerRecordValue, ValueType.TIMER, TimerIntent.CREATED);

    // when
    final Schema.TimerRecord timerRecord =
        (Schema.TimerRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(timerRecord.getMetadata(), "TIMER", "CREATED");

    assertThat(timerRecord.getDueDate()).isEqualTo(1000L);
    assertThat(timerRecord.getRepetitions()).isEqualTo(1);
    assertThat(timerRecord.getElementInstanceKey()).isEqualTo(1L);
    assertThat(timerRecord.getTargetElementId()).isEqualTo("timerCatch");
    assertThat(timerRecord.getProcessInstanceKey()).isEqualTo(2L);
    assertThat(timerRecord.getProcessDefinitionKey()).isEqualTo(3L);
    assertThat(timerRecord.getTenantId()).isEqualTo(TENANT_ID);
  }

  @Test
  public void shouldTransformMessageSubscription() {
    // given
    final MessageSubscriptionRecordValue value = mockMessageSubscriptionRecordValue();
    final Record<MessageSubscriptionRecordValue> mockedRecord =
        mockRecord(value, ValueType.MESSAGE_SUBSCRIPTION, MessageSubscriptionIntent.CORRELATED);

    // when
    final Schema.MessageSubscriptionRecord messageSubscriptionRecord =
        (Schema.MessageSubscriptionRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(messageSubscriptionRecord.getMetadata(), "MESSAGE_SUBSCRIPTION", "CORRELATED");

    assertThat(messageSubscriptionRecord.getCorrelationKey()).isEqualTo("key");
    assertThat(messageSubscriptionRecord.getMessageName()).isEqualTo("message");
    assertThat(messageSubscriptionRecord.getElementInstanceKey()).isEqualTo(12L);
    assertThat(messageSubscriptionRecord.getProcessInstanceKey()).isEqualTo(1L);
    assertThat(messageSubscriptionRecord.getBpmnProcessId()).isEqualTo(value.getBpmnProcessId());
    assertThat(messageSubscriptionRecord.getMessageKey()).isEqualTo(value.getMessageKey());
    assertThat(messageSubscriptionRecord.getIsInterrupting()).isTrue();
    assertThat(messageSubscriptionRecord.getTenantId()).isEqualTo(value.getTenantId());
    assertVariables(messageSubscriptionRecord.getVariables());
  }

  @Test
  public void shouldTransformProcessMessageSubscription() {
    // given
    final ProcessMessageSubscriptionRecordValue value = mockProcessMessageSubscriptionRecordValue();
    final Record<ProcessMessageSubscriptionRecordValue> mockedRecord =
        mockRecord(
            value,
            ValueType.PROCESS_MESSAGE_SUBSCRIPTION,
            ProcessMessageSubscriptionIntent.CORRELATED);

    // when
    final Schema.ProcessMessageSubscriptionRecord workflowInstanceSubscriptionRecord =
        (Schema.ProcessMessageSubscriptionRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(
        workflowInstanceSubscriptionRecord.getMetadata(),
        "PROCESS_MESSAGE_SUBSCRIPTION",
        "CORRELATED");

    assertThat(workflowInstanceSubscriptionRecord.getMessageName()).isEqualTo("message");
    assertThat(workflowInstanceSubscriptionRecord.getElementInstanceKey()).isEqualTo(4L);
    assertThat(workflowInstanceSubscriptionRecord.getProcessInstanceKey()).isEqualTo(1L);
    assertThat(workflowInstanceSubscriptionRecord.getBpmnProcessId())
        .isEqualTo(value.getBpmnProcessId());
    assertThat(workflowInstanceSubscriptionRecord.getMessageKey()).isEqualTo(value.getMessageKey());
    assertThat(workflowInstanceSubscriptionRecord.getCorrelationKey())
        .isEqualTo(value.getCorrelationKey());
    assertThat(workflowInstanceSubscriptionRecord.getElementId()).isEqualTo(value.getElementId());
    assertThat(workflowInstanceSubscriptionRecord.getIsInterrupting()).isTrue();
    assertThat(workflowInstanceSubscriptionRecord.getTenantId()).isEqualTo(value.getTenantId());
    assertStruct(workflowInstanceSubscriptionRecord.getVariables(), value.getVariables());
  }

  @Test
  public void shouldTransformVariableRecordValue() {
    // given
    final VariableRecordValue variableRecordValue = mockVariableRecordValue();
    final Record<VariableRecordValue> mockedRecord =
        mockRecord(variableRecordValue, ValueType.VARIABLE, VariableIntent.CREATED);

    // when
    final Schema.VariableRecord variableRecord =
        (Schema.VariableRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(variableRecord.getMetadata(), "VARIABLE", "CREATED");
    assertThat(variableRecord.getName()).isEqualTo(variableRecordValue.getName());
    assertThat(variableRecord.getScopeKey()).isEqualTo(variableRecordValue.getScopeKey());
    assertThat(variableRecord.getValue()).isEqualTo(variableRecordValue.getValue());
    assertThat(variableRecord.getProcessInstanceKey())
        .isEqualTo(variableRecordValue.getProcessInstanceKey());
    assertThat(variableRecord.getProcessDefinitionKey())
        .isEqualTo(variableRecordValue.getProcessDefinitionKey());
    assertThat(variableRecord.getBpmnProcessId()).isEqualTo(variableRecordValue.getBpmnProcessId());
    assertThat(variableRecord.getTenantId()).isEqualTo(variableRecordValue.getTenantId());
  }

  @Test
  public void shouldTransformVariableDocumentRecordValue() {
    // given
    final VariableDocumentRecordValue variableDocumentRecordValue =
        mockVariableDocumentRecordValue();
    final Record<VariableDocumentRecordValue> mockedRecord =
        mockRecord(
            variableDocumentRecordValue,
            ValueType.VARIABLE_DOCUMENT,
            VariableDocumentIntent.UPDATED);

    // when
    final Schema.VariableDocumentRecord variableRecord =
        (Schema.VariableDocumentRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(variableRecord.getMetadata(), "VARIABLE_DOCUMENT", "UPDATED");
    assertThat(variableRecord.getScopeKey()).isEqualTo(variableDocumentRecordValue.getScopeKey());
    assertThat(variableRecord.getUpdateSemantics().name())
        .isEqualTo(variableDocumentRecordValue.getUpdateSemantics().name());
    assertThat(variableRecord.getTenantId()).isEqualTo(variableDocumentRecordValue.getTenantId());
    assertStruct(variableRecord.getVariables(), variableDocumentRecordValue.getVariables());
  }

  @Test
  public void shouldTransformMessageStartEventSubscriptionRecordValue() {
    // given
    final MessageStartEventSubscriptionRecordValue value =
        mockMessageStartEventSubscriptionRecordValue();
    final Record<MessageStartEventSubscriptionRecordValue> record =
        mockRecord(
            value,
            ValueType.MESSAGE_START_EVENT_SUBSCRIPTION,
            MessageStartEventSubscriptionIntent.CREATED);

    // when
    final Schema.MessageStartEventSubscriptionRecord transformed =
        (Schema.MessageStartEventSubscriptionRecord) RecordTransformer.toProtobufMessage(record);

    // then
    assertMetadata(transformed.getMetadata(), "MESSAGE_START_EVENT_SUBSCRIPTION", "CREATED");
    assertThat(transformed.getMessageName()).isEqualTo(value.getMessageName());
    assertThat(transformed.getStartEventId()).isEqualTo(value.getStartEventId());
    assertThat(transformed.getProcessDefinitionKey()).isEqualTo(value.getProcessDefinitionKey());
    assertThat(transformed.getBpmnProcessId()).isEqualTo(value.getBpmnProcessId());
    assertThat(transformed.getCorrelationKey()).isEqualTo(value.getCorrelationKey());
    assertThat(transformed.getMessageKey()).isEqualTo(value.getMessageKey());
    assertThat(transformed.getProcessInstanceKey()).isEqualTo(value.getProcessInstanceKey());
    assertThat(transformed.getTenantId()).isEqualTo(value.getTenantId());

    assertVariables(transformed.getVariables());
  }

  @Test
  public void shouldTransformProcessEvent() {
    // given
    final var recordValue = mockProcessEventRecordValue();
    final Record<ProcessEventRecordValue> mockedRecord =
        mockRecord(recordValue, ValueType.PROCESS_EVENT, ProcessEventIntent.TRIGGERING);

    // when
    final var transformedRecord =
        (Schema.ProcessEventRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(transformedRecord.getMetadata(), "PROCESS_EVENT", "TRIGGERING");

    assertThat(transformedRecord.getProcessDefinitionKey())
        .isEqualTo(recordValue.getProcessDefinitionKey());
    assertThat(transformedRecord.getScopeKey()).isEqualTo(recordValue.getScopeKey());
    assertThat(transformedRecord.getTargetElementId()).isEqualTo(recordValue.getTargetElementId());
    assertThat(transformedRecord.getTenantId()).isEqualTo(recordValue.getTenantId());

    assertVariables(transformedRecord.getVariables());
  }

  @Test
  public void shouldTransformErrorRecordValue() {
    // given
    final ErrorRecordValue value = mockErrorRecordValue();
    final Record<ErrorRecordValue> record = mockRecord(value, ValueType.ERROR, ErrorIntent.CREATED);

    // when
    final Schema.ErrorRecord transformed =
        (Schema.ErrorRecord) RecordTransformer.toProtobufMessage(record);

    // then
    assertMetadata(transformed.getMetadata(), "ERROR", "CREATED");
    assertThat(transformed.getExceptionMessage()).isEqualTo(value.getExceptionMessage());
    assertThat(transformed.getStacktrace()).isEqualTo(value.getStacktrace());
    assertThat(transformed.getErrorEventPosition()).isEqualTo(value.getErrorEventPosition());
    assertThat(transformed.getProcessInstanceKey()).isEqualTo(value.getProcessInstanceKey());
  }

  @Test
  public void shouldTransformRecordType() {

    final List<String> recordTypes =
        Arrays.stream(RecordType.values())
            .filter(t -> t != RecordType.NULL_VAL && t != RecordType.SBE_UNKNOWN)
            .map(RecordType::name)
            .collect(Collectors.toList());

    assertThat(RecordMetadata.RecordType.values())
        .extracting(RecordMetadata.RecordType::name)
        .containsAll(recordTypes);
  }

  @Test
  public void shouldTransformValueType() {

    final var ignoredValueTypes = Set.of(ValueType.NULL_VAL, ValueType.SBE_UNKNOWN);

    final List<String> valueTypes =
        Arrays.stream(ValueType.values())
            .filter(not(ignoredValueTypes::contains))
            .map(ValueType::name)
            .collect(Collectors.toList());

    assertThat(RecordMetadata.ValueType.values())
        .extracting(RecordMetadata.ValueType::name)
        .containsAll(valueTypes);
  }

  @Test
  public void shouldTransformUpdateSemantics() {

    final List<String> updateSemantics =
        Arrays.stream(VariableDocumentUpdateSemantic.values())
            .map(VariableDocumentUpdateSemantic::name)
            .collect(Collectors.toList());

    assertThat(UpdateSemantics.values())
        .extracting(UpdateSemantics::name)
        .containsAll(updateSemantics);
  }

  @Test
  public void shouldTransformToGenericRecord() throws InvalidProtocolBufferException {
    // given
    final var recordValue = mockJobRecordValue();
    final var record = mockRecord(recordValue, ValueType.JOB, JobIntent.CREATED);

    final var expectedProtobufRecord = RecordTransformer.toProtobufMessage(record);

    // when
    final var genericProtobufRecord = RecordTransformer.toGenericRecord(record);

    // then
    assertThat(genericProtobufRecord.getRecord().is(Schema.JobRecord.class)).isTrue();

    final var unpackedRecord = genericProtobufRecord.getRecord().unpack(JobRecord.class);
    assertThat(unpackedRecord).isEqualTo(expectedProtobufRecord);
  }

  @Test
  public void shouldTransformDecisionRecord() {
    // given
    final var recordValue = mockDecisionRecordValue();
    final Record<DecisionRecordValue> mockedRecord =
        mockRecord(recordValue, ValueType.DECISION, DecisionIntent.CREATED);

    // when
    final var transformedRecord =
        (Schema.DecisionRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(transformedRecord.getMetadata(), "DECISION", "CREATED");

    assertThat(transformedRecord.getDecisionRequirementsKey())
        .isEqualTo(recordValue.getDecisionRequirementsKey());
    assertThat(transformedRecord.getDecisionRequirementsId())
        .isEqualTo(recordValue.getDecisionRequirementsId());
    assertThat(transformedRecord.getVersion()).isEqualTo(recordValue.getVersion());
    assertThat(transformedRecord.getDecisionId()).isEqualTo(recordValue.getDecisionId());
    assertThat(transformedRecord.getDecisionName()).isEqualTo(recordValue.getDecisionName());
    assertThat(transformedRecord.getDecisionKey()).isEqualTo(recordValue.getDecisionKey());
    assertThat(transformedRecord.getIsDuplicate()).isEqualTo(recordValue.isDuplicate());
    assertThat(transformedRecord.getTenantId()).isEqualTo(recordValue.getTenantId());
    assertThat(transformedRecord.getDeploymentKey()).isEqualTo(7L);
    assertThat(transformedRecord.getVersionTag()).isEqualTo("v7");
  }

  @Test
  public void shouldTransformDecisionRequirementsRecord() {
    // given
    final var recordValue = mockDecisionRequirementsRecordValue();
    final Record<DecisionRequirementsRecordValue> mockedRecord =
        mockRecord(
            recordValue, ValueType.DECISION_REQUIREMENTS, DecisionRequirementsIntent.CREATED);

    // when
    final var transformedRecord =
        (Schema.DecisionRequirementsRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(transformedRecord.getMetadata(), "DECISION_REQUIREMENTS", "CREATED");

    assertThat(transformedRecord.getDecisionRequirementsMetadata().getDecisionRequirementsKey())
        .isEqualTo(recordValue.getDecisionRequirementsKey());
    assertThat(transformedRecord.getDecisionRequirementsMetadata().getDecisionRequirementsId())
        .isEqualTo(recordValue.getDecisionRequirementsId());
    assertThat(transformedRecord.getDecisionRequirementsMetadata().getDecisionRequirementsName())
        .isEqualTo(recordValue.getDecisionRequirementsName());
    assertThat(transformedRecord.getDecisionRequirementsMetadata().getDecisionRequirementsVersion())
        .isEqualTo(recordValue.getDecisionRequirementsVersion());
    assertThat(transformedRecord.getDecisionRequirementsMetadata().getNamespace())
        .isEqualTo(recordValue.getNamespace());
    assertThat(transformedRecord.getDecisionRequirementsMetadata().getResourceName())
        .isEqualTo(recordValue.getResourceName());
    assertThat(transformedRecord.getDecisionRequirementsMetadata().getChecksum().toByteArray())
        .isEqualTo(recordValue.getChecksum());
    assertThat(transformedRecord.getDecisionRequirementsMetadata().getIsDuplicate())
        .isEqualTo(recordValue.isDuplicate());
    assertThat(transformedRecord.getTenantId()).isEqualTo(recordValue.getTenantId());
  }

  @Test
  public void shouldTransformDecisionEvaluationRecord() {
    // given
    final var recordValue = mockDecisionEvaluationRecordValue();
    final Record<DecisionEvaluationRecordValue> mockedRecord =
        mockRecord(recordValue, ValueType.DECISION_EVALUATION, DecisionEvaluationIntent.EVALUATED);

    // when
    final var transformedRecord =
        (Schema.DecisionEvaluationRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(transformedRecord.getMetadata(), "DECISION_EVALUATION", "EVALUATED");

    assertThat(transformedRecord.getDecisionKey()).isEqualTo(recordValue.getDecisionKey());
    assertThat(transformedRecord.getDecisionId()).isEqualTo(recordValue.getDecisionId());
    assertThat(transformedRecord.getDecisionName()).isEqualTo(recordValue.getDecisionName());
    assertThat(transformedRecord.getDecisionVersion()).isEqualTo(recordValue.getDecisionVersion());
    assertThat(transformedRecord.getDecisionRequirementsId())
        .isEqualTo(recordValue.getDecisionRequirementsId());
    assertThat(transformedRecord.getDecisionRequirementsKey())
        .isEqualTo(recordValue.getDecisionRequirementsKey());
    assertThat(transformedRecord.getDecisionOutput()).isEqualTo(recordValue.getDecisionOutput());
    assertThat(transformedRecord.getBpmnProcessId()).isEqualTo(recordValue.getBpmnProcessId());
    assertThat(transformedRecord.getProcessDefinitionKey())
        .isEqualTo(recordValue.getProcessDefinitionKey());
    assertThat(transformedRecord.getProcessInstanceKey())
        .isEqualTo(recordValue.getProcessInstanceKey());
    assertThat(transformedRecord.getElementId()).isEqualTo(recordValue.getElementId());
    assertThat(transformedRecord.getElementInstanceKey())
        .isEqualTo(recordValue.getElementInstanceKey());
    assertThat(transformedRecord.getEvaluationFailureMessage())
        .isEqualTo(recordValue.getEvaluationFailureMessage());
    assertThat(transformedRecord.getFailedDecisionId())
        .isEqualTo(recordValue.getFailedDecisionId());
    assertThat(transformedRecord.getTenantId()).isEqualTo(recordValue.getTenantId());

    assertThat(transformedRecord.getEvaluatedDecisionsList()).hasSize(1);
    assertThat(recordValue.getEvaluatedDecisions()).hasSize(1);
    final Schema.DecisionEvaluationRecord.EvaluatedDecision evaluatedDecision =
        transformedRecord.getEvaluatedDecisionsList().get(0);
    assertEvaluatedDecision(evaluatedDecision, recordValue.getEvaluatedDecisions().get(0));
  }

  @Test
  public void shouldTransformProcessInstanceModificationRecord() {
    // given
    final var recordValue = mock(ProcessInstanceModificationRecordValue.class);
    when(recordValue.getTenantId()).thenReturn(TENANT_ID);
    when(recordValue.getProcessInstanceKey()).thenReturn(10L);
    when(recordValue.getActivateInstructions())
        .thenAnswer(
            invocation -> {
              final var instruction =
                  mock(
                      ProcessInstanceModificationRecordValue
                          .ProcessInstanceModificationActivateInstructionValue.class);
              when(instruction.getElementId()).thenReturn("element-id");
              when(instruction.getAncestorScopeKey()).thenReturn(20L);
              when(instruction.getAncestorScopeKeys()).thenReturn(Set.of(30L, 31L));
              when(instruction.getVariableInstructions())
                  .thenAnswer(
                      invocation2 -> {
                        final var variableInstruction =
                            mock(
                                ProcessInstanceModificationRecordValue
                                    .ProcessInstanceModificationVariableInstructionValue.class);
                        when(variableInstruction.getElementId()).thenReturn("variable-element-id");
                        when(variableInstruction.getVariables()).thenReturn(VARIABLES);
                        return List.of(variableInstruction);
                      });
              return List.of(instruction);
            });
    when(recordValue.getTerminateInstructions())
        .thenAnswer(
            invocation -> {
              final var instruction =
                  mock(
                      ProcessInstanceModificationRecordValue
                          .ProcessInstanceModificationTerminateInstructionValue.class);
              when(instruction.getElementInstanceKey()).thenReturn(40L);
              return List.of(instruction);
            });

    final Record<ProcessInstanceModificationRecordValue> mockedRecord =
        mockRecord(
            recordValue,
            ValueType.PROCESS_INSTANCE_MODIFICATION,
            ProcessInstanceModificationIntent.MODIFIED);

    // when
    final var transformedRecord =
        (Schema.ProcessInstanceModificationRecord)
            RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(transformedRecord.getMetadata(), "PROCESS_INSTANCE_MODIFICATION", "MODIFIED");

    assertThat(transformedRecord.getProcessInstanceKey())
        .isEqualTo(recordValue.getProcessInstanceKey());
    assertThat(transformedRecord.getActivateInstructionsList())
        .hasSize(1)
        .allSatisfy(
            transformedInstruction -> {
              final var activateInstruction = recordValue.getActivateInstructions().get(0);
              assertThat(transformedInstruction.getElementId())
                  .isEqualTo(activateInstruction.getElementId());
              assertThat(transformedInstruction.getAncestorScopeKey())
                  .isEqualTo(activateInstruction.getAncestorScopeKey());
              assertThat(transformedInstruction.getAncestorScopeKeysList())
                  .containsAll(activateInstruction.getAncestorScopeKeys());
              assertThat(transformedInstruction.getVariableInstructionsList())
                  .hasSize(1)
                  .allSatisfy(
                      transformedVariableInstruction -> {
                        final var variableInstruction =
                            activateInstruction.getVariableInstructions().get(0);
                        assertThat(transformedVariableInstruction.getElementId())
                            .isEqualTo(variableInstruction.getElementId());
                        assertVariables(transformedVariableInstruction.getVariables());
                      });
            });
    assertThat(transformedRecord.getTerminateInstructionsList())
        .hasSize(1)
        .allSatisfy(
            transformedInstruction -> {
              final var terminateInstruction = recordValue.getTerminateInstructions().get(0);
              assertThat(transformedInstruction.getElementInstanceKey())
                  .isEqualTo(terminateInstruction.getElementInstanceKey());
            });
    assertThat(transformedRecord.getTenantId()).isEqualTo(recordValue.getTenantId());
  }

  @Test
  public void shouldTransformCheckpointRecord() {
    // given
    final var recordValue = mock(CheckpointRecordValue.class);
    when(recordValue.getCheckpointId()).thenReturn(10L);
    when(recordValue.getCheckpointPosition()).thenReturn(20L);

    final Record<CheckpointRecordValue> mockedRecord =
        mockRecord(recordValue, ValueType.CHECKPOINT, CheckpointIntent.CREATED);

    // when
    final var transformedRecord =
        (Schema.CheckpointRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(transformedRecord.getMetadata(), "CHECKPOINT", "CREATED");

    assertThat(transformedRecord.getCheckpointId()).isEqualTo(recordValue.getCheckpointId());
    assertThat(transformedRecord.getCheckpointPosition())
        .isEqualTo(recordValue.getCheckpointPosition());
  }

  @Test
  public void shouldTransformSignalRecord() {
    // given
    final var recordValue = mock(SignalRecordValue.class);
    when(recordValue.getSignalName()).thenReturn("signal");
    when(recordValue.getVariables()).thenReturn(VARIABLES);
    when(recordValue.getTenantId()).thenReturn(TENANT_ID);

    final Record<SignalRecordValue> mockedRecord =
        mockRecord(recordValue, ValueType.SIGNAL, SignalIntent.BROADCAST);

    // when
    final var transformedRecord =
        (Schema.SignalRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(transformedRecord.getMetadata(), "SIGNAL", "BROADCAST");

    assertThat(transformedRecord.getSignalName()).isEqualTo(recordValue.getSignalName());
    assertThat(transformedRecord.getTenantId()).isEqualTo(recordValue.getTenantId());
    assertVariables(transformedRecord.getVariables());
  }

  @Test
  public void shouldTransformSignalSubscriptionRecord() {
    // given
    final var recordValue = mock(SignalSubscriptionRecordValue.class);
    when(recordValue.getSignalName()).thenReturn("signal");
    when(recordValue.getProcessDefinitionKey()).thenReturn(10L);
    when(recordValue.getBpmnProcessId()).thenReturn("process");
    when(recordValue.getCatchEventId()).thenReturn("event");
    when(recordValue.getCatchEventInstanceKey()).thenReturn(20L);
    when(recordValue.getTenantId()).thenReturn(TENANT_ID);

    final Record<SignalSubscriptionRecordValue> mockedRecord =
        mockRecord(recordValue, ValueType.SIGNAL_SUBSCRIPTION, SignalSubscriptionIntent.CREATED);

    // when
    final var transformedRecord =
        (Schema.SignalSubscriptionRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(transformedRecord.getMetadata(), "SIGNAL_SUBSCRIPTION", "CREATED");

    assertThat(transformedRecord.getSignalName()).isEqualTo(recordValue.getSignalName());
    assertThat(transformedRecord.getProcessDefinitionKey())
        .isEqualTo(recordValue.getProcessDefinitionKey());
    assertThat(transformedRecord.getBpmnProcessId()).isEqualTo(recordValue.getBpmnProcessId());
    assertThat(transformedRecord.getCatchEventId()).isEqualTo(recordValue.getCatchEventId());
    assertThat(transformedRecord.getCatchEventInstanceKey())
        .isEqualTo(recordValue.getCatchEventInstanceKey());
    assertThat(transformedRecord.getTenantId()).isEqualTo(recordValue.getTenantId());
  }

  @Test
  public void shouldTransformFormRecord() {
    // given
    final var recordValue = mockFormRecordValue();
    final Record<Form> mockedRecord = mockRecord(recordValue, ValueType.FORM, FormIntent.CREATED);

    // when
    final var transformedRecord =
        (Schema.FormRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(transformedRecord.getMetadata(), "FORM", "CREATED");

    assertThat(transformedRecord.getFormId()).isEqualTo(recordValue.getFormId());
    assertThat(transformedRecord.getVersion()).isEqualTo(recordValue.getVersion());
    assertThat(transformedRecord.getFormKey()).isEqualTo(recordValue.getFormKey());
    assertThat(transformedRecord.getResourceName()).isEqualTo(recordValue.getResourceName());
    assertThat(transformedRecord.getChecksum().toByteArray()).isEqualTo(recordValue.getChecksum());
    assertThat(transformedRecord.getResource().toByteArray()).isEqualTo(recordValue.getResource());
    assertThat(transformedRecord.getTenantId()).isEqualTo(recordValue.getTenantId());
  }

  @Test
  public void shouldTransformResourceDeletionRecord() {
    // given
    final var recordValue = mockResourceDeletionRecordValue();
    final Record<ResourceDeletionRecordValue> mockedRecord =
        mockRecord(recordValue, ValueType.RESOURCE_DELETION, ResourceDeletionIntent.DELETED);

    // when
    final var transformedRecord =
        (Schema.ResourceDeletionRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(transformedRecord.getMetadata(), "RESOURCE_DELETION", "DELETED");

    assertThat(transformedRecord.getResourceKey()).isEqualTo(recordValue.getResourceKey());
    assertThat(transformedRecord.getTenantId()).isEqualTo(recordValue.getTenantId());
  }

  @Test
  public void shouldTransformUserTaskRecord() {
    // given
    final var recordValue = mockUserTaskRecordValue();
    final Record<UserTaskRecordValue> mockedRecord =
        mockRecord(recordValue, ValueType.USER_TASK, UserTaskIntent.CREATED);

    // when
    final var transformedRecord =
        (Schema.UserTaskRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(transformedRecord.getMetadata(), "USER_TASK", "CREATED");

    assertThat(transformedRecord.getUserTaskKey()).isEqualTo(recordValue.getUserTaskKey());
    assertThat(transformedRecord.getAssignee()).isEqualTo(recordValue.getAssignee());
    assertThat(transformedRecord.getCandidateGroupsList())
        .isEqualTo(recordValue.getCandidateGroupsList());
    assertThat(transformedRecord.getCandidateUsersList())
        .isEqualTo(recordValue.getCandidateUsersList());
    assertThat(transformedRecord.getDueDate()).isEqualTo(recordValue.getDueDate());
    assertThat(transformedRecord.getFollowUpDate()).isEqualTo(recordValue.getFollowUpDate());
    assertThat(transformedRecord.getFormKey()).isEqualTo(recordValue.getFormKey());
    assertThat(transformedRecord.getBpmnProcessId()).isEqualTo(recordValue.getBpmnProcessId());
    assertThat(transformedRecord.getProcessDefinitionVersion())
        .isEqualTo(recordValue.getProcessDefinitionVersion());
    assertThat(transformedRecord.getProcessDefinitionKey())
        .isEqualTo(recordValue.getProcessDefinitionKey());
    assertThat(transformedRecord.getProcessInstanceKey())
        .isEqualTo(recordValue.getProcessInstanceKey());
    assertThat(transformedRecord.getElementId()).isEqualTo(recordValue.getElementId());
    assertThat(transformedRecord.getElementInstanceKey())
        .isEqualTo(recordValue.getElementInstanceKey());
    assertThat(transformedRecord.getTenantId()).isEqualTo(recordValue.getTenantId());
    assertVariables(transformedRecord.getVariables());
    assertThat(transformedRecord.getExternalFormReference())
        .isEqualTo(recordValue.getExternalFormReference());
    assertStruct(transformedRecord.getCustomHeaders(), recordValue.getCustomHeaders());
    assertThat(transformedRecord.getChangedAttributesList())
        .isEqualTo(recordValue.getChangedAttributes());
    assertThat(transformedRecord.getAction()).isEqualTo(recordValue.getAction());
    assertThat(transformedRecord.getCreationTimestamp())
        .isEqualTo(recordValue.getCreationTimestamp());
    assertThat(transformedRecord.getPriority()).isEqualTo(100);
  }

  @Test
  public void shouldTransformCompensationSubscriptionRecord() {
    // given
    final var recordValue = mockCompensationSubscriptionRecordValue();
    final Record<CompensationSubscriptionRecordValue> mockedRecord =
        mockRecord(
            recordValue,
            ValueType.COMPENSATION_SUBSCRIPTION,
            CompensationSubscriptionIntent.CREATED);

    // when
    final var transformedRecord =
        (Schema.CompensationSubscriptionRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(transformedRecord.getMetadata(), "COMPENSATION_SUBSCRIPTION", "CREATED");

    assertThat(transformedRecord.getTenantId()).isEqualTo(recordValue.getTenantId());
    assertThat(transformedRecord.getProcessInstanceKey())
        .isEqualTo(recordValue.getProcessInstanceKey());
    assertThat(transformedRecord.getProcessDefinitionKey())
        .isEqualTo(recordValue.getProcessDefinitionKey());
    assertThat(transformedRecord.getCompensableActivityId())
        .isEqualTo(recordValue.getCompensableActivityId());
    assertThat(transformedRecord.getThrowEventId()).isEqualTo(recordValue.getThrowEventId());
    assertThat(transformedRecord.getThrowEventInstanceKey())
        .isEqualTo(recordValue.getThrowEventInstanceKey());
    assertThat(transformedRecord.getCompensationHandlerId())
        .isEqualTo(recordValue.getCompensationHandlerId());
    assertThat(transformedRecord.getCompensationHandlerInstanceKey())
        .isEqualTo(recordValue.getCompensationHandlerInstanceKey());
    assertThat(transformedRecord.getCompensableActivityScopeKey())
        .isEqualTo(recordValue.getCompensableActivityScopeKey());
    assertThat(transformedRecord.getCompensableActivityInstanceKey())
        .isEqualTo(recordValue.getCompensableActivityInstanceKey());
    assertStruct(transformedRecord.getVariables(), recordValue.getVariables());
  }

  @Test
  public void shouldTransformEscalationRecord() {
    // given
    final var recordValue = mockEscalationRecordValue();
    final Record<EscalationRecordValue> mockedRecord =
        mockRecord(recordValue, ValueType.ESCALATION, EscalationIntent.ESCALATED);

    // when
    final var transformedRecord =
        (Schema.EscalationRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(transformedRecord.getMetadata(), "ESCALATION", "ESCALATED");

    assertThat(transformedRecord.getProcessInstanceKey())
        .isEqualTo(recordValue.getProcessInstanceKey());
    assertThat(transformedRecord.getEscalationCode()).isEqualTo(recordValue.getEscalationCode());
    assertThat(transformedRecord.getThrowElementId()).isEqualTo(recordValue.getThrowElementId());
    assertThat(transformedRecord.getCatchElementId()).isEqualTo(recordValue.getCatchElementId());
  }

  @Test
  public void shouldTransformProcessInstanceMigrationRecord() {
    // given
    final var recordValue = mockProcessInstanceMigrationRecordValue();
    final Record<ProcessInstanceMigrationRecordValue> mockedRecord =
        mockRecord(
            recordValue,
            ValueType.PROCESS_INSTANCE_MIGRATION,
            ProcessInstanceMigrationIntent.MIGRATED);

    // when
    final var transformedRecord =
        (Schema.ProcessInstanceMigrationRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(transformedRecord.getMetadata(), "PROCESS_INSTANCE_MIGRATION", "MIGRATED");
    assertThat(transformedRecord.getProcessInstanceKey())
        .isEqualTo(recordValue.getProcessInstanceKey());
    assertThat(transformedRecord.getTargetProcessDefinitionKey())
        .isEqualTo(recordValue.getTargetProcessDefinitionKey());
    assertThat(transformedRecord.getMappingInstructionsCount()).isEqualTo(1);
    assertThat(transformedRecord.getMappingInstructions(0).getSourceElementId())
        .isEqualTo(recordValue.getMappingInstructions().get(0).getSourceElementId());
    assertThat(transformedRecord.getMappingInstructions(0).getTargetElementId())
        .isEqualTo(recordValue.getMappingInstructions().get(0).getTargetElementId());
  }

  @Test
  public void shouldTransformMessageBatchRecord() {
    // given
    final var recordValue = mockMessageBatchRecordValue();

    final Record<MessageBatchRecordValue> mockedRecord =
        mockRecord(recordValue, ValueType.MESSAGE_BATCH, MessageBatchIntent.EXPIRE);

    // when
    final var transformedRecord =
        (Schema.MessageBatchRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(transformedRecord.getMetadata(), "MESSAGE_BATCH", "EXPIRE");
    assertThat(transformedRecord.getMessageKeysList()).containsExactly(40L, 41L, 42L);
  }

  @Test
  public void shouldTransformClockRecord() {
    // given
    final var recordValue = mockClockRecordValue();
    final Record<ClockRecordValue> mockedRecord =
        mockRecord(recordValue, ValueType.CLOCK, ClockIntent.RESET);

    // when
    final var transformedRecord =
        (Schema.ClockRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(transformedRecord.getMetadata(), "CLOCK", "RESET");
    assertThat(transformedRecord.getTime()).isEqualTo(recordValue.getTime());
  }

  @Test
  public void shouldTransformMessageCorrelationRecord() {
    // given
    final var recordValue = mockMessageCorrelationRecordValue();
    final Record<MessageCorrelationRecordValue> mockedRecord =
        mockRecord(recordValue, ValueType.MESSAGE_CORRELATION, MessageCorrelationIntent.CORRELATED);

    // when
    final var transformedRecord =
        (Schema.MessageCorrelationRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(transformedRecord.getMetadata(), "MESSAGE_CORRELATION", "CORRELATED");
    assertThat(transformedRecord.getName()).isEqualTo(recordValue.getName());
    assertThat(transformedRecord.getCorrelationKey()).isEqualTo(recordValue.getCorrelationKey());
    assertThat(transformedRecord.getMessageKey()).isEqualTo(recordValue.getMessageKey());
    assertThat(transformedRecord.getRequestId()).isEqualTo(recordValue.getRequestId());
    assertThat(transformedRecord.getRequestStreamId()).isEqualTo(recordValue.getRequestStreamId());
  }

  @Test
  public void shouldTransformResource() {
    // given
    final var recordValue = mockResource();
    final Record<Resource> mockedRecord =
        mockRecord(recordValue, ValueType.RESOURCE, ResourceIntent.CREATED);

    // when
    final var transformedRecord =
        (Schema.ResourceRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(transformedRecord.getMetadata(), "RESOURCE", "CREATED");
    assertThat(transformedRecord.getResourceId()).isEqualTo(recordValue.getResourceId());
    assertThat(transformedRecord.getVersion()).isEqualTo(recordValue.getVersion());
    assertThat(transformedRecord.getVersionTag()).isEqualTo(recordValue.getVersionTag());
    assertThat(transformedRecord.getResourceKey()).isEqualTo(recordValue.getResourceKey());
    assertThat(transformedRecord.getChecksum().toByteArray()).isEqualTo(recordValue.getChecksum());
    assertThat(transformedRecord.getResourceName()).isEqualTo(recordValue.getResourceName());
    assertThat(transformedRecord.getIsDuplicate()).isEqualTo(recordValue.isDuplicate());
    assertThat(transformedRecord.getDeploymentKey()).isEqualTo(recordValue.getDeploymentKey());
    assertThat(transformedRecord.getTenantId()).isEqualTo(recordValue.getTenantId());
    assertThat(transformedRecord.getResourceProp()).isEqualTo(recordValue.getResourceProp());
  }

  @Test
  public void shouldTransformUserRecord() {
    // given
    final var recordValue = mockUserRecordValue();
    final Record<UserRecordValue> mockedRecord =
        mockRecord(recordValue, ValueType.USER, UserIntent.CREATED);

    // when
    final var transformedRecord =
        (Schema.UserRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(transformedRecord.getMetadata(), "USER", "CREATED");
    assertThat(transformedRecord.getUserKey()).isEqualTo(recordValue.getUserKey());
    assertThat(transformedRecord.getUsername()).isEqualTo(recordValue.getUsername());
    assertThat(transformedRecord.getName()).isEqualTo(recordValue.getName());
    assertThat(transformedRecord.getEmail()).isEqualTo(recordValue.getEmail());
    assertThat(transformedRecord.getPassword()).isEqualTo(recordValue.getPassword());
    assertThat(transformedRecord.getUserType()).isEqualTo(Schema.UserRecord.UserType.DEFAULT);
  }

  @Test
  public void shouldTransformAuthorizationRecord() {
    // given
    final var recordValue = mockAuthorizationRecordValue();
    final Record<AuthorizationRecordValue> mockedRecord =
        mockRecord(recordValue, ValueType.AUTHORIZATION, AuthorizationIntent.ADD_PERMISSION);

    // when
    final var transformedRecord =
        (Schema.AuthorizationRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(transformedRecord.getMetadata(), "AUTHORIZATION", "ADD_PERMISSION");
    assertThat(transformedRecord.getAction())
        .isEqualTo(Schema.AuthorizationRecord.PermissionAction.ADD);
    assertThat(transformedRecord.getOwnerKey()).isEqualTo(recordValue.getOwnerKey());
    assertThat(transformedRecord.getOwnerType())
        .isEqualTo(Schema.AuthorizationRecord.AuthorizationOwnerType._USER);
    assertThat(transformedRecord.getResourceType())
        .isEqualTo(Schema.AuthorizationRecord.AuthorizationResourceType.PROCESS_DEFINITION);
    assertThat(transformedRecord.getPermissionsList()).hasSize(1);
    var transformedPermission = transformedRecord.getPermissions(0);
    assertThat(transformedPermission.getPermissionType())
        .isEqualTo(Schema.AuthorizationRecord.PermissionValue.PermissionType.READ);
    assertThat(transformedPermission.getResourceIdsList())
        .containsExactly("resource-1", "resource-2");
  }

  private void assertEvaluatedDecision(
      final Schema.DecisionEvaluationRecord.EvaluatedDecision transformedRecord,
      final EvaluatedDecisionValue recordValue) {
    assertThat(transformedRecord.getDecisionId()).isEqualTo(recordValue.getDecisionId());
    assertThat(transformedRecord.getDecisionName()).isEqualTo(recordValue.getDecisionName());
    assertThat(transformedRecord.getDecisionKey()).isEqualTo(recordValue.getDecisionKey());
    assertThat(transformedRecord.getDecisionVersion()).isEqualTo(recordValue.getDecisionVersion());
    assertThat(transformedRecord.getDecisionType()).isEqualTo(recordValue.getDecisionType());
    assertThat(transformedRecord.getDecisionOutput()).isEqualTo(recordValue.getDecisionOutput());
    assertThat(transformedRecord.getTenantId()).isEqualTo(recordValue.getTenantId());

    assertThat(transformedRecord.getEvaluatedInputsList()).hasSize(1);
    assertThat(recordValue.getEvaluatedInputs()).hasSize(1);
    final Schema.DecisionEvaluationRecord.EvaluatedInput evaluatedInput =
        transformedRecord.getEvaluatedInputsList().get(0);
    assertEvaluatedInput(evaluatedInput, recordValue.getEvaluatedInputs().get(0));

    assertThat(transformedRecord.getMatchedRulesList()).hasSize(1);
    assertThat(recordValue.getMatchedRules()).hasSize(1);
    final Schema.DecisionEvaluationRecord.MatchedRule matchedRule =
        transformedRecord.getMatchedRulesList().get(0);
    assertMatchedRule(matchedRule, recordValue.getMatchedRules().get(0));
  }

  private void assertMatchedRule(
      final Schema.DecisionEvaluationRecord.MatchedRule transformedRecord,
      final MatchedRuleValue recordValue) {
    assertThat(transformedRecord.getRuleId()).isEqualTo(recordValue.getRuleId());
    assertThat(transformedRecord.getRuleIndex()).isEqualTo(recordValue.getRuleIndex());

    assertThat(transformedRecord.getEvaluatedOutputsList()).hasSize(1);
    assertThat(recordValue.getEvaluatedOutputs()).hasSize(1);
    final Schema.DecisionEvaluationRecord.EvaluatedOutput evaluatedOutput =
        transformedRecord.getEvaluatedOutputsList().get(0);
    assertEvaluatedOutput(evaluatedOutput, recordValue.getEvaluatedOutputs().get(0));
  }

  private void assertEvaluatedOutput(
      final Schema.DecisionEvaluationRecord.EvaluatedOutput transformedRecord,
      final EvaluatedOutputValue recordValue) {
    assertThat(transformedRecord.getOutputId()).isEqualTo(recordValue.getOutputId());
    assertThat(transformedRecord.getOutputName()).isEqualTo(recordValue.getOutputName());
    assertThat(transformedRecord.getOutputValue()).isEqualTo(recordValue.getOutputValue());
  }

  private void assertEvaluatedInput(
      final Schema.DecisionEvaluationRecord.EvaluatedInput transformedRecord,
      final EvaluatedInputValue recordValue) {
    assertThat(transformedRecord.getInputId()).isEqualTo(recordValue.getInputId());
    assertThat(transformedRecord.getInputName()).isEqualTo(recordValue.getInputName());
    assertThat(transformedRecord.getInputValue()).isEqualTo(recordValue.getInputValue());
  }

  private MessageRecordValue mockMessageRecordValue() {
    final MessageRecordValue messageRecordValue = mock(MessageRecordValue.class);

    when(messageRecordValue.getCorrelationKey()).thenReturn("key");
    when(messageRecordValue.getMessageId()).thenReturn("msgId");
    when(messageRecordValue.getName()).thenReturn("message");
    when(messageRecordValue.getTimeToLive()).thenReturn(1000L);
    when(messageRecordValue.getVariables()).thenReturn(Collections.singletonMap("foo", 23));
    when(messageRecordValue.getTenantId()).thenReturn(TENANT_ID);
    when(messageRecordValue.getDeadline()).thenReturn(424242L);

    return messageRecordValue;
  }

  private MessageBatchRecordValue mockMessageBatchRecordValue() {
    final MessageBatchRecordValue messageBatchRecordValue = mock(MessageBatchRecordValue.class);

    when(messageBatchRecordValue.getMessageKeys()).thenReturn(List.of(40L, 41L, 42L));

    return messageBatchRecordValue;
  }

  private MessageStartEventSubscriptionRecordValue mockMessageStartEventSubscriptionRecordValue() {
    final MessageStartEventSubscriptionRecordValue value =
        mock(MessageStartEventSubscriptionRecordValue.class);

    when(value.getMessageName()).thenReturn("message");
    when(value.getStartEventId()).thenReturn("start");
    when(value.getProcessDefinitionKey()).thenReturn(1L);
    when(value.getBpmnProcessId()).thenReturn("bpmnProcessId");
    when(value.getCorrelationKey()).thenReturn("correlationKey");
    when(value.getMessageKey()).thenReturn(2L);
    when(value.getProcessInstanceKey()).thenReturn(3L);
    when(value.getVariables()).thenReturn(VARIABLES);
    when(value.getTenantId()).thenReturn(TENANT_ID);

    return value;
  }

  private TimerRecordValue mockTimerRecordValue() {
    final TimerRecordValue timerRecordValue = mock(TimerRecordValue.class);

    when(timerRecordValue.getDueDate()).thenReturn(1000L);
    when(timerRecordValue.getRepetitions()).thenReturn(1);
    when(timerRecordValue.getElementInstanceKey()).thenReturn(1L);
    when(timerRecordValue.getTargetElementId()).thenReturn("timerCatch");
    when(timerRecordValue.getProcessInstanceKey()).thenReturn(2L);
    when(timerRecordValue.getProcessDefinitionKey()).thenReturn(3L);
    when(timerRecordValue.getTenantId()).thenReturn(TENANT_ID);

    return timerRecordValue;
  }

  private VariableRecordValue mockVariableRecordValue() {
    final VariableRecordValue variableRecordValue = mock(VariableRecordValue.class);

    when(variableRecordValue.getName()).thenReturn("var");
    when(variableRecordValue.getScopeKey()).thenReturn(1L);
    when(variableRecordValue.getValue()).thenReturn("true");
    when(variableRecordValue.getProcessInstanceKey()).thenReturn(1L);
    when(variableRecordValue.getProcessDefinitionKey()).thenReturn(2L);
    when(variableRecordValue.getBpmnProcessId()).thenReturn("process");
    when(variableRecordValue.getTenantId()).thenReturn(TENANT_ID);

    return variableRecordValue;
  }

  private VariableDocumentRecordValue mockVariableDocumentRecordValue() {
    final VariableDocumentRecordValue variableRecordValue = mock(VariableDocumentRecordValue.class);

    when(variableRecordValue.getScopeKey()).thenReturn(1L);
    when(variableRecordValue.getUpdateSemantics())
        .thenReturn(VariableDocumentUpdateSemantic.PROPAGATE);
    when(variableRecordValue.getVariables()).thenReturn(Collections.singletonMap("foo", 23));
    when(variableRecordValue.getTenantId()).thenReturn(TENANT_ID);

    return variableRecordValue;
  }

  private MessageSubscriptionRecordValue mockMessageSubscriptionRecordValue() {
    final MessageSubscriptionRecordValue messageSubscriptionRecordValue =
        mock(MessageSubscriptionRecordValue.class);

    when(messageSubscriptionRecordValue.getCorrelationKey()).thenReturn("key");
    when(messageSubscriptionRecordValue.getElementInstanceKey()).thenReturn(12L);
    when(messageSubscriptionRecordValue.getMessageName()).thenReturn("message");
    when(messageSubscriptionRecordValue.getProcessInstanceKey()).thenReturn(1L);
    when(messageSubscriptionRecordValue.getBpmnProcessId()).thenReturn("bpmnProcessId");
    when(messageSubscriptionRecordValue.getMessageKey()).thenReturn(2L);
    when(messageSubscriptionRecordValue.getVariables()).thenReturn(VARIABLES);
    when(messageSubscriptionRecordValue.isInterrupting()).thenReturn(true);
    when(messageSubscriptionRecordValue.getTenantId()).thenReturn(TENANT_ID);

    return messageSubscriptionRecordValue;
  }

  private ProcessMessageSubscriptionRecordValue mockProcessMessageSubscriptionRecordValue() {
    final ProcessMessageSubscriptionRecordValue workflowInstanceSubscriptionRecordValue =
        mock(ProcessMessageSubscriptionRecordValue.class);

    when(workflowInstanceSubscriptionRecordValue.getMessageName()).thenReturn("message");
    when(workflowInstanceSubscriptionRecordValue.getProcessInstanceKey()).thenReturn(1L);
    when(workflowInstanceSubscriptionRecordValue.getElementInstanceKey()).thenReturn(4L);
    when(workflowInstanceSubscriptionRecordValue.getBpmnProcessId()).thenReturn("bpmnProcessId");
    when(workflowInstanceSubscriptionRecordValue.getMessageKey()).thenReturn(2L);
    when(workflowInstanceSubscriptionRecordValue.getCorrelationKey()).thenReturn("correlationKey");
    when(workflowInstanceSubscriptionRecordValue.getElementId()).thenReturn("elementId");

    when(workflowInstanceSubscriptionRecordValue.getVariables())
        .thenReturn(Collections.singletonMap("foo", 23));
    when(workflowInstanceSubscriptionRecordValue.isInterrupting()).thenReturn(true);

    when(workflowInstanceSubscriptionRecordValue.getTenantId()).thenReturn(TENANT_ID);
    return workflowInstanceSubscriptionRecordValue;
  }

  private JobBatchRecordValue mockJobBatchRecordValue() {
    final JobBatchRecordValue jobBatchRecordValue = mock(JobBatchRecordValue.class);

    when(jobBatchRecordValue.getJobKeys()).thenReturn(Collections.singletonList(5L));
    final List<JobRecordValue> jobRecordValues = Collections.singletonList(mockJobRecordValue());
    when(jobBatchRecordValue.getJobs()).thenReturn(jobRecordValues);
    when(jobBatchRecordValue.getMaxJobsToActivate()).thenReturn(1);
    when(jobBatchRecordValue.getTimeout()).thenReturn(1000L);
    when(jobBatchRecordValue.getType()).thenReturn("jobType");
    when(jobBatchRecordValue.getWorker()).thenReturn("myveryownworker");
    when(jobBatchRecordValue.isTruncated()).thenReturn(true);
    when(jobBatchRecordValue.getTenantIds()).thenReturn(List.of(TENANT_ID));

    return jobBatchRecordValue;
  }

  private JobRecordValue mockJobRecordValue() {
    final JobRecordValue jobRecordValue = mock(JobRecordValue.class);

    when(jobRecordValue.getDeadline()).thenReturn(1000L);
    when(jobRecordValue.getErrorMessage()).thenReturn(null);
    when(jobRecordValue.getRetries()).thenReturn(3);
    when(jobRecordValue.getType()).thenReturn("jobType");
    when(jobRecordValue.getWorker()).thenReturn("myveryownworker");

    when(jobRecordValue.getCustomHeaders()).thenReturn(Collections.singletonMap("foo", "bar"));
    when(jobRecordValue.getVariables()).thenReturn(VARIABLES);

    when(jobRecordValue.getBpmnProcessId()).thenReturn("process");
    when(jobRecordValue.getElementId()).thenReturn("task");
    when(jobRecordValue.getElementInstanceKey()).thenReturn(3L);
    when(jobRecordValue.getProcessDefinitionVersion()).thenReturn(1);
    when(jobRecordValue.getProcessInstanceKey()).thenReturn(1L);
    when(jobRecordValue.getProcessDefinitionKey()).thenReturn(4L);
    when(jobRecordValue.getTenantId()).thenReturn(TENANT_ID);

    when(jobRecordValue.getErrorCode()).thenReturn(null);
    when(jobRecordValue.getJobKind()).thenReturn(JobKind.EXECUTION_LISTENER);
    when(jobRecordValue.getJobListenerEventType()).thenReturn(JobListenerEventType.START);

    return jobRecordValue;
  }

  private DeploymentRecordValue mockDeploymentRecordValue() {
    final DeploymentRecordValue deploymentRecordValue = mock(DeploymentRecordValue.class);

    final List<ProcessMetadataValue> workflows = new ArrayList<>();
    final ProcessMetadataValue processMetadata = mock(ProcessMetadataValue.class);
    when(processMetadata.getBpmnProcessId()).thenReturn("process");
    when(processMetadata.getResourceName()).thenReturn("process.bpmn");
    when(processMetadata.getVersion()).thenReturn(1);
    when(processMetadata.getProcessDefinitionKey()).thenReturn(4L);
    when(processMetadata.getChecksum()).thenReturn("checksum".getBytes());
    when(processMetadata.isDuplicate()).thenReturn(false);
    when(processMetadata.getTenantId()).thenReturn(TENANT_ID);
    when(processMetadata.getDeploymentKey()).thenReturn(7L);
    when(processMetadata.getVersionTag()).thenReturn("v7");
    workflows.add(processMetadata);

    when(deploymentRecordValue.getProcessesMetadata()).thenReturn(workflows);

    final List<DecisionRequirementsMetadataValue> decisionRequirementsMetadata = new ArrayList<>();
    decisionRequirementsMetadata.add(mockDecisionRequirementsRecordValue());
    when(deploymentRecordValue.getDecisionRequirementsMetadata())
        .thenReturn(decisionRequirementsMetadata);

    final List<DecisionRecordValue> decisionRecordValues = new ArrayList<>();
    decisionRecordValues.add(mockDecisionRecordValue());
    when(deploymentRecordValue.getDecisionsMetadata()).thenReturn(decisionRecordValues);

    final List<FormMetadataValue> formMetadata = new ArrayList<>();
    formMetadata.add(mockFormRecordValue());
    when(deploymentRecordValue.getFormMetadata()).thenReturn(formMetadata);

    final List<DeploymentResource> resources = new ArrayList<>();
    final DeploymentResource deploymentResource = mock(DeploymentResource.class);
    when(deploymentResource.getResource()).thenReturn("resourceContent".getBytes());
    when(deploymentResource.getResourceName()).thenReturn("process.bpmn");
    resources.add(deploymentResource);
    when(deploymentRecordValue.getResources()).thenReturn(resources);

    final ResourceMetadataValue resourceMetadata = mock(ResourceMetadataValue.class);
    when(resourceMetadata.getResourceId()).thenReturn("resourceId");
    when(resourceMetadata.getResourceName()).thenReturn("resourceName");
    when(resourceMetadata.getResourceKey()).thenReturn(1L);
    when(resourceMetadata.getChecksum()).thenReturn("checksum".getBytes());
    when(resourceMetadata.isDuplicate()).thenReturn(false);
    when(resourceMetadata.getTenantId()).thenReturn(TENANT_ID);
    when(resourceMetadata.getVersion()).thenReturn(42);
    when(resourceMetadata.getVersionTag()).thenReturn("v7");
    when(resourceMetadata.getDeploymentKey()).thenReturn(7L);
    when(deploymentRecordValue.getResourceMetadata())
        .thenReturn(Collections.singletonList(resourceMetadata));

    when(deploymentRecordValue.getTenantId()).thenReturn(TENANT_ID);
    when(deploymentRecordValue.getDeploymentKey()).thenReturn(12L);
    return deploymentRecordValue;
  }

  private DeploymentDistributionRecordValue mockDeploymentDistributionRecordValue() {
    final DeploymentDistributionRecordValue value = mock(DeploymentDistributionRecordValue.class);
    when(value.getPartitionId()).thenReturn(1);
    return value;
  }

  private Process mockProcessRecordValue() {
    final Process value = mock(Process.class);
    when(value.getBpmnProcessId()).thenReturn("process");
    when(value.getResourceName()).thenReturn("process.bpmn");
    when(value.getResource()).thenReturn("resourceContent".getBytes());
    when(value.getVersion()).thenReturn(1);
    when(value.getProcessDefinitionKey()).thenReturn(2L);
    when(value.getChecksum()).thenReturn("checksum".getBytes());
    when(value.getTenantId()).thenReturn(TENANT_ID);
    return value;
  }

  private ProcessInstanceRecordValue mockProcessInstanceRecordValue() {
    final ProcessInstanceRecordValue workflowInstanceRecordValue =
        mock(ProcessInstanceRecordValue.class);

    when(workflowInstanceRecordValue.getProcessInstanceKey()).thenReturn(1L);
    when(workflowInstanceRecordValue.getBpmnProcessId()).thenReturn("process");
    when(workflowInstanceRecordValue.getElementId()).thenReturn("startEvent");
    when(workflowInstanceRecordValue.getFlowScopeKey()).thenReturn(-1L);
    when(workflowInstanceRecordValue.getVersion()).thenReturn(1);
    when(workflowInstanceRecordValue.getProcessDefinitionKey()).thenReturn(4L);
    when(workflowInstanceRecordValue.getBpmnElementType()).thenReturn(BpmnElementType.START_EVENT);
    when(workflowInstanceRecordValue.getBpmnEventType()).thenReturn(BpmnEventType.NONE);
    when(workflowInstanceRecordValue.getParentProcessInstanceKey()).thenReturn(-1L);
    when(workflowInstanceRecordValue.getParentElementInstanceKey()).thenReturn(-1L);
    when(workflowInstanceRecordValue.getTenantId()).thenReturn(TENANT_ID);

    return workflowInstanceRecordValue;
  }

  private ProcessInstanceCreationRecordValue mockProcessInstanceCreationRecordValue() {
    final ProcessInstanceCreationRecordValue processInstanceCreationRecordValue =
        mock(ProcessInstanceCreationRecordValue.class);

    when(processInstanceCreationRecordValue.getBpmnProcessId()).thenReturn("process");
    when(processInstanceCreationRecordValue.getVersion()).thenReturn(1);
    when(processInstanceCreationRecordValue.getProcessDefinitionKey()).thenReturn(4L);
    when(processInstanceCreationRecordValue.getProcessInstanceKey()).thenReturn(1L);
    when(processInstanceCreationRecordValue.getVariables())
        .thenReturn(Collections.singletonMap("foo", 23));
    when(processInstanceCreationRecordValue.getTenantId()).thenReturn(TENANT_ID);
    List<ProcessInstanceCreationRecordValue.ProcessInstanceCreationStartInstructionValue>
        startInstructions =
            List.of(
                ImmutableProcessInstanceCreationStartInstructionValue.builder()
                    .withElementId("startId")
                    .build());
    when(processInstanceCreationRecordValue.getStartInstructions()).thenReturn(startInstructions);

    return processInstanceCreationRecordValue;
  }

  private ProcessInstanceResultRecordValue mockProcessInstanceResultRecordValue() {
    final ProcessInstanceResultRecordValue processInstanceCreationRecordValue =
        mock(ProcessInstanceResultRecordValue.class);

    when(processInstanceCreationRecordValue.getBpmnProcessId()).thenReturn("process");
    when(processInstanceCreationRecordValue.getVersion()).thenReturn(1);
    when(processInstanceCreationRecordValue.getProcessDefinitionKey()).thenReturn(4L);
    when(processInstanceCreationRecordValue.getProcessInstanceKey()).thenReturn(1L);
    when(processInstanceCreationRecordValue.getVariables())
        .thenReturn(Collections.singletonMap("foo", 23));
    when(processInstanceCreationRecordValue.getTenantId()).thenReturn(TENANT_ID);

    return processInstanceCreationRecordValue;
  }

  private ProcessInstanceBatchRecordValue mockProcessInstanceBatchRecordValue() {
    final ProcessInstanceBatchRecordValue processInstanceBatchRecordValue =
        mock(ProcessInstanceBatchRecordValue.class);

    when(processInstanceBatchRecordValue.getProcessInstanceKey()).thenReturn(1L);
    when(processInstanceBatchRecordValue.getBatchElementInstanceKey()).thenReturn(2L);
    when(processInstanceBatchRecordValue.getIndex()).thenReturn(3L);
    when(processInstanceBatchRecordValue.getTenantId()).thenReturn(TENANT_ID);

    return processInstanceBatchRecordValue;
  }

  private IncidentRecordValue mockIncidentRecordValue() {
    final IncidentRecordValue incidentRecordValue = mock(IncidentRecordValue.class);

    when(incidentRecordValue.getBpmnProcessId()).thenReturn("process");
    when(incidentRecordValue.getProcessDefinitionKey()).thenReturn(32L);
    when(incidentRecordValue.getElementId()).thenReturn("gateway");
    when(incidentRecordValue.getElementInstanceKey()).thenReturn(1L);
    when(incidentRecordValue.getProcessInstanceKey()).thenReturn(1L);
    when(incidentRecordValue.getVariableScopeKey()).thenReturn(1L);

    when(incidentRecordValue.getErrorMessage()).thenReturn("failed");
    when(incidentRecordValue.getErrorType()).thenReturn(ErrorType.JOB_NO_RETRIES);

    when(incidentRecordValue.getJobKey()).thenReturn(12L);
    when(incidentRecordValue.getTenantId()).thenReturn(TENANT_ID);

    return incidentRecordValue;
  }

  private ProcessEventRecordValue mockProcessEventRecordValue() {
    final var value = mock(ProcessEventRecordValue.class);
    when(value.getProcessDefinitionKey()).thenReturn(1L);
    when(value.getScopeKey()).thenReturn(2L);
    when(value.getTargetElementId()).thenReturn("targetElementId");
    when(value.getVariables()).thenReturn(VARIABLES);
    when(value.getTenantId()).thenReturn(TENANT_ID);
    return value;
  }

  private DecisionRecordValue mockDecisionRecordValue() {
    final var value = mock(DecisionRecordValue.class);
    when(value.getDecisionRequirementsKey()).thenReturn(1L);
    when(value.getDecisionRequirementsId()).thenReturn("decisionRequirementsId");
    when(value.getVersion()).thenReturn(2);
    when(value.getDecisionId()).thenReturn("decision");
    when(value.getDecisionName()).thenReturn("decisionName");
    when(value.getDecisionKey()).thenReturn(3L);
    when(value.isDuplicate()).thenReturn(false);
    when(value.getTenantId()).thenReturn(TENANT_ID);
    when(value.getDeploymentKey()).thenReturn(7L);
    when(value.getVersionTag()).thenReturn("v7");
    return value;
  }

  private DecisionRequirementsRecordValue mockDecisionRequirementsRecordValue() {
    final var value = mock(DecisionRequirementsRecordValue.class);
    when(value.getDecisionRequirementsKey()).thenReturn(1L);
    when(value.getDecisionRequirementsId()).thenReturn("decisionRequirementsId");
    when(value.getDecisionRequirementsName()).thenReturn("decisionRequirementsName");
    when(value.getDecisionRequirementsVersion()).thenReturn(3);
    when(value.getNamespace()).thenReturn("namespace");
    when(value.getResourceName()).thenReturn("resourceName");
    when(value.getChecksum()).thenReturn("checksum".getBytes());
    when(value.getResource()).thenReturn("resource".getBytes());
    when(value.isDuplicate()).thenReturn(false);
    when(value.getTenantId()).thenReturn(TENANT_ID);
    return value;
  }

  private DecisionEvaluationRecordValue mockDecisionEvaluationRecordValue() {
    final var value = mock(DecisionEvaluationRecordValue.class);

    when(value.getDecisionVersion()).thenReturn(2);
    when(value.getDecisionKey()).thenReturn(2L);
    when(value.getDecisionId()).thenReturn("decisionId");
    when(value.getDecisionName()).thenReturn("decisionName");
    when(value.getDecisionVersion()).thenReturn(2);
    when(value.getDecisionRequirementsId()).thenReturn("decisionRequirementsId");
    when(value.getDecisionRequirementsKey()).thenReturn(4L);
    when(value.getDecisionOutput()).thenReturn("decisionOutput");
    when(value.getBpmnProcessId()).thenReturn("bpmnProcessId");
    when(value.getProcessDefinitionKey()).thenReturn(3L);
    when(value.getProcessInstanceKey()).thenReturn(1L);
    when(value.getElementId()).thenReturn("elementId");
    when(value.getElementInstanceKey()).thenReturn(3L);
    when(value.getEvaluationFailureMessage()).thenReturn("evaluationFailureMessage");
    when(value.getFailedDecisionId()).thenReturn("failedDecisionId");

    final List<EvaluatedDecisionValue> evaluatedDecisions =
        Collections.singletonList(mockEvaluatedDecisionValue());
    when(value.getEvaluatedDecisions()).thenReturn(evaluatedDecisions);
    when(value.getTenantId()).thenReturn(TENANT_ID);
    return value;
  }

  private EvaluatedDecisionValue mockEvaluatedDecisionValue() {
    final var value = mock(EvaluatedDecisionValue.class);

    when(value.getDecisionId()).thenReturn("decisionId");
    when(value.getDecisionName()).thenReturn("decisionName");
    when(value.getDecisionKey()).thenReturn(2L);
    when(value.getDecisionVersion()).thenReturn(2);
    when(value.getDecisionType()).thenReturn("decisionType");
    when(value.getDecisionOutput()).thenReturn("decisionOutput");

    final List<EvaluatedInputValue> evaluatedInputs =
        Collections.singletonList(mockEvaluatedInputValue());
    when(value.getEvaluatedInputs()).thenReturn(evaluatedInputs);
    final List<MatchedRuleValue> matchedRules = Collections.singletonList(mockMatchedRuleValue());
    when(value.getMatchedRules()).thenReturn(matchedRules);
    when(value.getTenantId()).thenReturn(TENANT_ID);
    return value;
  }

  private MatchedRuleValue mockMatchedRuleValue() {
    final var value = mock(MatchedRuleValue.class);
    when(value.getRuleId()).thenReturn("ruleId");
    when(value.getRuleIndex()).thenReturn(3);
    final List<EvaluatedOutputValue> evaluatedOutputs =
        Collections.singletonList(mockEvaluatedOutputValue());
    when(value.getEvaluatedOutputs()).thenReturn(evaluatedOutputs);
    return value;
  }

  private EvaluatedOutputValue mockEvaluatedOutputValue() {
    final var value = mock(EvaluatedOutputValue.class);
    when(value.getOutputId()).thenReturn("outputId");
    when(value.getOutputName()).thenReturn("outputName");
    when(value.getOutputValue()).thenReturn("outputValue");
    return value;
  }

  private EvaluatedInputValue mockEvaluatedInputValue() {
    final var value = mock(EvaluatedInputValue.class);
    when(value.getInputId()).thenReturn("inputId");
    when(value.getInputName()).thenReturn("inputName");
    when(value.getInputValue()).thenReturn("inputValue");
    return value;
  }

  private ErrorRecordValue mockErrorRecordValue() {
    final ErrorRecordValue errorRecordValue = mock(ErrorRecordValue.class);

    when(errorRecordValue.getExceptionMessage()).thenReturn("exceptionMessage");
    when(errorRecordValue.getStacktrace()).thenReturn("stacktrace");
    when(errorRecordValue.getErrorEventPosition()).thenReturn(123L);
    when(errorRecordValue.getProcessInstanceKey()).thenReturn(1L);
    return errorRecordValue;
  }

  private Form mockFormRecordValue() {
    final var value = mock(Form.class);
    when(value.getFormId()).thenReturn("formId");
    when(value.getVersion()).thenReturn(2);
    when(value.getFormKey()).thenReturn(1L);
    when(value.getResourceName()).thenReturn("resourceName");
    when(value.getChecksum()).thenReturn("checksum".getBytes());
    when(value.getResource()).thenReturn("resource".getBytes());
    when(value.isDuplicate()).thenReturn(false);
    when(value.getTenantId()).thenReturn(TENANT_ID);
    when(value.getDeploymentKey()).thenReturn(7L);
    when(value.getVersionTag()).thenReturn("v7");
    return value;
  }

  private ResourceDeletionRecordValue mockResourceDeletionRecordValue() {
    final var value = mock(ResourceDeletionRecordValue.class);
    when(value.getResourceKey()).thenReturn(1L);
    when(value.getTenantId()).thenReturn(TENANT_ID);
    return value;
  }

  private UserTaskRecordValue mockUserTaskRecordValue() {
    final var value = mock(UserTaskRecordValue.class);
    when(value.getUserTaskKey()).thenReturn(1L);
    when(value.getAssignee()).thenReturn("assignee");
    when(value.getCandidateGroupsList()).thenReturn(List.of("group1", "group2"));
    when(value.getCandidateUsersList()).thenReturn(List.of("user1", "user2"));
    when(value.getDueDate()).thenReturn("2024-04-01T12:00:00Z");
    when(value.getFollowUpDate()).thenReturn("2024-04-02T12:00:00Z");
    when(value.getFormKey()).thenReturn(2L);
    when(value.getVariables()).thenReturn(VARIABLES);
    when(value.getBpmnProcessId()).thenReturn("bpmn-process-id");
    when(value.getProcessDefinitionVersion()).thenReturn(3);
    when(value.getProcessDefinitionKey()).thenReturn(4L);
    when(value.getProcessInstanceKey()).thenReturn(5L);
    when(value.getElementId()).thenReturn("element-id");
    when(value.getElementInstanceKey()).thenReturn(6L);
    when(value.getTenantId()).thenReturn(TENANT_ID);
    when(value.getExternalFormReference()).thenReturn("external-form-reference");
    when(value.getCustomHeaders()).thenReturn(Map.of("custom-header", "h1"));
    when(value.getChangedAttributes()).thenReturn(List.of("a1", "a2"));
    when(value.getAction()).thenReturn("action");
    when(value.getCreationTimestamp()).thenReturn(7L);
    when(value.getPriority()).thenReturn(100);
    return value;
  }

  private CompensationSubscriptionRecordValue mockCompensationSubscriptionRecordValue() {
    final var value = mock(CompensationSubscriptionRecordValue.class);
    when(value.getTenantId()).thenReturn(TENANT_ID);
    when(value.getProcessInstanceKey()).thenReturn(1L);
    when(value.getProcessDefinitionKey()).thenReturn(2L);
    when(value.getCompensableActivityId()).thenReturn("compensable-activity-id");
    when(value.getThrowEventId()).thenReturn("throw-event-id");
    when(value.getThrowEventInstanceKey()).thenReturn(3L);
    when(value.getCompensationHandlerId()).thenReturn("compensation-handler-id");
    when(value.getCompensationHandlerInstanceKey()).thenReturn(4L);
    when(value.getCompensableActivityScopeKey()).thenReturn(5L);
    when(value.getCompensableActivityInstanceKey()).thenReturn(6L);
    when(value.getVariables()).thenReturn(VARIABLES);
    return value;
  }

  private EscalationRecordValue mockEscalationRecordValue() {
    final var value = mock(EscalationRecordValue.class);
    when(value.getTenantId()).thenReturn(TENANT_ID);
    when(value.getProcessInstanceKey()).thenReturn(1L);
    when(value.getEscalationCode()).thenReturn("escalation-code");
    when(value.getThrowElementId()).thenReturn("throw-element-id");
    when(value.getCatchElementId()).thenReturn("catch-element-id");
    return value;
  }

  private ProcessInstanceMigrationRecordValue mockProcessInstanceMigrationRecordValue() {
    final var value = mock(ProcessInstanceMigrationRecordValue.class);
    when(value.getProcessInstanceKey()).thenReturn(1L);
    when(value.getTargetProcessDefinitionKey()).thenReturn(2L);

    final var instruction =
        mock(
            ProcessInstanceMigrationRecordValue.ProcessInstanceMigrationMappingInstructionValue
                .class);
    when(instruction.getSourceElementId()).thenReturn("source-element-id");
    when(instruction.getTargetElementId()).thenReturn("target-element-id");
    when(value.getMappingInstructions()).thenReturn(Collections.singletonList(instruction));

    return value;
  }

  private ClockRecordValue mockClockRecordValue() {
    final var value = mock(ClockRecordValue.class);
    when(value.getTime()).thenReturn(42L);
    return value;
  }

  private MessageCorrelationRecordValue mockMessageCorrelationRecordValue() {
    final var value = mock(MessageCorrelationRecordValue.class);

    when(value.getName()).thenReturn("name");
    when(value.getCorrelationKey()).thenReturn("correlation-key");
    when(value.getMessageKey()).thenReturn(1L);
    when(value.getRequestId()).thenReturn(2L);
    when(value.getRequestStreamId()).thenReturn(3);

    return value;
  }

  private Resource mockResource() {
    final var value = mock(Resource.class);

    when(value.getResourceId()).thenReturn("resourceId");
    when(value.getVersion()).thenReturn(42);
    when(value.getVersionTag()).thenReturn("v7");
    when(value.getResourceKey()).thenReturn(1L);
    when(value.getChecksum()).thenReturn("checksum".getBytes());
    when(value.getResourceName()).thenReturn("resourceName");
    when(value.isDuplicate()).thenReturn(false);
    when(value.getDeploymentKey()).thenReturn(7L);
    when(value.getTenantId()).thenReturn(TENANT_ID);
    when(value.getResourceProp()).thenReturn("resourceProp");

    return value;
  }

  private UserRecordValue mockUserRecordValue() {
    final var value = mock(UserRecordValue.class);

    when(value.getUserKey()).thenReturn(1L);
    when(value.getUsername()).thenReturn("username");
    when(value.getName()).thenReturn("name");
    when(value.getEmail()).thenReturn("email");
    when(value.getPassword()).thenReturn("password");
    when(value.getUserType()).thenReturn(UserType.DEFAULT);

    return value;
  }

  private AuthorizationRecordValue mockAuthorizationRecordValue() {
    final var value = mock(AuthorizationRecordValue.class);

    when(value.getAction()).thenReturn(PermissionAction.ADD);
    when(value.getOwnerKey()).thenReturn(1L);
    when(value.getOwnerType()).thenReturn(AuthorizationOwnerType.USER);
    when(value.getResourceType()).thenReturn(AuthorizationResourceType.PROCESS_DEFINITION);

    final var permission = mock(AuthorizationRecordValue.PermissionValue.class);
    when(permission.getPermissionType()).thenReturn(PermissionType.READ);
    when(permission.getResourceIds()).thenReturn(List.of("resource-1", "resource-2"));

    when(value.getPermissions()).thenReturn(List.of(permission));

    return value;
  }

  private void assertVariables(final Struct variables) {
    assertStruct(variables, VARIABLES);
  }

  private void assertStruct(final Struct actual, final Map<String, ?> expected) {
    assertThat(actual.getFieldsCount()).isEqualTo(expected.size());
    assertThat(actual.getFieldsMap().keySet()).containsExactlyElementsOf(expected.keySet());
    assertThat(actual.getFieldsMap())
        .allSatisfy(
            (key, value) -> {
              final Object expectedValue = expected.get(key);
              assertThat(expectedValue).isInstanceOfAny(String.class, Integer.class);

              if (expectedValue instanceof String) {
                assertThat(value.getKindCase()).isEqualTo(Value.KindCase.STRING_VALUE);
                assertThat(expectedValue).isEqualTo(value.getStringValue());

              } else if (expectedValue instanceof Integer) {
                assertThat(value.getKindCase()).isEqualTo(Value.KindCase.NUMBER_VALUE);
                assertThat(expectedValue).isEqualTo((int) value.getNumberValue());
              }
            });
  }

  private void assertJobRecord(final JobRecord jobRecord) {
    assertThat(jobRecord.getBpmnProcessId()).isEqualTo("process");
    assertThat(jobRecord.getElementId()).isEqualTo("task");
    assertThat(jobRecord.getProcessDefinitionKey()).isEqualTo(4L);
    assertThat(jobRecord.getProcessDefinitionVersion()).isEqualTo(1);
    assertThat(jobRecord.getProcessInstanceKey()).isEqualTo(1L);
    assertThat(jobRecord.getElementInstanceKey()).isEqualTo(3L);

    assertStruct(jobRecord.getCustomHeaders(), Map.of("foo", "bar"));
    assertStruct(jobRecord.getVariables(), VARIABLES);

    assertThat(jobRecord.getDeadline()).isEqualTo(1000L);
    assertThat(jobRecord.getErrorMessage()).isEqualTo("");
    assertThat(jobRecord.getRetries()).isEqualTo(3);
    assertThat(jobRecord.getType()).isEqualTo("jobType");
    assertThat(jobRecord.getWorker()).isEqualTo("myveryownworker");
    assertThat(jobRecord.getTenantId()).isEqualTo(TENANT_ID);
    assertThat(jobRecord.getErrorCode()).isEqualTo("");
    assertThat(jobRecord.getJobKind()).isEqualTo(JobRecord.JobKind.EXECUTION_LISTENER);
    assertThat(jobRecord.getJobListenerEventType()).isEqualTo(JobRecord.JobListenerEventType.START);
  }

  private void assertMetadata(
      final Schema.RecordMetadata metadata, final String valueType, final String intent) {
    assertThat(metadata.getRecordType()).isEqualTo(Schema.RecordMetadata.RecordType.COMMAND);
    assertThat(metadata.getValueType())
        .isEqualTo(Schema.RecordMetadata.ValueType.valueOf(valueType));
    assertThat(metadata.getIntent()).isEqualTo(intent);
    assertThat(metadata.getKey()).isEqualTo(KEY);
    assertThat(metadata.getPartitionId()).isEqualTo(PARTITION_ID);
    assertThat(metadata.getPosition()).isEqualTo(POSITION);
    assertThat(metadata.getSourceRecordPosition()).isEqualTo(SOURCE_POSITION);
    assertThat(metadata.getRejectionReason()).isEqualTo("failed");
    assertThat(metadata.getRejectionType()).isEqualTo("INVALID_ARGUMENT");
    assertThat(metadata.getTimestamp()).isEqualTo(TIMESTAMP);
  }

  private <V extends RecordValue> Record<V> mockRecord(
      final V recordValue, final ValueType valueType, final Intent intent) {

    final Record record = mock(Record.class);

    when(record.getKey()).thenReturn(KEY);
    when(record.getPosition()).thenReturn(POSITION);
    when(record.getSourceRecordPosition()).thenReturn(SOURCE_POSITION);
    when(record.getTimestamp()).thenReturn(TIMESTAMP);
    when(record.getPartitionId()).thenReturn(PARTITION_ID);

    when(record.getRejectionReason()).thenReturn("failed");
    when(record.getRejectionType()).thenReturn(RejectionType.INVALID_ARGUMENT);
    when(record.getRecordType()).thenReturn(RecordType.COMMAND);
    when(record.getValueType()).thenReturn(valueType);
    when(record.getIntent()).thenReturn(intent);

    when(record.getValue()).thenReturn(recordValue);

    return record;
  }
}
