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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.MapEntry.entry;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.zeebe.exporter.proto.Schema.JobRecord;
import io.zeebe.exporter.proto.Schema.RecordMetadata;
import io.zeebe.exporter.proto.Schema.VariableDocumentRecord.UpdateSemantics;
import io.zeebe.exporter.proto.Schema.WorkflowInstanceRecord;
import io.zeebe.protocol.record.Record;
import io.zeebe.protocol.record.RecordType;
import io.zeebe.protocol.record.RecordValue;
import io.zeebe.protocol.record.RejectionType;
import io.zeebe.protocol.record.ValueType;
import io.zeebe.protocol.record.intent.DeploymentIntent;
import io.zeebe.protocol.record.intent.ErrorIntent;
import io.zeebe.protocol.record.intent.IncidentIntent;
import io.zeebe.protocol.record.intent.Intent;
import io.zeebe.protocol.record.intent.JobBatchIntent;
import io.zeebe.protocol.record.intent.JobIntent;
import io.zeebe.protocol.record.intent.MessageIntent;
import io.zeebe.protocol.record.intent.MessageStartEventSubscriptionIntent;
import io.zeebe.protocol.record.intent.MessageSubscriptionIntent;
import io.zeebe.protocol.record.intent.TimerIntent;
import io.zeebe.protocol.record.intent.VariableDocumentIntent;
import io.zeebe.protocol.record.intent.VariableIntent;
import io.zeebe.protocol.record.intent.WorkflowInstanceCreationIntent;
import io.zeebe.protocol.record.intent.WorkflowInstanceIntent;
import io.zeebe.protocol.record.intent.WorkflowInstanceResultIntent;
import io.zeebe.protocol.record.intent.WorkflowInstanceSubscriptionIntent;
import io.zeebe.protocol.record.value.BpmnElementType;
import io.zeebe.protocol.record.value.DeploymentRecordValue;
import io.zeebe.protocol.record.value.ErrorRecordValue;
import io.zeebe.protocol.record.value.ErrorType;
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
import io.zeebe.protocol.record.value.deployment.ResourceType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;

public class RecordTransformTest {

  public static final long KEY = 200;
  public static final int PARTITION_ID = 1;
  public static final long POSITION = 300L;
  public static final long TIMESTAMP = 1000L;
  public static final long SOURCE_POSITION = 100L;

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

    final List<Schema.DeploymentRecord.Resource> resourcesList = deployment.getResourcesList();
    assertThat(resourcesList).hasSize(1);

    final Schema.DeploymentRecord.Resource resource = resourcesList.get(0);
    assertThat(resource.getResource().toStringUtf8()).isEqualTo("resourceContent");
    assertThat(resource.getResourceName()).isEqualTo("process.bpmn");
    assertThat(resource.getResourceType()).isEqualTo("BPMN_XML");

    final List<Schema.DeploymentRecord.Workflow> workflowsList =
        deployment.getDeployedWorkflowsList();
    assertThat(workflowsList).hasSize(1);

    final Schema.DeploymentRecord.Workflow workflow = workflowsList.get(0);
    assertThat(workflow.getBpmnProcessId()).isEqualTo("process");
    assertThat(workflow.getResourceName()).isEqualTo("process.bpmn");
    assertThat(workflow.getWorkflowKey()).isEqualTo(4L);
    assertThat(workflow.getVersion()).isEqualTo(1);
  }

  @Test
  public void shouldTransformWorkflowInstance() {
    // given
    final WorkflowInstanceRecordValue workflowInstanceRecordValue =
        mockWorkflowInstanceRecordValue();
    final Record<WorkflowInstanceRecordValue> mockedRecord =
        mockRecord(
            workflowInstanceRecordValue,
            ValueType.WORKFLOW_INSTANCE,
            WorkflowInstanceIntent.ELEMENT_ACTIVATED);

    // when
    final Schema.WorkflowInstanceRecord workflowInstance =
        (Schema.WorkflowInstanceRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(workflowInstance.getMetadata(), "WORKFLOW_INSTANCE", "ELEMENT_ACTIVATED");

    assertThat(workflowInstance.getBpmnProcessId()).isEqualTo("process");
    assertThat(workflowInstance.getElementId()).isEqualTo("startEvent");
    assertThat(workflowInstance.getWorkflowKey()).isEqualTo(4L);
    assertThat(workflowInstance.getVersion()).isEqualTo(1);
    assertThat(workflowInstance.getWorkflowInstanceKey()).isEqualTo(1L);
    assertThat(workflowInstance.getFlowScopeKey()).isEqualTo(-1L);
    assertThat(workflowInstance.getBpmnElementType())
        .isEqualTo(Schema.WorkflowInstanceRecord.BpmnElementType.START_EVENT);
    assertThat(workflowInstance.getParentWorkflowInstanceKey()).isEqualTo(-1L);
    assertThat(workflowInstance.getParentElementInstanceKey()).isEqualTo(-1L);
  }

  @Test
  public void shouldTransformWorkflowInstanceCreation() {
    // given
    final WorkflowInstanceCreationRecordValue workflowInstanceCreationRecordValue =
        mockWorkflowInstanceCreationRecordValue();
    final Record<WorkflowInstanceCreationRecordValue> mockedRecord =
        mockRecord(
            workflowInstanceCreationRecordValue,
            ValueType.WORKFLOW_INSTANCE_CREATION,
            WorkflowInstanceCreationIntent.CREATED);

    // when
    final Schema.WorkflowInstanceCreationRecord workflowInstanceCreation =
        (Schema.WorkflowInstanceCreationRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(workflowInstanceCreation.getMetadata(), "WORKFLOW_INSTANCE_CREATION", "CREATED");

    assertThat(workflowInstanceCreation.getBpmnProcessId()).isEqualTo("process");
    assertThat(workflowInstanceCreation.getWorkflowKey()).isEqualTo(4L);
    assertThat(workflowInstanceCreation.getVersion()).isEqualTo(1);
    assertThat(workflowInstanceCreation.getWorkflowInstanceKey()).isEqualTo(1L);
    assertVariables(workflowInstanceCreation.getVariables());
  }

  @Test
  public void shouldTransformWorkflowInstanceResult() {
    // given
    final WorkflowInstanceResultRecordValue recordValue = mockWorkflowInstanceResultRecordValue();
    final Record<WorkflowInstanceResultRecordValue> mockedRecord =
        mockRecord(
            recordValue,
            ValueType.WORKFLOW_INSTANCE_RESULT,
            WorkflowInstanceResultIntent.COMPLETED);

    // when
    final Schema.WorkflowInstanceResultRecord workflowInstanceResult =
        (Schema.WorkflowInstanceResultRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(workflowInstanceResult.getMetadata(), "WORKFLOW_INSTANCE_RESULT", "COMPLETED");

    assertThat(workflowInstanceResult.getBpmnProcessId()).isEqualTo("process");
    assertThat(workflowInstanceResult.getWorkflowKey()).isEqualTo(4L);
    assertThat(workflowInstanceResult.getVersion()).isEqualTo(1);
    assertThat(workflowInstanceResult.getWorkflowInstanceKey()).isEqualTo(1L);
    assertVariables(workflowInstanceResult.getVariables());
  }

  @Test
  public void shouldTransformJob() {
    // given
    final JobRecordValue jobRecordValue = mockJobRecordValue();
    final Record<JobRecordValue> mockedRecord =
        mockRecord(jobRecordValue, ValueType.JOB, JobIntent.CREATE);

    // when
    final Schema.JobRecord jobRecord =
        (Schema.JobRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(jobRecord.getMetadata(), "JOB", "CREATE");
    assertJobRecord(jobRecord);
  }

  @Test
  public void shouldTransformJobBatch() {
    // given
    final JobBatchRecordValue jobBatchRecordValue = mockJobBatchRecordValue();
    final Record<JobBatchRecordValue> mockedRecord =
        mockRecord(jobBatchRecordValue, ValueType.JOB_BATCH, JobBatchIntent.ACTIVATE);

    // when
    final Schema.JobBatchRecord jobBatchRecord =
        (Schema.JobBatchRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(jobBatchRecord.getMetadata(), "JOB_BATCH", "ACTIVATE");

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
        mockRecord(incidentRecordValue, ValueType.INCIDENT, IncidentIntent.CREATE);

    // when
    final Schema.IncidentRecord incidentRecord =
        (Schema.IncidentRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(incidentRecord.getMetadata(), "INCIDENT", "CREATE");

    assertThat(incidentRecord.getBpmnProcessId()).isEqualTo("process");
    assertThat(incidentRecord.getElementId()).isEqualTo("gateway");
    assertThat(incidentRecord.getElementInstanceKey()).isEqualTo(1L);
    assertThat(incidentRecord.getWorkflowInstanceKey()).isEqualTo(1L);
    assertThat(incidentRecord.getWorkflowKey()).isEqualTo(32L);
    assertThat(incidentRecord.getVariableScopeKey()).isEqualTo(1L);

    assertThat(incidentRecord.getErrorMessage()).isEqualTo("failed");
    assertThat(incidentRecord.getErrorType()).isEqualTo(ErrorType.JOB_NO_RETRIES.name());

    assertThat(incidentRecord.getJobKey()).isEqualTo(12L);
  }

  @Test
  public void shouldTransformMessage() {
    // given
    final MessageRecordValue messageRecordValue = mockMessageRecordValue();
    final Record<MessageRecordValue> mockedRecord =
        mockRecord(messageRecordValue, ValueType.MESSAGE, MessageIntent.PUBLISH);

    // when
    final Schema.MessageRecord messageRecord =
        (Schema.MessageRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(messageRecord.getMetadata(), "MESSAGE", "PUBLISH");

    assertThat(messageRecord.getCorrelationKey()).isEqualTo("key");
    assertThat(messageRecord.getMessageId()).isEqualTo("msgId");
    assertThat(messageRecord.getName()).isEqualTo("message");
    assertThat(messageRecord.getTimeToLive()).isEqualTo(1000L);

    assertVariables(messageRecord.getVariables());
  }

  @Test
  public void shouldTransformTimer() {
    // given
    final TimerRecordValue timerRecordValue = mockTimerRecordValue();
    final Record<TimerRecordValue> mockedRecord =
        mockRecord(timerRecordValue, ValueType.TIMER, TimerIntent.CREATE);

    // when
    final Schema.TimerRecord timerRecord =
        (Schema.TimerRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(timerRecord.getMetadata(), "TIMER", "CREATE");

    assertThat(timerRecord.getDueDate()).isEqualTo(1000L);
    assertThat(timerRecord.getRepetitions()).isEqualTo(1);
    assertThat(timerRecord.getElementInstanceKey()).isEqualTo(1L);
    assertThat(timerRecord.getTargetFlowNodeId()).isEqualTo("timerCatch");
    assertThat(timerRecord.getWorkflowInstanceKey()).isEqualTo(2L);
    assertThat(timerRecord.getWorkflowKey()).isEqualTo(3L);
  }

  @Test
  public void shouldTransformMessageSubscription() {
    // given
    final MessageSubscriptionRecordValue value = mockMessageSubscriptionRecordValue();
    final Record<MessageSubscriptionRecordValue> mockedRecord =
        mockRecord(value, ValueType.MESSAGE_SUBSCRIPTION, MessageSubscriptionIntent.CORRELATE);

    // when
    final Schema.MessageSubscriptionRecord messageSubscriptionRecord =
        (Schema.MessageSubscriptionRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(messageSubscriptionRecord.getMetadata(), "MESSAGE_SUBSCRIPTION", "CORRELATE");

    assertThat(messageSubscriptionRecord.getCorrelationKey()).isEqualTo("key");
    assertThat(messageSubscriptionRecord.getMessageName()).isEqualTo("message");
    assertThat(messageSubscriptionRecord.getElementInstanceKey()).isEqualTo(12L);
    assertThat(messageSubscriptionRecord.getWorkflowInstanceKey()).isEqualTo(1L);
    assertThat(messageSubscriptionRecord.getBpmnProcessId()).isEqualTo(value.getBpmnProcessId());
    assertThat(messageSubscriptionRecord.getMessageKey()).isEqualTo(value.getMessageKey());
  }

  @Test
  public void shouldTransformWorkflowInstanceSubscription() {
    // given
    final WorkflowInstanceSubscriptionRecordValue value =
        mockWorkflowInstanceSubscriptionRecordValue();
    final Record<WorkflowInstanceSubscriptionRecordValue> mockedRecord =
        mockRecord(
            value,
            ValueType.WORKFLOW_INSTANCE_SUBSCRIPTION,
            WorkflowInstanceSubscriptionIntent.CORRELATE);

    // when
    final Schema.WorkflowInstanceSubscriptionRecord workflowInstanceSubscriptionRecord =
        (Schema.WorkflowInstanceSubscriptionRecord)
            RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(
        workflowInstanceSubscriptionRecord.getMetadata(),
        "WORKFLOW_INSTANCE_SUBSCRIPTION",
        "CORRELATE");

    assertVariables(workflowInstanceSubscriptionRecord.getVariables());

    assertThat(workflowInstanceSubscriptionRecord.getMessageName()).isEqualTo("message");
    assertThat(workflowInstanceSubscriptionRecord.getElementInstanceKey()).isEqualTo(4L);
    assertThat(workflowInstanceSubscriptionRecord.getWorkflowInstanceKey()).isEqualTo(1L);
    assertThat(workflowInstanceSubscriptionRecord.getBpmnProcessId())
        .isEqualTo(value.getBpmnProcessId());
    assertThat(workflowInstanceSubscriptionRecord.getMessageKey()).isEqualTo(value.getMessageKey());
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
    assertThat(variableRecord.getWorkflowInstanceKey())
        .isEqualTo(variableRecordValue.getWorkflowInstanceKey());
    assertThat(variableRecord.getWorkflowKey()).isEqualTo(variableRecordValue.getWorkflowKey());
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
    assertVariables(variableRecord.getVariables());
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
            MessageStartEventSubscriptionIntent.OPEN);

    // when
    final Schema.MessageStartEventSubscriptionRecord transformed =
        (Schema.MessageStartEventSubscriptionRecord) RecordTransformer.toProtobufMessage(record);

    // then
    assertMetadata(transformed.getMetadata(), "MESSAGE_START_EVENT_SUBSCRIPTION", "OPEN");
    assertThat(transformed.getMessageName()).isEqualTo(value.getMessageName());
    assertThat(transformed.getStartEventId()).isEqualTo(value.getStartEventId());
    assertThat(transformed.getWorkflowKey()).isEqualTo(value.getWorkflowKey());
    assertThat(transformed.getBpmnProcessId()).isEqualTo(value.getBpmnProcessId());
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
    assertThat(transformed.getWorkflowInstanceKey()).isEqualTo(value.getWorkflowInstanceKey());
  }

  @Test
  public void shouldTransformBpmnElementType() {

    final List<String> bpmnElementTypes =
        Arrays.stream(BpmnElementType.values())
            .map(BpmnElementType::name)
            .collect(Collectors.toList());

    assertThat(WorkflowInstanceRecord.BpmnElementType.values())
        .extracting(WorkflowInstanceRecord.BpmnElementType::name)
        .containsAll(bpmnElementTypes);
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

    final List<String> valueTypes =
        Arrays.stream(ValueType.values())
            .filter(t -> t != ValueType.NULL_VAL && t != ValueType.SBE_UNKNOWN)
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

  private MessageRecordValue mockMessageRecordValue() {
    final MessageRecordValue messageRecordValue = mock(MessageRecordValue.class);

    when(messageRecordValue.getCorrelationKey()).thenReturn("key");
    when(messageRecordValue.getMessageId()).thenReturn("msgId");
    when(messageRecordValue.getName()).thenReturn("message");
    when(messageRecordValue.getTimeToLive()).thenReturn(1000L);

    when(messageRecordValue.getVariables()).thenReturn(Collections.singletonMap("foo", 23));

    return messageRecordValue;
  }

  private MessageStartEventSubscriptionRecordValue mockMessageStartEventSubscriptionRecordValue() {
    final MessageStartEventSubscriptionRecordValue value =
        mock(MessageStartEventSubscriptionRecordValue.class);

    when(value.getMessageName()).thenReturn("message");
    when(value.getStartEventId()).thenReturn("start");
    when(value.getWorkflowKey()).thenReturn(1L);
    when(value.getBpmnProcessId()).thenReturn("bpmnProcessId");

    return value;
  }

  private TimerRecordValue mockTimerRecordValue() {
    final TimerRecordValue timerRecordValue = mock(TimerRecordValue.class);

    when(timerRecordValue.getDueDate()).thenReturn(1000L);
    when(timerRecordValue.getRepetitions()).thenReturn(1);
    when(timerRecordValue.getElementInstanceKey()).thenReturn(1L);
    when(timerRecordValue.getTargetElementId()).thenReturn("timerCatch");
    when(timerRecordValue.getWorkflowInstanceKey()).thenReturn(2L);
    when(timerRecordValue.getWorkflowKey()).thenReturn(3L);

    return timerRecordValue;
  }

  private VariableRecordValue mockVariableRecordValue() {
    final VariableRecordValue variableRecordValue = mock(VariableRecordValue.class);

    when(variableRecordValue.getName()).thenReturn("var");
    when(variableRecordValue.getScopeKey()).thenReturn(1L);
    when(variableRecordValue.getValue()).thenReturn("true");
    when(variableRecordValue.getWorkflowInstanceKey()).thenReturn(1L);
    when(variableRecordValue.getWorkflowKey()).thenReturn(2L);

    return variableRecordValue;
  }

  private VariableDocumentRecordValue mockVariableDocumentRecordValue() {
    final VariableDocumentRecordValue variableRecordValue = mock(VariableDocumentRecordValue.class);

    when(variableRecordValue.getScopeKey()).thenReturn(1L);
    when(variableRecordValue.getUpdateSemantics())
        .thenReturn(VariableDocumentUpdateSemantic.PROPAGATE);
    when(variableRecordValue.getVariables()).thenReturn(Collections.singletonMap("foo", 23));

    return variableRecordValue;
  }

  private MessageSubscriptionRecordValue mockMessageSubscriptionRecordValue() {
    final MessageSubscriptionRecordValue messageSubscriptionRecordValue =
        mock(MessageSubscriptionRecordValue.class);

    when(messageSubscriptionRecordValue.getCorrelationKey()).thenReturn("key");
    when(messageSubscriptionRecordValue.getElementInstanceKey()).thenReturn(12L);
    when(messageSubscriptionRecordValue.getMessageName()).thenReturn("message");
    when(messageSubscriptionRecordValue.getWorkflowInstanceKey()).thenReturn(1L);
    when(messageSubscriptionRecordValue.getBpmnProcessId()).thenReturn("bpmnProcessId");
    when(messageSubscriptionRecordValue.getMessageKey()).thenReturn(2L);

    return messageSubscriptionRecordValue;
  }

  private WorkflowInstanceSubscriptionRecordValue mockWorkflowInstanceSubscriptionRecordValue() {
    final WorkflowInstanceSubscriptionRecordValue workflowInstanceSubscriptionRecordValue =
        mock(WorkflowInstanceSubscriptionRecordValue.class);

    when(workflowInstanceSubscriptionRecordValue.getMessageName()).thenReturn("message");
    when(workflowInstanceSubscriptionRecordValue.getWorkflowInstanceKey()).thenReturn(1L);
    when(workflowInstanceSubscriptionRecordValue.getElementInstanceKey()).thenReturn(4L);
    when(workflowInstanceSubscriptionRecordValue.getBpmnProcessId()).thenReturn("bpmnProcessId");
    when(workflowInstanceSubscriptionRecordValue.getMessageKey()).thenReturn(2L);

    when(workflowInstanceSubscriptionRecordValue.getVariables())
        .thenReturn(Collections.singletonMap("foo", 23));

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

    return jobBatchRecordValue;
  }

  private JobRecordValue mockJobRecordValue() {
    final JobRecordValue jobRecordValue = mock(JobRecordValue.class);

    when(jobRecordValue.getDeadline()).thenReturn(1000L);
    when(jobRecordValue.getErrorMessage()).thenReturn("this is an error msg");
    when(jobRecordValue.getRetries()).thenReturn(3);
    when(jobRecordValue.getType()).thenReturn("jobType");
    when(jobRecordValue.getWorker()).thenReturn("myveryownworker");

    when(jobRecordValue.getCustomHeaders()).thenReturn(Collections.singletonMap("foo", "bar"));
    when(jobRecordValue.getVariables()).thenReturn(Collections.singletonMap("foo", 23));

    when(jobRecordValue.getBpmnProcessId()).thenReturn("process");
    when(jobRecordValue.getElementId()).thenReturn("task");
    when(jobRecordValue.getElementInstanceKey()).thenReturn(3L);
    when(jobRecordValue.getWorkflowDefinitionVersion()).thenReturn(1);
    when(jobRecordValue.getWorkflowInstanceKey()).thenReturn(1L);
    when(jobRecordValue.getWorkflowKey()).thenReturn(4L);

    return jobRecordValue;
  }

  private DeploymentRecordValue mockDeploymentRecordValue() {
    final DeploymentRecordValue deploymentRecordValue = mock(DeploymentRecordValue.class);

    final List<DeployedWorkflow> workflows = new ArrayList<>();
    final DeployedWorkflow deployedWorkflow = mock(DeployedWorkflow.class);
    when(deployedWorkflow.getBpmnProcessId()).thenReturn("process");
    when(deployedWorkflow.getResourceName()).thenReturn("process.bpmn");
    when(deployedWorkflow.getVersion()).thenReturn(1);
    when(deployedWorkflow.getWorkflowKey()).thenReturn(4L);
    workflows.add(deployedWorkflow);

    when(deploymentRecordValue.getDeployedWorkflows()).thenReturn(workflows);

    final List<DeploymentResource> resources = new ArrayList<>();
    final DeploymentResource deploymentResource = mock(DeploymentResource.class);
    when(deploymentResource.getResource()).thenReturn("resourceContent".getBytes());
    when(deploymentResource.getResourceName()).thenReturn("process.bpmn");
    when(deploymentResource.getResourceType()).thenReturn(ResourceType.BPMN_XML);
    resources.add(deploymentResource);

    when(deploymentRecordValue.getResources()).thenReturn(resources);
    return deploymentRecordValue;
  }

  private WorkflowInstanceRecordValue mockWorkflowInstanceRecordValue() {
    final WorkflowInstanceRecordValue workflowInstanceRecordValue =
        mock(WorkflowInstanceRecordValue.class);

    when(workflowInstanceRecordValue.getWorkflowInstanceKey()).thenReturn(1L);
    when(workflowInstanceRecordValue.getBpmnProcessId()).thenReturn("process");
    when(workflowInstanceRecordValue.getElementId()).thenReturn("startEvent");
    when(workflowInstanceRecordValue.getFlowScopeKey()).thenReturn(-1L);
    when(workflowInstanceRecordValue.getVersion()).thenReturn(1);
    when(workflowInstanceRecordValue.getWorkflowKey()).thenReturn(4L);
    when(workflowInstanceRecordValue.getBpmnElementType()).thenReturn(BpmnElementType.START_EVENT);
    when(workflowInstanceRecordValue.getParentWorkflowInstanceKey()).thenReturn(-1L);
    when(workflowInstanceRecordValue.getParentElementInstanceKey()).thenReturn(-1L);

    return workflowInstanceRecordValue;
  }

  private WorkflowInstanceCreationRecordValue mockWorkflowInstanceCreationRecordValue() {
    final WorkflowInstanceCreationRecordValue workflowInstanceCreationRecordValue =
        mock(WorkflowInstanceCreationRecordValue.class);

    when(workflowInstanceCreationRecordValue.getBpmnProcessId()).thenReturn("process");
    when(workflowInstanceCreationRecordValue.getVersion()).thenReturn(1);
    when(workflowInstanceCreationRecordValue.getWorkflowKey()).thenReturn(4L);
    when(workflowInstanceCreationRecordValue.getWorkflowInstanceKey()).thenReturn(1L);
    when(workflowInstanceCreationRecordValue.getVariables())
        .thenReturn(Collections.singletonMap("foo", 23));

    return workflowInstanceCreationRecordValue;
  }

  private WorkflowInstanceResultRecordValue mockWorkflowInstanceResultRecordValue() {
    final WorkflowInstanceResultRecordValue recordValue =
        mock(WorkflowInstanceResultRecordValue.class);

    when(recordValue.getBpmnProcessId()).thenReturn("process");
    when(recordValue.getVersion()).thenReturn(1);
    when(recordValue.getWorkflowKey()).thenReturn(4L);
    when(recordValue.getWorkflowInstanceKey()).thenReturn(1L);
    when(recordValue.getVariables()).thenReturn(Collections.singletonMap("foo", 23));

    return recordValue;
  }

  private IncidentRecordValue mockIncidentRecordValue() {
    final IncidentRecordValue incidentRecordValue = mock(IncidentRecordValue.class);

    when(incidentRecordValue.getBpmnProcessId()).thenReturn("process");
    when(incidentRecordValue.getWorkflowKey()).thenReturn(32L);
    when(incidentRecordValue.getElementId()).thenReturn("gateway");
    when(incidentRecordValue.getElementInstanceKey()).thenReturn(1L);
    when(incidentRecordValue.getWorkflowInstanceKey()).thenReturn(1L);
    when(incidentRecordValue.getVariableScopeKey()).thenReturn(1L);

    when(incidentRecordValue.getErrorMessage()).thenReturn("failed");
    when(incidentRecordValue.getErrorType()).thenReturn(ErrorType.JOB_NO_RETRIES);

    when(incidentRecordValue.getJobKey()).thenReturn(12L);

    return incidentRecordValue;
  }

  private ErrorRecordValue mockErrorRecordValue() {
    final ErrorRecordValue errorRecordValue = mock(ErrorRecordValue.class);

    when(errorRecordValue.getExceptionMessage()).thenReturn("exceptionMessage");
    when(errorRecordValue.getStacktrace()).thenReturn("stacktrace");
    when(errorRecordValue.getErrorEventPosition()).thenReturn(123L);
    when(errorRecordValue.getWorkflowInstanceKey()).thenReturn(1L);
    return errorRecordValue;
  }

  private void assertVariables(final Struct payload) {
    assertThat(payload.getFieldsCount()).isEqualTo(1);
    assertThat(payload.getFieldsMap())
        .containsExactly(entry("foo", Value.newBuilder().setNumberValue(23).build()));
  }

  private void assertJobRecord(final JobRecord jobRecord) {
    assertThat(jobRecord.getBpmnProcessId()).isEqualTo("process");
    assertThat(jobRecord.getElementId()).isEqualTo("task");
    assertThat(jobRecord.getWorkflowKey()).isEqualTo(4L);
    assertThat(jobRecord.getWorkflowDefinitionVersion()).isEqualTo(1);
    assertThat(jobRecord.getWorkflowInstanceKey()).isEqualTo(1L);
    assertThat(jobRecord.getElementInstanceKey()).isEqualTo(3L);

    assertThat(jobRecord.getCustomHeaders().getFieldsCount()).isEqualTo(1);
    assertThat(jobRecord.getCustomHeaders().getFieldsMap())
        .containsExactly(entry("foo", Value.newBuilder().setStringValue("bar").build()));
    assertVariables(jobRecord.getVariables());

    assertThat(jobRecord.getDeadline()).isEqualTo(1000L);
    assertThat(jobRecord.getErrorMessage()).isEqualTo("this is an error msg");
    assertThat(jobRecord.getRetries()).isEqualTo(3);
    assertThat(jobRecord.getType()).isEqualTo("jobType");
    assertThat(jobRecord.getWorker()).isEqualTo("myveryownworker");
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
