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

import com.google.protobuf.Empty;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import io.zeebe.exporter.api.record.Record;
import io.zeebe.exporter.api.record.RecordMetadata;
import io.zeebe.exporter.api.record.RecordValue;
import io.zeebe.exporter.api.record.value.DeploymentRecordValue;
import io.zeebe.exporter.api.record.value.ErrorRecordValue;
import io.zeebe.exporter.api.record.value.IncidentRecordValue;
import io.zeebe.exporter.api.record.value.JobBatchRecordValue;
import io.zeebe.exporter.api.record.value.JobRecordValue;
import io.zeebe.exporter.api.record.value.MessageRecordValue;
import io.zeebe.exporter.api.record.value.MessageStartEventSubscriptionRecordValue;
import io.zeebe.exporter.api.record.value.MessageSubscriptionRecordValue;
import io.zeebe.exporter.api.record.value.RaftRecordValue;
import io.zeebe.exporter.api.record.value.TimerRecordValue;
import io.zeebe.exporter.api.record.value.VariableDocumentRecordValue;
import io.zeebe.exporter.api.record.value.VariableRecordValue;
import io.zeebe.exporter.api.record.value.WorkflowInstanceCreationRecordValue;
import io.zeebe.exporter.api.record.value.WorkflowInstanceRecordValue;
import io.zeebe.exporter.api.record.value.WorkflowInstanceSubscriptionRecordValue;
import io.zeebe.exporter.api.record.value.deployment.DeployedWorkflow;
import io.zeebe.exporter.api.record.value.deployment.DeploymentResource;
import io.zeebe.exporter.api.record.value.deployment.ResourceType;
import io.zeebe.exporter.api.record.value.job.Headers;
import io.zeebe.exporter.api.record.value.raft.RaftMember;
import io.zeebe.exporter.proto.Schema.JobRecord;
import io.zeebe.exporter.proto.Schema.RaftRecord.Member;
import io.zeebe.protocol.VariableDocumentUpdateSemantic;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.clientapi.RejectionType;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.intent.DeploymentIntent;
import io.zeebe.protocol.intent.ErrorIntent;
import io.zeebe.protocol.intent.IncidentIntent;
import io.zeebe.protocol.intent.Intent;
import io.zeebe.protocol.intent.JobBatchIntent;
import io.zeebe.protocol.intent.JobIntent;
import io.zeebe.protocol.intent.MessageIntent;
import io.zeebe.protocol.intent.MessageStartEventSubscriptionIntent;
import io.zeebe.protocol.intent.MessageSubscriptionIntent;
import io.zeebe.protocol.intent.RaftIntent;
import io.zeebe.protocol.intent.TimerIntent;
import io.zeebe.protocol.intent.VariableDocumentIntent;
import io.zeebe.protocol.intent.VariableIntent;
import io.zeebe.protocol.intent.WorkflowInstanceCreationIntent;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import io.zeebe.protocol.intent.WorkflowInstanceSubscriptionIntent;
import io.zeebe.test.exporter.record.MockRecordMetadata;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class RecordTransformTest {

  @Test
  public void shouldTransformDeployment() {
    // given
    final DeploymentRecordValue deploymentRecordValue = mockDeploymentRecordValue();
    final RecordMetadata recordMetadata =
        mockRecordMetadata(ValueType.DEPLOYMENT, DeploymentIntent.CREATE);
    final Record<DeploymentRecordValue> mockedRecord =
        mockRecord(deploymentRecordValue, recordMetadata);

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

    final List<Schema.DeploymentRecord.Workflow> workflowsList = deployment.getWorkflowsList();
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
    final RecordMetadata recordMetadata =
        mockRecordMetadata(ValueType.WORKFLOW_INSTANCE, WorkflowInstanceIntent.ELEMENT_ACTIVATED);
    final Record<WorkflowInstanceRecordValue> mockedRecord =
        mockRecord(workflowInstanceRecordValue, recordMetadata);

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
  }

  @Test
  public void shouldTransformWorkflowInstanceCreation() {
    // given
    final WorkflowInstanceCreationRecordValue workflowInstanceCreationRecordValue =
        mockWorkflowInstanceCreationRecordValue();
    final RecordMetadata recordMetadata =
        mockRecordMetadata(
            ValueType.WORKFLOW_INSTANCE_CREATION, WorkflowInstanceCreationIntent.CREATED);
    final Record<WorkflowInstanceCreationRecordValue> mockedRecord =
        mockRecord(workflowInstanceCreationRecordValue, recordMetadata);

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
  public void shouldTransformJob() {
    // given
    final JobRecordValue jobRecordValue = mockJobRecordValue();
    final RecordMetadata recordMetadata = mockRecordMetadata(ValueType.JOB, JobIntent.CREATE);
    final Record<JobRecordValue> mockedRecord = mockRecord(jobRecordValue, recordMetadata);

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
    final RecordMetadata recordMetadata =
        mockRecordMetadata(ValueType.JOB_BATCH, JobBatchIntent.ACTIVATE);
    final Record<JobBatchRecordValue> mockedRecord =
        mockRecord(jobBatchRecordValue, recordMetadata);

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

    assertThat(jobBatchRecord.getJobsList()).hasSize(1);
    final JobRecord jobRecord = jobBatchRecord.getJobsList().get(0);
    assertJobRecord(jobRecord);
  }

  @Test
  public void shouldTransformIncident() {
    // given
    final IncidentRecordValue incidentRecordValue = mockIncidentRecordValue();
    final RecordMetadata recordMetadata =
        mockRecordMetadata(ValueType.INCIDENT, IncidentIntent.CREATE);
    final Record<IncidentRecordValue> mockedRecord =
        mockRecord(incidentRecordValue, recordMetadata);

    // when
    final Schema.IncidentRecord incidentRecord =
        (Schema.IncidentRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(incidentRecord.getMetadata(), "INCIDENT", "CREATE");

    assertThat(incidentRecord.getBpmnProcessId()).isEqualTo("process");
    assertThat(incidentRecord.getElementId()).isEqualTo("gateway");
    assertThat(incidentRecord.getElementInstanceKey()).isEqualTo(1L);
    assertThat(incidentRecord.getWorkflowInstanceKey()).isEqualTo(1L);

    assertThat(incidentRecord.getErrorMessage()).isEqualTo("failed");
    assertThat(incidentRecord.getErrorType()).isEqualTo("boom");

    assertThat(incidentRecord.getJobKey()).isEqualTo(12L);
  }

  @Test
  public void shouldTransformMessage() {
    // given
    final MessageRecordValue messageRecordValue = mockMessageRecordValue();
    final RecordMetadata recordMetadata =
        mockRecordMetadata(ValueType.MESSAGE, MessageIntent.PUBLISH);
    final Record<MessageRecordValue> mockedRecord = mockRecord(messageRecordValue, recordMetadata);

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
    final RecordMetadata recordMetadata = mockRecordMetadata(ValueType.TIMER, TimerIntent.CREATE);
    final Record<TimerRecordValue> mockedRecord = mockRecord(timerRecordValue, recordMetadata);

    // when
    final Schema.TimerRecord timerRecord =
        (Schema.TimerRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(timerRecord.getMetadata(), "TIMER", "CREATE");

    assertThat(timerRecord.getDueDate()).isEqualTo(1000L);
    assertThat(timerRecord.getElementInstanceKey()).isEqualTo(1L);
    assertThat(timerRecord.getHandlerFlowNodeId()).isEqualTo("timerCatch");
    assertThat(timerRecord.getWorkflowInstanceKey()).isEqualTo(2L);
  }

  @Test
  public void shouldTransformMessageSubscription() {
    // given
    final MessageSubscriptionRecordValue messageSubscriptionRecordValue =
        mockMessageSubscriptionRecordValue();
    final RecordMetadata recordMetadata =
        mockRecordMetadata(ValueType.MESSAGE_SUBSCRIPTION, MessageSubscriptionIntent.CORRELATE);
    final Record<MessageSubscriptionRecordValue> mockedRecord =
        mockRecord(messageSubscriptionRecordValue, recordMetadata);

    // when
    final Schema.MessageSubscriptionRecord messageSubscriptionRecord =
        (Schema.MessageSubscriptionRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(messageSubscriptionRecord.getMetadata(), "MESSAGE_SUBSCRIPTION", "CORRELATE");

    assertThat(messageSubscriptionRecord.getCorrelationKey()).isEqualTo("key");
    assertThat(messageSubscriptionRecord.getMessageName()).isEqualTo("message");
    assertThat(messageSubscriptionRecord.getElementInstanceKey()).isEqualTo(12L);
    assertThat(messageSubscriptionRecord.getWorkflowInstanceKey()).isEqualTo(1L);
  }

  @Test
  public void shouldTransformWorkflowInstanceSubscription() {
    // given
    final WorkflowInstanceSubscriptionRecordValue workflowInstanceSubscriptionRecordValue =
        mockWorkflowInstanceSubscriptionRecordValue();
    final RecordMetadata recordMetadata =
        mockRecordMetadata(
            ValueType.WORKFLOW_INSTANCE_SUBSCRIPTION, WorkflowInstanceSubscriptionIntent.CORRELATE);
    final Record<WorkflowInstanceSubscriptionRecordValue> mockedRecord =
        mockRecord(workflowInstanceSubscriptionRecordValue, recordMetadata);

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
  }

  @Test
  public void shouldTransformRaftRecordValue() {
    // given
    final RaftRecordValue raftRecordValue = mockRaftRecordValue();
    final RecordMetadata recordMetadata =
        mockRecordMetadata(ValueType.RAFT, RaftIntent.MEMBER_ADDED);
    final Record<RaftRecordValue> mockedRecord = mockRecord(raftRecordValue, recordMetadata);

    // when
    final Schema.RaftRecord raftRecord =
        (Schema.RaftRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(raftRecord.getMetadata(), "RAFT", "MEMBER_ADDED");

    assertThat(raftRecord.getMembersList()).hasSize(1);
    final Member member = raftRecord.getMembersList().get(0);
    assertThat(member.getNodeId()).isEqualTo(1);
  }

  @Test
  public void shouldTransformVariableRecordValue() {
    // given
    final VariableRecordValue variableRecordValue = mockVariableRecordValue();
    final RecordMetadata recordMetadata =
        mockRecordMetadata(ValueType.VARIABLE, VariableIntent.CREATED);
    final Record<VariableRecordValue> mockedRecord =
        mockRecord(variableRecordValue, recordMetadata);

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
  }

  @Test
  public void shouldTransformVariableDocumentRecordValue() {
    // given
    final VariableDocumentRecordValue variableDocumentRecordValue =
        mockVariableDocumentRecordValue();
    final RecordMetadata recordMetadata =
        mockRecordMetadata(ValueType.VARIABLE_DOCUMENT, VariableDocumentIntent.UPDATED);
    final Record<VariableDocumentRecordValue> mockedRecord =
        mockRecord(variableDocumentRecordValue, recordMetadata);

    // when
    final Schema.VariableDocumentRecord variableRecord =
        (Schema.VariableDocumentRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(variableRecord.getMetadata(), "VARIABLE_DOCUMENT", "UPDATED");
    assertThat(variableRecord.getScopeKey()).isEqualTo(variableDocumentRecordValue.getScopeKey());
    assertThat(variableRecord.getUpdateSemantics())
        .isEqualTo(variableDocumentRecordValue.getUpdateSemantics().name());
    assertVariables(variableRecord.getDocument());
  }

  @Test
  public void shouldTransformMessageStartEventSubscriptionRecordValue() {
    // given
    final MessageStartEventSubscriptionRecordValue value =
        mockMessageStartEventSubscriptionRecordValue();
    final RecordMetadata metadata =
        mockRecordMetadata(
            ValueType.MESSAGE_START_EVENT_SUBSCRIPTION, MessageStartEventSubscriptionIntent.OPEN);
    final Record<MessageStartEventSubscriptionRecordValue> record = mockRecord(value, metadata);

    // when
    final Schema.MessageStartEventSubscriptionRecord transformed =
        (Schema.MessageStartEventSubscriptionRecord) RecordTransformer.toProtobufMessage(record);

    // then
    assertMetadata(transformed.getMetadata(), "MESSAGE_START_EVENT_SUBSCRIPTION", "OPEN");
    assertThat(transformed.getMessageName()).isEqualTo(value.getMessageName());
    assertThat(transformed.getStartEventId()).isEqualTo(value.getStartEventId());
    assertThat(transformed.getWorkflowKey()).isEqualTo(value.getWorkflowKey());
  }

  @Test
  public void shouldTransformErrorRecordValue() {
    // given
    final ErrorRecordValue value = mockErrorRecordValue();
    final RecordMetadata metadata = mockRecordMetadata(ValueType.ERROR, ErrorIntent.CREATED);
    final Record<ErrorRecordValue> record = mockRecord(value, metadata);

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
  public void shouldReturnEmptyForNOOP() {
    // given
    final RecordMetadata recordMetadata = mockRecordMetadata(ValueType.NOOP, Intent.UNKNOWN);

    final Record mockedRecord = mock(Record.class);
    when(mockedRecord.getMetadata()).thenReturn(recordMetadata);

    // when
    final GeneratedMessageV3 generatedMessageV3 = RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertThat(generatedMessageV3).isEqualTo(Empty.getDefaultInstance());
  }

  @Test
  public void shouldReturnEmptyForNullValue() {
    // given
    final RecordMetadata recordMetadata = mockRecordMetadata(ValueType.NULL_VAL, Intent.UNKNOWN);

    final Record mockedRecord = mock(Record.class);
    when(mockedRecord.getMetadata()).thenReturn(recordMetadata);

    // when
    final GeneratedMessageV3 generatedMessageV3 = RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertThat(generatedMessageV3).isEqualTo(Empty.getDefaultInstance());
  }

  @Test
  public void shouldReturnEmptyForSBE() {
    // given
    final RecordMetadata recordMetadata = mockRecordMetadata(ValueType.SBE_UNKNOWN, Intent.UNKNOWN);

    final Record mockedRecord = mock(Record.class);
    when(mockedRecord.getMetadata()).thenReturn(recordMetadata);

    // when
    final GeneratedMessageV3 generatedMessageV3 = RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertThat(generatedMessageV3).isEqualTo(Empty.getDefaultInstance());
  }

  @Test
  public void shouldReturnEmptyForExporter() {
    // given
    final RecordMetadata recordMetadata = mockRecordMetadata(ValueType.EXPORTER, Intent.UNKNOWN);

    final Record mockedRecord = mock(Record.class);
    when(mockedRecord.getMetadata()).thenReturn(recordMetadata);

    // when
    final GeneratedMessageV3 generatedMessageV3 = RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertThat(generatedMessageV3).isEqualTo(Empty.getDefaultInstance());
  }

  private RaftRecordValue mockRaftRecordValue() {
    final RaftRecordValue raftRecordValue = mock(RaftRecordValue.class);

    final RaftMember raftMember = mock(RaftMember.class);
    when(raftMember.getNodeId()).thenReturn(1);
    when(raftRecordValue.getMembers()).thenReturn(Collections.singletonList(raftMember));

    return raftRecordValue;
  }

  private MessageRecordValue mockMessageRecordValue() {
    final MessageRecordValue messageRecordValue = mock(MessageRecordValue.class);

    when(messageRecordValue.getCorrelationKey()).thenReturn("key");
    when(messageRecordValue.getMessageId()).thenReturn("msgId");
    when(messageRecordValue.getName()).thenReturn("message");
    when(messageRecordValue.getTimeToLive()).thenReturn(1000L);

    when(messageRecordValue.getVariables()).thenReturn("{\"foo\":23}");
    when(messageRecordValue.getVariablesAsMap()).thenReturn(Collections.singletonMap("foo", 23));

    return messageRecordValue;
  }

  private MessageStartEventSubscriptionRecordValue mockMessageStartEventSubscriptionRecordValue() {
    final MessageStartEventSubscriptionRecordValue value =
        mock(MessageStartEventSubscriptionRecordValue.class);

    when(value.getMessageName()).thenReturn("message");
    when(value.getStartEventId()).thenReturn("start");
    when(value.getWorkflowKey()).thenReturn(1L);

    return value;
  }

  private TimerRecordValue mockTimerRecordValue() {
    final TimerRecordValue timerRecordValue = mock(TimerRecordValue.class);

    when(timerRecordValue.getDueDate()).thenReturn(1000L);
    when(timerRecordValue.getElementInstanceKey()).thenReturn(1L);
    when(timerRecordValue.getHandlerFlowNodeId()).thenReturn("timerCatch");
    when(timerRecordValue.getWorkflowInstanceKey()).thenReturn(2L);

    return timerRecordValue;
  }

  private VariableRecordValue mockVariableRecordValue() {
    final VariableRecordValue variableRecordValue = mock(VariableRecordValue.class);

    when(variableRecordValue.getName()).thenReturn("var");
    when(variableRecordValue.getScopeKey()).thenReturn(1L);
    when(variableRecordValue.getValue()).thenReturn("true");
    when(variableRecordValue.getWorkflowInstanceKey()).thenReturn(1L);

    return variableRecordValue;
  }

  private VariableDocumentRecordValue mockVariableDocumentRecordValue() {
    final VariableDocumentRecordValue variableRecordValue = mock(VariableDocumentRecordValue.class);

    when(variableRecordValue.getScopeKey()).thenReturn(1L);
    when(variableRecordValue.getUpdateSemantics())
        .thenReturn(VariableDocumentUpdateSemantic.PROPAGATE);
    when(variableRecordValue.getDocument()).thenReturn(Collections.singletonMap("foo", 23));

    return variableRecordValue;
  }

  private MessageSubscriptionRecordValue mockMessageSubscriptionRecordValue() {
    final MessageSubscriptionRecordValue messageSubscriptionRecordValue =
        mock(MessageSubscriptionRecordValue.class);

    when(messageSubscriptionRecordValue.getCorrelationKey()).thenReturn("key");
    when(messageSubscriptionRecordValue.getElementInstanceKey()).thenReturn(12L);
    when(messageSubscriptionRecordValue.getMessageName()).thenReturn("message");
    when(messageSubscriptionRecordValue.getWorkflowInstanceKey()).thenReturn(1L);

    return messageSubscriptionRecordValue;
  }

  private WorkflowInstanceSubscriptionRecordValue mockWorkflowInstanceSubscriptionRecordValue() {
    final WorkflowInstanceSubscriptionRecordValue workflowInstanceSubscriptionRecordValue =
        mock(WorkflowInstanceSubscriptionRecordValue.class);

    when(workflowInstanceSubscriptionRecordValue.getMessageName()).thenReturn("message");
    when(workflowInstanceSubscriptionRecordValue.getWorkflowInstanceKey()).thenReturn(1L);
    when(workflowInstanceSubscriptionRecordValue.getElementInstanceKey()).thenReturn(4L);

    when(workflowInstanceSubscriptionRecordValue.getVariables()).thenReturn("{\"foo\":23}");
    when(workflowInstanceSubscriptionRecordValue.getVariablesAsMap())
        .thenReturn(Collections.singletonMap("foo", 23));

    return workflowInstanceSubscriptionRecordValue;
  }

  private JobBatchRecordValue mockJobBatchRecordValue() {
    final JobBatchRecordValue jobBatchRecordValue = mock(JobBatchRecordValue.class);

    when(jobBatchRecordValue.getJobKeys()).thenReturn(Collections.singletonList(5L));
    final List<JobRecordValue> jobRecordValues = Collections.singletonList(mockJobRecordValue());
    when(jobBatchRecordValue.getJobs()).thenReturn(jobRecordValues);
    when(jobBatchRecordValue.getMaxJobsToActivate()).thenReturn(1);
    when(jobBatchRecordValue.getTimeout()).thenReturn(Duration.ofMillis(1000));
    when(jobBatchRecordValue.getType()).thenReturn("jobType");
    when(jobBatchRecordValue.getWorker()).thenReturn("myveryownworker");

    return jobBatchRecordValue;
  }

  private JobRecordValue mockJobRecordValue() {
    final JobRecordValue jobRecordValue = mock(JobRecordValue.class);

    when(jobRecordValue.getDeadline()).thenReturn(Instant.ofEpochSecond(123));
    when(jobRecordValue.getErrorMessage()).thenReturn("this is an error msg");
    when(jobRecordValue.getRetries()).thenReturn(3);
    when(jobRecordValue.getType()).thenReturn("jobType");
    when(jobRecordValue.getWorker()).thenReturn("myveryownworker");

    when(jobRecordValue.getCustomHeaders()).thenReturn(Collections.singletonMap("foo", "bar"));
    when(jobRecordValue.getVariables()).thenReturn("{\"foo\":23}");
    when(jobRecordValue.getVariablesAsMap()).thenReturn(Collections.singletonMap("foo", 23));

    final Headers jobHeaders = mock(Headers.class);

    when(jobHeaders.getBpmnProcessId()).thenReturn("process");
    when(jobHeaders.getElementId()).thenReturn("task");
    when(jobHeaders.getElementInstanceKey()).thenReturn(3L);
    when(jobHeaders.getWorkflowDefinitionVersion()).thenReturn(1);
    when(jobHeaders.getWorkflowInstanceKey()).thenReturn(1L);
    when(jobHeaders.getWorkflowKey()).thenReturn(4L);

    when(jobRecordValue.getHeaders()).thenReturn(jobHeaders);

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

    return workflowInstanceRecordValue;
  }

  private WorkflowInstanceCreationRecordValue mockWorkflowInstanceCreationRecordValue() {
    final WorkflowInstanceCreationRecordValue workflowInstanceCreationRecordValue =
        mock(WorkflowInstanceCreationRecordValue.class);

    when(workflowInstanceCreationRecordValue.getBpmnProcessId()).thenReturn("process");
    when(workflowInstanceCreationRecordValue.getVersion()).thenReturn(1);
    when(workflowInstanceCreationRecordValue.getKey()).thenReturn(4L);
    when(workflowInstanceCreationRecordValue.getInstanceKey()).thenReturn(1L);
    when(workflowInstanceCreationRecordValue.getVariables())
        .thenReturn(Collections.singletonMap("foo", 23));

    return workflowInstanceCreationRecordValue;
  }

  private IncidentRecordValue mockIncidentRecordValue() {
    final IncidentRecordValue incidentRecordValue = mock(IncidentRecordValue.class);

    when(incidentRecordValue.getBpmnProcessId()).thenReturn("process");
    when(incidentRecordValue.getElementId()).thenReturn("gateway");
    when(incidentRecordValue.getElementInstanceKey()).thenReturn(1L);
    when(incidentRecordValue.getWorkflowInstanceKey()).thenReturn(1L);

    when(incidentRecordValue.getErrorMessage()).thenReturn("failed");
    when(incidentRecordValue.getErrorType()).thenReturn("boom");

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

  private void assertVariables(Struct payload) {
    assertThat(payload.getFieldsCount()).isEqualTo(1);
    assertThat(payload.getFieldsMap())
        .containsExactly(entry("foo", Value.newBuilder().setNumberValue(23).build()));
  }

  private void assertJobRecord(JobRecord jobRecord) {

    final JobRecord.Headers headers = jobRecord.getHeaders();
    assertThat(headers.getBpmnProcessId()).isEqualTo("process");
    assertThat(headers.getElementId()).isEqualTo("task");
    assertThat(headers.getWorkflowKey()).isEqualTo(4L);
    assertThat(headers.getWorkflowDefinitionVersion()).isEqualTo(1);
    assertThat(headers.getWorkflowInstanceKey()).isEqualTo(1L);
    assertThat(headers.getElementInstanceKey()).isEqualTo(3L);

    assertThat(jobRecord.getCustomHeaders().getFieldsCount()).isEqualTo(1);
    assertThat(jobRecord.getCustomHeaders().getFieldsMap())
        .containsExactly(entry("foo", Value.newBuilder().setStringValue("bar").build()));
    assertVariables(jobRecord.getVariables());

    assertThat(jobRecord.getDeadline()).isEqualTo(Timestamp.newBuilder().setSeconds(123).build());
    assertThat(jobRecord.getErrorMessage()).isEqualTo("this is an error msg");
    assertThat(jobRecord.getRetries()).isEqualTo(3);
    assertThat(jobRecord.getType()).isEqualTo("jobType");
    assertThat(jobRecord.getWorker()).isEqualTo("myveryownworker");
  }

  private void assertMetadata(Schema.RecordMetadata metadata, String valueType, String intent) {
    assertThat(metadata.getRecordType()).isEqualTo("COMMAND");
    assertThat(metadata.getValueType()).isEqualTo(valueType);
    assertThat(metadata.getIntent()).isEqualTo(intent);
    assertThat(metadata.getKey()).isEqualTo(1);
    assertThat(metadata.getPartitionId()).isEqualTo(0);
    assertThat(metadata.getPosition()).isEqualTo(265L);
    assertThat(metadata.getProducerId()).isEqualTo(1);
    assertThat(metadata.getRaftTerm()).isEqualTo(3);
    assertThat(metadata.getRejectionReason()).isEqualTo("failed");
    assertThat(metadata.getRejectionType()).isEqualTo("INVALID_ARGUMENT");
  }

  private MockRecordMetadata mockRecordMetadata(ValueType valueType, Intent intent) {
    return new MockRecordMetadata()
        .setRecordType(RecordType.COMMAND)
        .setValueType(valueType)
        .setIntent(intent)
        .setPartitionId(0)
        .setRejectionReason("failed")
        .setRejectionType(RejectionType.INVALID_ARGUMENT);
  }

  // todo: on Zeebe release 0.16.0, MockRecord#setValue and MockRecord#setMetadata will accept
  // normal RecordValue and RecordMetadata, so we should stop mocking here
  private <V extends RecordValue> Record<V> mockRecord(
      V recordValue, RecordMetadata recordMetadata) {
    final Record record = mock(Record.class);

    when(record.getKey()).thenReturn(1L);
    when(record.getPosition()).thenReturn(265L);
    when(record.getProducerId()).thenReturn(1);
    when(record.getRaftTerm()).thenReturn(3);
    when(record.getSourceRecordPosition()).thenReturn(-1L);
    when(record.getTimestamp()).thenReturn(Instant.ofEpochSecond(2000));

    when(record.getMetadata()).thenReturn(recordMetadata);
    when(record.getValue()).thenReturn(recordValue);

    return record;
  }
}
