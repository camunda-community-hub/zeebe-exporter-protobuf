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

import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import io.zeebe.exporter.proto.Schema.JobRecord;
import io.zeebe.exporter.record.Record;
import io.zeebe.exporter.record.RecordMetadata;
import io.zeebe.exporter.record.RecordValue;
import io.zeebe.exporter.record.value.DeploymentRecordValue;
import io.zeebe.exporter.record.value.IncidentRecordValue;
import io.zeebe.exporter.record.value.JobBatchRecordValue;
import io.zeebe.exporter.record.value.JobRecordValue;
import io.zeebe.exporter.record.value.MessageRecordValue;
import io.zeebe.exporter.record.value.WorkflowInstanceRecordValue;
import io.zeebe.exporter.record.value.deployment.DeployedWorkflow;
import io.zeebe.exporter.record.value.deployment.DeploymentResource;
import io.zeebe.exporter.record.value.deployment.ResourceType;
import io.zeebe.exporter.record.value.job.Headers;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.clientapi.RejectionType;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.intent.DeploymentIntent;
import io.zeebe.protocol.intent.IncidentIntent;
import io.zeebe.protocol.intent.Intent;
import io.zeebe.protocol.intent.JobBatchIntent;
import io.zeebe.protocol.intent.JobIntent;
import io.zeebe.protocol.intent.MessageIntent;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
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
        mockRecordMetadata(ValueType.WORKFLOW_INSTANCE, WorkflowInstanceIntent.CREATE);
    final Record<WorkflowInstanceRecordValue> mockedRecord =
        mockRecord(workflowInstanceRecordValue, recordMetadata);

    // when
    final Schema.WorkflowInstanceRecord workflowInstance =
        (Schema.WorkflowInstanceRecord) RecordTransformer.toProtobufMessage(mockedRecord);

    // then
    assertMetadata(workflowInstance.getMetadata(), "WORKFLOW_INSTANCE", "CREATE");

    assertThat(workflowInstance.getBpmnProcessId()).isEqualTo("process");
    assertThat(workflowInstance.getElementId()).isEqualTo("startEvent");
    assertThat(workflowInstance.getWorkflowKey()).isEqualTo(4L);
    assertThat(workflowInstance.getVersion()).isEqualTo(1);
    assertThat(workflowInstance.getWorkflowInstanceKey()).isEqualTo(1L);
    assertThat(workflowInstance.getScopeInstanceKey()).isEqualTo(-1L);

    assertPayload(workflowInstance.getPayload());
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
    assertThat(jobBatchRecord.getAmount()).isEqualTo(1);
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

    assertPayload(messageRecord.getPayload());
  }

  private MessageRecordValue mockMessageRecordValue() {
    final MessageRecordValue messageRecordValue = mock(MessageRecordValue.class);

    when(messageRecordValue.getCorrelationKey()).thenReturn("key");
    when(messageRecordValue.getMessageId()).thenReturn("msgId");
    when(messageRecordValue.getName()).thenReturn("message");
    when(messageRecordValue.getTimeToLive()).thenReturn(1000L);

    when(messageRecordValue.getPayload()).thenReturn("{\"foo\":23}");
    when(messageRecordValue.getPayloadAsMap()).thenReturn(Collections.singletonMap("foo", 23));

    return messageRecordValue;
  }

  private JobBatchRecordValue mockJobBatchRecordValue() {
    final JobBatchRecordValue jobBatchRecordValue = mock(JobBatchRecordValue.class);

    when(jobBatchRecordValue.getJobKeys()).thenReturn(Collections.singletonList(5L));
    final List<JobRecordValue> jobRecordValues = Collections.singletonList(mockJobRecordValue());
    when(jobBatchRecordValue.getJobs()).thenReturn(jobRecordValues);
    when(jobBatchRecordValue.getAmount()).thenReturn(1);
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
    when(jobRecordValue.getPayload()).thenReturn("{\"foo\":23}");
    when(jobRecordValue.getPayloadAsMap()).thenReturn(Collections.singletonMap("foo", 23));

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
    when(workflowInstanceRecordValue.getPayload()).thenReturn("{\"foo\":23}");
    when(workflowInstanceRecordValue.getPayloadAsMap())
        .thenReturn(Collections.singletonMap("foo", 23));
    when(workflowInstanceRecordValue.getBpmnProcessId()).thenReturn("process");
    when(workflowInstanceRecordValue.getElementId()).thenReturn("startEvent");
    when(workflowInstanceRecordValue.getScopeInstanceKey()).thenReturn(-1L);
    when(workflowInstanceRecordValue.getVersion()).thenReturn(1);
    when(workflowInstanceRecordValue.getWorkflowKey()).thenReturn(4L);

    return workflowInstanceRecordValue;
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

  private void assertPayload(Struct payload) {
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
    assertPayload(jobRecord.getPayload());

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
    assertThat(metadata.getRejectionType()).isEqualTo("BAD_VALUE");
  }

  private RecordMetadata mockRecordMetadata(ValueType valueType, Intent intent) {
    final RecordMetadata recordMetadata = mock(RecordMetadata.class);

    when(recordMetadata.getRecordType()).thenReturn(RecordType.COMMAND);
    when(recordMetadata.getValueType()).thenReturn(valueType);
    when(recordMetadata.getIntent()).thenReturn(intent);
    when(recordMetadata.getPartitionId()).thenReturn(0);
    when(recordMetadata.getRejectionReason()).thenReturn("failed");
    when(recordMetadata.getRejectionType()).thenReturn(RejectionType.BAD_VALUE);

    return recordMetadata;
  }

  private <Value extends RecordValue> Record<Value> mockRecord(
      Value recordValue, RecordMetadata recordMetadata) {
    final Record<Value> record = mock(Record.class);

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
