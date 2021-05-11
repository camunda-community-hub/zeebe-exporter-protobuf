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
import static org.assertj.core.data.MapEntry.entry;
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
import io.camunda.zeebe.protocol.record.intent.DeploymentDistributionIntent;
import io.camunda.zeebe.protocol.record.intent.DeploymentIntent;
import io.camunda.zeebe.protocol.record.intent.ErrorIntent;
import io.camunda.zeebe.protocol.record.intent.IncidentIntent;
import io.camunda.zeebe.protocol.record.intent.Intent;
import io.camunda.zeebe.protocol.record.intent.JobBatchIntent;
import io.camunda.zeebe.protocol.record.intent.JobIntent;
import io.camunda.zeebe.protocol.record.intent.MessageIntent;
import io.camunda.zeebe.protocol.record.intent.MessageStartEventSubscriptionIntent;
import io.camunda.zeebe.protocol.record.intent.MessageSubscriptionIntent;
import io.camunda.zeebe.protocol.record.intent.ProcessEventIntent;
import io.camunda.zeebe.protocol.record.intent.ProcessInstanceCreationIntent;
import io.camunda.zeebe.protocol.record.intent.ProcessInstanceIntent;
import io.camunda.zeebe.protocol.record.intent.ProcessIntent;
import io.camunda.zeebe.protocol.record.intent.ProcessMessageSubscriptionIntent;
import io.camunda.zeebe.protocol.record.intent.TimerIntent;
import io.camunda.zeebe.protocol.record.intent.VariableDocumentIntent;
import io.camunda.zeebe.protocol.record.intent.VariableIntent;
import io.camunda.zeebe.protocol.record.value.BpmnElementType;
import io.camunda.zeebe.protocol.record.value.DeploymentDistributionRecordValue;
import io.camunda.zeebe.protocol.record.value.DeploymentRecordValue;
import io.camunda.zeebe.protocol.record.value.ErrorRecordValue;
import io.camunda.zeebe.protocol.record.value.ErrorType;
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

    final List<Schema.DeploymentRecord.ProcessMetadata> processMetadataList =
        deployment.getProcessMetadataList();
    assertThat(processMetadataList).hasSize(1);

    final Schema.DeploymentRecord.ProcessMetadata processMetadata = processMetadataList.get(0);
    assertThat(processMetadata.getBpmnProcessId()).isEqualTo("process");
    assertThat(processMetadata.getResourceName()).isEqualTo("process.bpmn");
    assertThat(processMetadata.getProcessDefinitionKey()).isEqualTo(4L);
    assertThat(processMetadata.getVersion()).isEqualTo(1);
    assertThat(processMetadata.getChecksum()).isEqualTo("checksum");
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
    assertThat(transformedRecord.getChecksum()).isEqualTo("checksum");
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
    assertThat(workflowInstance.getParentProcessInstanceKey()).isEqualTo(-1L);
    assertThat(workflowInstance.getParentElementInstanceKey()).isEqualTo(-1L);
  }

  @Test
  public void shouldTransformWorkflowInstanceCreation() {
    // given
    final ProcessInstanceCreationRecordValue workflowInstanceCreationRecordValue =
        mockWorkflowInstanceCreationRecordValue();
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
    assertVariables(workflowInstanceCreation.getVariables());
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

    assertVariables(messageRecord.getVariables());
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

    assertVariables(workflowInstanceSubscriptionRecord.getVariables());

    assertThat(workflowInstanceSubscriptionRecord.getMessageName()).isEqualTo("message");
    assertThat(workflowInstanceSubscriptionRecord.getElementInstanceKey()).isEqualTo(4L);
    assertThat(workflowInstanceSubscriptionRecord.getProcessInstanceKey()).isEqualTo(1L);
    assertThat(workflowInstanceSubscriptionRecord.getBpmnProcessId())
        .isEqualTo(value.getBpmnProcessId());
    assertThat(workflowInstanceSubscriptionRecord.getMessageKey()).isEqualTo(value.getMessageKey());
    assertThat(workflowInstanceSubscriptionRecord.getCorrelationKey())
        .isEqualTo(value.getCorrelationKey());
    assertThat(workflowInstanceSubscriptionRecord.getElementId()).isEqualTo(value.getElementId());

    assertVariables(workflowInstanceSubscriptionRecord.getVariables());
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

    final var ignoredValueTypes =
        Set.of(ValueType.NULL_VAL, ValueType.SBE_UNKNOWN, ValueType.PROCESS_INSTANCE_RESULT);

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
    when(value.getProcessDefinitionKey()).thenReturn(1L);
    when(value.getBpmnProcessId()).thenReturn("bpmnProcessId");
    when(value.getCorrelationKey()).thenReturn("correlationKey");
    when(value.getMessageKey()).thenReturn(2L);
    when(value.getProcessInstanceKey()).thenReturn(3L);
    when(value.getVariables()).thenReturn(Map.of("foo", 23));

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

    return timerRecordValue;
  }

  private VariableRecordValue mockVariableRecordValue() {
    final VariableRecordValue variableRecordValue = mock(VariableRecordValue.class);

    when(variableRecordValue.getName()).thenReturn("var");
    when(variableRecordValue.getScopeKey()).thenReturn(1L);
    when(variableRecordValue.getValue()).thenReturn("true");
    when(variableRecordValue.getProcessInstanceKey()).thenReturn(1L);
    when(variableRecordValue.getProcessDefinitionKey()).thenReturn(2L);

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
    when(messageSubscriptionRecordValue.getProcessInstanceKey()).thenReturn(1L);
    when(messageSubscriptionRecordValue.getBpmnProcessId()).thenReturn("bpmnProcessId");
    when(messageSubscriptionRecordValue.getMessageKey()).thenReturn(2L);
    when(messageSubscriptionRecordValue.getVariables()).thenReturn(Map.of("foo", 23));

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
    when(jobRecordValue.getProcessDefinitionVersion()).thenReturn(1);
    when(jobRecordValue.getProcessInstanceKey()).thenReturn(1L);
    when(jobRecordValue.getProcessDefinitionKey()).thenReturn(4L);

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
    workflows.add(processMetadata);

    when(deploymentRecordValue.getProcessesMetadata()).thenReturn(workflows);

    final List<DeploymentResource> resources = new ArrayList<>();
    final DeploymentResource deploymentResource = mock(DeploymentResource.class);
    when(deploymentResource.getResource()).thenReturn("resourceContent".getBytes());
    when(deploymentResource.getResourceName()).thenReturn("process.bpmn");
    resources.add(deploymentResource);

    when(deploymentRecordValue.getResources()).thenReturn(resources);
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
    when(workflowInstanceRecordValue.getParentProcessInstanceKey()).thenReturn(-1L);
    when(workflowInstanceRecordValue.getParentElementInstanceKey()).thenReturn(-1L);

    return workflowInstanceRecordValue;
  }

  private ProcessInstanceCreationRecordValue mockWorkflowInstanceCreationRecordValue() {
    final ProcessInstanceCreationRecordValue workflowInstanceCreationRecordValue =
        mock(ProcessInstanceCreationRecordValue.class);

    when(workflowInstanceCreationRecordValue.getBpmnProcessId()).thenReturn("process");
    when(workflowInstanceCreationRecordValue.getVersion()).thenReturn(1);
    when(workflowInstanceCreationRecordValue.getProcessDefinitionKey()).thenReturn(4L);
    when(workflowInstanceCreationRecordValue.getProcessInstanceKey()).thenReturn(1L);
    when(workflowInstanceCreationRecordValue.getVariables())
        .thenReturn(Collections.singletonMap("foo", 23));

    return workflowInstanceCreationRecordValue;
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

    return incidentRecordValue;
  }

  private ProcessEventRecordValue mockProcessEventRecordValue() {
    final var value = mock(ProcessEventRecordValue.class);
    when(value.getProcessDefinitionKey()).thenReturn(1L);
    when(value.getScopeKey()).thenReturn(2L);
    when(value.getTargetElementId()).thenReturn("targetElementId");
    when(value.getVariables()).thenReturn(Map.of("foo", 23));
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

  private void assertVariables(final Struct payload) {
    assertThat(payload.getFieldsCount()).isEqualTo(1);
    assertThat(payload.getFieldsMap())
        .containsExactly(entry("foo", Value.newBuilder().setNumberValue(23).build()));
  }

  private void assertJobRecord(final JobRecord jobRecord) {
    assertThat(jobRecord.getBpmnProcessId()).isEqualTo("process");
    assertThat(jobRecord.getElementId()).isEqualTo("task");
    assertThat(jobRecord.getProcessDefinitionKey()).isEqualTo(4L);
    assertThat(jobRecord.getWorkflowDefinitionVersion()).isEqualTo(1);
    assertThat(jobRecord.getProcessInstanceKey()).isEqualTo(1L);
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
