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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.zeebe.exporter.record.Record;
import io.zeebe.exporter.record.RecordMetadata;
import io.zeebe.exporter.record.RecordValue;
import io.zeebe.exporter.record.value.DeploymentRecordValue;
import io.zeebe.exporter.record.value.deployment.DeployedWorkflow;
import io.zeebe.exporter.record.value.deployment.DeploymentResource;
import io.zeebe.exporter.record.value.deployment.ResourceType;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.clientapi.RejectionType;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.intent.DeploymentIntent;
import io.zeebe.protocol.intent.Intent;
import java.time.Instant;
import java.util.ArrayList;
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

    final Schema.RecordMetadata metadata = deployment.getMetadata();

    assertThat(metadata.getRecordType()).isEqualTo("COMMAND");
    assertThat(metadata.getValueType()).isEqualTo("DEPLOYMENT");
    assertThat(metadata.getIntent()).isEqualTo("CREATE");
    assertThat(metadata.getKey()).isEqualTo(1);
    assertThat(metadata.getPartitionId()).isEqualTo(0);
    assertThat(metadata.getPosition()).isEqualTo(265L);
    assertThat(metadata.getProducerId()).isEqualTo(1);
    assertThat(metadata.getRaftTerm()).isEqualTo(3);
    assertThat(metadata.getRejectionReason()).isEqualTo("failed");
    assertThat(metadata.getRejectionType()).isEqualTo("BAD_VALUE");

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
