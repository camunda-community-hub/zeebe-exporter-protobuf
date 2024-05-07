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

import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class ProtobufUtilTest {

  @Test
  public void shouldTransformEmptyStruct() {
    // given
    final Struct emptyStruct = Struct.newBuilder().build();
    // when/then
    assertThat(ProtobufUtil.toMap(emptyStruct)).isEqualTo(Collections.emptyMap());
  }

  @Test
  public void shouldTransformStructWithNull() {
    // given
    final Struct struct =
        Struct.newBuilder()
            .putFields("a", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .build();
    // when/then
    assertThat(ProtobufUtil.toMap(struct)).hasSize(1).containsEntry("a", null);
  }

  @Test
  public void shouldTransformStructWithValues() {
    // given
    final Struct struct =
        Struct.newBuilder()
            .putFields("boolean", Value.newBuilder().setBoolValue(true).build())
            .putFields("number", Value.newBuilder().setNumberValue(1).build())
            .putFields("string", Value.newBuilder().setStringValue("foo").build())
            .build();
    // when/then
    assertThat(ProtobufUtil.toMap(struct))
        .hasSize(3)
        .containsEntry("boolean", true)
        .containsEntry("number", 1.0)
        .containsEntry("string", "foo");
  }

  @Test
  public void shouldTransformStructWithList() {
    // given
    var listValue =
        ListValue.newBuilder()
            .addValues(Value.newBuilder().setStringValue("a"))
            .addValues(Value.newBuilder().setStringValue("b"));

    final Struct struct =
        Struct.newBuilder()
            .putFields("list", Value.newBuilder().setListValue(listValue).build())
            .build();
    // when/then
    assertThat(ProtobufUtil.toMap(struct)).hasSize(1).containsEntry("list", List.of("a", "b"));
  }

  @Test
  public void shouldTransformStructWithNestedStruct() {
    // given
    var structValue =
        Struct.newBuilder()
            .putFields("a", Value.newBuilder().setStringValue("a1").build())
            .putFields("b", Value.newBuilder().setStringValue("b2").build());

    final Struct struct =
        Struct.newBuilder()
            .putFields("struct", Value.newBuilder().setStructValue(structValue).build())
            .build();
    // when/then
    assertThat(ProtobufUtil.toMap(struct))
        .hasSize(1)
        .containsEntry("struct", Map.ofEntries(Map.entry("a", "a1"), Map.entry("b", "b2")));
  }
}
