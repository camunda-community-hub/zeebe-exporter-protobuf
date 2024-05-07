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

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class ProtobufUtil {

  public static Map<String, Object> toMap(final Struct struct) {
    final Map<String, Object> result = new HashMap<>();

    struct.getFieldsMap().forEach((key, value) -> result.put(key, transformValue(value)));

    return result;
  }

  private static Object transformValue(Value value) {
    switch (value.getKindCase()) {
      case NULL_VALUE:
        return null;
      case BOOL_VALUE:
        return value.getBoolValue();
      case NUMBER_VALUE:
        return value.getNumberValue();
      case STRING_VALUE:
        return value.getStringValue();
      case LIST_VALUE:
        return value.getListValue().getValuesList().stream()
            .map(ProtobufUtil::transformValue)
            .collect(Collectors.toList());
      case STRUCT_VALUE:
        return ProtobufUtil.toMap(value.getStructValue());
      default:
        return "???";
    }
  }
}
