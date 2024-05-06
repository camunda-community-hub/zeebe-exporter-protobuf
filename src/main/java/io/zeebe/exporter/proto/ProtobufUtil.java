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
