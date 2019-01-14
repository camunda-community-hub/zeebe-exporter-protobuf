package io.zeebe.exporter.proto.converters;

import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Value;
import io.zeebe.exporter.proto.MessageConverter;
import java.util.List;
import java.util.Map;

public class ValueConverter implements MessageConverter<Object, Value.Builder> {
  private final StructConverter structConverter;

  public ValueConverter() {
    this(new StructConverter());
  }

  public ValueConverter(StructConverter structConverter) {
    this.structConverter = structConverter;
  }

  @Override
  public Value.Builder convert(Object value) {
    final Value.Builder builder = Value.newBuilder();

    if (value == null) {
      builder.setNullValue(NullValue.NULL_VALUE);
    } else if (value instanceof Number) {
      builder.setNumberValue(((Number) value).doubleValue());
    } else if (value instanceof Boolean) {
      builder.setBoolValue((Boolean) value);
    } else if (value instanceof List) {
      final List list = (List) value;
      final ListValue.Builder listBuilder = ListValue.newBuilder();

      for (final Object item : list) {
        listBuilder.addValues(convert(item));
      }

      builder.setListValue(listBuilder.build());
    } else if (value instanceof Map) {
      builder.setStructValue(structConverter.convert((Map) value));
    } else if (value instanceof String) {
      builder.setStringValue((String) value);
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Unexpected struct value of type %s, should be one of: null, Number, Boolean, List, Map, String",
              value.getClass().getCanonicalName()));
    }

    return builder;
  }
}
