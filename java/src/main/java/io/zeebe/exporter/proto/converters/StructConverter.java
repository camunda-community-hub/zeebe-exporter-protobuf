package io.zeebe.exporter.proto.converters;

import com.google.protobuf.Struct;
import io.zeebe.exporter.proto.MessageConverter;
import java.util.Map;

public class StructConverter implements MessageConverter<Map<?, ?>, Struct.Builder> {
  private final ValueConverter valueConverter;

  public StructConverter() {
    this(new ValueConverter());
  }

  public StructConverter(ValueConverter valueConverter) {
    this.valueConverter = valueConverter;
  }

  @Override
  public Struct.Builder convert(Map<?, ?> value) {
    final Struct.Builder builder = Struct.newBuilder();

    for (final Map.Entry<?, ?> entry : value.entrySet()) {
      builder.putFields(
          entry.getKey().toString(), valueConverter.convert(entry.getValue()).build());
    }

    return builder;
  }
}
