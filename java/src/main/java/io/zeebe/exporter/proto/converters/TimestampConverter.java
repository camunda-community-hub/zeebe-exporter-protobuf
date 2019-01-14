package io.zeebe.exporter.proto.converters;

import com.google.protobuf.Timestamp;
import io.zeebe.exporter.proto.MessageConverter;
import java.time.Instant;

public class TimestampConverter implements MessageConverter<Instant, Timestamp.Builder> {
  @Override
  public Timestamp.Builder convert(Instant value) {
    return Timestamp.newBuilder().setSeconds(value.getEpochSecond()).setNanos(value.getNano());
  }
}
