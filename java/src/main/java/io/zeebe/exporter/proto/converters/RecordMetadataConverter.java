package io.zeebe.exporter.proto.converters;

import io.zeebe.exporter.proto.MessageConverter;
import io.zeebe.exporter.proto.Schema;
import io.zeebe.exporter.record.Record;
import io.zeebe.exporter.record.RecordMetadata;
import io.zeebe.protocol.clientapi.RejectionType;

public class RecordMetadataConverter
    implements MessageConverter<Record, Schema.RecordMetadata.Builder> {
  private final TimestampConverter timestampConverter;

  public RecordMetadataConverter() {
    this(new TimestampConverter());
  }

  public RecordMetadataConverter(TimestampConverter timestampConverter) {
    this.timestampConverter = timestampConverter;
  }

  @Override
  public Schema.RecordMetadata.Builder convert(Record value) {
    final RecordMetadata metadata = value.getMetadata();
    final Schema.RecordMetadata.Builder builder =
        Schema.RecordMetadata.newBuilder()
            .setIntent(metadata.getIntent().name())
            .setKey(value.getKey())
            .setProducerId(value.getProducerId())
            .setRaftTerm(value.getRaftTerm())
            .setRecordType(metadata.getRecordType().name())
            .setSourceRecordPosition(value.getSourceRecordPosition())
            .setPosition(value.getPosition())
            .setTimestamp(timestampConverter.convert(value.getTimestamp()));

    if (metadata.getRejectionType() != RejectionType.NULL_VAL) {
      builder.setRejectionType(metadata.getRejectionType().name());
      builder.setRejectionReason(metadata.getRejectionReason());
    }

    return builder;
  }
}
