package io.zeebe.exporter.proto.converters;

import com.google.protobuf.Message;
import io.zeebe.exporter.proto.MessageConverter;
import io.zeebe.exporter.proto.Schema;
import io.zeebe.exporter.record.Record;
import io.zeebe.exporter.record.RecordValue;
import io.zeebe.exporter.record.value.JobBatchRecordValue;
import io.zeebe.exporter.record.value.JobRecordValue;

public class RecordConverter implements MessageConverter<Record<?>, Message.Builder> {
  private final RecordMetadataConverter metadataFactory;
  private final JobConverter jobConverter;
  private final JobBatchConverter jobBatchConverter;

  public RecordConverter(
      RecordMetadataConverter metadataFactory,
      JobConverter jobConverter,
      JobBatchConverter jobBatchConverter) {
    this.metadataFactory = metadataFactory;
    this.jobConverter = jobConverter;
    this.jobBatchConverter = jobBatchConverter;
  }

  @Override
  public Message.Builder convert(Record<?> record) {
    final RecordValue value = record.getValue();
    final Schema.RecordMetadata.Builder metadata = metadataFactory.convert(record);

    if (value instanceof JobRecordValue) {
      return jobConverter.convert((JobRecordValue) value).setMetadata(metadata);
    }

    if (value instanceof JobBatchRecordValue) {
      return jobBatchConverter.convert((JobBatchRecordValue) value).setMetadata(metadata);
    }

    return null;
  }
}
