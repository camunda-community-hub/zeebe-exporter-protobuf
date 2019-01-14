package io.zeebe.exporter.proto.converters;

import io.zeebe.exporter.proto.MessageConverter;
import io.zeebe.exporter.proto.Schema;
import io.zeebe.exporter.proto.converters.job.HeadersConverter;
import io.zeebe.exporter.record.value.JobRecordValue;

public class JobConverter implements MessageConverter<JobRecordValue, Schema.JobRecord.Builder> {
  private final TimestampConverter timestampConverter;
  private final StructConverter structConverter;
  private final HeadersConverter headersConverter;

  public JobConverter() {
    this(new TimestampConverter(), new StructConverter(), new HeadersConverter());
  }

  public JobConverter(
      TimestampConverter timestampConverter,
      StructConverter structConverter,
      HeadersConverter headersConverter) {
    this.timestampConverter = timestampConverter;
    this.structConverter = structConverter;
    this.headersConverter = headersConverter;
  }

  @Override
  public Schema.JobRecord.Builder convert(JobRecordValue value) {
    return Schema.JobRecord.newBuilder()
        .setDeadline(timestampConverter.convert(value.getDeadline()))
        .setErrorMessage(value.getErrorMessage())
        .setRetries(value.getRetries())
        .setType(value.getType())
        .setWorker(value.getWorker())
        .setPayload(structConverter.convert(value.getPayloadAsMap()))
        .setCustomHeaders(structConverter.convert(value.getCustomHeaders()))
        .setHeaders(headersConverter.convert(value.getHeaders()));
  }
}
