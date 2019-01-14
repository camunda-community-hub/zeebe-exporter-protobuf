package io.zeebe.exporter.proto;

import com.google.protobuf.Message;

/**
 * Represents a strategy to convert a single value type to a protobuf message builder. Returning
 * builders is more flexible and allows reuse, as opposed to returned built objects.
 *
 * @param <ValueType> object that can be converted
 * @param <BuilderType> the message's builder type
 */
public interface MessageConverter<ValueType, BuilderType extends Message.Builder> {
  BuilderType convert(ValueType value);
}
