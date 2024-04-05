/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
package com.dremio.datastore;

import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import com.google.protobuf.UnknownFieldSet;
import com.google.protobuf.UnknownFieldSet.Field;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * a Serializer implementation for protobuf generated classes which were originally generated with
 * protostuff
 *
 * <p>Protostuff binary format (as described in {@code io.protostuff.ProtostuffOutput} uses groups
 * for embedded field messages, whereas Protobuf binary format uses length delimited encoding (and
 * group support is deprecated).
 *
 * <p>This class allows to deserialize either Protobuf or Protostuff binary format by checking for
 * unknown group fields in the deserialized object and rewriting the message by reparsing the
 * unknown fields as regular fields if required.
 *
 * <p>Note: This class can be removed after no data generated using Protostuff is present in the KV
 * store
 *
 * @param <T> a protostuff generated class
 * @deprecated Use {@code ProtobufSerializer} for any new code.
 */
@Deprecated
public class LegacyProtobufSerializer<T extends Message> extends ProtobufSerializer<T> {
  public LegacyProtobufSerializer(Class<T> clazz, Parser<T> parser) {
    super(clazz, parser);
  }

  @Override
  public T revert(byte[] bytes) {
    try {
      return rewriteProtostuff(super.revert(bytes));
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Parses a Protobuf message which might have been written by Protostuff
   *
   * @param <M> Protobuf message type
   * @param parser parser for the message
   * @param bytes the message bytes
   * @return the decoded protobuf message
   * @throws InvalidProtocolBufferException
   */
  public static <M extends Message> M parseFrom(Parser<M> parser, ByteString bytes)
      throws InvalidProtocolBufferException {
    return rewriteProtostuff(parser.parseFrom(bytes));
  }

  /**
   * Parses a Protobuf message which might have been written by Protostuff
   *
   * @param <M> Protobuf message type
   * @param parser parser for the message
   * @param bytes the message bytes
   * @return the decoded protobuf message
   * @throws InvalidProtocolBufferException
   */
  public static <M extends Message> M parseFrom(Parser<M> parser, byte[] bytes)
      throws InvalidProtocolBufferException {
    return rewriteProtostuff(parser.parseFrom(bytes));
  }

  public static <M extends Message> M parseFrom(Parser<M> parser, ByteBuffer bytes)
      throws InvalidProtocolBufferException {
    return rewriteProtostuff(parser.parseFrom(bytes));
  }

  private static <M extends Message> M rewriteProtostuff(M message)
      throws InvalidProtocolBufferException {
    if (message == null) {
      return null;
    }

    final Descriptor descriptor = message.getDescriptorForType();

    // Check if the message schema has any embedded message field at all, and if yes, confirm that
    // all those fields are set. If this is the case, assumes the message was encoded using
    // Protobuf.
    // (Protostuff would have used groups which would then be stored in unknown fields)
    boolean hasEmbeddedMessageField = false;
    for (FieldDescriptor field : descriptor.getFields()) {
      if (field.getJavaType() == JavaType.MESSAGE) {
        if (hasField(message, field)) {
          // Early exit: the embedded message field has some value set
          return message;
        }
        hasEmbeddedMessageField = true;
      }
    }
    if (!hasEmbeddedMessageField) {
      // Early exit: the message has no embedded message field, or at least one of the embedded
      // message
      // field has a value set
      return message;
    }

    // No need to go over regular fields again as we know that no embedded message field was set
    // (and there's no need to rewrite default field values...)
    // Only thing left is to go over unknown fields to see if unknown group fields are
    // present, and that one of those fields would have a regular field counterpart.
    final UnknownFieldSet unknownFields = message.getUnknownFields();
    if (unknownFields.asMap().entrySet().stream()
        .noneMatch(
            entry ->
                entry.getValue().getGroupList() != null
                    && descriptor.findFieldByNumber(entry.getKey()) != null)) {
      return message;
    }

    // Slow path: a conversion from protostuff to protobuf is required
    // Keep track of unknown fields which are not converted so that they might be added
    // later to the object.
    final UnknownFieldSet.Builder unknownFieldSetBuilder = UnknownFieldSet.newBuilder();
    final Message.Builder builder = message.toBuilder();
    for (Map.Entry<Integer, Field> entry : unknownFields.asMap().entrySet()) {
      int fieldId = entry.getKey();
      final Field field = entry.getValue();
      final List<UnknownFieldSet> groupList = field.getGroupList();
      if (groupList == null) {
        // Skip if not a group list field
        unknownFieldSetBuilder.addField(fieldId, field);
        continue;
      }

      final FieldDescriptor fieldDescriptor = descriptor.findFieldByNumber(fieldId);
      if (fieldDescriptor == null) {
        // Skip unknown fields which are not mapped to existing fields
        unknownFieldSetBuilder.addField(fieldId, field);
        continue;
      }

      // Reparse unknown field as the actual field.
      if (fieldDescriptor.isRepeated()) {
        // Need to parse/fix all values
        for (UnknownFieldSet group : groupList) {
          final Message.Builder fieldBuilder = builder.newBuilderForField(fieldDescriptor);
          builder.addRepeatedField(fieldDescriptor, convertUnknownSet(fieldBuilder, group));
        }
      } else {
        // Parse only the last value
        final Message.Builder fieldBuilder = builder.newBuilderForField(fieldDescriptor);
        builder.setField(
            fieldDescriptor, convertUnknownSet(fieldBuilder, Iterables.getLast(groupList)));
      }
    }

    builder.setUnknownFields(unknownFieldSetBuilder.build());
    return (M) builder.build();
  }

  /**
   * Checks if field is set in message.
   *
   * <p>For repeated fields, check if at least one element is present
   */
  private static <M extends Message> boolean hasField(M message, FieldDescriptor field) {
    return field.isRepeated() ? message.getRepeatedFieldCount(field) > 0 : message.hasField(field);
  }

  private static Message convertUnknownSet(final Message.Builder builder, UnknownFieldSet group)
      throws InvalidProtocolBufferException {
    return rewriteProtostuff(builder.mergeFrom(group.toByteString()).build());
  }
}
