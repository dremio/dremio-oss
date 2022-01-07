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
package com.hubspot.jackson.datatype.protobuf.builtin.serializers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.PropertyNamingStrategy.PropertyNamingStrategyBase;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.util.NameTransformer;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.Descriptors.FileDescriptor.Syntax;
import com.google.protobuf.ExtensionRegistry.ExtensionInfo;
import com.google.protobuf.GeneratedMessageV3.ExtendableMessageOrBuilder;
import com.google.protobuf.MessageOrBuilder;
import com.hubspot.jackson.datatype.protobuf.ExtensionRegistryWrapper;
import com.hubspot.jackson.datatype.protobuf.PropertyNamingStrategyWrapper;
import com.hubspot.jackson.datatype.protobuf.ProtobufJacksonConfig;
import com.hubspot.jackson.datatype.protobuf.ProtobufSerializer;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A Modified MessageSerializer to add support for Unwrapped Serialization.
 *
 * This file submitted as a PR to the jackson protobuf library.
 * PR - https://github.com/HubSpot/jackson-datatype-protobuf/pull/79
 */
public class MessageSerializer extends ProtobufSerializer<MessageOrBuilder> {
  @SuppressFBWarnings(value="SE_BAD_FIELD")
  private final ProtobufJacksonConfig config;
  private final boolean unwrappingSerializer;
  private final NameTransformer nameTransformer;

  /**
   * @deprecated use {@link #MessageSerializer(ProtobufJacksonConfig)} instead
   */
  @Deprecated
  public MessageSerializer(ExtensionRegistryWrapper extensionRegistry) {
    this(ProtobufJacksonConfig.builder().extensionRegistry(extensionRegistry).build());
  }

  public MessageSerializer(ProtobufJacksonConfig config) {
    this(config, false);
  }

  public MessageSerializer(ProtobufJacksonConfig config, boolean unwrappingSerializer) {
    this(config, null, unwrappingSerializer);
  }

  public MessageSerializer(ProtobufJacksonConfig config, NameTransformer nameTransformer, boolean unwrappingSerializer) {
    super(MessageOrBuilder.class);
    this.config = config;
    this.unwrappingSerializer = unwrappingSerializer;
    if (nameTransformer == null) {
      this.nameTransformer = NameTransformer.NOP;
    } else {
      this.nameTransformer = nameTransformer;
    }
  }

  @Override
  public void serialize(
          MessageOrBuilder message,
          JsonGenerator generator,
          SerializerProvider serializerProvider
  ) throws IOException {
    if (!isUnwrappingSerializer()) {
      generator.writeStartObject();
    }

    boolean proto3 = message.getDescriptorForType().getFile().getSyntax() == Syntax.PROTO3;
    Include include = serializerProvider.getConfig().getDefaultPropertyInclusion().getValueInclusion();
    boolean writeDefaultValues = proto3 && include != Include.NON_DEFAULT;
    boolean writeEmptyCollections = include != Include.NON_DEFAULT && include != Include.NON_EMPTY;

    //If NamingTransformer is provided (in case of UnwrappingSerializer), we chain it on top of
    // the namingStrategy.
    final PropertyNamingStrategyBase namingStrategy = new PropertyNamingStrategyBase() {
      @Override
      public String translate(String fieldName) {
        PropertyNamingStrategyBase configuredNamingStrategy =
                new PropertyNamingStrategyWrapper(serializerProvider.getConfig().getPropertyNamingStrategy());
        return nameTransformer.transform(configuredNamingStrategy.translate(fieldName));
      }
    };


    Descriptor descriptor = message.getDescriptorForType();
    List<FieldDescriptor> fields = new ArrayList<>(descriptor.getFields());
    if (message instanceof ExtendableMessageOrBuilder<?>) {
      for (ExtensionInfo extensionInfo : config.extensionRegistry().getExtensionsByDescriptor(descriptor)) {
        fields.add(extensionInfo.descriptor);
      }
    }

    for (FieldDescriptor field : fields) {
      if (field.isRepeated()) {
        List<?> valueList = (List<?>) message.getField(field);

        if (!valueList.isEmpty() || writeEmptyCollections) {
          if (field.isMapField()) {
            generator.writeFieldName(nameTransformer.transform(namingStrategy.translate(field.getName())));
            writeMap(field, valueList, generator, serializerProvider);
          } else if (valueList.size() == 1 && writeSingleElementArraysUnwrapped(serializerProvider)) {
            generator.writeFieldName(nameTransformer.transform(namingStrategy.translate(field.getName())));
            writeValue(field, valueList.get(0), generator, serializerProvider);
          } else {
            generator.writeArrayFieldStart(nameTransformer.transform(namingStrategy.translate(field.getName())));
            for (Object subValue : valueList) {
              writeValue(field, subValue, generator, serializerProvider);
            }
            generator.writeEndArray();
          }
        }
      } else if (message.hasField(field) || (writeDefaultValues && !supportsFieldPresence(field) && field.getContainingOneof() == null)) {
        generator.writeFieldName(nameTransformer.transform(namingStrategy.translate(field.getName())));
        writeValue(field, message.getField(field), generator, serializerProvider);
      } else if (include == Include.ALWAYS && field.getContainingOneof() == null) {
        generator.writeFieldName(nameTransformer.transform(namingStrategy.translate(field.getName())));
        generator.writeNull();
      }
    }

    if (!isUnwrappingSerializer()) {
      generator.writeEndObject();
    }
  }

  @Override
  public boolean isUnwrappingSerializer() {
    return unwrappingSerializer;
  }

  @Override
  public MessageSerializer unwrappingSerializer(NameTransformer nameTransformer) {
    return new MessageSerializer(config, nameTransformer, true);
  }

  private static boolean supportsFieldPresence(FieldDescriptor field) {
    // messages still support field presence in proto3
    return field.getJavaType() == JavaType.MESSAGE;
  }

  private static boolean writeEmptyArrays(SerializerProvider config) {
    return config.isEnabled(SerializationFeature.WRITE_EMPTY_JSON_ARRAYS);
  }

  private static boolean writeSingleElementArraysUnwrapped(SerializerProvider config) {
    return config.isEnabled(SerializationFeature.WRITE_SINGLE_ELEM_ARRAYS_UNWRAPPED);
  }
}
