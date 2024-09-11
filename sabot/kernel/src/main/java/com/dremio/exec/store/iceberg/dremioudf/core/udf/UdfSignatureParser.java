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
package com.dremio.exec.store.iceberg.dremioudf.core.udf;

import com.dremio.exec.store.iceberg.dremioudf.api.udf.UdfSignature;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;

public class UdfSignatureParser {

  private static final String SIGNATURE_ID = "signature-id";
  private static final String PARAMETERS = "parameters";
  private static final String RETURN_TYPE = "return-type";
  private static final String DETERMINISTIC = "deterministic";
  private static final String TYPE = "type";
  private static final String STRUCT = "struct";
  private static final String LIST = "list";
  private static final String MAP = "map";
  private static final String FIELDS = "fields";
  private static final String ELEMENT = "element";
  private static final String KEY = "key";
  private static final String VALUE = "value";
  private static final String DOC = "doc";
  private static final String NAME = "name";
  private static final String ID = "id";
  private static final String ELEMENT_ID = "element-id";
  private static final String KEY_ID = "key-id";
  private static final String VALUE_ID = "value-id";
  private static final String REQUIRED = "required";
  private static final String ELEMENT_REQUIRED = "element-required";
  private static final String VALUE_REQUIRED = "value-required";

  private UdfSignatureParser() {}

  public static void toJson(UdfSignature signature, JsonGenerator generator) throws IOException {
    Preconditions.checkArgument(signature != null, "Cannot serialize null UDF version");
    generator.writeStartObject();

    generator.writeStringField(SIGNATURE_ID, signature.signatureId());
    generator.writeArrayFieldStart(PARAMETERS);
    for (Types.NestedField parameter : signature.parameters()) {
      generator.writeStartObject();
      generator.writeNumberField(ID, parameter.fieldId());
      generator.writeStringField(NAME, parameter.name());
      generator.writeBooleanField(REQUIRED, parameter.isRequired());
      generator.writeFieldName(TYPE);
      toJson(parameter.type(), generator);
      if (parameter.doc() != null) {
        generator.writeStringField(DOC, parameter.doc());
      }
      generator.writeEndObject();
    }
    generator.writeEndArray();
    generator.writeFieldName(RETURN_TYPE);
    toJson(signature.returnType(), generator);
    generator.writeBooleanField(DETERMINISTIC, signature.deterministic());

    generator.writeEndObject();
  }

  public static String toJson(UdfSignature version) {
    return JsonUtil.generate(gen -> toJson(version, gen), false);
  }

  static UdfSignature fromJson(String json) {
    Preconditions.checkArgument(json != null, "Cannot parse UDF signature from null string");
    return JsonUtil.parse(json, UdfSignatureParser::fromJson);
  }

  public static UdfSignature fromJson(JsonNode node) {
    Preconditions.checkArgument(node != null, "Cannot parse UDF signature from null object");
    Preconditions.checkArgument(
        node.isObject(), "Cannot parse UDF signature from a non-object: %s", node);

    String signatureId = JsonUtil.getString(SIGNATURE_ID, node);
    List<Types.NestedField> parameters = parametersFromJson(node);
    Type returnType = typeFromJson(JsonUtil.get(RETURN_TYPE, node));
    Boolean deterministic = node.has(DETERMINISTIC) ? JsonUtil.getBool(DETERMINISTIC, node) : false;

    return ImmutableUdfSignature.builder()
        .signatureId(signatureId)
        .addAllParameters(parameters)
        .returnType(returnType)
        .deterministic(deterministic)
        .build();
  }

  private static void toJson(Types.StructType struct, JsonGenerator generator) throws IOException {
    generator.writeStartObject();

    generator.writeStringField(TYPE, STRUCT);
    generator.writeArrayFieldStart(FIELDS);
    for (Types.NestedField field : struct.fields()) {
      generator.writeStartObject();
      generator.writeNumberField(ID, field.fieldId());
      generator.writeStringField(NAME, field.name());
      generator.writeBooleanField(REQUIRED, field.isRequired());
      generator.writeFieldName(TYPE);
      toJson(field.type(), generator);
      if (field.doc() != null) {
        generator.writeStringField(DOC, field.doc());
      }
      generator.writeEndObject();
    }
    generator.writeEndArray();

    generator.writeEndObject();
  }

  static void toJson(Types.ListType list, JsonGenerator generator) throws IOException {
    generator.writeStartObject();

    generator.writeStringField(TYPE, LIST);

    generator.writeNumberField(ELEMENT_ID, list.elementId());
    generator.writeFieldName(ELEMENT);
    toJson(list.elementType(), generator);
    generator.writeBooleanField(ELEMENT_REQUIRED, !list.isElementOptional());

    generator.writeEndObject();
  }

  static void toJson(Types.MapType map, JsonGenerator generator) throws IOException {
    generator.writeStartObject();

    generator.writeStringField(TYPE, MAP);

    generator.writeNumberField(KEY_ID, map.keyId());
    generator.writeFieldName(KEY);
    toJson(map.keyType(), generator);

    generator.writeNumberField(VALUE_ID, map.valueId());
    generator.writeFieldName(VALUE);
    toJson(map.valueType(), generator);
    generator.writeBooleanField(VALUE_REQUIRED, !map.isValueOptional());

    generator.writeEndObject();
  }

  static void toJson(Type.PrimitiveType primitive, JsonGenerator generator) throws IOException {
    generator.writeString(primitive.toString());
  }

  static void toJson(Type type, JsonGenerator generator) throws IOException {
    if (type.isPrimitiveType()) {
      toJson(type.asPrimitiveType(), generator);
    } else {
      Type.NestedType nested = type.asNestedType();
      switch (type.typeId()) {
        case STRUCT:
          toJson(nested.asStructType(), generator);
          break;
        case LIST:
          toJson(nested.asListType(), generator);
          break;
        case MAP:
          toJson(nested.asMapType(), generator);
          break;
        default:
          throw new IllegalArgumentException("Cannot write unknown type: " + type);
      }
    }
  }

  private static Type typeFromJson(JsonNode json) {
    if (json.isTextual()) {
      return Types.fromPrimitiveString(json.asText());
    } else if (json.isObject()) {
      JsonNode typeObj = json.get(TYPE);
      if (typeObj != null) {
        String type = typeObj.asText();
        if (STRUCT.equals(type)) {
          return structFromJson(json);
        } else if (LIST.equals(type)) {
          return listFromJson(json);
        } else if (MAP.equals(type)) {
          return mapFromJson(json);
        }
      }
    }

    throw new IllegalArgumentException("Cannot parse type from json: " + json);
  }

  private static Types.StructType structFromJson(JsonNode json) {
    JsonNode fieldArray = JsonUtil.get(FIELDS, json);
    Preconditions.checkArgument(
        fieldArray.isArray(), "Cannot parse struct fields from non-array: %s", fieldArray);

    List<Types.NestedField> fields = Lists.newArrayListWithExpectedSize(fieldArray.size());
    Iterator<JsonNode> iterator = fieldArray.elements();
    while (iterator.hasNext()) {
      JsonNode field = iterator.next();
      Preconditions.checkArgument(
          field.isObject(), "Cannot parse struct field from non-object: %s", field);

      int id = JsonUtil.getInt(ID, field);
      String name = JsonUtil.getString(NAME, field);
      Type type = typeFromJson(JsonUtil.get(TYPE, field));

      String doc = JsonUtil.getStringOrNull(DOC, field);
      boolean isRequired = JsonUtil.getBool(REQUIRED, field);
      if (isRequired) {
        fields.add(Types.NestedField.required(id, name, type, doc));
      } else {
        fields.add(Types.NestedField.optional(id, name, type, doc));
      }
    }

    return Types.StructType.of(fields);
  }

  private static Types.ListType listFromJson(JsonNode json) {
    int elementId = JsonUtil.getInt(ELEMENT_ID, json);
    Type elementType = typeFromJson(JsonUtil.get(ELEMENT, json));
    boolean isRequired = JsonUtil.getBool(ELEMENT_REQUIRED, json);

    if (isRequired) {
      return Types.ListType.ofRequired(elementId, elementType);
    } else {
      return Types.ListType.ofOptional(elementId, elementType);
    }
  }

  private static Types.MapType mapFromJson(JsonNode json) {
    int keyId = JsonUtil.getInt(KEY_ID, json);
    Type keyType = typeFromJson(JsonUtil.get(KEY, json));

    int valueId = JsonUtil.getInt(VALUE_ID, json);
    Type valueType = typeFromJson(JsonUtil.get(VALUE, json));

    boolean isRequired = JsonUtil.getBool(VALUE_REQUIRED, json);

    if (isRequired) {
      return Types.MapType.ofRequired(keyId, valueId, keyType, valueType);
    } else {
      return Types.MapType.ofOptional(keyId, valueId, keyType, valueType);
    }
  }

  private static List<Types.NestedField> parametersFromJson(JsonNode json) {
    JsonNode parameterArray = JsonUtil.get(PARAMETERS, json);
    Preconditions.checkArgument(
        parameterArray.isArray(), "Cannot parse parameters from non-array: %s", parameterArray);

    List<Types.NestedField> parameters = Lists.newArrayListWithExpectedSize(parameterArray.size());
    Iterator<JsonNode> iterator = parameterArray.elements();
    while (iterator.hasNext()) {
      JsonNode field = iterator.next();
      Preconditions.checkArgument(
          field.isObject(), "Cannot parse struct field from non-object: %s", field);

      int id = JsonUtil.getInt(ID, field);
      String name = JsonUtil.getString(NAME, field);
      Type type = typeFromJson(JsonUtil.get(TYPE, field));

      String doc = JsonUtil.getStringOrNull(DOC, field);
      boolean isRequired = JsonUtil.getBool(REQUIRED, field);
      if (isRequired) {
        parameters.add(Types.NestedField.required(id, name, type, doc));
      } else {
        parameters.add(Types.NestedField.optional(id, name, type, doc));
      }
    }

    return parameters;
  }
}
