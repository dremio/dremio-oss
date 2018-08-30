/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.common.serde;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.NullNode;

/**
 * Custom deserializer for Arrow Pojos Schema/Field to avoid issues
 * with incompatible change of Arrow removing "typeLayout" field
 */
public class BackwardCompatibleSchemaDe extends StdDeserializer<Schema> {

  private static final ObjectMapper mapper = new ObjectMapper();
  private static final SimpleModule module = new SimpleModule();

  static {
    module.addDeserializer(Schema.class, new BackwardCompatibleSchemaDe());
    module.addDeserializer(Field.class, new BackwardCompatibleFieldDeserializer());
    mapper.registerModule(module);
  }

  private static final ObjectReader fieldsReader = mapper.readerFor(new TypeReference<List<Field>>() {});

  protected BackwardCompatibleSchemaDe() {
    this(null);
  }

  protected BackwardCompatibleSchemaDe(Class<?> vc) {
    super(vc);
  }

  public static Schema fromJSON(String json) throws IOException {
      return mapper.readValue(checkNotNull(json), Schema.class);
  }

  @Override
  public Schema deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
    JsonNode node = jsonParser.getCodec().readTree(jsonParser);
    JsonNode metadataNode = node.get("metadata");
    Map<String,String> metadata = mapper.convertValue(metadataNode, Map.class);

    JsonNode fieldsNode = node.get("fields");
    Iterable<Field> fields = fieldsReader.readValue(fieldsNode);

    return new Schema(fields, metadata);
  }

  public static class BackwardCompatibleFieldDeserializer extends StdDeserializer<Field> {

    public BackwardCompatibleFieldDeserializer() {
      this(null);
    }

    public  BackwardCompatibleFieldDeserializer(Class<?> vc) {
      super(vc);
    }

    @Override
    public Field deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
      JsonNode node = jsonParser.getCodec().readTree(jsonParser);

      JsonNode nameNode = node.get("name");
      final String name;
      if (nameNode instanceof NullNode) {
        name = null;
      } else {
        name = nameNode.asText();
      }

      boolean nullable = node.get("nullable").asBoolean();

      JsonNode arrowTypeNode = node.get("type");
      ArrowType arrowType = mapper.convertValue(arrowTypeNode, ArrowType.class);
      JsonNode dictionaryNode = node.get("dictionary");
      DictionaryEncoding dictionary = mapper.convertValue(dictionaryNode, DictionaryEncoding.class);
      JsonNode childrenNode = node.get("children");

      List<Field> children = fieldsReader.readValue(childrenNode);

      JsonNode metadataNode = node.get("metadata");
      Map<String,String> metadata = mapper.convertValue(metadataNode, Map.class);

      FieldType fieldType = new FieldType(nullable, arrowType, dictionary, metadata);
      return new Field(name, fieldType, children);
    }
  }
}
