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
package com.dremio.exec.store.hive;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.ResolvableDeserializer;

/**
 * Class contains miscellaneous utility functions for Hive Common
 */
public class HiveCommonUtilities {

  public static <T> T deserialize(JsonParser jsonParser, DeserializationContext context, JsonNode node,
                           Class<? extends T> clazz) throws IOException, JsonProcessingException {
    final JavaType javaType = context.getTypeFactory().constructType(clazz);
    final BeanDescription description = context.getConfig().introspect(javaType);
    final JsonDeserializer<Object> deserializer = context.getFactory().createBeanDeserializer(
      context, javaType, description);
    if (deserializer instanceof ResolvableDeserializer) {
      ((ResolvableDeserializer) deserializer).resolve(context);
    }
    final JsonParser parser = jsonParser.getCodec().treeAsTokens(node);
    context.getConfig().initialize(parser);
    if (parser.getCurrentToken() == null) {
      parser.nextToken();
    }
    return clazz.cast(deserializer.deserialize(parser, context));
  }
}
