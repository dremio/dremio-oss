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
package com.dremio.plugins.elastic.mapping;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.plugins.elastic.mapping.ElasticMappingSet.Type;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.google.common.collect.ImmutableMap;

/**
 * Custom enumeration deserializer that maps all unknown values to ElasticMappingSet.Type.UNKNOWN
 */
public class ElasticTypeDeserializer extends StdDeserializer<Type>{

  private static final Logger logger = LoggerFactory.getLogger(ElasticTypeDeserializer.class);
  private static ImmutableMap<String, Type> TYPES;

  protected ElasticTypeDeserializer() {
    super(Type.class);
  }

  static {
    ImmutableMap.Builder<String, Type> builder = ImmutableMap.builder();
    for(Type t : Type.values()){
      builder.put(t.name().toLowerCase(), t);
    }
    TYPES = builder.build();
  }


  @Override
  public Type deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
    String enumStr = p.getValueAsString();
    Type type = TYPES.get(enumStr.toLowerCase());
    if(type != null){
      return type;
    }
    logger.debug("Dremio is unable to consume the field type {}, hiding from schema.", enumStr);
    return ElasticMappingSet.Type.UNKNOWN;
  }

}
