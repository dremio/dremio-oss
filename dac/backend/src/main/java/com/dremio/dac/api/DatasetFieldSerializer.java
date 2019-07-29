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
package com.dremio.dac.api;

import java.io.IOException;
import java.util.List;

import org.apache.arrow.vector.types.pojo.Field;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

/**
 * Serializes a list of arrow field definitions into JSON
 */
public class DatasetFieldSerializer extends JsonSerializer<List<Field>> {
  @Override
  public void serialize(List<Field> fields, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException, JsonProcessingException {
    jsonGenerator.writeStartArray();

    for (Field field: fields) {
      APIFieldDescriber.FieldDescriber describer = new APIFieldDescriber.FieldDescriber(jsonGenerator, field, false);
      field.getType().accept(describer);
    }

    jsonGenerator.writeEndArray();
  }
}
