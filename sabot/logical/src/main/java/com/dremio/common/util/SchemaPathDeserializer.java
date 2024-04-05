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
package com.dremio.common.util;

import com.dremio.common.expression.SchemaPath;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** SchemaPath deserialization utility, that can work on compound paths. */
public class SchemaPathDeserializer extends StdDeserializer<SchemaPath> {

  protected SchemaPathDeserializer() {
    super(SchemaPath.class);
  }

  @Override
  public SchemaPath deserialize(
      JsonParser jsonParser, DeserializationContext deserializationContext)
      throws IOException, JacksonException {
    JsonNode node = jsonParser.getCodec().readTree(jsonParser);
    if (node == null) {
      return null;
    }
    return deserializeSchemaPath(node.asText());
  }

  public static SchemaPath deserializeSchemaPath(String serializedStr) {
    List<String> subPaths = new ArrayList<>();
    StringBuilder currentSubPathBuilder = new StringBuilder();
    boolean activeTokenPosition = false;
    boolean delimiterExpectedNext = false;

    // The values are delimited by ".", enclosed in "`", whereas "\" is used as an escape character.
    for (int i = 0; i < serializedStr.length(); i++) {
      char c = serializedStr.charAt(i);
      if (delimiterExpectedNext) {
        Preconditions.checkState(c == '.', "Invalid format, delimiter expected at " + i);
        delimiterExpectedNext = false;
        continue;
      }

      if (!activeTokenPosition && c == '`') {
        activeTokenPosition = true;
        continue;
      }

      if (activeTokenPosition && c == '`') {
        activeTokenPosition = false;
        delimiterExpectedNext = true;
        String subPath = currentSubPathBuilder.toString();
        Preconditions.checkState(!subPath.isEmpty(), "Invalid format, empty path at " + i);
        subPaths.add(subPath);
        currentSubPathBuilder = new StringBuilder();
        continue;
      }

      if (c == '\\') { // escape
        i++;
        c = serializedStr.charAt(i);
      }

      currentSubPathBuilder.append(c);
    }
    Preconditions.checkState(
        !activeTokenPosition, "Invalid format, path segment not closed properly");
    Preconditions.checkState(!subPaths.isEmpty(), "No paths found");

    String[] subPathArr = subPaths.toArray(new String[subPaths.size()]);
    return SchemaPath.getCompoundPath(subPathArr);
  }
}
