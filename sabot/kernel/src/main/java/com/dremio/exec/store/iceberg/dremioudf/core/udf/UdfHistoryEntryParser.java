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

import com.dremio.exec.store.iceberg.dremioudf.api.udf.UdfHistoryEntry;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import java.io.IOException;
import org.apache.iceberg.util.JsonUtil;

class UdfHistoryEntryParser {

  private UdfHistoryEntryParser() {}

  static final String VERSION_ID = "version-id";
  static final String TIMESTAMP_MS = "timestamp-ms";

  static String toJson(UdfHistoryEntry entry) {
    return JsonUtil.generate(gen -> toJson(entry, gen), false);
  }

  static void toJson(UdfHistoryEntry entry, JsonGenerator generator) throws IOException {
    Preconditions.checkArgument(entry != null, "Invalid UDF history entry: null");
    generator.writeStartObject();
    generator.writeNumberField(TIMESTAMP_MS, entry.timestampMillis());
    generator.writeStringField(VERSION_ID, entry.versionId());
    generator.writeEndObject();
  }

  static UdfHistoryEntry fromJson(String json) {
    return JsonUtil.parse(json, UdfHistoryEntryParser::fromJson);
  }

  static UdfHistoryEntry fromJson(JsonNode node) {
    Preconditions.checkArgument(node != null, "Cannot parse UDF history entry from null object");
    Preconditions.checkArgument(
        node.isObject(), "Cannot parse UDF history entry from non-object: %s", node);
    return ImmutableUdfHistoryEntry.builder()
        .versionId(JsonUtil.getString(VERSION_ID, node))
        .timestampMillis(JsonUtil.getLong(TIMESTAMP_MS, node))
        .build();
  }
}
