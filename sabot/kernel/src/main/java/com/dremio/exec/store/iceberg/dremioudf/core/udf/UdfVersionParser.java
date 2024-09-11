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

import com.dremio.exec.store.iceberg.dremioudf.api.udf.UdfRepresentation;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.UdfVersion;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.util.JsonUtil;

public class UdfVersionParser {

  private static final String VERSION_ID = "version-id";
  private static final String SIGNATURE_ID = "signature-id";
  private static final String TIMESTAMP_MS = "timestamp-ms";
  private static final String SUMMARY = "summary";
  private static final String REPRESENTATIONS = "representations";
  private static final String DEFAULT_CATALOG = "default-catalog";
  private static final String DEFAULT_NAMESPACE = "default-namespace";

  private UdfVersionParser() {}

  public static void toJson(UdfVersion version, JsonGenerator generator) throws IOException {
    Preconditions.checkArgument(version != null, "Cannot serialize null UDF version");
    generator.writeStartObject();

    generator.writeStringField(VERSION_ID, version.versionId());
    generator.writeStringField(SIGNATURE_ID, version.signatureId());
    generator.writeNumberField(TIMESTAMP_MS, version.timestampMillis());
    JsonUtil.writeStringMap(SUMMARY, version.summary(), generator);

    if (version.defaultCatalog() != null) {
      generator.writeStringField(DEFAULT_CATALOG, version.defaultCatalog());
    }

    JsonUtil.writeStringArray(
        DEFAULT_NAMESPACE, Arrays.asList(version.defaultNamespace().levels()), generator);

    generator.writeArrayFieldStart(REPRESENTATIONS);
    for (UdfRepresentation representation : version.representations()) {
      UdfRepresentationParser.toJson(representation, generator);
    }
    generator.writeEndArray();

    generator.writeEndObject();
  }

  public static String toJson(UdfVersion version) {
    return JsonUtil.generate(gen -> toJson(version, gen), false);
  }

  static UdfVersion fromJson(String json) {
    Preconditions.checkArgument(json != null, "Cannot parse UDF version from null string");
    return JsonUtil.parse(json, UdfVersionParser::fromJson);
  }

  public static UdfVersion fromJson(JsonNode node) {
    Preconditions.checkArgument(node != null, "Cannot parse UDF version from null object");
    Preconditions.checkArgument(
        node.isObject(), "Cannot parse UDF version from a non-object: %s", node);

    String versionId = JsonUtil.getString(VERSION_ID, node);
    String signatureId = JsonUtil.getString(SIGNATURE_ID, node);
    long timestamp = JsonUtil.getLong(TIMESTAMP_MS, node);
    Map<String, String> summary = JsonUtil.getStringMap(SUMMARY, node);

    JsonNode serializedRepresentations = node.get(REPRESENTATIONS);
    ImmutableList.Builder<UdfRepresentation> representations = ImmutableList.builder();
    for (JsonNode serializedRepresentation : serializedRepresentations) {
      UdfRepresentation representation = UdfRepresentationParser.fromJson(serializedRepresentation);
      representations.add(representation);
    }

    String defaultCatalog = JsonUtil.getStringOrNull(DEFAULT_CATALOG, node);

    Namespace defaultNamespace =
        Namespace.of(JsonUtil.getStringArray(JsonUtil.get(DEFAULT_NAMESPACE, node)));

    return ImmutableUdfVersion.builder()
        .versionId(versionId)
        .signatureId(signatureId)
        .timestampMillis(timestamp)
        .summary(summary)
        .defaultNamespace(defaultNamespace)
        .defaultCatalog(defaultCatalog)
        .representations(representations.build())
        .build();
  }
}
