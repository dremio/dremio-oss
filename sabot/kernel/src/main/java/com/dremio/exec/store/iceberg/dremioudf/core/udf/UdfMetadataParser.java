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
import com.dremio.exec.store.iceberg.dremioudf.api.udf.UdfSignature;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.UdfVersion;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.iceberg.TableMetadataParser.Codec;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.JsonUtil;

public class UdfMetadataParser {

  static final String FUNCTION_UUID = "function-uuid";
  static final String FORMAT_VERSION = "format-version";
  static final String LOCATION = "location";
  static final String CURRENT_VERSION_ID = "current-version-id";
  static final String VERSIONS = "versions";
  static final String SIGNATURES = "signatures";
  static final String VERSION_LOG = "version-log";
  static final String PROPERTIES = "properties";

  private UdfMetadataParser() {}

  public static String toJson(UdfMetadata metadata) {
    return toJson(metadata, false);
  }

  public static String toJson(UdfMetadata metadata, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(metadata, gen), pretty);
  }

  public static void toJson(UdfMetadata metadata, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != metadata, "Invalid UDF metadata: null");

    gen.writeStartObject();

    gen.writeStringField(FUNCTION_UUID, metadata.uuid());
    gen.writeNumberField(FORMAT_VERSION, metadata.formatVersion());
    gen.writeStringField(LOCATION, metadata.location());
    if (!metadata.properties().isEmpty()) {
      JsonUtil.writeStringMap(PROPERTIES, metadata.properties(), gen);
    }

    gen.writeStringField(CURRENT_VERSION_ID, metadata.currentVersionId());
    gen.writeArrayFieldStart(VERSIONS);
    for (UdfVersion version : metadata.versions()) {
      UdfVersionParser.toJson(version, gen);
    }
    gen.writeEndArray();

    gen.writeArrayFieldStart(SIGNATURES);
    for (UdfSignature signature : metadata.signatures()) {
      UdfSignatureParser.toJson(signature, gen);
    }
    gen.writeEndArray();

    gen.writeArrayFieldStart(VERSION_LOG);
    for (UdfHistoryEntry udfHistoryEntry : metadata.history()) {
      UdfHistoryEntryParser.toJson(udfHistoryEntry, gen);
    }
    gen.writeEndArray();

    gen.writeEndObject();
  }

  public static UdfMetadata fromJson(String metadataLocation, String json) {
    return JsonUtil.parse(json, node -> UdfMetadataParser.fromJson(metadataLocation, node));
  }

  public static UdfMetadata fromJson(String json) {
    Preconditions.checkArgument(json != null, "Cannot parse UDF metadata from null string");
    return JsonUtil.parse(json, UdfMetadataParser::fromJson);
  }

  public static UdfMetadata fromJson(JsonNode json) {
    return fromJson(null, json);
  }

  public static UdfMetadata fromJson(String metadataLocation, JsonNode json) {
    Preconditions.checkArgument(json != null, "Cannot parse UDF metadata from null object");
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse UDF metadata from non-object: %s", json);

    String uuid = JsonUtil.getString(FUNCTION_UUID, json);
    int formatVersion = JsonUtil.getInt(FORMAT_VERSION, json);
    String location = JsonUtil.getString(LOCATION, json);
    Map<String, String> properties =
        json.has(PROPERTIES) ? JsonUtil.getStringMap(PROPERTIES, json) : ImmutableMap.of();

    String currentVersionId = JsonUtil.getString(CURRENT_VERSION_ID, json);
    JsonNode versionsNode = JsonUtil.get(VERSIONS, json);
    Preconditions.checkArgument(
        versionsNode.isArray(), "Cannot parse versions from non-array: %s", versionsNode);
    List<UdfVersion> versions = Lists.newArrayListWithExpectedSize(versionsNode.size());
    for (JsonNode versionNode : versionsNode) {
      versions.add(UdfVersionParser.fromJson(versionNode));
    }

    JsonNode signaturesNode = JsonUtil.get(SIGNATURES, json);
    Preconditions.checkArgument(
        signaturesNode.isArray(), "Cannot parse signatures from non-array: %s", signaturesNode);
    List<UdfSignature> signatures = Lists.newArrayListWithExpectedSize(signaturesNode.size());
    for (JsonNode signatureNode : signaturesNode) {
      signatures.add(UdfSignatureParser.fromJson(signatureNode));
    }

    JsonNode versionLogNode = JsonUtil.get(VERSION_LOG, json);
    Preconditions.checkArgument(
        versionLogNode.isArray(), "Cannot parse version-log from non-array: %s", versionLogNode);
    List<UdfHistoryEntry> historyEntries =
        Lists.newArrayListWithExpectedSize(versionLogNode.size());
    for (JsonNode vLog : versionLogNode) {
      historyEntries.add(UdfHistoryEntryParser.fromJson(vLog));
    }

    return ImmutableUdfMetadata.of(
        uuid,
        formatVersion,
        location,
        currentVersionId,
        versions,
        signatures,
        historyEntries,
        properties,
        ImmutableList.of(),
        metadataLocation);
  }

  public static void overwrite(UdfMetadata metadata, OutputFile outputFile) {
    internalWrite(metadata, outputFile, true);
  }

  public static void write(UdfMetadata metadata, OutputFile outputFile) {
    internalWrite(metadata, outputFile, false);
  }

  public static UdfMetadata read(InputFile file) {
    Codec codec = Codec.fromFileName(file.location());
    try (InputStream is =
        codec == Codec.GZIP ? new GZIPInputStream(file.newStream()) : file.newStream()) {
      return fromJson(file.location(), JsonUtil.mapper().readValue(is, JsonNode.class));
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to read json file: %s", file), e);
    }
  }

  private static void internalWrite(
      UdfMetadata metadata, OutputFile outputFile, boolean overwrite) {
    boolean isGzip = Codec.fromFileName(outputFile.location()) == Codec.GZIP;
    OutputStream stream = overwrite ? outputFile.createOrOverwrite() : outputFile.create();
    try (OutputStreamWriter writer =
        new OutputStreamWriter(
            isGzip ? new GZIPOutputStream(stream) : stream, StandardCharsets.UTF_8)) {
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      generator.useDefaultPrettyPrinter();
      toJson(metadata, generator);
      generator.flush();
    } catch (IOException e) {
      throw new UncheckedIOException(
          String.format("Failed to write json to file: %s", outputFile), e);
    }
  }
}
