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
package com.dremio.exec.store.iceberg;

import static com.dremio.test.UserExceptionAssert.assertThatThrownBy;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.dremio.exec.store.iceberg.dremioudf.api.udf.SQLUdfRepresentation;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.UdfSignature;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.UdfVersion;
import com.dremio.exec.store.iceberg.dremioudf.core.udf.ImmutableSQLUdfRepresentation;
import com.dremio.exec.store.iceberg.dremioudf.core.udf.ImmutableUdfSignature;
import com.dremio.exec.store.iceberg.dremioudf.core.udf.ImmutableUdfVersion;
import com.dremio.exec.store.iceberg.dremioudf.core.udf.UdfMetadata;
import com.dremio.exec.store.iceberg.dremioudf.core.udf.UdfMetadataParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;
import org.junit.jupiter.api.Test;

class TestVersionedUdfMetadata {
  private String TEST_UUID = UUID.randomUUID().toString();
  private String TEST_VERSION_ID = UUID.randomUUID().toString();
  private static String TEST_SIGNATURE_ID = UUID.randomUUID().toString();
  private long UDF_TIMESTAMP = 11111111;
  protected static final UdfSignature TEST_SIGNATURE =
      ImmutableUdfSignature.builder()
          .signatureId(TEST_SIGNATURE_ID)
          .addParameters(
              required(1, "x", Types.DoubleType.get(), "X coordinate"),
              required(2, "y", Types.DoubleType.get()))
          .returnType(Types.DoubleType.get())
          .deterministic(true)
          .build();
  private static final String warehouseLocation = "s3://bucket/test/warehouselocation";
  private static final Map<String, String> properties = ImmutableMap.of("some-key", "some-value");
  private static final String metadataJsonlocation =
      "s3://bucket/test/warehouselocation/testudf/abc-123.metadata.json";

  SQLUdfRepresentation sqlUdfRepresentation =
      ImmutableSQLUdfRepresentation.builder()
          .body("select * from ns.tbl")
          .comment("Test udf")
          .dialect("DremioSQL")
          .build();

  UdfVersion udfVersion =
      ImmutableUdfVersion.builder()
          .versionId(TEST_VERSION_ID)
          .addRepresentations(sqlUdfRepresentation)
          .timestampMillis(UDF_TIMESTAMP)
          .defaultNamespace(Namespace.empty())
          .signatureId(TEST_SIGNATURE_ID)
          .build();
  UdfMetadata udfMetadata =
      UdfMetadata.builder()
          .setLocation(warehouseLocation)
          .assignUUID(TEST_UUID)
          .setCurrentVersion(udfVersion, TEST_SIGNATURE)
          .build();

  SQLUdfRepresentation sparkUdfRepresentation =
      ImmutableSQLUdfRepresentation.builder()
          .body("select * from ns.tbl")
          .comment("Test udf")
          .dialect("spark-sql")
          .build();

  UdfVersion sparkUdfVersion =
      ImmutableUdfVersion.builder()
          .versionId(TEST_VERSION_ID)
          .addRepresentations(sparkUdfRepresentation)
          .timestampMillis(UDF_TIMESTAMP)
          .defaultNamespace(Namespace.empty())
          .signatureId(TEST_SIGNATURE_ID)
          .build();
  UdfMetadata sparkUdfMetadata =
      UdfMetadata.builder()
          .setLocation(warehouseLocation)
          .assignUUID(TEST_UUID)
          .setCurrentVersion(sparkUdfVersion, TEST_SIGNATURE)
          .build();

  VersionedUdfMetadata versionedUdfMetadata = VersionedUdfMetadataImpl.of(udfMetadata);

  @Test
  void testGetMethods() {
    assertEquivalent(versionedUdfMetadata, udfMetadata);
  }

  @Test
  void TestToJson() throws JsonProcessingException {
    // Setup
    String jsonFromUdfMetadata =
        JsonUtil.generate(gen -> UdfMetadataParser.toJson(udfMetadata, gen), true);
    String jsonFromIcebergUdfMetadata = versionedUdfMetadata.toJson();
    // Act and Assert
    assertEquals(jsonFromIcebergUdfMetadata, jsonFromUdfMetadata);
  }

  @Test
  void testFromJson() {
    // Setup
    String json = JsonUtil.generate(gen -> UdfMetadataParser.toJson(udfMetadata, gen), true);
    // Act
    VersionedUdfMetadata versionedUdfMetadataFromJson = VersionedUdfMetadataUtils.fromJson(json);
    // Assert
    assertEquivalent(versionedUdfMetadataFromJson, udfMetadata);
  }

  @Test
  void TestToAndFromJson() throws JsonProcessingException {
    // Setup
    String jsonFromUdfMetadata =
        JsonUtil.generate(gen -> UdfMetadataParser.toJson(udfMetadata, gen), true);
    String jsonFromIcebergUdfMetadata = versionedUdfMetadata.toJson();
    // Act
    UdfMetadata udfMetadataFromIcebergUdfJson =
        UdfMetadataParser.fromJson(jsonFromIcebergUdfMetadata);
    VersionedUdfMetadata versionedUdfMetadataFromUdfMetadataJson =
        VersionedUdfMetadataUtils.fromJson(jsonFromUdfMetadata);

    // Assert
    assertEquivalent(versionedUdfMetadataFromUdfMetadataJson, udfMetadataFromIcebergUdfJson);
  }

  private void assertEquivalent(
      VersionedUdfMetadata versionedUdfMetadata, UdfMetadata udfMetadata) {
    ImmutableSQLUdfRepresentation representationFromVersionedUdfMetadata =
        ImmutableSQLUdfRepresentation.builder()
            .body(versionedUdfMetadata.getBody())
            .comment(versionedUdfMetadata.getComment())
            .dialect(versionedUdfMetadata.getDialect())
            .build();

    assertEquals(
        versionedUdfMetadata.getFormatVersion(),
        VersionedUdfMetadata.SupportedVersionedUdfSpecVersion.of(udfMetadata.formatVersion()));
    assertEquals(versionedUdfMetadata.getLocation(), udfMetadata.location());
    assertEquals(versionedUdfMetadata.getMetadataLocation(), udfMetadata.metadataFileLocation());
    assertEquals(versionedUdfMetadata.getProperties(), udfMetadata.properties());
    assertEquals(
        versionedUdfMetadata.getCreatedAt(), udfMetadata.currentVersion().timestampMillis());
    assertEquals(
        versionedUdfMetadata.getLastModifiedAt(), udfMetadata.currentVersion().timestampMillis());
    assertEquals(versionedUdfMetadata.getUniqueId(), udfMetadata.uuid());
    assertEquals(udfMetadata.currentVersion().representations().size(), 1);
    assertEquals(
        representationFromVersionedUdfMetadata,
        udfMetadata.currentVersion().representations().get(0));
  }

  @Test
  void testUnsupportedDialect() {
    List<String> supportedUdfDialects =
        Arrays.stream(VersionedUdfMetadata.SupportedUdfDialects.values())
            .map(Enum::name)
            .collect(Collectors.toList());
    assertThatThrownBy(() -> VersionedUdfMetadataImpl.of(sparkUdfMetadata).getBody())
        .hasMessageContaining(
            "This udf contains unsupported SQL dialects. Supported dialects are: %s. Actual dialects in representations: [spark-sql]",
            supportedUdfDialects);
  }
}
