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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dremio.exec.store.iceberg.IcebergViewMetadata.SupportedIcebergViewSpecVersion;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;
import org.apache.iceberg.view.ImmutableSQLViewRepresentation;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewMetadataParser;
import org.apache.iceberg.view.ViewVersion;
import org.junit.jupiter.api.Test;

class TestIcebergViewMetadataWithV1 {
  private static final Schema TEST_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "x", Types.LongType.get()),
          Types.NestedField.required(2, "y", Types.LongType.get(), "comment"),
          Types.NestedField.required(3, "z", Types.LongType.get()));
  private static final String warehouseLocation = "s3://bucket/test/warehouselocation";
  private static final Map<String, String> properties = ImmutableMap.of("some-key", "some-value");
  private static final String metadataJsonlocation =
      "s3://bucket/test/warehouselocation/testview/abc-123.metadata.json";

  ViewVersion testSparkversion =
      ImmutableViewVersion.builder()
          .versionId(1)
          .timestampMillis(4353L)
          .summary(ImmutableMap.of("user", "some-user"))
          .schemaId(0)
          .defaultCatalog("some-catalog")
          .defaultNamespace(Namespace.empty())
          .addRepresentations(
              ImmutableSQLViewRepresentation.builder()
                  .sql("select 'foo' foo")
                  .dialect("spark-sql")
                  .build())
          .build();
  ViewVersion testDremioVersion1 =
      ImmutableViewVersion.builder()
          .versionId(1)
          .timestampMillis(4353L)
          .summary(ImmutableMap.of("user", "some-user"))
          .schemaId(0)
          .defaultCatalog("some-catalog")
          .defaultNamespace(Namespace.empty())
          .addRepresentations(
              ImmutableSQLViewRepresentation.builder()
                  .sql("select 'foo' foo")
                  .dialect("DremioSQL")
                  .build())
          .build();

  ViewVersion testDremioVersion2 =
      ImmutableViewVersion.builder()
          .versionId(2)
          .schemaId(0)
          .timestampMillis(5555L)
          .summary(ImmutableMap.of("user", "some-user"))
          .defaultCatalog("some-catalog")
          .defaultNamespace(Namespace.empty())
          .addRepresentations(
              ImmutableSQLViewRepresentation.builder()
                  .sql("select 1 id, 'abc' data")
                  .dialect("DremioSQL")
                  .build())
          .build();

  private ViewMetadata testDremioViewMetadata =
      ViewMetadata.builder()
          .assignUUID("fa6506c3-7681-40c8-86dc-e36561f83385")
          .addSchema(TEST_SCHEMA)
          .addVersion(testDremioVersion1)
          .addVersion(testDremioVersion2)
          .setLocation("s3://bucket/test/location")
          .setProperties(ImmutableMap.of("some-key", "some-value"))
          .setCurrentVersionId(2)
          .upgradeFormatVersion(1)
          .build();
  private ViewMetadata testSparkViewMetadata =
      ViewMetadata.builder()
          .assignUUID("fa6506c3-7681-40c8-86dc-e36561f83385")
          .addSchema(TEST_SCHEMA)
          .addVersion(testSparkversion)
          .setLocation("s3://bucket/test/location")
          .setProperties(ImmutableMap.of("some-key", "some-value"))
          .setCurrentVersionId(1)
          .upgradeFormatVersion(1)
          .build();

  IcebergViewMetadata icebergViewMetadata = IcebergViewMetadataImplV1.of(testDremioViewMetadata);

  IcebergViewMetadata icebergViewMetadataSpark =
      IcebergViewMetadataImplV1.of(testSparkViewMetadata);

  @Test
  void testGetMethods() {
    assertEquivalent(icebergViewMetadata, testDremioViewMetadata);
  }

  @Test
  void testUnsupportedDialect() {
    List<String> supportedDialects =
        Arrays.stream(IcebergViewMetadata.SupportedViewDialectsForRead.values())
            .map(Enum::name)
            .collect(Collectors.toList());
    assertThatThrownBy(() -> IcebergViewMetadataImplV1.of(testSparkViewMetadata).getSql())
        .hasMessageContaining(
            "This view contains unsupported SQL dialects. Supported dialects are: %s. Actual dialects in representations: [spark-sql]",
            supportedDialects);
  }

  @Test
  void TestToJson() throws JsonProcessingException {
    // Setup
    String jsonFromViewMetadata =
        JsonUtil.generate(gen -> ViewMetadataParser.toJson(testDremioViewMetadata, gen), true);
    String jsonFromIcebergViewMetadata = icebergViewMetadata.toJson();
    // Act and Assert
    assertEquals(jsonFromIcebergViewMetadata, jsonFromViewMetadata);
  }

  @Test
  void testFromJson() {
    // Setup

    String json =
        JsonUtil.generate(gen -> ViewMetadataParser.toJson(testDremioViewMetadata, gen), true);
    // Act
    IcebergViewMetadata icebergViewMetadataFromJson =
        IcebergViewMetadataUtils.fromJson(metadataJsonlocation, json);
    // Assert
    assertEquivalent(icebergViewMetadataFromJson, testDremioViewMetadata);
  }

  /* @Test
    void TestToAndFromJson() throws JsonProcessingException {
      // Setup
      String jsonFromViewVersionMetadata =
          JsonUtil.generate(gen -> ViewVersionMetadataParser.toJson(viewVersionMetadata, gen), true);
      String jsonFromIcebergViewMetadata = icebergViewMetadata.toJson();
      // Act
      ViewVersionMetadata viewVersionMetadataFromIcebergViewJson =
          DremioViewVersionMetadataParser.fromJson(jsonFromIcebergViewMetadata);
      IcebergViewMetadata icebergViewMetadataFromViewVersionJson =
          IcebergViewMetadataUtils.fromJson(metadataJsonlocation, jsonFromViewVersionMetadata);

      // Assert
      assertEquivalent(
          icebergViewMetadataFromViewVersionJson, viewVersionMetadataFromIcebergViewJson);
    }
  */
  private void assertEquivalent(
      IcebergViewMetadata icebergViewMetadata, ViewMetadata viewMetadata) {
    assertEquals(SupportedIcebergViewSpecVersion.V1, icebergViewMetadata.getFormatVersion());
    assertTrue(viewMetadata.schema().sameSchema(icebergViewMetadata.getSchema()));
    SQLViewRepresentation sqlViewRepresentation =
        (SQLViewRepresentation)
            viewMetadata
                .currentVersion()
                .representations()
                .get(viewMetadata.currentVersion().representations().size() - 1);
    assertEquals(sqlViewRepresentation.sql(), icebergViewMetadata.getSql());
    assertEquals(sqlViewRepresentation.dialect(), icebergViewMetadata.getDialect());
    assertEquals(viewMetadata.location(), icebergViewMetadata.getLocation());
    assertEquals(viewMetadata.properties(), icebergViewMetadata.getProperties());
    assertEquals(
        viewMetadata.history().get(0).timestampMillis(), icebergViewMetadata.getCreatedAt());
    assertEquals(
        viewMetadata.currentVersion().timestampMillis(), icebergViewMetadata.getLastModifiedAt());
    assertEquals(viewMetadata.uuid(), icebergViewMetadata.getUniqueId());
  }
}
