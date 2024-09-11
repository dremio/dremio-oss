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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dremio.exec.store.iceberg.IcebergViewMetadata.SupportedIcebergViewSpecVersion;
import com.dremio.exec.store.iceberg.viewdepoc.BaseVersion;
import com.dremio.exec.store.iceberg.viewdepoc.DremioViewVersionMetadataParser;
import com.dremio.exec.store.iceberg.viewdepoc.VersionSummary;
import com.dremio.exec.store.iceberg.viewdepoc.ViewDefinition;
import com.dremio.exec.store.iceberg.viewdepoc.ViewVersionMetadata;
import com.dremio.exec.store.iceberg.viewdepoc.ViewVersionMetadataParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;
import org.junit.jupiter.api.Test;

class TestIcebergViewMetadataWithV0 {
  private static final Schema TEST_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "x", Types.LongType.get()),
          Types.NestedField.required(2, "y", Types.LongType.get(), "comment"),
          Types.NestedField.required(3, "z", Types.LongType.get()));
  private static final String warehouseLocation = "s3://bucket/test/warehouselocation";
  private static final Map<String, String> properties = ImmutableMap.of("some-key", "some-value");
  private static final String metadataJsonlocation =
      "s3://bucket/test/warehouselocation/testview/abc-123.metadata.json";
  private static final ViewDefinition viewDefinition =
      ViewDefinition.of(
          "select * from mySource.myTable",
          TEST_SCHEMA,
          "session-catalog",
          Collections.singletonList("session-namespace"));
  private static final VersionSummary versionSummary = new VersionSummary(properties);
  private static final BaseVersion baseVersion =
      new BaseVersion(1, 1, 1, versionSummary, viewDefinition);
  ViewVersionMetadata viewVersionMetadata =
      ViewVersionMetadata.newViewVersionMetadata(
          baseVersion, warehouseLocation, viewDefinition, properties);
  IcebergViewMetadata icebergViewMetadata =
      IcebergViewMetadataImplV0.of(viewVersionMetadata, metadataJsonlocation);

  @Test
  void testGetMethods() {
    assertEquivalent(icebergViewMetadata, viewVersionMetadata);
  }

  @Test
  void TestToJson() throws JsonProcessingException {
    // Setup
    String jsonFromViewVersionMetadata =
        JsonUtil.generate(gen -> ViewVersionMetadataParser.toJson(viewVersionMetadata, gen), true);
    String jsonFromIcebergViewMetadata = icebergViewMetadata.toJson();
    // Act and Assert
    assertEquals(jsonFromIcebergViewMetadata, jsonFromViewVersionMetadata);
  }

  @Test
  void testFromJson() {
    // Setup
    ViewVersionMetadata viewVersionMetadata =
        ViewVersionMetadata.newViewVersionMetadata(
            baseVersion, metadataJsonlocation, viewDefinition, properties);
    String json =
        JsonUtil.generate(gen -> ViewVersionMetadataParser.toJson(viewVersionMetadata, gen), true);
    // Act
    IcebergViewMetadata icebergViewMetadataFromJson =
        IcebergViewMetadataUtils.fromJson(metadataJsonlocation, json);
    // Assert
    assertEquivalent(icebergViewMetadataFromJson, viewVersionMetadata);
    assertEquals(icebergViewMetadataFromJson.getMetadataLocation(), metadataJsonlocation);
  }

  @Test
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

  private void assertEquivalent(
      IcebergViewMetadata icebergViewMetadata, ViewVersionMetadata viewVersionMetadata) {
    assertEquals(SupportedIcebergViewSpecVersion.V0, icebergViewMetadata.getFormatVersion());
    assertTrue(
        viewVersionMetadata.definition().schema().sameSchema(icebergViewMetadata.getSchema()));
    assertEquals(viewVersionMetadata.definition().sql(), icebergViewMetadata.getSql());
    assertEquals(
        viewVersionMetadata.definition().sessionNamespace(), icebergViewMetadata.getSchemaPath());
    assertEquals(viewVersionMetadata.location(), icebergViewMetadata.getLocation());
    assertEquals(viewVersionMetadata.properties(), icebergViewMetadata.getProperties());
    assertEquals(
        viewVersionMetadata.history().get(0).timestampMillis(), icebergViewMetadata.getCreatedAt());
    assertEquals(
        viewVersionMetadata.currentVersion().timestampMillis(),
        icebergViewMetadata.getLastModifiedAt());
    String uuid =
        metadataJsonlocation.substring(
            metadataJsonlocation.lastIndexOf("/") + 1,
            metadataJsonlocation.lastIndexOf(".metadata.json"));
    assertEquals(uuid, icebergViewMetadata.getUniqueId());
  }
}
