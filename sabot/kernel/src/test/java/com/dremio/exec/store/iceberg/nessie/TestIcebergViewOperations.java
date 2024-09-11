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
package com.dremio.exec.store.iceberg.nessie;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.exec.store.iceberg.BaseIcebergViewTest;
import com.dremio.exec.store.iceberg.IcebergViewMetadata;
import com.dremio.exec.store.iceberg.IcebergViewMetadata.SupportedIcebergViewSpecVersion;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.projectnessie.model.ContentKey;

class TestIcebergViewOperations extends BaseIcebergViewTest {
  private static final String CREATE_VIEW_SQL = "select id from tb1";
  private static final Schema CREATE_VIEW_SCHEMA =
      new Schema(Types.StructType.of(required(1, "id", Types.LongType.get())).fields());
  private static final String UPDATED_VIEW_SQL = "select name from tb1";
  private static final Schema UPDATED_VIEW_SCHEMA =
      new Schema(Types.StructType.of(required(1, "name", Types.StringType.get())).fields());
  private static final String CATALOG_NAME = "nessie";
  private static final String VIEW_USER = "test_user";
  private static final String TEST_BRANCH = "test_branch";
  private static final List<String> VIEW_PATH = Arrays.asList("folder1", "folder2");

  private static final List<String> viewKey = Arrays.asList("view", "foo", "bar");

  @ParameterizedTest
  @EnumSource(
      value = SupportedIcebergViewSpecVersion.class,
      names = {"UNKNOWN"},
      mode = EnumSource.Mode.EXCLUDE)
  void testCreate(SupportedIcebergViewSpecVersion viewSpecVersion) {
    createBranch(TEST_BRANCH, VersionContext.NOT_SPECIFIED);
    createNamespacesIfMissing(TEST_BRANCH, ContentKey.of(viewKey));
    IcebergViewOperationsBuilder icebergViewOperationsBuilder =
        IcebergViewOperationsBuilder.newViewOps()
            .withViewMetadataWarehouseLocation(getWarehouseLocation())
            .withCatalogName(CATALOG_NAME)
            .withFileIO(getFileIO())
            .withUserName(VIEW_USER)
            .withSanitizer(s -> s)
            .withViewSpecVersion(viewSpecVersion)
            .withMetadataLoader(
                (viewSpecVersion == SupportedIcebergViewSpecVersion.V1)
                    ? (BaseIcebergViewTest::viewMetadataLoader)
                    : (BaseIcebergViewTest::viewVersionMetadataLoader))
            .withNessieClient(nessieClient);

    icebergViewOperationsBuilder
        .build()
        .create(
            viewKey,
            CREATE_VIEW_SQL,
            CREATE_VIEW_SCHEMA,
            Collections.emptyList(),
            getVersion(TEST_BRANCH));

    String createdMetadataLocation = getMetadataLocation(viewKey, getVersion(TEST_BRANCH)).get();

    final IcebergViewMetadata createdViewMetadata =
        IcebergViewOperationsBuilder.newViewOps()
            .withFileIO(getFileIO())
            .build()
            .refreshFromMetadataLocation(createdMetadataLocation);

    assertTrue(createdViewMetadata.getSchema().sameSchema(CREATE_VIEW_SCHEMA));
    assertTrue(createdViewMetadata.getSql().equalsIgnoreCase(CREATE_VIEW_SQL));
    dropBranch(TEST_BRANCH, getVersion(TEST_BRANCH));
  }

  @ParameterizedTest
  @EnumSource(
      value = SupportedIcebergViewSpecVersion.class,
      names = {"UNKNOWN"},
      mode = EnumSource.Mode.EXCLUDE)
  void testUpdate(SupportedIcebergViewSpecVersion viewSpecVersion) {
    createBranch(TEST_BRANCH, VersionContext.NOT_SPECIFIED);
    createNamespacesIfMissing(TEST_BRANCH, ContentKey.of(viewKey));
    IcebergViewOperationsBuilder icebergViewOperationsBuilder =
        IcebergViewOperationsBuilder.newViewOps()
            .withViewMetadataWarehouseLocation(getWarehouseLocation())
            .withCatalogName(CATALOG_NAME)
            .withFileIO(getFileIO())
            .withSanitizer(s -> s)
            .withUserName(VIEW_USER)
            .withViewSpecVersion(viewSpecVersion)
            .withMetadataLoader(
                (viewSpecVersion == SupportedIcebergViewSpecVersion.V1)
                    ? (BaseIcebergViewTest::viewMetadataLoader)
                    : (BaseIcebergViewTest::viewVersionMetadataLoader))
            .withNessieClient(nessieClient);

    icebergViewOperationsBuilder
        .build()
        .create(viewKey, CREATE_VIEW_SQL, CREATE_VIEW_SCHEMA, VIEW_PATH, getVersion(TEST_BRANCH));

    String createdMetadataLocation = getMetadataLocation(viewKey, getVersion(TEST_BRANCH)).get();
    final IcebergViewMetadata createdViewMetadata =
        IcebergViewOperationsBuilder.newViewOps()
            .withFileIO(getFileIO())
            .build()
            .refreshFromMetadataLocation(createdMetadataLocation);
    assertTrue(createdViewMetadata.getSchema().sameSchema(CREATE_VIEW_SCHEMA));

    icebergViewOperationsBuilder
        .build()
        .update(
            viewKey,
            UPDATED_VIEW_SQL,
            UPDATED_VIEW_SCHEMA,
            VIEW_PATH,
            Collections.emptyMap(),
            getVersion(TEST_BRANCH));
    // Update view
    String updatedMetadataLocation = getMetadataLocation(viewKey, getVersion(TEST_BRANCH)).get();
    final IcebergViewMetadata updatedViewMetadata =
        IcebergViewOperationsBuilder.newViewOps()
            .withFileIO(getFileIO())
            .build()
            .refreshFromMetadataLocation(updatedMetadataLocation);
    assertTrue(updatedViewMetadata.getSchema().sameSchema(UPDATED_VIEW_SCHEMA));
    assertEquals(updatedViewMetadata.getSchemaPath(), VIEW_PATH);
    assertTrue(createdViewMetadata.getLastModifiedAt() < updatedViewMetadata.getLastModifiedAt());
    assertEquals(updatedViewMetadata.getCreatedAt(), createdViewMetadata.getCreatedAt());
    dropBranch(TEST_BRANCH, getVersion(TEST_BRANCH));
  }

  @ParameterizedTest
  @EnumSource(
      value = SupportedIcebergViewSpecVersion.class,
      names = {"UNKNOWN"},
      mode = EnumSource.Mode.EXCLUDE)
  void testDrop(SupportedIcebergViewSpecVersion viewSpecVersion) {
    createBranch(TEST_BRANCH, VersionContext.NOT_SPECIFIED);
    createNamespacesIfMissing(TEST_BRANCH, ContentKey.of(viewKey));
    IcebergViewOperationsBuilder icebergViewOperationsBuilder =
        IcebergViewOperationsBuilder.newViewOps()
            .withViewMetadataWarehouseLocation(getWarehouseLocation())
            .withCatalogName(CATALOG_NAME)
            .withSanitizer(s -> s)
            .withFileIO(getFileIO())
            .withUserName(VIEW_USER)
            .withViewSpecVersion(viewSpecVersion)
            .withMetadataLoader(
                (viewSpecVersion == SupportedIcebergViewSpecVersion.V1)
                    ? (BaseIcebergViewTest::viewMetadataLoader)
                    : (BaseIcebergViewTest::viewVersionMetadataLoader))
            .withNessieClient(nessieClient);

    icebergViewOperationsBuilder
        .build()
        .create(
            viewKey,
            CREATE_VIEW_SQL,
            CREATE_VIEW_SCHEMA,
            Collections.emptyList(),
            getVersion(TEST_BRANCH));
    String createdMetadataLocation = getMetadataLocation(viewKey, getVersion(TEST_BRANCH)).get();
    final IcebergViewMetadata createdViewMetadata =
        IcebergViewOperationsBuilder.newViewOps()
            .withFileIO(getFileIO())
            .build()
            .refreshFromMetadataLocation(createdMetadataLocation);
    assertTrue(createdViewMetadata.getSchema().sameSchema(CREATE_VIEW_SCHEMA));

    IcebergViewOperationsBuilder icebergViewOperationsBuilderForDrop =
        IcebergViewOperationsBuilder.newViewOps()
            .withViewMetadataWarehouseLocation(getWarehouseLocation())
            .withCatalogName(CATALOG_NAME)
            .withFileIO(getFileIO())
            .withUserName(VIEW_USER)
            .withMetadataLoader(BaseIcebergViewTest::viewMetadataLoader)
            .withNessieClient(nessieClient);
    icebergViewOperationsBuilder.build().drop(viewKey, getVersion(TEST_BRANCH));
    Optional<String> droppedMetadataLocation =
        getMetadataLocation(viewKey, getVersion(TEST_BRANCH));
    assertFalse(droppedMetadataLocation.isPresent());
    dropBranch(TEST_BRANCH, getVersion(TEST_BRANCH));
  }

  @ParameterizedTest
  @EnumSource(
      value = SupportedIcebergViewSpecVersion.class,
      names = {"UNKNOWN"},
      mode = EnumSource.Mode.EXCLUDE)
  void testRefresh(SupportedIcebergViewSpecVersion viewSpecVersion) {
    createBranch(TEST_BRANCH, VersionContext.NOT_SPECIFIED);
    createNamespacesIfMissing(TEST_BRANCH, ContentKey.of(viewKey));
    IcebergViewOperationsBuilder icebergViewOperationsBuilder =
        IcebergViewOperationsBuilder.newViewOps()
            .withViewMetadataWarehouseLocation(getWarehouseLocation())
            .withCatalogName(CATALOG_NAME)
            .withFileIO(getFileIO())
            .withSanitizer(s -> s)
            .withUserName(VIEW_USER)
            .withViewSpecVersion(viewSpecVersion)
            .withNessieClient(nessieClient);

    icebergViewOperationsBuilder
        .build()
        .create(
            viewKey,
            CREATE_VIEW_SQL,
            CREATE_VIEW_SCHEMA,
            Collections.emptyList(),
            getVersion(TEST_BRANCH));

    ResolvedVersionContext resolvedVersionContext =
        nessieClient.resolveVersionContext(VersionContext.ofBranch(TEST_BRANCH));

    final IcebergViewMetadata createdViewMetadata =
        IcebergViewOperationsBuilder.newViewOps()
            .withFileIO(getFileIO())
            .withNessieClient(nessieClient)
            .build()
            .refresh(viewKey, resolvedVersionContext);
    assertTrue(createdViewMetadata.getSchema().sameSchema(CREATE_VIEW_SCHEMA));
    assertTrue(createdViewMetadata.getSql().equalsIgnoreCase(CREATE_VIEW_SQL));
    dropBranch(TEST_BRANCH, getVersion(TEST_BRANCH));
  }

  private String getWarehouseLocation() {
    return warehouseLocation;
  }
}
