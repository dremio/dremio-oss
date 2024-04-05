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

import static com.dremio.dac.api.TestCatalogResource.getVDSConfig;
import static com.dremio.exec.planner.physical.PlannerSettings.VDS_AUTO_FIX_THRESHOLD;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.common.util.TestTools;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.sources.PhysicalDatasetPath;
import com.dremio.dac.model.sources.SourceUI;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.service.source.SourceService;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUser;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.MetadataRequestOptions;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.dfs.NASConf;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.file.proto.ParquetFileConfig;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.dremio.service.users.SystemUser;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for view schema learning. View schema learning is checked for every view used in every
 * query. It's really important to not silently trigger schema learning or else queries will have
 * long validation times.
 */
public class TestViewSchemaLearning extends BaseTestServer {
  private static final String CATALOG_PATH = "/catalog/";
  private static SourceConfig sourceConfig;
  private int viewNumber;

  @BeforeClass
  public static void init() throws Exception {
    BaseTestServer.init();
    BaseTestServer.getPopulator().populateTestUsers();

    // setup space
    NamespaceKey key = new NamespaceKey("mySpace");
    SpaceConfig spaceConfig = new SpaceConfig();
    spaceConfig.setName("mySpace");
    newNamespaceService().addOrUpdateSpace(key, spaceConfig);

    // create a NAS source
    SourceUI source = new SourceUI();
    source.setName("source-vsl");
    source.setCtime(System.currentTimeMillis());
    NASConf nasConf = new NASConf();
    nasConf.path = TestTools.getWorkingPath() + "/src/test/resources/viewschemalearn";
    source.setConfig(nasConf);
    sourceConfig =
        p(SourceService.class)
            .get()
            .registerSourceWithRuntime(source.asSourceConfig(), SystemUser.SYSTEM_USERNAME);

    setSystemOption(VDS_AUTO_FIX_THRESHOLD, "0");
  }

  @AfterClass
  public static void shutdown() throws Exception {
    NamespaceKey key = new NamespaceKey("mySpace");
    SpaceConfig space = newNamespaceService().getSpace(key);
    newNamespaceService().deleteSpace(key, space.getTag());

    SourceUI source = new SourceUI();
    source.setName("source-vsl");
    p(SourceService.class).get().deleteSource(sourceConfig);
  }

  private void createPds(String fileName) throws NamespaceException {
    PhysicalDatasetPath datasetPath =
        new PhysicalDatasetPath(asList(sourceConfig.getName(), fileName));
    DatasetConfig dacSample1 = new DatasetConfig();
    dacSample1.setCreatedAt(System.currentTimeMillis());
    dacSample1.setFullPathList(asList(sourceConfig.getName(), fileName));
    dacSample1.setType(DatasetType.PHYSICAL_DATASET_SOURCE_FILE);
    dacSample1.setName(fileName);
    dacSample1.setPhysicalDataset(
        new PhysicalDataset().setFormatSettings(new ParquetFileConfig().asFileConfig()));
    newNamespaceService().addOrUpdateDataset(datasetPath.toNamespaceKey(), dacSample1);
  }

  /**
   * Verifies that updating a Sonar Catalog view doesn't cause the catalog to lose the complex type
   * information.
   *
   * @throws Exception
   */
  @Test
  public void testViewUpdateWithList() throws Exception {
    final List<String> viewPath = Arrays.asList("mySpace", "myVDS");
    Dataset newVDS =
        getVDSConfig(
            viewPath,
            "SELECT REGEXP_SPLIT('REGULAR AIR', 'R', 'LAST', -1) AS R_LESS_SHIPMENT_TYPE");
    Dataset vds =
        expectSuccess(
            getBuilder(getPublicAPI(3).path(CATALOG_PATH)).buildPost(Entity.json(newVDS)),
            new GenericType<Dataset>() {});

    Catalog catalog =
        getSabotContext()
            .getCatalogService()
            .getCatalog(
                MetadataRequestOptions.newBuilder()
                    .setSchemaConfig(
                        SchemaConfig.newBuilder(CatalogUser.from(SystemUser.SYSTEM_USERNAME))
                            .build())
                    .build());
    DremioTable viewTable = catalog.getTable(new NamespaceKey(viewPath));
    assertThat(viewTable.getSchema().getColumn(0).getType().getTypeID())
        .isEqualTo(ArrowType.ArrowTypeID.List);
    String originalViewTag = viewTable.getDatasetConfig().getTag();

    // Update the view and re-verify the view schema
    Dataset updatedVDS =
        new Dataset(
            vds.getId(),
            vds.getType(),
            Arrays.asList("mySpace", "myVDS"),
            null,
            null,
            vds.getTag(),
            null,
            vds.getSql() + " -- SALT", // A little salt to make the SQL different
            null,
            null,
            null);
    vds =
        expectSuccess(
            getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(updatedVDS.getId()))
                .buildPut(Entity.json(updatedVDS)),
            new GenericType<Dataset>() {});

    // Grab a new catalog since we don't want to hit caching catalog cache
    catalog =
        getSabotContext()
            .getCatalogService()
            .getCatalog(
                MetadataRequestOptions.newBuilder()
                    .setSchemaConfig(
                        SchemaConfig.newBuilder(CatalogUser.from(SystemUser.SYSTEM_USERNAME))
                            .build())
                    .build());
    viewTable = catalog.getTable(new NamespaceKey(viewPath));
    assertThat(viewTable.getSchema().getColumn(0).getType().getTypeID())
        .isEqualTo(ArrowType.ArrowTypeID.List);
    assertThat(viewTable.getDatasetConfig().getTag()).isNotEqualTo(originalViewTag);

    verifyNoSchemaLearningHelper(viewTable, ImmutableList.of(ArrowType.ArrowTypeID.List));

    // Clean up
    expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(vds.getId())).buildDelete());
  }

  @Test
  public void testMap() throws Exception {
    createPds("map_list_struct.parquet");
    verifyNoSchemaLearning(
        "select * from \"source-vsl\".\"map_list_struct.parquet\"",
        ImmutableList.of(
            ArrowType.ArrowTypeID.Int, ArrowType.ArrowTypeID.Map, ArrowType.ArrowTypeID.Map));
  }

  @Test
  public void testNonComplexTypes() throws Exception {
    createPds("alldatatypes.parquet");
    verifyNoSchemaLearning(
        "select * from \"source-vsl\".\"alldatatypes.parquet\"",
        ImmutableList.of(
            ArrowType.ArrowTypeID.Utf8,
            ArrowType.ArrowTypeID.Int,
            ArrowType.ArrowTypeID.Int,
            ArrowType.ArrowTypeID.FloatingPoint,
            ArrowType.ArrowTypeID.FloatingPoint,
            ArrowType.ArrowTypeID.FloatingPoint,
            ArrowType.ArrowTypeID.Date,
            ArrowType.ArrowTypeID.Time,
            ArrowType.ArrowTypeID.Timestamp,
            ArrowType.ArrowTypeID.Bool,
            ArrowType.ArrowTypeID.Decimal,
            ArrowType.ArrowTypeID.Decimal,
            ArrowType.ArrowTypeID.Decimal,
            ArrowType.ArrowTypeID.Decimal));
  }

  @Test
  public void testStruct() throws Exception {
    createPds("struct_of_list.parquet");
    verifyNoSchemaLearning(
        "select * from \"source-vsl\".\"struct_of_list.parquet\"",
        ImmutableList.of(ArrowType.ArrowTypeID.Struct));
  }

  @Test
  public void testList() throws Exception {
    createPds("list.parquet");
    verifyNoSchemaLearning(
        "select * from \"source-vsl\".\"list.parquet\"",
        ImmutableList.of(ArrowType.ArrowTypeID.Int, ArrowType.ArrowTypeID.List));
  }

  @Test
  public void testNull() throws Exception {
    verifyNoSchemaLearning("select null as foo;", ImmutableList.of(ArrowType.ArrowTypeID.Int));
  }

  @Test
  public void testInterval() throws Exception {
    verifyNoSchemaLearning(
        "select INTERVAL '3' DAY", ImmutableList.of(ArrowType.ArrowTypeID.Interval));
  }

  @Test
  public void testConvertFromAny() throws Exception {
    verifyNoSchemaLearning(
        "select convert_from('{a:3}', 'JSON') as payload",
        ImmutableList.of(ArrowType.ArrowTypeID.Struct));
  }

  @Test
  public void testAmbiguousCols() throws Exception {
    verifyNoSchemaLearning(
        "select *, 3 as a from ((select 1 as a) x cross join (select 2 as a) y)",
        ImmutableList.of(
            ArrowType.ArrowTypeID.Int, ArrowType.ArrowTypeID.Int, ArrowType.ArrowTypeID.Int));
  }

  private void verifyNoSchemaLearningHelper(
      final DremioTable viewTable, final List<ArrowType.ArrowTypeID> expectedTypes)
      throws Exception {

    // Query the view to see if schema learning happens... it should not
    try {
      submitJobAndWaitUntilCompletion(
          JobRequest.newBuilder()
              .setSqlQuery(
                  new SqlQuery(
                      "select * from " + viewTable.getPath().getSchemaPath(),
                      ImmutableList.of(),
                      DEFAULT_USERNAME))
              .setQueryType(QueryType.UI_INTERNAL_RUN)
              .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
              .build());
    } catch (RuntimeException e) {
      // Ignore since we don't care if job completed or not.
    }

    // Grab a new catalog since we don't want to hit caching catalog cache
    Catalog catalog =
        getSabotContext()
            .getCatalogService()
            .getCatalog(
                MetadataRequestOptions.newBuilder()
                    .setSchemaConfig(
                        SchemaConfig.newBuilder(CatalogUser.from(SystemUser.SYSTEM_USERNAME))
                            .build())
                    .build());

    // Verify Arrow types are the same still
    DremioTable viewTable2 = catalog.getTable(viewTable.getPath());
    for (int i = 0; i < expectedTypes.size(); i++) {
      assertThat(viewTable2.getSchema().getColumn(i).getType().getTypeID())
          .isEqualTo(expectedTypes.get(i));
    }
    // Verify view was not silently updated
    assertThat(viewTable.getDatasetConfig().getTag())
        .isEqualTo(viewTable.getDatasetConfig().getTag());
  }

  private void verifyNoSchemaLearning(
      final String viewSql, final List<ArrowType.ArrowTypeID> expectedTypes) throws Exception {

    // Create view on SQL
    final List<String> viewPath = Arrays.asList("mySpace", "sl_bug" + viewNumber++);
    Dataset newVDS = getVDSConfig(viewPath, viewSql);
    Dataset vds =
        expectSuccess(
            getBuilder(getPublicAPI(3).path(CATALOG_PATH)).buildPost(Entity.json(newVDS)),
            new GenericType<Dataset>() {});

    Catalog catalog =
        getSabotContext()
            .getCatalogService()
            .getCatalog(
                MetadataRequestOptions.newBuilder()
                    .setSchemaConfig(
                        SchemaConfig.newBuilder(CatalogUser.from(SystemUser.SYSTEM_USERNAME))
                            .build())
                    .build());
    // Verify we get the expected Arrow types after executing and saving the view
    DremioTable viewTable = catalog.getTable(new NamespaceKey(viewPath));
    for (int i = 0; i < expectedTypes.size(); i++) {
      assertThat(viewTable.getSchema().getColumn(i).getType().getTypeID())
          .isEqualTo(expectedTypes.get(i));
    }

    verifyNoSchemaLearningHelper(viewTable, expectedTypes);

    // Clean up
    expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(vds.getId())).buildDelete());
  }
}
