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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.dremio.common.util.TestTools;
import com.dremio.common.utils.PathUtils;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.common.Field;
import com.dremio.dac.model.sources.SourceUI;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.service.catalog.CatalogServiceHelper;
import com.dremio.dac.util.DatasetsUtil;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.server.ContextService;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.NASConf;
import com.dremio.options.OptionValue;
import com.dremio.options.OptionValue.OptionType;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.JobSummaryRequest;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.file.FileFormat;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.namespace.file.proto.JsonFileConfig;
import com.dremio.service.namespace.file.proto.TextFileConfig;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.dremio.test.UserExceptionMatcher;
import com.google.common.collect.ImmutableList;

/**
 * Tests for CatalogResource
 */
public class TestCatalogResource extends BaseTestServer {
  private static final String CATALOG_PATH = "/catalog/";
  private static final int SRC_INFORMATION_SCHEMA = 1;
  private static final int SRC_SYS = 2;
  private static final int SRC_EXTERNAL = 3;

  @BeforeClass
  public static void init() throws Exception {
    BaseTestServer.init();

    // setup space
    NamespaceKey key = new NamespaceKey("mySpace");
    SpaceConfig spaceConfig = new SpaceConfig();
    spaceConfig.setName("mySpace");
    newNamespaceService().addOrUpdateSpace(key, spaceConfig);
  }

  @AfterClass
  public static void shutdown() throws Exception {
    // setup space
    NamespaceKey key = new NamespaceKey("mySpace");
    SpaceConfig space = newNamespaceService().getSpace(key);
    newNamespaceService().deleteSpace(key, space.getTag());
  }

  @Test
  public void testListTopLevelCatalog() throws Exception {
    // home space always exists
    int topLevelCount = newSourceService().getSources().size() + newNamespaceService().getSpaces().size() + 1;

    ResponseList<CatalogItem> items = getRootEntities(null);

    assertEquals(items.getData().size(), topLevelCount);

    int homeCount = 0;
    int spaceCount = 0;
    int sourceCount = 0;

    for (CatalogItem item : items.getData()) {
      if (item.getType() == CatalogItem.CatalogItemType.CONTAINER) {
         if (item.getContainerType() == CatalogItem.ContainerSubType.HOME) {
           homeCount++;
         }

        if (item.getContainerType() == CatalogItem.ContainerSubType.SPACE) {
           spaceCount++;
        }


        if (item.getContainerType() == CatalogItem.ContainerSubType.SOURCE) {
          sourceCount++;
        }
      }
    }

    assertEquals(homeCount, 1);
    assertEquals(spaceCount, newNamespaceService().getSpaces().size());
    assertEquals(sourceCount, 1);
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testSpace() throws Exception {
    // create a new space
    Space newSpace = new Space(null, "final frontier", null, null, null);

    Space space = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH)).buildPost(Entity.json(newSpace)), new GenericType<Space>() {});
    SpaceConfig spaceConfig = newNamespaceService().getSpace(new NamespaceKey(newSpace.getName()));

    assertEquals(space.getId(), spaceConfig.getId().getId());
    assertEquals(space.getName(), spaceConfig.getName());
    assertNotEquals(newSpace.getCreatedAt(), spaceConfig.getCtime());

    // make sure that trying to create the space again fails
    expectStatus(Response.Status.CONFLICT, getBuilder(getPublicAPI(3).path(CATALOG_PATH)).buildPost(Entity.json(newSpace)));

    Space newSpace1 = new Space(space.getId(), space.getName(), space.getTag(), null, null);
    expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(space.getId())).buildPut(Entity.json(newSpace1)));
    Space space1 = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(space.getId())).buildGet(), new GenericType<Space>() {});
    assertEquals(spaceConfig.getCtime(), space1.getCreatedAt());

    // delete the space
    expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(spaceConfig.getId().getId())).buildDelete());
    thrown.expect(NamespaceException.class);
    newNamespaceService().getSpace(new NamespaceKey(spaceConfig.getName()));
  }

  @Test
  public void testFoldersInSpace() throws Exception {
    // create a new space
    Space newSpace = new Space(null, "final frontier", null, null, null);
    Space space = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH)).buildPost(Entity.json(newSpace)), new GenericType<Space>() {});

    // no children at this point
    assertNull(space.getChildren());

    // add a folder
    Folder newFolder = getFolderConfig(Arrays.asList(space.getName(), "myFolder"));
    Folder folder = createFolder(newFolder);
    assertEquals(newFolder.getPath(), folder.getPath());

    // make sure folder shows up under space
    space = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(space.getId())).buildGet(), new GenericType<Space>() {});

    // make sure that trying to create the folder again fails
    expectStatus(Response.Status.CONFLICT, getBuilder(getPublicAPI(3).path(CATALOG_PATH)).buildPost(Entity.json(newFolder)));

    // one child at this point
    assertEquals(space.getChildren().size(), 1);
    assertEquals(space.getChildren().get(0).getId(), folder.getId());

    // delete the folder
    expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(folder.getId())).buildDelete());
    space = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(space.getId())).buildGet(), new GenericType<Space>() {});
    assertEquals(space.getChildren().size(), 0);

    newNamespaceService().deleteSpace(new NamespaceKey(space.getName()), space.getTag());
  }

  @Test
  public void testDatasetCount() throws Exception {
    Space space = createSpace("dataset count test");

    createVDS(Arrays.asList(space.getName(), "vds1"), "select * from sys.version");
    createVDS(Arrays.asList(space.getName(), "vds2"), "select * from sys.version");

    ResponseList<CatalogItem> items = getRootEntities(null);

    for (CatalogItem item : items.getData()) {
      assertNull("CatalogItemStats should be empty if datasetCount parameter is not provided", item.getStats());
    }

    checkSpaceDatasetCount(space.getId(), 2);

    Folder folder = createFolder(Arrays.asList(space.getName(), "test folder"));
    List<String> vdsPath = new ArrayList<>(folder.getPath());
    vdsPath.add("vds1");

    createVDS(vdsPath, "select * from sys.version");

    checkSpaceDatasetCount(space.getId(), 3);

    newNamespaceService().deleteSpace(new NamespaceKey(space.getName()), space.getTag());
  }

  private void checkSpaceDatasetCount(String spaceId, int expectedDatasetCount) {
    ResponseList<CatalogItem> items = getRootEntities(Arrays.asList(CatalogServiceHelper.DetailType.datasetCount));

    Optional<CatalogItem> space = items.getData().stream()
      .filter(item -> item.getId().equals(spaceId))
      .findFirst();

    assertTrue("created space must be returned", space.isPresent());
    CatalogItemStats stats = space.get().getStats();
    assertNotNull(stats);
    assertEquals(expectedDatasetCount, stats.getDatasetCount());
  }

  private ResponseList<CatalogItem> getRootEntities(final List<CatalogServiceHelper.DetailType> detailsToInclude) {
    WebTarget api = getPublicAPI(3).path(CATALOG_PATH);

    if (detailsToInclude != null) {
      for (CatalogServiceHelper.DetailType detail : detailsToInclude) {
        api = api.queryParam("include", detail.name());
      }
    }

    return expectSuccess(getBuilder(api).buildGet(), new GenericType<ResponseList<CatalogItem>>() {});
  }

  private Space createSpace(final String spaceName) {
    Space newSpace = new Space(null, spaceName, null, null, null);
    return expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH)).buildPost(Entity.json(newSpace)), new GenericType<Space>() {});
  }

  @Test
  public void testVDSInSpace() throws Exception {
    // create a new space
    Space space = createSpace("final frontier");

    // add a folder
    Folder newFolder = new Folder(null, Arrays.asList(space.getName(), "myFolder"), null, null);
    Folder folder = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH)).buildPost(Entity.json(newFolder)), new GenericType<Folder>() {});

    // create a VDS in the space
    Dataset newVDS = getVDSConfig(Arrays.asList(space.getName(), "myFolder", "myVDS"),"select * from sys.version");
    Dataset vds = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH)).buildPost(Entity.json(newVDS)), new GenericType<Dataset>() {});

    // make sure that trying to create the vds again fails
    expectStatus(Response.Status.CONFLICT, getBuilder(getPublicAPI(3).path(CATALOG_PATH)).buildPost(Entity.json(newVDS)));

    // folder should now have children
    folder = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(folder.getId())).buildGet(), new GenericType<Folder>() {});
    assertEquals(folder.getChildren().size(), 1);
    assertEquals(folder.getChildren().get(0).getId(), vds.getId());

    // test rename of a vds
    Dataset renamedVDS = new Dataset(
      vds.getId(),
      vds.getType(),
      Arrays.asList(space.getName(), "myFolder", "myVDSRenamed"),
      null,
      null,
      vds.getTag(),
      null,
      vds.getSql(),
      null,
      null,
      null
    );
    vds = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(renamedVDS.getId())).buildPut(Entity.json(renamedVDS)), new GenericType<Dataset>() {});

    folder = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(folder.getId())).buildGet(), new GenericType<Folder>() {});
    assertEquals(1, folder.getChildren().size());
    assertEquals(vds.getId(), folder.getChildren().get(0).getId());
    assertEquals("myVDSRenamed", folder.getChildren().get(0).getPath().get(2));

    // test changing sql
    Dataset modifiedVDS = new Dataset(
      vds.getId(),
      vds.getType(),
      vds.getPath(),
      null,
      null,
      vds.getTag(),
      null,
      "select version from sys.version",
      null,
      null,
      null
    );
    vds = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(modifiedVDS.getId())).buildPut(Entity.json(modifiedVDS)), new GenericType<Dataset>() {});
    assertEquals(modifiedVDS.getSql(), vds.getSql());
    assertNotEquals(modifiedVDS.getTag(), vds.getTag()); // make sure it stores a new version

    // we currently doesn't allow deserializing of fields so manually check them
    List<Field> fieldsFromDatasetConfig = DatasetsUtil.getFieldsFromDatasetConfig(newNamespaceService().findDatasetByUUID(vds.getId()));
    assertEquals(1, fieldsFromDatasetConfig.size());
    assertEquals("version", fieldsFromDatasetConfig.get(0).getName());

    // delete the vds
    expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(vds.getId())).buildDelete());
    folder = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(folder.getId())).buildGet(), new GenericType<Folder>() {});
    assertEquals(0, folder.getChildren().size());

    newNamespaceService().deleteSpace(new NamespaceKey(space.getName()), space.getTag());
  }

  @Test
  public void testVDSInSpaceWithSameName() throws Exception {
    final String sourceName = "src_" + System.currentTimeMillis();

    SourceUI source = new SourceUI();
    source.setName(sourceName);
    source.setCtime(1000L);

    TemporaryFolder folder = new TemporaryFolder();
    folder.create();

    final NASConf config = new NASConf();
    config.path = folder.getRoot().getAbsolutePath();
    source.setConfig(config);

    java.io.File srcFolder = folder.getRoot();

    PrintStream file = new PrintStream(new java.io.File(srcFolder.getAbsolutePath(), "myFile.json"));
    for (int i = 0; i < 10; i++) {
      file.println("{a:{b:[1,2]}}");
    }
    file.close();

    newSourceService().registerSourceWithRuntime(source);

    final DatasetPath path1 = new DatasetPath(ImmutableList.of(sourceName, "myFile.json"));
    final DatasetConfig dataset1 = new DatasetConfig()
      .setType(DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER)
      .setFullPathList(path1.toPathList())
      .setName(path1.getLeaf().getName())
      .setCreatedAt(System.currentTimeMillis())
      .setTag(null)
      .setOwner(DEFAULT_USERNAME)
      .setPhysicalDataset(new PhysicalDataset()
        .setFormatSettings(new FileConfig().setType(FileType.JSON)));
    p(NamespaceService.class).get().addOrUpdateDataset(path1.toNamespaceKey(), dataset1);

    DatasetPath vdsPath = new DatasetPath(ImmutableList.of("@dremio", "myFile.json"));
    createDatasetFromSQLAndSave(vdsPath, "SELECT * FROM \"myFile.json\"", asList(sourceName));

    final String query = "select * from \"myFile.json\"";
    submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery(query, ImmutableList.of("@dremio"), DEFAULT_USERNAME))
        .setQueryType(QueryType.UI_INTERNAL_RUN)
        .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
        .build());
  }

  @Test
  public void testCrossSourceSelectVDSDefault() throws Exception {

    final String sourceName1 = "src_" + System.currentTimeMillis();
    final String vdsName1 = sourceName1 + "VDS";
    Source newSource1 = createDatasetFromSource(sourceName1, "myFile.json", SRC_EXTERNAL, vdsName1);
    final String sourceName2 = "src_" + System.currentTimeMillis();
    final String vdsName2 = sourceName2 + "VDS";
    Source newSource2 = createDatasetFromSource(sourceName2, "myFile2.json", SRC_EXTERNAL, vdsName2);

    l(ContextService.class).get().getOptionManager().setOption(OptionValue.createBoolean(OptionType.SYSTEM, "planner.cross_source_select.disable", false));

    final String query = String.format("select * from \"@dremio\".\"%s\" as d join \"@dremio\".\"%s\" as e on d.name = e.name ", vdsName1, vdsName2);

    submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery(query, ImmutableList.of("@dremio"), DEFAULT_USERNAME))
        .setQueryType(QueryType.UI_INTERNAL_RUN)
        .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
        .build());
  }

  @Test
  public void testCrossSourceSelectVDSOptionEnabled() throws Exception {

    final String sourceName1 = "src_" + System.currentTimeMillis();
    final String vdsName1 = sourceName1 + "VDS";
    Source newSource1 = createDatasetFromSource(sourceName1, "myFile.json", SRC_EXTERNAL, vdsName1);
    final String sourceName2 = "src_" + System.currentTimeMillis();
    final String vdsName2 = sourceName2 + "VDS";
    Source newSource2 = createDatasetFromSource(sourceName2, "myFile2.json", SRC_EXTERNAL, vdsName2);

    l(ContextService.class).get().getOptionManager().setOption(OptionValue.createBoolean(OptionType.SYSTEM, "planner.cross_source_select.disable", true));

    final String query = String.format("select * from \"@dremio\".\"%s\" as d join \"@dremio\".\"%s\" as e on d.name = e.name ", vdsName1, vdsName2);

    final String msg = String.format("Cross select is disabled between sources '%s', '%s'.", sourceName1, sourceName2);
    thrown.expect(new UserExceptionMatcher(UserBitShared.DremioPBError.ErrorType.VALIDATION, msg));

    submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery(query, ImmutableList.of("@dremio"), DEFAULT_USERNAME))
        .setQueryType(QueryType.UI_INTERNAL_RUN)
        .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
        .build());
  }

  @Test
  public void testCrossSourceSelectVDSAllowSource() throws Exception {

    final String sourceName1 = "src_" + System.currentTimeMillis();
    final String vdsName1 = sourceName1 + "VDS";
    Source newSource1 = createDatasetFromSource(sourceName1, "myFile.json", SRC_EXTERNAL, vdsName1);
    final String sourceName2 = "src_" + System.currentTimeMillis();
    final String vdsName2 = sourceName2 + "VDS";
    Source newSource2 = createDatasetFromSource(sourceName2, "myFile2.json", SRC_EXTERNAL, vdsName2);

    newSource2.setAllowCrossSourceSelection(true);
    newSource2 = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(newSource2.getId())).buildPut(Entity.json(newSource2)), new GenericType<Source>() {});

    l(ContextService.class).get().getOptionManager().setOption(OptionValue.createBoolean(OptionType.SYSTEM, "planner.cross_source_select.disable", true));

    final String query = String.format("select * from \"@dremio\".\"%s\" as d join \"@dremio\".\"%s\" as e on d.name = e.name ", vdsName1, vdsName2);

    submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery(query, ImmutableList.of("@dremio"), DEFAULT_USERNAME))
        .setQueryType(QueryType.UI_INTERNAL_RUN)
        .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
        .build());

  }

  @Test
  public void testCrossSourceSelectMixVDS() throws Exception {

    final String sourceName1 = "src_" + System.currentTimeMillis();
    final String vdsName1 = sourceName1 + "VDS";
    Source newSource1 = createDatasetFromSource(sourceName1, "myFile.json", SRC_EXTERNAL, vdsName1);
    final String sourceName2 = "src_" + System.currentTimeMillis();
    final String vdsName2 = sourceName2 + "VDS";
    Source newSource2 = createDatasetFromSource(sourceName2, "myFile2.json", SRC_EXTERNAL, vdsName2);
    final String sourceName3 = "src_" + System.currentTimeMillis();
    final String vdsName3 = sourceName3 + "VDS";
    Source newSource3 = createDatasetFromSource(sourceName3, "myFile3.json", SRC_EXTERNAL, vdsName3);

    newSource2.setAllowCrossSourceSelection(true);
    newSource2 = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(newSource2.getId())).buildPut(Entity.json(newSource2)), new GenericType<Source>() {});

    l(ContextService.class).get().getOptionManager().setOption(OptionValue.createBoolean(OptionType.SYSTEM, "planner.cross_source_select.disable", true));

    final String query = String.format("select * from \"@dremio\".\"%s\" as d join \"@dremio\".\"%s\" as e on d.name = e.name ", vdsName1, vdsName2);
    final String query2 = query + String.format("join \"@dremio\".\"%s\" as f on d.name = f.name", vdsName3);

    final String msg = String.format("Cross select is disabled between sources '%s', '%s'.", sourceName1, sourceName3);
    thrown.expect(new UserExceptionMatcher(UserBitShared.DremioPBError.ErrorType.VALIDATION, msg));

    submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery(query2, ImmutableList.of("@dremio"), DEFAULT_USERNAME))
        .setQueryType(QueryType.UI_INTERNAL_RUN)
        .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
        .build());

  }

  @Test
  public void testCrossSourceSelectDefault() throws Exception {

    final String sourceName1 = "src_" + System.currentTimeMillis();
    Source newSource1 = createDatasetFromSource(sourceName1, "myFile.json", SRC_EXTERNAL, null);
    final String sourceName2 = "src_" + System.currentTimeMillis();
    Source newSource2 = createDatasetFromSource(sourceName2, "myFile2.json", SRC_EXTERNAL, null);

    l(ContextService.class).get().getOptionManager().setOption(OptionValue.createBoolean(OptionType.SYSTEM, "planner.cross_source_select.disable", false));

    final String query = String.format("select * from %s.\"myFile.json\" as d join %s.\"myFile2.json\" as e on d.name = e.name", sourceName1, sourceName2);

    submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery(query, ImmutableList.of("@dremio"), DEFAULT_USERNAME))
        .setQueryType(QueryType.UI_INTERNAL_RUN)
        .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
        .build());
  }

  @Test
  public void testCrossSourceSelectOptionEnabled() throws Exception {

    final String sourceName1 = "src_" + System.currentTimeMillis();
    Source newSource1 = createDatasetFromSource(sourceName1, "myFile.json", SRC_EXTERNAL, null);
    final String sourceName2 = "src_" + System.currentTimeMillis();
    Source newSource2 = createDatasetFromSource(sourceName2, "myFile2.json", SRC_EXTERNAL, null);

    l(ContextService.class).get().getOptionManager().setOption(OptionValue.createBoolean(OptionType.SYSTEM, "planner.cross_source_select.disable", true));

    final String query = String.format("select * from %s.\"myFile.json\" as d join %s.\"myFile2.json\" as e on d.name = e.name", sourceName1, sourceName2);

    final String msg = String.format("Cross select is disabled between sources '%s', '%s'.", sourceName1, sourceName2);
    thrown.expect(new UserExceptionMatcher(UserBitShared.DremioPBError.ErrorType.VALIDATION, msg));

    submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery(query, ImmutableList.of("@dremio"), DEFAULT_USERNAME))
        .setQueryType(QueryType.UI_INTERNAL_RUN)
        .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
        .build());
  }

  @Test
  public void testCrossSourceSelectAllowSource() throws Exception {

    final String sourceName1 = "src_" + System.currentTimeMillis();
    Source newSource1 = createDatasetFromSource(sourceName1, "myFile.json", SRC_EXTERNAL, null);
    final String sourceName2 = "src_" + System.currentTimeMillis();
    Source newSource2 = createDatasetFromSource(sourceName2, "myFile2.json", SRC_EXTERNAL, null);

    newSource2.setAllowCrossSourceSelection(true);
    newSource2 = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(newSource2.getId())).buildPut(Entity.json(newSource2)), new GenericType<Source>() {});

    l(ContextService.class).get().getOptionManager().setOption(OptionValue.createBoolean(OptionType.SYSTEM, "planner.cross_source_select.disable", true));

    final String query = String.format("select * from %s.\"myFile.json\" as d join %s.\"myFile2.json\" as e on d.name = e.name", sourceName1, sourceName2);

    submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery(query, ImmutableList.of("@dremio"), DEFAULT_USERNAME))
        .setQueryType(QueryType.UI_INTERNAL_RUN)
        .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
        .build());

  }

  @Test
  public void testCrossSourceSelectMixed() throws Exception {

    final String sourceName1 = "src_" + System.currentTimeMillis();
    Source newSource1 = createDatasetFromSource(sourceName1, "myFile.json", SRC_EXTERNAL, null);
    final String sourceName2 = "src_" + System.currentTimeMillis();
    Source newSource2 = createDatasetFromSource(sourceName2, "myFile2.json", SRC_EXTERNAL, null);
    final String sourceName3 = "src_" + System.currentTimeMillis();
    Source newSource3 = createDatasetFromSource(sourceName3, "myFile3.json", SRC_EXTERNAL, null);

    newSource2.setAllowCrossSourceSelection(true);
    newSource2 = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(newSource2.getId())).buildPut(Entity.json(newSource2)), new GenericType<Source>() {});

    l(ContextService.class).get().getOptionManager().setOption(OptionValue.createBoolean(OptionType.SYSTEM, "planner.cross_source_select.disable", true));

    final String query = String.format("select * from %s.\"myFile.json\" as d join %s.\"myFile2.json\" as e on d.name = e.name ", sourceName1, sourceName2);
    final String query2 = query + String.format("join %s.\"myFile3.json\" as f on d.name = f.name", sourceName3);

    final String msg = String.format("Cross select is disabled between sources '%s', '%s'.", sourceName1, sourceName3);
    thrown.expect(new UserExceptionMatcher(UserBitShared.DremioPBError.ErrorType.VALIDATION, msg));

    submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery(query2, ImmutableList.of("@dremio"), DEFAULT_USERNAME))
        .setQueryType(QueryType.UI_INTERNAL_RUN)
        .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
        .build());

  }

  @Test
  public void testCrossSourceSelectInformationSchema() throws Exception {

    final String sourceName = "src_" + System.currentTimeMillis();
    final Source newSource = createDatasetFromSource(sourceName, "myFile.json", SRC_INFORMATION_SCHEMA, null);
    l(ContextService.class).get().getOptionManager().setOption(OptionValue.createBoolean(OptionType.SYSTEM, "planner.cross_source_select.disable", true));

    final String query = String.format("select * from %s.\"myFile.json\" as d join INFORMATION_SCHEMA.catalogs as e on d.catalog = e.catalog_name", sourceName);

    final JobId jobId = submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery(query, ImmutableList.of("@dremio"), DEFAULT_USERNAME))
        .setQueryType(QueryType.UI_INTERNAL_RUN)
        .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
        .build());

    final JobSummary job = l(JobsService.class).getJobSummary(JobSummaryRequest.newBuilder().setJobId(JobsProtoUtil.toBuf(jobId)).build());
    assertTrue(JobsProtoUtil.toStuff(job.getJobState()) == JobState.COMPLETED);
  }

  @Test
  public void testCrossSourceSelectSys() throws Exception {

    final String sourceName = "src_" + System.currentTimeMillis();
    final Source newSource = createDatasetFromSource(sourceName, "myFile.json", SRC_SYS, null);
    l(ContextService.class).get().getOptionManager().setOption(OptionValue.createBoolean(OptionType.SYSTEM, "planner.cross_source_select.disable", true));

    final String query = String.format("select * from %s.\"myFile.json\" as d join sys.options as e on d.type = e.type", sourceName);

    final JobId jobId = submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery(query, ImmutableList.of("@dremio"), DEFAULT_USERNAME))
        .setQueryType(QueryType.UI_INTERNAL_RUN)
        .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
        .build());
    final JobSummary job = l(JobsService.class).getJobSummary(JobSummaryRequest.newBuilder().setJobId(JobsProtoUtil.toBuf(jobId)).build());
    assertTrue(JobsProtoUtil.toStuff(job.getJobState()) == JobState.COMPLETED);
  }

  private Source createDatasetFromSource(String sourceName, String fileName, int sourceType, String vdsName) throws Exception {
    TemporaryFolder folder = new TemporaryFolder();
    folder.create();

    final NASConf config = new NASConf();
    config.path = folder.getRoot().getAbsolutePath();

    java.io.File srcFolder = folder.getRoot();
    try (PrintStream file = new PrintStream(new java.io.File(srcFolder.getAbsolutePath(), fileName))) {
      if (sourceType == SRC_INFORMATION_SCHEMA) {
        file.println("{\"catalog\":\"DREMIO\",\"name\":\"fred ovid\",\"age\":76,\"gpa\":1.55,\"studentnum\":692315658449}");
      } else if (sourceType == SRC_SYS) {
        file.println("{\"type\":\"SYSTEM\",\"name\":\"fred ovid\",\"age\":76,\"gpa\":1.55,\"studentnum\":692315658449}");
      } else {
        file.println("{\"rownum\":1,\"name\":\"fred ovid\",\"age\":76,\"gpa\":1.55,\"studentnum\":692315658449,\"create_time\":\"2014-05-27 00:26:07\", \"interests\": [ \"Reading\", \"Mountain Biking\", \"Hacking\" ], \"favorites\": {\"color\": \"Blue\", \"sport\": \"Soccer\", \"food\": \"Spaghetti\"}}");
      }
    }

    Source newSource = new Source();
    newSource.setName(sourceName);
    newSource.setType("NAS");
    newSource.setConfig(config);
    newSource.setCreatedAt(1000L);
    newSource.setAllowCrossSourceSelection(false);

    Source source = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH)).buildPost(Entity.json(newSource)),  new GenericType<Source>() {});
    newSource = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(source.getId())).buildGet(), new GenericType<Source>() {});

    final DatasetPath path = new DatasetPath(ImmutableList.of(sourceName, fileName));
    final DatasetConfig dataset = new DatasetConfig()
      .setId(new EntityId().setId(UUID.randomUUID().toString()))
      .setType(DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER)
      .setFullPathList(path.toPathList())
      .setName(path.getLeaf().getName())
      .setCreatedAt(System.currentTimeMillis())
      .setTag(null)
      .setOwner(DEFAULT_USERNAME)
      .setPhysicalDataset(new PhysicalDataset()
        .setFormatSettings(new FileConfig().setType(FileType.JSON)));
    p(NamespaceService.class).get().addOrUpdateDataset(path.toNamespaceKey(), dataset);

    if (vdsName != null) {
      DatasetPath vdsPath = new DatasetPath(ImmutableList.of("@dremio", vdsName));
      final String query = String.format("SELECT * FROM %s.\"%s\"", sourceName, fileName);
      createDatasetFromSQLAndSave(vdsPath, query, asList(sourceName));
    }

    return newSource;
  }

  @Test
  public void testMetadataTooLarge() throws Exception {

    final String sourceName1 = "src_" + System.currentTimeMillis();

    TemporaryFolder folder = new TemporaryFolder();
    folder.create();

    final NASConf config = new NASConf();
    config.path = folder.getRoot().getAbsolutePath();

    java.io.File srcFolder = folder.getRoot();
    try (PrintStream file = new PrintStream(new java.io.File(srcFolder.getAbsolutePath(), "zmyFile1.json"))) {
      file.println("{\"rownum\":1,\"name\":\"fred ovid\",\"age\":76,\"gpa\":1.55,\"studentnum\":692315658449,\"create_time\":\"2014-05-27 00:26:07\", \"interests\": [ \"Reading\", \"Mountain Biking\", \"Hacking\" ], \"favorites\": {\"color\": \"Blue\", \"sport\": \"Soccer\", \"food\": \"Spaghetti\"}}");
    }

    String nestedDir = srcFolder + java.io.File.separator + "nestedDir";
    java.io.File dir = new java.io.File(nestedDir);
    dir.mkdirs();

    try (PrintStream file = new PrintStream(new java.io.File(dir.getAbsolutePath(), "nestedFile1.json"))) {
      file.println("{\"rownum\":1,\"name\":\"fred ovid\",\"age\":76,\"gpa\":1.55,\"studentnum\":692315658449,\"create_time\":\"2014-05-27 00:26:07\", \"interests\": [ \"Reading\", \"Mountain Biking\", \"Hacking\" ], \"favorites\": {\"color\": \"Blue\", \"sport\": \"Soccer\", \"food\": \"Spaghetti\"}}");
    }
    try (PrintStream file = new PrintStream(new java.io.File(dir.getAbsolutePath(), "nestedFile2.json"))) {
      file.println("{\"rownum\":1,\"name\":\"fred ovid\",\"age\":76,\"gpa\":1.55,\"studentnum\":692315658449,\"create_time\":\"2014-05-27 00:26:07\", \"interests\": [ \"Reading\", \"Mountain Biking\", \"Hacking\" ], \"favorites\": {\"color\": \"Blue\", \"sport\": \"Soccer\", \"food\": \"Spaghetti\"}}");
    }

    Source newSource = new Source();
    newSource.setName(sourceName1);
    newSource.setType("NAS");
    newSource.setConfig(config);
    newSource.setCreatedAt(1000L);

    Source source = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH)).buildPost(Entity.json(newSource)), new GenericType<Source>() {});
    newSource = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(source.getId())).buildGet(), new GenericType<Source>() {});

    DatasetPath path = new DatasetPath(ImmutableList.of(sourceName1, "nestedDir"));
    DatasetConfig dataset = new DatasetConfig()
      .setType(DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER)
      .setFullPathList(path.toPathList())
      .setName(path.getLeaf().getName())
      .setCreatedAt(System.currentTimeMillis())
      .setTag(null)
      .setOwner(DEFAULT_USERNAME)
      .setPhysicalDataset(new PhysicalDataset()
        .setFormatSettings(new FileConfig().setType(FileType.JSON)));
    p(NamespaceService.class).get().addOrUpdateDataset(path.toNamespaceKey(), dataset);

    path = new DatasetPath(ImmutableList.of(sourceName1, "zmyFile1.json"));
    dataset = new DatasetConfig()
      .setType(DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER)
      .setFullPathList(path.toPathList())
      .setName(path.getLeaf().getName())
      .setCreatedAt(System.currentTimeMillis())
      .setTag(null)
      .setOwner(DEFAULT_USERNAME)
      .setPhysicalDataset(new PhysicalDataset()
        .setFormatSettings(new FileConfig().setType(FileType.JSON)));
    p(NamespaceService.class).get().addOrUpdateDataset(path.toNamespaceKey(), dataset);

    NamespaceKey datasetKey = new DatasetPath(ImmutableList.of(sourceName1, "zmyFile1.json")).toNamespaceKey();
    DatasetConfig dataset1 = p(NamespaceService.class).get().getDataset(datasetKey);

    l(ContextService.class).get().getOptionManager().setOption(OptionValue.createLong(OptionType.SYSTEM, "dremio.store.dfs.max_files", 1));

    java.io.File deleted = new java.io.File(srcFolder.getAbsolutePath(), "zmyFile1.json");
    boolean bool = deleted.delete();

    // After delete "zmyFile1.json" from the source, if the refresh succeeds, it will delete the dataset from the kv store.
    // If we query "nestedDir", it will be still in the kv store. If we query "zmyFile1.json", it will throw namespace exception.
    ((CatalogServiceImpl) l(ContextService.class).get().getCatalogService()).refreshSource(new NamespaceKey(sourceName1), CatalogService.REFRESH_EVERYTHING_NOW, CatalogServiceImpl.UpdateType.FULL);
    dataset1 = p(NamespaceService.class).get().getDataset(new DatasetPath(ImmutableList.of(sourceName1, "nestedDir")).toNamespaceKey());
    assertEquals("nestedDir", dataset1.getName());
    thrown.expect(NamespaceException.class);
    p(NamespaceService.class).get().getDataset(datasetKey);

  }

  private boolean isComplete(DatasetConfig config) {
    return config != null
      && DatasetHelper.getSchemaBytes(config) != null
      && config.getReadDefinition() != null;
  }

  @Test
  public void testForgetTableDefault() throws Exception {

    final String sourceName = "src_" + System.currentTimeMillis();
    final Source newSource = createDatasetFromSource(sourceName, "myFile.json", SRC_EXTERNAL, null);

    // Do an inline refresh to build complete DatasetConfig before forget metadata
    final String queryForInlineRefresh = String.format("select * from %s.\"myFile.json\"", sourceName);
    JobId jobId = submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery(queryForInlineRefresh, ImmutableList.of("@dremio"), DEFAULT_USERNAME))
        .setQueryType(QueryType.UI_INTERNAL_RUN)
        .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
        .build());
    JobSummary job = l(JobsService.class).getJobSummary(JobSummaryRequest.newBuilder().setJobId(JobsProtoUtil.toBuf(jobId)).build());
    assertTrue(JobsProtoUtil.toStuff(job.getJobState()) == JobState.COMPLETED);

    DatasetConfig dataset1 = p(NamespaceService.class).get().getDataset(new DatasetPath(ImmutableList.of(sourceName, "myFile.json")).toNamespaceKey());
    assertEquals("myFile.json", dataset1.getName());
    assertTrue(isComplete(dataset1));

    // Forget metadata
    final String query = String.format("alter table %s.\"myFile.json\" forget metadata", sourceName);
    jobId = submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery(query, ImmutableList.of("@dremio"), DEFAULT_USERNAME))
        .setQueryType(QueryType.UI_INTERNAL_RUN)
        .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
        .build());

    // Expect to delete the metadata record in kv store successfully
    job = l(JobsService.class).getJobSummary(JobSummaryRequest.newBuilder().setJobId(JobsProtoUtil.toBuf(jobId)).build());
    assertTrue(JobsProtoUtil.toStuff(job.getJobState()) == JobState.COMPLETED);

    // Expect to receive an NamespaceException if try to find the deleted metadata in kv store
    thrown.expect(NamespaceException.class);
    p(NamespaceService.class).get().getDataset(new DatasetPath(ImmutableList.of(sourceName, "myFile.json")).toNamespaceKey());

  }

  @Test
  public void testForgetTableMultiple() throws Exception {

    final String sourceName = "src_" + System.currentTimeMillis();
    final Source newSource = createDatasetFromSource(sourceName, "myFile.json", SRC_EXTERNAL, null);

    // Do a full refresh to build complete DatasetConfig before forget metadata
    ((CatalogServiceImpl) l(ContextService.class).get().getCatalogService()).refreshSource(new NamespaceKey(sourceName), CatalogService.REFRESH_EVERYTHING_NOW, CatalogServiceImpl.UpdateType.FULL);
    DatasetConfig dataset1 = p(NamespaceService.class).get().getDataset(new DatasetPath(ImmutableList.of(sourceName, "myFile.json")).toNamespaceKey());
    assertEquals("myFile.json", dataset1.getName());
    assertTrue(isComplete(dataset1));

    // Forget metadata
    final String query = String.format("alter table %s.\"myFile.json\" forget metadata", sourceName);
    JobId jobId = submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery(query, ImmutableList.of("@dremio"), DEFAULT_USERNAME))
        .setQueryType(QueryType.UI_INTERNAL_RUN)
        .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
        .build());

    // Expect to delete the metadata record in kv store successfully
    final JobSummary job = l(JobsService.class).getJobSummary(JobSummaryRequest.newBuilder().setJobId(JobsProtoUtil.toBuf(jobId)).build());
    assertTrue(JobsProtoUtil.toStuff(job.getJobState()) == JobState.COMPLETED);

    // Expect to receive an UserException if try to forget metadata on the deleted metadata in kv store
    final String msg = String.format("PARSE ERROR: Unable to find table %s.\"myFile.json\"", sourceName);
    thrown.expect(new UserExceptionMatcher(UserBitShared.DremioPBError.ErrorType.PARSE, msg));

    // Forget metadata again
    jobId = submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery(query, ImmutableList.of("@dremio"), DEFAULT_USERNAME))
        .setQueryType(QueryType.UI_INTERNAL_RUN)
        .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
        .build());

  }

  @Test
  public void testForgetTableWithNameRefresh() throws Exception {

    final String sourceName = "src_" + System.currentTimeMillis();
    final Source newSource = createDatasetFromSource(sourceName, "myFile.json", SRC_EXTERNAL, null);

    // Do an inline refresh to build complete DatasetConfig before forget metadata
    final String queryForInlineRefresh = String.format("select * from %s.\"myFile.json\"", sourceName);
    JobId jobId = submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery(queryForInlineRefresh, ImmutableList.of("@dremio"), DEFAULT_USERNAME))
        .setQueryType(QueryType.UI_INTERNAL_RUN)
        .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
        .build());
    JobSummary job = l(JobsService.class).getJobSummary(JobSummaryRequest.newBuilder().setJobId(JobsProtoUtil.toBuf(jobId)).build());
    assertTrue(JobsProtoUtil.toStuff(job.getJobState()) == JobState.COMPLETED);

    DatasetConfig dataset1 = p(NamespaceService.class).get().getDataset(new DatasetPath(ImmutableList.of(sourceName, "myFile.json")).toNamespaceKey());
    assertEquals("myFile.json", dataset1.getName());
    assertTrue(isComplete(dataset1));

    // Forget metadata
    String query = String.format("alter table %s.\"myFile.json\" forget metadata", sourceName);
    jobId = submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery(query, ImmutableList.of("@dremio"), DEFAULT_USERNAME))
        .setQueryType(QueryType.UI_INTERNAL_RUN)
        .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
        .build());

    // Expect to delete the metadata record in kv store successfully
    job = l(JobsService.class).getJobSummary(JobSummaryRequest.newBuilder().setJobId(JobsProtoUtil.toBuf(jobId)).build());
    assertTrue(JobsProtoUtil.toStuff(job.getJobState()) == JobState.COMPLETED);

    // Emulate NameRefresh to put a shallow metadata into kv store
    DatasetPath path = new DatasetPath(ImmutableList.of(sourceName, "myFile.json"));
    DatasetConfig dataset = new DatasetConfig()
      .setId(new EntityId().setId(UUID.randomUUID().toString()))
      .setType(DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER)
      .setFullPathList(path.toPathList())
      .setName(path.getLeaf().getName())
      .setCreatedAt(System.currentTimeMillis())
      .setTag(null);
    p(NamespaceService.class).get().addOrUpdateDataset(path.toNamespaceKey(), dataset);

    // Expect to find the metadata record when query the kv store
    dataset1 = p(NamespaceService.class).get().getDataset(new DatasetPath(ImmutableList.of(sourceName, "myFile.json")).toNamespaceKey());
    assertEquals("myFile.json", dataset1.getName());
    // Assert shallow dataset config is incomplete
    assertFalse(isComplete(dataset1));

    // Forget metadata again
    jobId = submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery(query, ImmutableList.of("@dremio"), DEFAULT_USERNAME))
        .setQueryType(QueryType.UI_INTERNAL_RUN)
        .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
        .build());

    // Expect to delete the metadata record in kv store successfully
    job = l(JobsService.class).getJobSummary(JobSummaryRequest.newBuilder().setJobId(JobsProtoUtil.toBuf(jobId)).build());
    assertTrue(JobsProtoUtil.toStuff(job.getJobState()) == JobState.COMPLETED);

    // Expect to receive an NamespaceException if try to find the deleted metadata in kv store
    thrown.expect(NamespaceException.class);
    p(NamespaceService.class).get().getDataset(new DatasetPath(ImmutableList.of(sourceName, "myFile.json")).toNamespaceKey());
  }

  @Test
  public void testVDSConcurrency() throws Exception {
    Space space = createSpace("concurrency");

    int max = 5;

    List<Thread> threads = new ArrayList<>();
    for (int i = 1; i <= max; i++) {
      threads.add(createVDSInSpace("vds" + i, space.getName(), "select " + i));
    }

    threads.forEach(Thread::start);

    for (Thread thread : threads) {
      thread.join();
    }

    // verify that the VDS were created with the correct SQL
    for (int i = 1; i <= max; i++) {
      DatasetConfig dataset = newNamespaceService().getDataset(new NamespaceKey(Arrays.asList(space.getName(), "vds" + i)));
      assertEquals(dataset.getVirtualDataset().getSql(), "select " + i);
    }

    newNamespaceService().deleteSpace(new NamespaceKey(space.getName()), space.getTag());
  }

  private Thread createVDSInSpace(String name, String spaceName, String sql) {
    return new Thread(() -> {
      createVDS(Arrays.asList(spaceName, name), sql);
    });
  }

  @Test
  public void testSource() throws Exception {
    Source source = createSource();

    // make sure we can fetch the source by id
    source = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(source.getId())).buildGet(), new GenericType<Source>() {});

    // make sure that trying to create the source again fails
    NASConf nasConf = new NASConf();
    nasConf.path = TestTools.getWorkingPath() + "/src/test/resources";

    Source newSource = new Source();
    newSource.setName("catalog-test");
    newSource.setType("NAS");
    newSource.setConfig(nasConf);
    expectStatus(Response.Status.CONFLICT, getBuilder(getPublicAPI(3).path(CATALOG_PATH)).buildPost(Entity.json(newSource)));

    // edit source
    source.setAccelerationRefreshPeriodMs(0);
    source = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(source.getId())).buildPut(Entity.json(source)), new GenericType<Source>() {});

    assertNotNull(source.getTag());
    assertEquals((long) source.getAccelerationRefreshPeriodMs(), 0);

    // adding a folder to a source should fail
    Folder newFolder = new Folder(null, Arrays.asList(source.getName(), "myFolder"), null, null);
    expectStatus(Response.Status.BAD_REQUEST, getBuilder(getPublicAPI(3).path(CATALOG_PATH)).buildPost(Entity.json(newFolder)));

    // delete source
    expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(source.getId())).buildDelete());

    thrown.expect(NamespaceException.class);
    newNamespaceService().getSource(new NamespaceKey(source.getName()));

  }

  @Test
  public void testSourceBrowsing() throws Exception {
    Source source = createSource();

    // browse to the json directory
    String id = getFolderIdByName(source.getChildren(), "\"json\"");
    assertNotNull(id, "Failed to find json directory");

    // deleting a folder on a source should fail
    expectStatus(Response.Status.BAD_REQUEST, getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(com.dremio.common.utils.PathUtils.encodeURIComponent(id))).buildDelete());

  }

  @Test
  public void testSourcePromoting() throws Exception {
    Source source = createSource();

    // browse to the json directory
    String id = getFolderIdByName(source.getChildren(), "\"json\"");
    assertNotNull(id, "Failed to find json directory");

    // load the json dir
    Folder folder = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(com.dremio.common.utils.PathUtils.encodeURIComponent(id))).buildGet(), new GenericType<Folder>() {});
    assertEquals(folder.getChildren().size(), 19);

    String fileId = null;

    for (CatalogItem item : folder.getChildren()) {
      List<String> path = item.getPath();
      // get the numbers.json file
      if (item.getType() == CatalogItem.CatalogItemType.FILE && path.get(path.size() - 1).equals("\"numbers.json\"")) {
        fileId = item.getId();
        break;
      }
    }

    assertNotNull(fileId, "Failed to find numbers.json file");

    // load the file
    File file = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(com.dremio.common.utils.PathUtils.encodeURIComponent(fileId))).buildGet(), new GenericType<File>() {});

    // promote the file (dac/backend/src/test/resources/json/numbers.json)
    Dataset dataset = createPDS(CatalogServiceHelper.getPathFromInternalId(file.getId()), new JsonFileConfig());

    dataset = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(com.dremio.common.utils.PathUtils.encodeURIComponent(fileId))).buildPost(Entity.json(dataset)), new GenericType<Dataset>() {});

    // load the dataset
    dataset = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(dataset.getId())).buildGet(), new GenericType<Dataset>() {});

    // verify listing
    folder = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(com.dremio.common.utils.PathUtils.encodeURIComponent(id))).buildGet(), new GenericType<Folder>() {});
    assertEquals(folder.getChildren().size(), 19);

    // unpromote file
    expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(dataset.getId())).buildDelete());

    // dataset should no longer exist
    expectStatus(Response.Status.NOT_FOUND, getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(dataset.getId())).buildGet());

    // verify listing
    folder = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(com.dremio.common.utils.PathUtils.encodeURIComponent(id))).buildGet(), new GenericType<Folder>() {});
    assertEquals(folder.getChildren().size(), 19);

    // promote a folder that contains several csv files (dac/backend/src/test/resources/datasets/folderdataset)
    String folderId = getFolderIdByName(source.getChildren(), "\"datasets\"");
    assertNotNull(folderId, "Failed to find datasets directory");

    Folder dsFolder = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(com.dremio.common.utils.PathUtils.encodeURIComponent(folderId))).buildGet(), new GenericType<Folder>() {});

    String folderDatasetId = getFolderIdByName(dsFolder.getChildren(), "\"folderdataset\"");
    assertNotNull(folderDatasetId, "Failed to find folderdataset directory");

    // we want to use the path that the backend gives us so fetch the full folder
    folder = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(com.dremio.common.utils.PathUtils.encodeURIComponent(folderDatasetId))).buildGet(), new GenericType<Folder>() {});

    TextFileConfig textFileConfig = new TextFileConfig();
    textFileConfig.setLineDelimiter("\n");
    dataset = createPDS(folder.getPath(), textFileConfig);

    dataset = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(com.dremio.common.utils.PathUtils.encodeURIComponent(folderDatasetId))).buildPost(Entity.json(dataset)), new GenericType<Dataset>() {});

    // load the promoted dataset
    dataset = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(dataset.getId())).buildGet(), new GenericType<Dataset>() {});

    // unpromote the folder
    expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(dataset.getId())).buildDelete());

    // dataset should no longer exist
    expectStatus(Response.Status.NOT_FOUND, getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(dataset.getId())).buildGet());

  }

  @Test
  public void testSourceMetadataRefresh() throws Exception {

    // create a temporary copy of some test data
    TemporaryFolder tempFolder = new TemporaryFolder();
    tempFolder.create();
    tempFolder.newFolder("json");

    // Copys test data
    String numbers_src = TestTools.getWorkingPath() + "/src/test/resources/json/numbers.json";
    java.io.File numbers = new java.io.File(numbers_src);
    java.io.File numbers_copy = tempFolder.newFile("json/numbers.json");
    FileUtils.copyFile(numbers, numbers_copy);

    NASConf nasConf = new NASConf();
    nasConf.path = tempFolder.getRoot().getAbsolutePath();
    Source newSource = new Source();
    newSource.setName("catalog-test");
    newSource.setType("NAS");
    newSource.setConfig(nasConf);

    Source source = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH)).buildPost(Entity.json(newSource)),  new GenericType<Source>() {});

    // browse to the json directory
    String id = getFolderIdByName(source.getChildren(), "\"json\"");
    assertNotNull(id, "Failed to find json directory");

    // load the json dir
    Folder folder = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(com.dremio.common.utils.PathUtils.encodeURIComponent(id))).buildGet(), new GenericType<Folder>() {
    });
    assertEquals(folder.getChildren().size(), 1);

    String fileId = null;

    for (CatalogItem item : folder.getChildren()) {
      List<String> path = item.getPath();
      // get the numbers.json file
      if (item.getType() == CatalogItem.CatalogItemType.FILE && path.get(path.size() - 1).equals("\"numbers.json\"")) {
        fileId = item.getId();
        break;
      }
    }

    assertNotNull(fileId, "Failed to find numbers.json file");

    // load the file
    File file = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(com.dremio.common.utils.PathUtils.encodeURIComponent(fileId))).buildGet(), new GenericType<File>() {
    });

    // promote the file (dac/backend/src/test/resources/json/numbers.json)
    Dataset dataset = createPDS(CatalogServiceHelper.getPathFromInternalId(file.getId()), new JsonFileConfig());

    dataset = expectSuccess(
      getBuilder(getPublicAPI(3)
        .path(CATALOG_PATH)
        .path(com.dremio.common.utils.PathUtils.encodeURIComponent(fileId)))
        .buildPost(Entity.json(dataset)),
      new GenericType<Dataset>() { });

    // load the dataset
    dataset = expectSuccess(
      getBuilder(getPublicAPI(3)
        .path(CATALOG_PATH)
        .path(dataset.getId())).buildGet(),
      new GenericType<Dataset>() { });

    // verify listing
    folder = expectSuccess(
      getBuilder(getPublicAPI(3)
        .path(CATALOG_PATH)
        .path(com.dremio.common.utils.PathUtils.encodeURIComponent(id)))
        .buildGet(),
      new GenericType<Folder>() { });
    assertEquals(folder.getChildren().size(), 1);

    // test metadata/refresh endpoint
    CatalogResource.MetadataRefreshResponse response = new CatalogResource.MetadataRefreshResponse(false, false);

    // test with wrong ID type (expect BAD_REQUEST)
    expectStatus(Response.Status.BAD_REQUEST,
      getBuilder(getPublicAPI(3)
        .path(CATALOG_PATH)
        .path(com.dremio.common.utils.PathUtils.encodeURIComponent(id))
        .path("metadata/refresh"))
        .buildPost(Entity.json(response)));

    // test with bad ID (expect NOT_FOUND)
     expectStatus(Response.Status.NOT_FOUND,
      getBuilder(getPublicAPI(3)
        .path(CATALOG_PATH)
        .path(com.dremio.common.utils.PathUtils.encodeURIComponent("asdfasdf"))
        .path("metadata/refresh"))
        .buildPost(Entity.json(response)));

    /*** test with promoted data ***/
    response = expectSuccess(getBuilder(getPublicAPI(3)
      .path(CATALOG_PATH)
      .path(com.dremio.common.utils.PathUtils.encodeURIComponent(dataset.getId()))
      .path("metadata/refresh"))
      .buildPost(Entity.json(response)),
      new GenericType<CatalogResource.MetadataRefreshResponse>() { });
    assertTrue(response.getChanged());
    assertFalse(response.getDeleted());

    // test forceUpdate
    response = expectSuccess(getBuilder(getPublicAPI(3)
      .path(CATALOG_PATH)
      .path(com.dremio.common.utils.PathUtils.encodeURIComponent(dataset.getId()))
      .path("metadata/refresh")
      .queryParam("forceUpdate", "true"))
      .buildPost(Entity.json(response)),
      new GenericType<CatalogResource.MetadataRefreshResponse>() { });
    assertTrue(response.getChanged());
    assertFalse(response.getDeleted());

    // test deleteWhenMissing
    response = expectSuccess(getBuilder(getPublicAPI(3)
        .path(CATALOG_PATH)
        .path(com.dremio.common.utils.PathUtils.encodeURIComponent(dataset.getId()))
        .path("metadata/refresh")
        .queryParam("deleteWhenMissing", "true"))
        .buildPost(Entity.json(response)),
      new GenericType<CatalogResource.MetadataRefreshResponse>() { });
    assertTrue(response.getChanged());
    assertFalse(response.getDeleted());

    // test autoPromotion
    response = expectSuccess(getBuilder(getPublicAPI(3)
        .path(CATALOG_PATH)
        .path(com.dremio.common.utils.PathUtils.encodeURIComponent(dataset.getId()))
        .path("metadata/refresh")
        .queryParam("autoPromotion", "true"))
        .buildPost(Entity.json(response)),
      new GenericType<CatalogResource.MetadataRefreshResponse>() { });
    assertTrue(response.getChanged());
    assertFalse(response.getDeleted());

    // test all query params
    response = expectSuccess(getBuilder(getPublicAPI(3)
        .path(CATALOG_PATH)
        .path(com.dremio.common.utils.PathUtils.encodeURIComponent(dataset.getId()))
        .path("metadata/refresh")
        .queryParam("forceUpdate", "true")
        .queryParam("deleteWhenMissing", "true")
        .queryParam("autoPromotion", "true"))
        .buildPost(Entity.json(response)),
      new GenericType<CatalogResource.MetadataRefreshResponse>() { });
    assertTrue(response.getChanged());
    assertFalse(response.getDeleted());

    // now delete the temporary folder
    numbers_copy.delete();

    // test keep missing metadata
    response = expectSuccess(getBuilder(getPublicAPI(3)
        .path(CATALOG_PATH)
        .path(com.dremio.common.utils.PathUtils.encodeURIComponent(dataset.getId()))
        .path("metadata/refresh")
        .queryParam("forceUpdate", "false")
        .queryParam("deleteWhenMissing", "false")
        .queryParam("autoPromotion", "false"))
        .buildPost(Entity.json(response)),
      new GenericType<CatalogResource.MetadataRefreshResponse>() { });
    assertFalse(response.getChanged());
    assertFalse(response.getDeleted());

    // test enabling metadata deletion when missing
    response = expectSuccess(getBuilder(getPublicAPI(3)
      .path(CATALOG_PATH)
      .path(com.dremio.common.utils.PathUtils.encodeURIComponent(dataset.getId()))
      .path("metadata/refresh")
      .queryParam("forceUpdate", "false")
      .queryParam("deleteWhenMissing", "true")
      .queryParam("autoPromotion", "false"))
      .buildPost(Entity.json(response)),
    new GenericType<CatalogResource.MetadataRefreshResponse>() { });
    assertTrue(response.getChanged());
    assertTrue(response.getDeleted());

    // cleanup
    tempFolder.delete();
    expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(source.getId())).buildDelete());
  }

  @Test
  public void testSourceEditWithoutSecret() throws Exception {
    // fakesource only works if password is the same as the name, else with fail to create
    final FakeSource fakeConf = new FakeSource();
    fakeConf.password = "fake";
    fakeConf.isAwesome = true;

    final Source newSource = new Source();
    newSource.setName("fake");
    newSource.setType("FAKESOURCE");
    newSource.setConfig(fakeConf);

    // create the source
    Source source = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH)).buildPost(Entity.json(newSource)), new GenericType<Source>() {});

    FakeSource config = (FakeSource) source.getConfig();

    // we should get back the use existing secret const
    assertTrue(config.isAwesome);
    assertEquals(config.password, ConnectionConf.USE_EXISTING_SECRET_VALUE);

    // verify that saving with the const works
    config.isAwesome = false;
    source.setConfig(config);

    source = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(source.getId())).buildPut(Entity.json(source)), new GenericType<Source>() {});
    config = (FakeSource) source.getConfig();

    assertFalse(config.isAwesome);
    assertNotNull(source.getTag());
  }

  private Source createSource() {
    NASConf nasConf = new NASConf();
    nasConf.path = TestTools.getWorkingPath() + "/src/test/resources";

    Source newSource = new Source();
    newSource.setName("catalog-test");
    newSource.setType("NAS");
    newSource.setConfig(nasConf);

    // create the source
    return expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH)).buildPost(Entity.json(newSource)),  new GenericType<Source>() {});
  }

  @After
  public void after() {
    CatalogServiceImpl catalog = (CatalogServiceImpl) l(CatalogService.class);
    if(catalog.getManagedSource("catalog-test") != null) {
      catalog.deleteSource("catalog-test");
    }
  }

  @Test
  public void testHome() throws Exception {
    ResponseList<CatalogItem> items = getRootEntities(null);

    String homeId = null;

    for (CatalogItem item : items.getData()) {
      if (item.getType() == CatalogItem.CatalogItemType.CONTAINER && item.getContainerType() == CatalogItem.ContainerSubType.HOME) {
        homeId = item.getId();
        break;
      }
    }

    // load home space
    Home home = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(homeId)).buildGet(), new GenericType<Home>() {});

    int size = home.getChildren().size();

    // add a folder
    Folder newFolder = new Folder(null, Arrays.asList(home.getName(), "myFolder"), null, null);
    Folder folder = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH)).buildPost(Entity.json(newFolder)), new GenericType<Folder>() {});
    assertEquals(newFolder.getPath(), folder.getPath());

    // make sure folder shows up under space
    home = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(homeId)).buildGet(), new GenericType<Home>() {});
    assertEquals(home.getChildren().size(), size + 1);

    // make sure that trying to create the folder again fails
    expectStatus(Response.Status.CONFLICT, getBuilder(getPublicAPI(3).path(CATALOG_PATH)).buildPost(Entity.json(newFolder)));

    // load folder
    folder = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(folder.getId())).buildGet(), new GenericType<Folder>() {});

    // store a VDS in the folder
    Dataset vds = getVDSConfig(Arrays.asList(home.getName(), "myFolder", "myVDS"), "select * from sys.version");

    Dataset newVDS = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH)).buildPost(Entity.json(vds)), new GenericType<Dataset>() {});

    // folder should have children now
    folder = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(folder.getId())).buildGet(), new GenericType<Folder>() {});
    assertEquals(folder.getChildren().size(), 1);

    // delete vds
    expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(newVDS.getId())).buildDelete());

    // folder should have no children now
    folder = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(folder.getId())).buildGet(), new GenericType<Folder>() {});
    assertEquals(folder.getChildren().size(), 0);

    // delete folder
    expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(folder.getId())).buildDelete());

    home = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(homeId)).buildGet(), new GenericType<Home>() {});
    assertEquals(home.getChildren().size(), size);
  }

  @Test
  public void testByPath() throws Exception {
    Source createdSource = createSource();

    // test getting a source by name
    Source source = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path("by-path").path(createdSource.getName())).buildGet(), new GenericType<Source>() {});
    assertEquals(source.getId(), createdSource.getId());

    // test getting a folder by path
    expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path("by-path").path(createdSource.getName()).path("json")).buildGet(), new GenericType<Folder>() {});

    // test getting a file with a url character in name (?)
    expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path("by-path").path(createdSource.getName()).path("testfiles").path("file_with_?.json")).buildGet(), new GenericType<File>() {});

  }

  @Test
  public void testRepromote() throws Exception {
    final Source source = createSource();

    // promote a folder that contains several csv files (dac/backend/src/test/resources/datasets/folderdataset)
    final String folderId = getFolderIdByName(source.getChildren(), "\"datasets\"");
    assertNotNull(folderId, "Failed to find datasets directory");

    Folder dsFolder = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(com.dremio.common.utils.PathUtils.encodeURIComponent(folderId))).buildGet(), new GenericType<Folder>() {});

    final String folderDatasetId = getFolderIdByName(dsFolder.getChildren(), "\"folderdataset\"");
    assertNotNull(folderDatasetId, "Failed to find folderdataset directory");

    // we want to use the path that the backend gives us so fetch the full folder
    Folder folder = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(com.dremio.common.utils.PathUtils.encodeURIComponent(folderDatasetId))).buildGet(), new GenericType<Folder>() {});

    final TextFileConfig textFileConfig = new TextFileConfig();
    textFileConfig.setLineDelimiter("\n");
    Dataset dataset = createPDS(folder.getPath(), textFileConfig);

    dataset = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(PathUtils.encodeURIComponent(folderDatasetId))).buildPost(Entity.json(dataset)), new GenericType<Dataset>() {});

    // load the promoted dataset
    dataset = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(dataset.getId())).buildGet(), new GenericType<Dataset>() {});

    // unpromote the folder
    expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(dataset.getId())).buildDelete());

    // dataset should no longer exist
    expectStatus(Response.Status.NOT_FOUND, getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(dataset.getId())).buildGet());

    // re-promote the folder by using by-path
    WebTarget target = getPublicAPI(3).path(CATALOG_PATH)
      .path("by-path")
      .path("catalog-test")
      .path("datasets")
      .path("folderdataset");

    folder = expectSuccess(getBuilder(target).buildGet(), new GenericType<Folder>() {});
    dataset = createPDS(folder.getPath(), textFileConfig);
    dataset = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(folder.getId())).buildPost(Entity.json(dataset)), new GenericType<Dataset>() {});

    // unpromote the folder
    expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(dataset.getId())).buildDelete());

  }

  @Test
  public void testErrors() throws Exception {
    // test non-existent id
    expectStatus(Response.Status.NOT_FOUND, getBuilder(getPublicAPI(3).path(CATALOG_PATH).path("bad-id")).buildGet());

    // test non-existent internal id
    expectStatus(Response.Status.NOT_FOUND, getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(CatalogServiceHelper.generateInternalId(Arrays.asList("bad-id")))).buildGet());

    // test non-existent path
    expectStatus(Response.Status.NOT_FOUND, getBuilder(getPublicAPI(3).path(CATALOG_PATH).path("by-path").path("doesnot").path("exist")).buildGet());
  }

  public static String getFolderIdByName(List<CatalogItem> items, String nameToFind) {
    for (CatalogItem item : items) {
      List<String> path = item.getPath();
      if (item.getContainerType() == CatalogItem.ContainerSubType.FOLDER && path.get(path.size() - 1).equals(nameToFind)) {
        return item.getId();
      }
    }

    return null;
  }

  private Dataset createPDS(List<String> path, FileFormat format) {
    return new Dataset(
      null,
      Dataset.DatasetType.PHYSICAL_DATASET,
      path,
      null,
      null,
      null,
      null,
      null,
      null,
      format,
      null
    );
  }

  private Dataset getVDSConfig(List<String> path, String sql) {
    return new Dataset(
      null,
      Dataset.DatasetType.VIRTUAL_DATASET,
      path,
      null,
      null,
      null,
      null,
      sql,
      null,
      null,
      null
    );
  }

  private Dataset createVDS(List<String> path, String sql) {
    Dataset vds = getVDSConfig(path, sql);
    return expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH)).buildPost(Entity.json(vds)), new GenericType<Dataset>() {});
  }

  private Folder getFolderConfig(List<String> path) {
    return new Folder(null, path, null, null);
  }

  private Folder createFolder(List<String> path) {
    return createFolder(getFolderConfig(path));
  }

  private Folder createFolder(Folder folder) {
    return expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH)).buildPost(Entity.json(folder)), new GenericType<Folder>() {});
  }
}
