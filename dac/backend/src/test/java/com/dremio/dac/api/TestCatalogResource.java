/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.dremio.common.util.TestTools;
import com.dremio.dac.model.common.Field;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.service.catalog.CatalogServiceHelper;
import com.dremio.dac.util.DatasetsUtil;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.store.dfs.NASConf;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.file.FileFormat;
import com.dremio.service.namespace.file.proto.JsonFileConfig;
import com.dremio.service.namespace.file.proto.TextFileConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;

/**
 * Tests for CatalogResource
 */
public class TestCatalogResource extends BaseTestServer {
  private static final String CATALOG_PATH = "/catalog/";

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
    newNamespaceService().deleteSpace(key, space.getVersion());
  }

  @Test
  public void testListTopLevelCatalog() throws Exception {
    // home space always exists
    int topLevelCount = newSourceService().getSources().size() + newNamespaceService().getSpaces().size() + 1;

    ResponseList<CatalogItem> items = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH)).buildGet(), new GenericType<ResponseList<CatalogItem>>() {});
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

    // make sure that trying to create the space again fails
    expectStatus(Response.Status.CONFLICT, getBuilder(getPublicAPI(3).path(CATALOG_PATH)).buildPost(Entity.json(newSpace)));

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
    Folder newFolder = new Folder(null, Arrays.asList(space.getName(), "myFolder"), null, null);
    Folder folder = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH)).buildPost(Entity.json(newFolder)), new GenericType<Folder>() {});
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

    newNamespaceService().deleteSpace(new NamespaceKey(space.getName()), Long.valueOf(space.getTag()));
  }

  @Test
  public void testVDSInSpace() throws Exception {
    // create a new space
    Space newSpace = new Space(null, "final frontier", null, null, null);
    Space space = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH)).buildPost(Entity.json(newSpace)), new GenericType<Space>() {});

    // add a folder
    Folder newFolder = new Folder(null, Arrays.asList(space.getName(), "myFolder"), null, null);
    Folder folder = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH)).buildPost(Entity.json(newFolder)), new GenericType<Folder>() {});

    // create a VDS in the space
    Dataset newVDS = createVDS(Arrays.asList(space.getName(), "myFolder", "myVDS"),"select * from sys.version");
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

    newNamespaceService().deleteSpace(new NamespaceKey(space.getName()), Long.valueOf(space.getTag()));
  }

  @Test
  public void testVDSConcurrency() throws Exception {
    Space newSpace = new Space(null, "concurrency", null, null, null);
    Space space = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH)).buildPost(Entity.json(newSpace)), new GenericType<Space>() {});

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

    newNamespaceService().deleteSpace(new NamespaceKey(space.getName()), Long.valueOf(space.getTag()));
  }

  private Thread createVDSInSpace(String name, String spaceName, String sql) {
    return new Thread(() -> {
      Dataset newVDS = createVDS(Arrays.asList(spaceName, name), sql);
      expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH)).buildPost(Entity.json(newVDS)));
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

    assertEquals(source.getTag(), "1");
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

    newNamespaceService().deleteSource(new NamespaceKey(source.getName()), Long.valueOf(source.getTag()));
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

    newNamespaceService().deleteSource(new NamespaceKey(source.getName()), Long.valueOf(source.getTag()));
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

    assertEquals(source.getTag(), "1");
    assertFalse(config.isAwesome);

    newNamespaceService().deleteSource(new NamespaceKey(source.getName()), Long.valueOf(source.getTag()));
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

  @Test
  public void testHome() throws Exception {
    ResponseList<CatalogItem> items = expectSuccess(getBuilder(getPublicAPI(3).path(CATALOG_PATH)).buildGet(), new GenericType<ResponseList<CatalogItem>>() {});

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
    Dataset vds = createVDS(Arrays.asList(home.getName(), "myFolder", "myVDS"), "select * from sys.version");

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

    newNamespaceService().deleteSource(new NamespaceKey(source.getName()), Long.valueOf(source.getTag()));
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

  private Dataset createVDS(List<String> path, String sql) {
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
}
