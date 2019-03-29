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

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.CONFLICT;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.ws.rs.client.Entity;

import org.junit.Test;

import com.dremio.common.util.TestTools;
import com.dremio.dac.daemon.DACDaemon;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.service.collaboration.CollaborationHelper;
import com.dremio.dac.service.collaboration.Tags;
import com.dremio.dac.service.collaboration.Wiki;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.exec.store.dfs.NASConf;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceUtils;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.VirtualDataset;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.dremio.service.users.SystemUser;
import com.google.common.base.Strings;

/**
 * Tests the {@link CollaborationResource} API
 */
public class TestCollaborationResource extends BaseTestServer {
  @Test
  public void testGetTags() throws Exception {
    // create space
    NamespaceKey spacePath = new NamespaceKey("testspace");
    List<String> vdsPath = Arrays.asList(spacePath.getRoot(), "testVDS");
    createSpaceAndVDS(spacePath, vdsPath);
    DatasetConfig dataset = newNamespaceService().getDataset(new NamespaceKey(vdsPath));

    // no tags initially, so expect a 404
    expectStatus(NOT_FOUND, getBuilder(getPublicAPI(3).path("catalog").path(dataset.getId().getId()).path("collaboration").path("tag")).buildGet());

    CollaborationHelper collaborationHelper = l(CollaborationHelper.class);

    List<String> tagList = Arrays.asList("tag1", "tag2");

    Tags newTags = new Tags(tagList, null);
    collaborationHelper.setTags(dataset.getId().getId(), newTags);

    // tags exist now
    Tags tags = expectSuccess(getBuilder(getPublicAPI(3).path("catalog").path(dataset.getId().getId()).path("collaboration").path("tag")).buildGet(), Tags.class);
    assertEquals(tags.getTags().size(), 2);
    assertTrue(tags.getTags().containsAll(tagList));

    // cleanup space
    newNamespaceService().deleteSpace(spacePath, NamespaceUtils.getVersion(spacePath, newNamespaceService()));
  }

  @Test
  public void testSetTags() throws Exception {
    // create space
    NamespaceKey spacePath = new NamespaceKey("testspace");
    List<String> vdsPath = Arrays.asList(spacePath.getRoot(), "testVDS");
    createSpaceAndVDS(spacePath, vdsPath);
    DatasetConfig dataset = newNamespaceService().getDataset(new NamespaceKey(vdsPath));

    List<String> tagList = Arrays.asList("tag1", "tag2");
    Tags newTags = new Tags(tagList, null);

    Tags tags = expectSuccess(getBuilder(getPublicAPI(3).path("catalog").path(dataset.getId().getId()).path("collaboration").path("tag")).buildPost(Entity.json(newTags)), Tags.class);
    assertEquals(tags.getTags().size(), 2);
    assertTrue(tags.getTags().containsAll(tagList));
    assertEquals(tags.getVersion(), "0");

    // test update of existing tags
    tagList = Arrays.asList("tag1", "tag3");
    newTags = new Tags(tagList, tags.getVersion());
    tags = expectSuccess(getBuilder(getPublicAPI(3).path("catalog").path(dataset.getId().getId()).path("collaboration").path("tag")).buildPost(Entity.json(newTags)), Tags.class);

    // verify the new tags
    assertEquals(tags.getTags().size(), 2);
    assertTrue(tags.getTags().containsAll(tagList));
    assertEquals(tags.getVersion(), "1");

    // clear out tags
    tagList = Arrays.asList();
    newTags = new Tags(tagList, tags.getVersion());
    tags = expectSuccess(getBuilder(getPublicAPI(3).path("catalog").path(dataset.getId().getId()).path("collaboration").path("tag")).buildPost(Entity.json(newTags)), Tags.class);

    // verify the new tags are empty
    assertEquals(tags.getTags().size(), 0);

    // cleanup space
    newNamespaceService().deleteSpace(spacePath, NamespaceUtils.getVersion(spacePath, newNamespaceService()));
  }

  @Test
  public void testGetTagsErrors() throws Exception {
    // invalid id
    expectStatus(BAD_REQUEST, getBuilder(getPublicAPI(3).path("catalog").path("bad-id").path("collaboration").path("tag")).buildGet());
  }

  @Test
  public void testSetTagsErrors() throws Exception {
    List<String> tagList = Arrays.asList("tag1", "tag2");
    Tags newTags = new Tags(tagList, null);

    // set tags for an invalid id
    expectStatus(BAD_REQUEST, getBuilder(getPublicAPI(3).path("catalog").path("bad-id").path("collaboration").path("tag")).buildPost(Entity.json(newTags)));

    // create space
    NamespaceKey spacePath = new NamespaceKey("testspace");
    List<String> vdsPath = Arrays.asList(spacePath.getRoot(), "testVDS");
    createSpaceAndVDS(spacePath, vdsPath);
    DatasetConfig dataset = newNamespaceService().getDataset(new NamespaceKey(vdsPath));

    // set invalid tags (duplicate)
    tagList = Arrays.asList("tag1", "tag2", "tag1");
    newTags = new Tags(tagList, null);
    expectStatus(BAD_REQUEST, getBuilder(getPublicAPI(3).path("catalog").path(dataset.getId().getId()).path("collaboration").path("tag")).buildPost(Entity.json(newTags)));

    // update tags with invalid version
    tagList = Arrays.asList("tag1", "tag2");
    newTags = new Tags(tagList, null);
    expectSuccess(getBuilder(getPublicAPI(3).path("catalog").path(dataset.getId().getId()).path("collaboration").path("tag")).buildPost(Entity.json(newTags)));

    newTags = new Tags(tagList, "5");
    expectStatus(CONFLICT, getBuilder(getPublicAPI(3).path("catalog").path(dataset.getId().getId()).path("collaboration").path("tag")).buildPost(Entity.json(newTags)));

    // test tag size limit - 128 max
    tagList = Arrays.asList("tag1", Strings.repeat("tag", 43));
    newTags = new Tags(tagList, null);
    expectStatus(BAD_REQUEST, getBuilder(getPublicAPI(3).path("catalog").path(dataset.getId().getId()).path("collaboration").path("tag")).buildPost(Entity.json(newTags)));

    // cleanup space
    newNamespaceService().deleteSpace(spacePath, NamespaceUtils.getVersion(spacePath, newNamespaceService()));
  }

  @Test
  public void testGetWiki() throws Exception {
    // create space
    NamespaceKey spacePath = new NamespaceKey("testspace");
    List<String> vdsPath = Arrays.asList(spacePath.getRoot(), "testVDS");
    createSpaceAndVDS(spacePath, vdsPath);
    DatasetConfig dataset = newNamespaceService().getDataset(new NamespaceKey(vdsPath));

    // no tags initially, so expect a 404
    expectStatus(NOT_FOUND, getBuilder(getPublicAPI(3).path("catalog").path(dataset.getId().getId()).path("collaboration").path("wiki")).buildGet());

    CollaborationHelper collaborationHelper = l(CollaborationHelper.class);

    Wiki newWiki = new Wiki("sample wiki text", null);
    collaborationHelper.setWiki(dataset.getId().getId(), newWiki);

    // tags exist now
    Wiki wiki = expectSuccess(getBuilder(getPublicAPI(3).path("catalog").path(dataset.getId().getId()).path("collaboration").path("wiki")).buildGet(), Wiki.class);
    assertEquals(wiki.getText(), newWiki.getText());

    // cleanup space
    newNamespaceService().deleteSpace(spacePath, NamespaceUtils.getVersion(spacePath, newNamespaceService()));
  }

  @Test
  public void testSetWiki() throws Exception {
    // create space
    NamespaceKey spacePath = new NamespaceKey("testspace");
    List<String> vdsPath = Arrays.asList(spacePath.getRoot(), "testVDS");
    createSpaceAndVDS(spacePath, vdsPath);
    DatasetConfig dataset = newNamespaceService().getDataset(new NamespaceKey(vdsPath));

    // create wiki
    Wiki newWiki = new Wiki("sample wiki text", null);
    Wiki wiki = expectSuccess(getBuilder(getPublicAPI(3).path("catalog").path(dataset.getId().getId()).path("collaboration").path("wiki")).buildPost(Entity.json(newWiki)), Wiki.class);
    assertEquals(wiki.getText(), newWiki.getText());
    assertEquals(wiki.getVersion().longValue(), 0L);

    // update wiki
    newWiki = new Wiki("some text", wiki.getVersion());
    expectSuccess(getBuilder(getPublicAPI(3).path("catalog").path(dataset.getId().getId()).path("collaboration").path("wiki")).buildPost(Entity.json(newWiki)));

    wiki = expectSuccess(getBuilder(getPublicAPI(3).path("catalog").path(dataset.getId().getId()).path("collaboration").path("wiki")).buildGet(), Wiki.class);
    assertEquals(wiki.getText(), newWiki.getText());
    assertEquals(wiki.getVersion().longValue(), 1L);

    // cleanup space
    newNamespaceService().deleteSpace(spacePath, NamespaceUtils.getVersion(spacePath, newNamespaceService()));
  }

  @Test
  public void testGetWikiErrors() throws Exception {
    // invalid id
    expectStatus(BAD_REQUEST, getBuilder(getPublicAPI(3).path("catalog").path("bad-id").path("collaboration").path("wiki")).buildGet());
  }

  @Test
  public void testSetWikiErrors() throws Exception {
    Wiki newWiki = new Wiki("sample wiki text", null);

    // set tags for an invalid id
    expectStatus(BAD_REQUEST, getBuilder(getPublicAPI(3).path("catalog").path("bad-id").path("collaboration").path("wiki")).buildPost(Entity.json(newWiki)));

    // create space
    NamespaceKey spacePath = new NamespaceKey("testspace");
    List<String> vdsPath = Arrays.asList(spacePath.getRoot(), "testVDS");
    createSpaceAndVDS(spacePath, vdsPath);
    DatasetConfig dataset = newNamespaceService().getDataset(new NamespaceKey(vdsPath));

    // update tags with invalid version
    newWiki = new Wiki("sample wiki text", null);
    expectSuccess(getBuilder(getPublicAPI(3).path("catalog").path(dataset.getId().getId()).path("collaboration").path("wiki")).buildPost(Entity.json(newWiki)));

    newWiki = new Wiki("sample wiki text", 5L);
    expectStatus(CONFLICT, getBuilder(getPublicAPI(3).path("catalog").path(dataset.getId().getId()).path("collaboration").path("wiki")).buildPost(Entity.json(newWiki)));

    // test wiki test size limit - 100k max
    //newWiki = new Wiki(Strings.repeat("f", 100_001), "0");
    //expectStatus(BAD_REQUEST, getBuilder(getPublicAPI(3).path("catalog").path(dataset.getId().getId()).path("collaboration").path("wiki")).buildPost(Entity.json(newWiki)));

    // cleanup space
    newNamespaceService().deleteSpace(spacePath, NamespaceUtils.getVersion(spacePath, newNamespaceService()));
  }

  @Test
  public void testOrphanPruning() throws Exception {
    // do a quick prune to clear the wiki/tags store
    final DACDaemon daemon = isMultinode() ? getMasterDremioDaemon() : getCurrentDremioDaemon();
    CollaborationHelper.pruneOrphans(daemon.getBindingProvider().lookup(KVStoreProvider.class));

    // create a source
    final NASConf nasConf = new NASConf();
    nasConf.path = TestTools.getWorkingPath() + "/src/test/resources";

    final NamespaceKey sourceKey = new NamespaceKey("mysource");
    final SourceConfig sourceConfig = new SourceConfig();
    sourceConfig.setName(sourceKey.getRoot());
    sourceConfig.setConfig(nasConf.toBytesString());
    sourceConfig.setType("NAS");
    newNamespaceService().addOrUpdateSource(sourceKey, sourceConfig);

    // create space
    final NamespaceKey spacePath = new NamespaceKey("testspace");
    final List<String> vdsPath = Arrays.asList(spacePath.getRoot(), "testVDS");
    createSpaceAndVDS(spacePath, vdsPath);

    // create folder in space
    createFolder(spacePath.getPathComponents(), "folder1");

    final List<String> vdsPath2 = Arrays.asList(spacePath.getRoot(), "testVDS2");
    createVDS(vdsPath2);

    addWiki(spacePath.getPathComponents(), "text");
    addWiki(vdsPath, "text");
    addWiki(vdsPath2, "text");
    addTags(vdsPath, Collections.singletonList("tag"));
    addWiki(Arrays.asList(spacePath.getRoot(), "folder1"), "text");

    // add wiki to the source
    addWiki(sourceKey.getPathComponents(), "text");

    // nothing deleted so no pruned items
    int pruneCount = CollaborationHelper.pruneOrphans(daemon.getBindingProvider().lookup(KVStoreProvider.class));
    assertEquals(0, pruneCount);

    // delete the space and children
    newNamespaceService().deleteSpace(spacePath, "0");
    pruneCount = CollaborationHelper.pruneOrphans(daemon.getBindingProvider().lookup(KVStoreProvider.class));
    assertEquals(5, pruneCount);

    // delete the source
    newNamespaceService().deleteSource(sourceKey, "0");
    pruneCount = CollaborationHelper.pruneOrphans(daemon.getBindingProvider().lookup(KVStoreProvider.class));
    assertEquals(1, pruneCount);
  }

  private void createFolder(List<String> path, String folderName) throws NamespaceException {
    final List<String> folderPath = new ArrayList<>(path);
    folderPath.add(folderName);

    final FolderConfig config = new FolderConfig();
    config.setName(folderName);
    config.setFullPathList(folderPath);

    newNamespaceService().addOrUpdateFolder(new NamespaceKey(folderPath), config);
  }

  private void addWiki(List<String> path, String text) throws Exception {
    final NameSpaceContainer container = newNamespaceService().getEntities(Collections.singletonList(new NamespaceKey(path))).get(0);
    final CollaborationHelper collaborationHelper = l(CollaborationHelper.class);

    collaborationHelper.setWiki(NamespaceUtils.getId(container), new Wiki(text, null));
  }

  private void addTags(List<String> path, List<String> tags) throws Exception {
    final NameSpaceContainer container = newNamespaceService().getEntities(Collections.singletonList(new NamespaceKey(path))).get(0);
    final CollaborationHelper collaborationHelper = l(CollaborationHelper.class);

    collaborationHelper.setTags(NamespaceUtils.getId(container), new Tags(tags, null));
  }

  private void createSpaceAndVDS(NamespaceKey spacePath, List<String> vdsPath) throws NamespaceException {
    // create space
    final SpaceConfig spaceConfig = new SpaceConfig();
    spaceConfig.setName(spacePath.getRoot());
    newNamespaceService().addOrUpdateSpace(spacePath, spaceConfig);

    createVDS(vdsPath);
  }

  private void createVDS(List<String> vdsPath) {
    // create vds
    final VirtualDataset virtualDataset = new VirtualDataset();
    virtualDataset.setSql("select * from sys.version");

    DatasetConfig datasetConfig = new DatasetConfig();
    datasetConfig.setName(vdsPath.get(vdsPath.size() - 1));
    datasetConfig.setFullPathList(vdsPath);
    datasetConfig.setType(DatasetType.VIRTUAL_DATASET);
    datasetConfig.setVirtualDataset(virtualDataset);

    getSabotContext().getViewCreator(SystemUser.SYSTEM_USERNAME).createView(vdsPath, "select * from sys.version", null);
  }
}
