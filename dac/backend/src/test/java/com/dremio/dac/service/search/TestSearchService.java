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
package com.dremio.dac.service.search;

import static com.dremio.dac.service.search.SearchServiceImpl.MAX_SEARCH_RESULTS;
import static org.junit.Assert.assertEquals;

import com.dremio.dac.model.sources.SourceUI;
import com.dremio.dac.model.sources.UIMetadataPolicy;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.service.collaboration.CollaborationHelper;
import com.dremio.dac.service.collaboration.Tags;
import com.dremio.dac.service.source.SourceService;
import com.dremio.exec.catalog.ViewCreatorFactory;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.NASConf;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.JobStatusListener;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.dremio.service.users.SystemUser;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/** Test Search Service */
public class TestSearchService extends BaseTestServer {

  @BeforeClass
  public static void before() throws Exception {
    populateInitialData();

    NamespaceService namespaceService = getNamespaceService();

    SpaceConfig spaceConfig = new SpaceConfig();
    spaceConfig.setName("searchSpace");

    namespaceService.addOrUpdateSpace(new NamespaceKey(spaceConfig.getName()), spaceConfig);
  }

  @AfterClass
  public static void after() throws Exception {
    NamespaceService namespaceService = getNamespaceService();
    NamespaceKey namespaceKey = new NamespaceKey("searchSpace");
    namespaceService.deleteSpace(namespaceKey, namespaceService.getSpace(namespaceKey).getTag());
  }

  @Test
  public void testSearch() throws Exception {
    SearchService searchService = getSearchService();

    createVDS("vds1", Arrays.asList("nar", "narwhal"), null);
    createVDS("vds2", Arrays.asList("nar"), null);
    createVDS("vds1111", null, null);
    createVDS("vds11", Arrays.asList("nar whal"), null);
    createVDS("narwhal", null, "select 1 as col1");

    doWakeup();

    // "vds1" should return 3 results
    List<SearchContainer> results = searchService.search("vds1", null);
    assertEquals(results.size(), 3);
    // first result should be vds1, since its an exact name match
    assertEquals(results.get(0).getNamespaceContainer().getDataset().getName(), "vds1");
    // make sure tags come back correctly
    assertEquals(
        results.get(0).getCollaborationTag().getTagsList(), Arrays.asList("nar", "narwhal"));

    // "vds11" should return 2 results
    results = searchService.search("vds11", null);
    assertEquals(results.size(), 2);
    // first result should be vds11, since its an exact name match
    assertEquals(results.get(0).getNamespaceContainer().getDataset().getName(), "vds11");
    assertEquals(results.get(1).getNamespaceContainer().getDataset().getName(), "vds1111");

    // "vds2" should return 1 results
    results = searchService.search("vds2", null);
    assertEquals(results.size(), 1);
    // first result should be vds1, since its an exact name match
    assertEquals(results.get(0).getNamespaceContainer().getDataset().getName(), "vds2");

    // "vds3" should return 0 results
    results = searchService.search("vds3", null);
    assertEquals(results.size(), 0);

    // "narwhal" should return 2 results
    results = searchService.search("narwhal", null);
    assertEquals(results.size(), 2);
    // first result should be the narwhal vds, since its an exact name match
    assertEquals(results.get(0).getNamespaceContainer().getDataset().getName(), "narwhal");
    assertEquals(results.get(1).getNamespaceContainer().getDataset().getName(), "vds1");

    // "nar" should return 4 results
    results = searchService.search("nar", null);
    assertEquals(results.size(), 4);

    // "nar whal" should return 4 results
    results = searchService.search("nar whal", null);
    assertEquals(results.size(), 4);
    // first result should be the vds11, since its an exact tag match
    assertEquals(results.get(0).getNamespaceContainer().getDataset().getName(), "vds11");

    // test column searching
    results = searchService.search("col", null);
    assertEquals(results.size(), 1);
    assertEquals(results.get(0).getNamespaceContainer().getDataset().getName(), "narwhal");
  }

  private void doWakeup() throws Exception {
    // make sure we call the master SearchService to do the wakeup call
    lMaster(SearchService.class).wakeupManager("");
    Thread.sleep(TimeUnit.MILLISECONDS.toMillis(200));
  }

  @Test
  public void testSearchLimit() throws Exception {
    SearchService searchService = getSearchService();

    for (int i = 0; i < 100; i++) {
      createVDS("lots" + i, null, null);
    }

    doWakeup();

    List<SearchContainer> lots = searchService.search("lots", null);

    assertEquals(lots.size(), MAX_SEARCH_RESULTS);
  }

  private void createVDS(String name, List<String> tags, String sql) throws Exception {
    if (sql == null) {
      sql = "select * from sys.version";
    }

    List<String> path = Arrays.asList("searchSpace", name);
    NamespaceKey namespaceKey = new NamespaceKey(path);

    ViewCreatorFactory vcf = l(ViewCreatorFactory.class);
    vcf.get(DEFAULT_USERNAME).createView(path, sql, Collections.emptyList(), false);

    DatasetConfig dataset = getNamespaceService().getDataset(namespaceKey);

    if (tags != null) {
      Tags tagsEntity = new Tags(tags, null);

      CollaborationHelper collaborationHelper = getCollaborationHelper();
      collaborationHelper.setTags(dataset.getId().getId(), tagsEntity);
    }
  }

  /**
   * Reproduction for DX-22065
   *
   * @throws Exception
   */
  @Test
  public void testSearchAndDelete() throws Exception {
    final SearchService searchService = getSearchService();
    final SourceService sourceService = getSourceService();

    // register source
    final SourceUI source = new SourceUI();
    source.setName("LocalFSSearch");
    source.setCtime(System.currentTimeMillis());
    final NASConf nas = new NASConf();
    nas.path = getPopulator().getPath().toFile().getPath();
    source.setConfig(nas);
    source.setMetadataPolicy(
        UIMetadataPolicy.of(CatalogService.DEFAULT_METADATA_POLICY_WITH_AUTO_PROMOTE));
    final SourceConfig sourceConfig =
        sourceService.registerSourceWithRuntime(
            source.asSourceConfig(), SystemUser.SYSTEM_USERNAME);

    // make a query
    final SqlQuery query =
        new SqlQuery("select * from LocalFSSearch.\"dac-sample1.json\"", DEFAULT_USERNAME);
    submitJobAndWaitUntilCompletion(
        JobRequest.newBuilder().setSqlQuery(query).build(), JobStatusListener.NO_OP);

    doWakeup();

    List<SearchContainer> results = searchService.search("dac-sample1.json", null);
    assertEquals(2, results.size());

    // delete source
    sourceService.deleteSource(sourceConfig);

    doWakeup();

    List<SearchContainer> newResults = searchService.search("dac-sample1.json", null);
    assertEquals(1, newResults.size());
  }

  private static SearchService getSearchService() {
    return l(SearchService.class);
  }
}
