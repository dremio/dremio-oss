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
package com.dremio.dac.resource;

import static java.util.Arrays.asList;

import com.dremio.dac.api.Folder;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.sources.SourceUI;
import com.dremio.dac.model.sources.UIMetadataPolicy;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.server.FamilyExpectation;
import com.dremio.dac.server.GenericErrorMessage;
import com.dremio.dac.service.autocomplete.model.AutocompleteRequest;
import com.dremio.dac.service.autocomplete.model.AutocompleteResponse;
import com.dremio.dac.service.autocomplete.model.SuggestionEntity;
import com.dremio.dac.service.autocomplete.model.SuggestionsType;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.NASConf;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Tests {@link com.dremio.dac.resource.SQLResource} API */
public class TestSQLResource extends BaseTestServer {
  private static final String SOURCE_NAME = "mysrc";
  private static final long DEFAULT_REFRESH_PERIOD = TimeUnit.HOURS.toMillis(4);
  private static final long DEFAULT_GRACE_PERIOD = TimeUnit.HOURS.toMillis(12);
  private static final DatasetPath DATASET_PATH_ONE =
      new DatasetPath(ImmutableList.of(SOURCE_NAME, "ds1"));
  private static final DatasetPath DATASET_PATH_TWO =
      new DatasetPath(ImmutableList.of(SOURCE_NAME, "ds2"));
  private static final DatasetPath DATASET_PATH_THREE =
      new DatasetPath(ImmutableList.of(SOURCE_NAME, "ds3"));

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  public void addPhysicalDataset(final DatasetPath path, final DatasetType type) throws Exception {
    NamespaceKey datasetPath = path.toNamespaceKey();
    final DatasetConfig datasetConfig = new DatasetConfig();
    datasetConfig.setName(datasetPath.getName());
    datasetConfig.setType(type);
    datasetConfig.setPhysicalDataset(new PhysicalDataset());
    getNamespaceService().tryCreatePhysicalDataset(datasetPath, datasetConfig);
  }

  @Before
  public void setup() throws Exception {
    final NASConf nas = new NASConf();
    nas.path = folder.getRoot().getPath();
    SourceUI source = new SourceUI();
    source.setName(SOURCE_NAME);
    source.setCtime(System.currentTimeMillis());
    source.setAccelerationRefreshPeriod(DEFAULT_REFRESH_PERIOD);
    source.setAccelerationGracePeriod(DEFAULT_GRACE_PERIOD);
    // Please note: if this source is ever refreshed, the physical dataset added below will
    // disappear
    source.setMetadataPolicy(UIMetadataPolicy.of(CatalogService.NEVER_REFRESH_POLICY));
    source.setConfig(nas);
    getSourceService().registerSourceWithRuntime(source);
    addPhysicalDataset(DATASET_PATH_ONE, DatasetType.PHYSICAL_DATASET);
    addPhysicalDataset(DATASET_PATH_TWO, DatasetType.PHYSICAL_DATASET);
    addPhysicalDataset(DATASET_PATH_THREE, DatasetType.PHYSICAL_DATASET);

    expectSuccess(
        getBuilder(getHttpClient().getAPIv3().path("/catalog/"))
            .buildPost(
                Entity.json(new com.dremio.dac.api.Space(null, "testSpace", null, null, null))),
        new GenericType<com.dremio.dac.api.Space>() {});

    DatasetPath d1Path = new DatasetPath("testSpace.supplier");
    getHttpClient()
        .getDatasetApi()
        .createDatasetFromSQLAndSave(
            d1Path, "select s_name, s_phone from cp.\"tpch/supplier.parquet\"", asList("cp"));

    Folder newFolder1 = new Folder(null, Arrays.asList("testSpace", "myFolder"), null, null);
    expectSuccess(
        getBuilder(getHttpClient().getAPIv3().path("/catalog/")).buildPost(Entity.json(newFolder1)),
        new GenericType<Folder>() {});
    Folder newFolder2 = new Folder(null, Arrays.asList("testSpace", "@dremio"), null, null);
    expectSuccess(
        getBuilder(getHttpClient().getAPIv3().path("/catalog/")).buildPost(Entity.json(newFolder2)),
        new GenericType<Folder>() {});

    Folder subFolder1 = new Folder(null, Arrays.asList("testSpace", "@dremio", "foo"), null, null);
    expectSuccess(
        getBuilder(getHttpClient().getAPIv3().path("/catalog/")).buildPost(Entity.json(subFolder1)),
        new GenericType<Folder>() {});
    Folder subFolder2 = new Folder(null, Arrays.asList("testSpace", "@dremio", "bar"), null, null);
    expectSuccess(
        getBuilder(getHttpClient().getAPIv3().path("/catalog/")).buildPost(Entity.json(subFolder2)),
        new GenericType<Folder>() {});

    DatasetPath d2Path = new DatasetPath("testSpace.myFolder.supplier");
    getHttpClient()
        .getDatasetApi()
        .createDatasetFromSQLAndSave(
            d2Path, "select s_name, s_phone from cp.\"tpch/supplier.parquet\"", asList("cp"));
  }

  @After
  public void clear() throws Exception {
    clearAllDataExceptUser();
  }

  @Test
  public void testNullCatalogEntityKeys() throws Exception {
    final String prefix = "";
    final SuggestionsType type = SuggestionsType.CONTAINER;
    final List<List<String>> catalogEntityKeys = null;
    final List<String> queryContext = Collections.emptyList();
    testAutocompleteError(prefix, type, catalogEntityKeys, queryContext);
  }

  @Test
  public void testCatalogEntityKeysForContainerTypeIsSize2() throws Exception {
    final String prefix = "";
    final SuggestionsType type = SuggestionsType.CONTAINER;
    final List<List<String>> catalogEntityKeys =
        Arrays.asList(Collections.EMPTY_LIST, Arrays.asList("@dremio", "mySpace"));
    final List<String> queryContext = Collections.emptyList();
    testAutocompleteError(prefix, type, catalogEntityKeys, queryContext);
  }

  @Test
  public void testCatalogEntityKeysForColumnTypeHasEmpty() throws Exception {
    final String prefix = "";
    final SuggestionsType type = SuggestionsType.COLUMN;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Collections.EMPTY_LIST);
    final List<String> queryContext = Collections.emptyList();
    testAutocompleteError(prefix, type, catalogEntityKeys, queryContext);
  }

  @Test
  public void testCatalogEntityKeysForReferenceTypeHasEmpty() throws Exception {
    final String prefix = "foo";
    final SuggestionsType type = SuggestionsType.REFERENCE;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Collections.EMPTY_LIST);
    final List<String> queryContext = Collections.emptyList();
    testAutocompleteError(prefix, type, catalogEntityKeys, queryContext);
  }

  @Test
  public void testCatalogEntityKeysForBranchTypeHasEmpty() throws Exception {
    final String prefix = "foo";
    final SuggestionsType type = SuggestionsType.BRANCH;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Collections.EMPTY_LIST);
    final List<String> queryContext = Collections.emptyList();
    testAutocompleteError(prefix, type, catalogEntityKeys, queryContext);
  }

  @Test
  public void testCatalogEntityKeysForTagTypeHasEmpty() throws Exception {
    final String prefix = "foo";
    final SuggestionsType type = SuggestionsType.TAG;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Collections.EMPTY_LIST);
    final List<String> queryContext = Collections.emptyList();
    testAutocompleteError(prefix, type, catalogEntityKeys, queryContext);
  }

  @Test
  public void testTopLevelContainersWithoutPrefix() {
    final String prefix = "";
    final SuggestionsType type = SuggestionsType.CONTAINER;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Collections.EMPTY_LIST);
    final List<String> queryContext = Arrays.asList("testSpace");
    final List<SuggestionEntity> expected =
        Arrays.asList(
            new SuggestionEntity(Arrays.asList("@dremio"), "home"),
            new SuggestionEntity(Arrays.asList("testSpace"), "space"),
            new SuggestionEntity(Arrays.asList("cp"), "source"),
            new SuggestionEntity(Arrays.asList("mysrc"), "source"),
            new SuggestionEntity(Arrays.asList("INFORMATION_SCHEMA"), "source"),
            new SuggestionEntity(Arrays.asList("sys"), "source"),
            new SuggestionEntity(Arrays.asList("$scratch"), "source"),
            new SuggestionEntity(Arrays.asList("testSpace", "@dremio"), "folder"),
            new SuggestionEntity(Arrays.asList("testSpace", "myFolder"), "folder"),
            new SuggestionEntity(Arrays.asList("testSpace", "supplier"), "virtual"));
    AutocompleteResponse containerSuggestions =
        testAutocompleteSuccess(
            prefix, type, catalogEntityKeys, queryContext, AutocompleteResponse.class);
    Assert.assertNotNull(containerSuggestions);
    Assert.assertTrue(
        containerSuggestions.getSuggestionsType().equals(SuggestionsType.CONTAINER.getType()));
    Assert.assertArrayEquals(expected.toArray(), containerSuggestions.getSuggestions().toArray());
  }

  @Test
  public void testTopLevelContainersWithPrefix() {
    final String prefix = "s";
    final SuggestionsType type = SuggestionsType.CONTAINER;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Collections.EMPTY_LIST);
    final List<String> queryContext = Arrays.asList("testSpace");
    final List<SuggestionEntity> expected =
        Arrays.asList(
            new SuggestionEntity(Arrays.asList("sys"), "source"),
            new SuggestionEntity(Arrays.asList("testSpace", "supplier"), "virtual"));
    AutocompleteResponse containerSuggestions =
        testAutocompleteSuccess(
            prefix, type, catalogEntityKeys, queryContext, AutocompleteResponse.class);
    Assert.assertNotNull(containerSuggestions);
    Assert.assertTrue(
        containerSuggestions.getSuggestionsType().equals(SuggestionsType.CONTAINER.getType()));
    Assert.assertArrayEquals(expected.toArray(), containerSuggestions.getSuggestions().toArray());
  }

  @Test
  public void testTopLevelContainersWithAmbiguousPrefix() {
    final String prefix = "@dre";
    final SuggestionsType type = SuggestionsType.CONTAINER;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Collections.EMPTY_LIST);
    final List<String> queryContext = Arrays.asList("testSpace");
    final List<SuggestionEntity> expected =
        Arrays.asList(
            new SuggestionEntity(Arrays.asList("@dremio"), "home"),
            new SuggestionEntity(Arrays.asList("testSpace", "@dremio"), "folder"));
    AutocompleteResponse containerSuggestions =
        testAutocompleteSuccess(
            prefix, type, catalogEntityKeys, queryContext, AutocompleteResponse.class);
    Assert.assertNotNull(containerSuggestions);
    Assert.assertTrue(
        containerSuggestions.getSuggestionsType().equals(SuggestionsType.CONTAINER.getType()));
    Assert.assertArrayEquals(expected.toArray(), containerSuggestions.getSuggestions().toArray());
  }

  @Test
  public void testTopLevelContainersWithPrefixMatchingQueryContext() {
    final String prefix = "test";
    final SuggestionsType type = SuggestionsType.CONTAINER;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Collections.EMPTY_LIST);
    final List<String> queryContext = Arrays.asList("testSpace");
    final List<SuggestionEntity> expected =
        Arrays.asList(new SuggestionEntity(Arrays.asList("testSpace"), "space"));
    AutocompleteResponse containerSuggestions =
        testAutocompleteSuccess(
            prefix, type, catalogEntityKeys, queryContext, AutocompleteResponse.class);
    Assert.assertNotNull(containerSuggestions);
    Assert.assertTrue(
        containerSuggestions.getSuggestionsType().equals(SuggestionsType.CONTAINER.getType()));
    Assert.assertArrayEquals(expected.toArray(), containerSuggestions.getSuggestions().toArray());
  }

  @Test
  public void testTopLevelContainersWithPrefixHappenToBeQueryContext() {
    final String prefix = "testSpace";
    final SuggestionsType type = SuggestionsType.CONTAINER;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Collections.EMPTY_LIST);
    final List<String> queryContext = Arrays.asList("testSpace");
    final List<SuggestionEntity> expected =
        Arrays.asList(new SuggestionEntity(Arrays.asList("testSpace"), "space"));
    AutocompleteResponse containerSuggestions =
        testAutocompleteSuccess(
            prefix, type, catalogEntityKeys, queryContext, AutocompleteResponse.class);
    Assert.assertNotNull(containerSuggestions);
    Assert.assertTrue(
        containerSuggestions.getSuggestionsType().equals(SuggestionsType.CONTAINER.getType()));
    Assert.assertArrayEquals(expected.toArray(), containerSuggestions.getSuggestions().toArray());
  }

  @Test
  public void testTopLevelContainersWithPrefixIgnoredCase() {
    final String prefix = "info";
    final SuggestionsType type = SuggestionsType.CONTAINER;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Collections.EMPTY_LIST);
    final List<String> queryContext = Arrays.asList("testSpace");
    final List<SuggestionEntity> expected =
        Arrays.asList(new SuggestionEntity(Arrays.asList("INFORMATION_SCHEMA"), "source"));
    AutocompleteResponse containerSuggestions =
        testAutocompleteSuccess(
            prefix, type, catalogEntityKeys, queryContext, AutocompleteResponse.class);
    Assert.assertNotNull(containerSuggestions);
    Assert.assertTrue(
        containerSuggestions.getSuggestionsType().equals(SuggestionsType.CONTAINER.getType()));
    Assert.assertArrayEquals(expected.toArray(), containerSuggestions.getSuggestions().toArray());
  }

  @Test
  public void testTopLevelContainersWithoutMatch() {
    final String prefix = "dremio";
    final SuggestionsType type = SuggestionsType.CONTAINER;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Collections.EMPTY_LIST);
    final List<String> queryContext = Arrays.asList("testSpace");
    final List<SuggestionEntity> expected = Collections.EMPTY_LIST;
    AutocompleteResponse containerSuggestions =
        testAutocompleteSuccess(
            prefix, type, catalogEntityKeys, queryContext, AutocompleteResponse.class);
    Assert.assertNotNull(containerSuggestions);
    Assert.assertTrue(
        containerSuggestions.getSuggestionsType().equals(SuggestionsType.CONTAINER.getType()));
    Assert.assertArrayEquals(expected.toArray(), containerSuggestions.getSuggestions().toArray());
  }

  /** Home space @dremio is empty */
  @Test
  public void testContainersInHomeWithoutPrefix() {
    final String prefix = "";
    final SuggestionsType type = SuggestionsType.CONTAINER;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Arrays.asList("@dremio"));
    final List<String> queryContext = Collections.EMPTY_LIST;
    final List<SuggestionEntity> expected = Collections.EMPTY_LIST;
    AutocompleteResponse containerSuggestions =
        testAutocompleteSuccess(
            prefix, type, catalogEntityKeys, queryContext, AutocompleteResponse.class);
    Assert.assertNotNull(containerSuggestions);
    Assert.assertTrue(
        containerSuggestions.getSuggestionsType().equals(SuggestionsType.CONTAINER.getType()));
    Assert.assertArrayEquals(expected.toArray(), containerSuggestions.getSuggestions().toArray());
  }

  @Test
  public void testContainersInHomeWithPrefix() {
    final String prefix = "no-match";
    final SuggestionsType type = SuggestionsType.CONTAINER;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Arrays.asList("@dremio"));
    final List<String> queryContext = Collections.EMPTY_LIST;
    final List<SuggestionEntity> expected = Collections.EMPTY_LIST;
    AutocompleteResponse containerSuggestions =
        testAutocompleteSuccess(
            prefix, type, catalogEntityKeys, queryContext, AutocompleteResponse.class);
    Assert.assertNotNull(containerSuggestions);
    Assert.assertTrue(
        containerSuggestions.getSuggestionsType().equals(SuggestionsType.CONTAINER.getType()));
    Assert.assertArrayEquals(expected.toArray(), containerSuggestions.getSuggestions().toArray());
  }

  @Test
  public void testContainersInQueryContextWithoutPrefix() {
    final String prefix = "";
    final SuggestionsType type = SuggestionsType.CONTAINER;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Arrays.asList("@dremio"));
    final List<String> queryContext = Arrays.asList("testSpace");
    final List<SuggestionEntity> expected =
        Arrays.asList(
            new SuggestionEntity(Arrays.asList("testSpace", "@dremio", "bar"), "folder"),
            new SuggestionEntity(Arrays.asList("testSpace", "@dremio", "foo"), "folder"));
    AutocompleteResponse containerSuggestions =
        testAutocompleteSuccess(
            prefix, type, catalogEntityKeys, queryContext, AutocompleteResponse.class);
    Assert.assertNotNull(containerSuggestions);
    Assert.assertTrue(
        containerSuggestions.getSuggestionsType().equals(SuggestionsType.CONTAINER.getType()));
    Assert.assertArrayEquals(expected.toArray(), containerSuggestions.getSuggestions().toArray());
  }

  @Test
  public void testContainersInQueryContextWithPrefix() {
    final String prefix = "f";
    final SuggestionsType type = SuggestionsType.CONTAINER;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Arrays.asList("@dremio"));
    final List<String> queryContext = Arrays.asList("testSpace");
    final List<SuggestionEntity> expected =
        Arrays.asList(new SuggestionEntity(Arrays.asList("testSpace", "@dremio", "foo"), "folder"));
    AutocompleteResponse containerSuggestions =
        testAutocompleteSuccess(
            prefix, type, catalogEntityKeys, queryContext, AutocompleteResponse.class);
    Assert.assertNotNull(containerSuggestions);
    Assert.assertTrue(
        containerSuggestions.getSuggestionsType().equals(SuggestionsType.CONTAINER.getType()));
    Assert.assertArrayEquals(expected.toArray(), containerSuggestions.getSuggestions().toArray());
  }

  @Test
  public void testContainersInSpaceWithoutPrefix() {
    final String prefix = "";
    final SuggestionsType type = SuggestionsType.CONTAINER;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Arrays.asList("testSpace"));
    final List<String> queryContext = Collections.EMPTY_LIST;
    final List<SuggestionEntity> expected =
        Arrays.asList(
            new SuggestionEntity(Arrays.asList("testSpace", "@dremio"), "folder"),
            new SuggestionEntity(Arrays.asList("testSpace", "myFolder"), "folder"),
            new SuggestionEntity(Arrays.asList("testSpace", "supplier"), "virtual"));
    AutocompleteResponse containerSuggestions =
        testAutocompleteSuccess(
            prefix, type, catalogEntityKeys, queryContext, AutocompleteResponse.class);
    Assert.assertNotNull(containerSuggestions);
    Assert.assertTrue(
        containerSuggestions.getSuggestionsType().equals(SuggestionsType.CONTAINER.getType()));
    Assert.assertArrayEquals(expected.toArray(), containerSuggestions.getSuggestions().toArray());
  }

  @Test
  public void testContainersInSpaceWithPrefix() {
    final String prefix = "My";
    final SuggestionsType type = SuggestionsType.CONTAINER;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Arrays.asList("testSpace"));
    final List<String> queryContext = Collections.EMPTY_LIST;
    final List<SuggestionEntity> expected =
        Arrays.asList(new SuggestionEntity(Arrays.asList("testSpace", "myFolder"), "folder"));
    AutocompleteResponse containerSuggestions =
        testAutocompleteSuccess(
            prefix, type, catalogEntityKeys, queryContext, AutocompleteResponse.class);
    Assert.assertNotNull(containerSuggestions);
    Assert.assertTrue(
        containerSuggestions.getSuggestionsType().equals(SuggestionsType.CONTAINER.getType()));
    Assert.assertArrayEquals(expected.toArray(), containerSuggestions.getSuggestions().toArray());
  }

  @Test
  public void testContainersInSpaceFolderWithoutPrefix() {
    final String prefix = "";
    final SuggestionsType type = SuggestionsType.CONTAINER;
    final List<List<String>> catalogEntityKeys =
        Arrays.asList(Arrays.asList("testSpace", "myFolder"));
    final List<String> queryContext = Collections.EMPTY_LIST;
    final List<SuggestionEntity> expected =
        Arrays.asList(
            new SuggestionEntity(Arrays.asList("testSpace", "myFolder", "supplier"), "virtual"));
    AutocompleteResponse containerSuggestions =
        testAutocompleteSuccess(
            prefix, type, catalogEntityKeys, queryContext, AutocompleteResponse.class);
    Assert.assertNotNull(containerSuggestions);
    Assert.assertTrue(
        containerSuggestions.getSuggestionsType().equals(SuggestionsType.CONTAINER.getType()));
    Assert.assertArrayEquals(expected.toArray(), containerSuggestions.getSuggestions().toArray());
  }

  @Test
  public void testContainersInSourceWithoutPrefix() {
    final String prefix = "";
    final SuggestionsType type = SuggestionsType.CONTAINER;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Arrays.asList("INFORMATION_SCHEMA"));
    final List<String> queryContext = Collections.EMPTY_LIST;
    final List<SuggestionEntity> expected =
        Arrays.asList(
            new SuggestionEntity(Arrays.asList("INFORMATION_SCHEMA", "CATALOGS"), "direct"),
            new SuggestionEntity(Arrays.asList("INFORMATION_SCHEMA", "COLUMNS"), "direct"),
            new SuggestionEntity(Arrays.asList("INFORMATION_SCHEMA", "SCHEMATA"), "direct"),
            new SuggestionEntity(Arrays.asList("INFORMATION_SCHEMA", "TABLES"), "direct"),
            new SuggestionEntity(Arrays.asList("INFORMATION_SCHEMA", "VIEWS"), "direct"));
    AutocompleteResponse containerSuggestions =
        testAutocompleteSuccess(
            prefix, type, catalogEntityKeys, queryContext, AutocompleteResponse.class);
    Assert.assertNotNull(containerSuggestions);
    Assert.assertTrue(
        containerSuggestions.getSuggestionsType().equals(SuggestionsType.CONTAINER.getType()));
    Assert.assertArrayEquals(expected.toArray(), containerSuggestions.getSuggestions().toArray());
  }

  @Test
  public void testContainersInSourceWithPrefix() {
    final String prefix = "c";
    final SuggestionsType type = SuggestionsType.CONTAINER;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Arrays.asList("INFORMATION_SCHEMA"));
    final List<String> queryContext = Collections.EMPTY_LIST;
    final List<SuggestionEntity> expected =
        Arrays.asList(
            new SuggestionEntity(Arrays.asList("INFORMATION_SCHEMA", "CATALOGS"), "direct"),
            new SuggestionEntity(Arrays.asList("INFORMATION_SCHEMA", "COLUMNS"), "direct"));
    AutocompleteResponse containerSuggestions =
        testAutocompleteSuccess(
            prefix, type, catalogEntityKeys, queryContext, AutocompleteResponse.class);
    Assert.assertNotNull(containerSuggestions);
    Assert.assertTrue(
        containerSuggestions.getSuggestionsType().equals(SuggestionsType.CONTAINER.getType()));
    Assert.assertArrayEquals(expected.toArray(), containerSuggestions.getSuggestions().toArray());
  }

  @Test
  public void testColumnsWithoutPrefix() {
    final String prefix = "";
    final SuggestionsType type = SuggestionsType.COLUMN;
    final List<List<String>> catalogEntityKeys =
        Arrays.asList(Arrays.asList("testSpace", "supplier"));
    final List<String> queryContext = Collections.EMPTY_LIST;
    final List<SuggestionEntity> expected =
        Arrays.asList(
            new SuggestionEntity(Arrays.asList("testSpace", "supplier", "s_name"), "TEXT"),
            new SuggestionEntity(Arrays.asList("testSpace", "supplier", "s_phone"), "TEXT"));
    AutocompleteResponse columnSuggestions =
        testAutocompleteSuccess(
            prefix, type, catalogEntityKeys, queryContext, AutocompleteResponse.class);
    Assert.assertNotNull(columnSuggestions);
    Assert.assertTrue(
        columnSuggestions.getSuggestionsType().equals(SuggestionsType.COLUMN.getType()));
    Assert.assertArrayEquals(expected.toArray(), columnSuggestions.getSuggestions().toArray());
  }

  @Test
  public void testColumnsWithPrefix() {
    final String prefix = "S_n";
    final SuggestionsType type = SuggestionsType.COLUMN;
    final List<List<String>> catalogEntityKeys =
        Arrays.asList(Arrays.asList("testSpace", "supplier"));
    final List<String> queryContext = Collections.EMPTY_LIST;
    final List<SuggestionEntity> expected =
        Arrays.asList(
            new SuggestionEntity(Arrays.asList("testSpace", "supplier", "s_name"), "TEXT"));
    AutocompleteResponse columnSuggestions =
        testAutocompleteSuccess(
            prefix, type, catalogEntityKeys, queryContext, AutocompleteResponse.class);
    Assert.assertNotNull(columnSuggestions);
    Assert.assertTrue(
        columnSuggestions.getSuggestionsType().equals(SuggestionsType.COLUMN.getType()));
    Assert.assertArrayEquals(expected.toArray(), columnSuggestions.getSuggestions().toArray());
  }

  @Test
  public void testColumnsWithQueryContextNotMatched() {
    final String prefix = "S_n";
    final SuggestionsType type = SuggestionsType.COLUMN;
    final List<List<String>> catalogEntityKeys =
        Arrays.asList(Arrays.asList("testSpace", "supplier"));
    final List<String> queryContext = Arrays.asList("testSpace");
    final List<SuggestionEntity> expected =
        Arrays.asList(
            new SuggestionEntity(Arrays.asList("testSpace", "supplier", "s_name"), "TEXT"));
    AutocompleteResponse columnSuggestions =
        testAutocompleteSuccess(
            prefix, type, catalogEntityKeys, queryContext, AutocompleteResponse.class);
    Assert.assertNotNull(columnSuggestions);
    Assert.assertTrue(
        columnSuggestions.getSuggestionsType().equals(SuggestionsType.COLUMN.getType()));
    Assert.assertArrayEquals(expected.toArray(), columnSuggestions.getSuggestions().toArray());
  }

  @Test
  public void testColumnsWithQueryContextMatched() {
    final String prefix = "S_n";
    final SuggestionsType type = SuggestionsType.COLUMN;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Arrays.asList("supplier"));
    final List<String> queryContext = Arrays.asList("testSpace");
    final List<SuggestionEntity> expected =
        Arrays.asList(
            new SuggestionEntity(Arrays.asList("testSpace", "supplier", "s_name"), "TEXT"));
    AutocompleteResponse columnSuggestions =
        testAutocompleteSuccess(
            prefix, type, catalogEntityKeys, queryContext, AutocompleteResponse.class);
    Assert.assertNotNull(columnSuggestions);
    Assert.assertTrue(
        columnSuggestions.getSuggestionsType().equals(SuggestionsType.COLUMN.getType()));
    Assert.assertArrayEquals(expected.toArray(), columnSuggestions.getSuggestions().toArray());
  }

  @Test
  public void testColumnsInInformationSchemaSourceWithoutPrefix() {
    final String prefix = "";
    final SuggestionsType type = SuggestionsType.COLUMN;
    final List<List<String>> catalogEntityKeys =
        Arrays.asList(Arrays.asList("INFORMATION_SCHEMA", "CATALOGS"));
    final List<String> queryContext = Collections.EMPTY_LIST;
    final List<SuggestionEntity> expected =
        Arrays.asList(
            new SuggestionEntity(
                Arrays.asList("INFORMATION_SCHEMA", "CATALOGS", "CATALOG_NAME"), "TEXT"),
            new SuggestionEntity(
                Arrays.asList("INFORMATION_SCHEMA", "CATALOGS", "CATALOG_DESCRIPTION"), "TEXT"),
            new SuggestionEntity(
                Arrays.asList("INFORMATION_SCHEMA", "CATALOGS", "CATALOG_CONNECT"), "TEXT"));
    AutocompleteResponse columnSuggestions =
        testAutocompleteSuccess(
            prefix, type, catalogEntityKeys, queryContext, AutocompleteResponse.class);
    Assert.assertNotNull(columnSuggestions);
    Assert.assertTrue(
        columnSuggestions.getSuggestionsType().equals(SuggestionsType.COLUMN.getType()));
    Assert.assertArrayEquals(expected.toArray(), columnSuggestions.getSuggestions().toArray());
  }

  @Test
  public void testColumnsInInformationSchemaSourceWithPrefix() {
    final String prefix = "catalog_con";
    final SuggestionsType type = SuggestionsType.COLUMN;
    final List<List<String>> catalogEntityKeys =
        Arrays.asList(Arrays.asList("INFORMATION_SCHEMA", "CATALOGS"));
    final List<String> queryContext = Collections.EMPTY_LIST;
    final List<SuggestionEntity> expected =
        Arrays.asList(
            new SuggestionEntity(
                Arrays.asList("INFORMATION_SCHEMA", "CATALOGS", "CATALOG_CONNECT"), "TEXT"));
    AutocompleteResponse columnSuggestions =
        testAutocompleteSuccess(
            prefix, type, catalogEntityKeys, queryContext, AutocompleteResponse.class);
    Assert.assertNotNull(columnSuggestions);
    Assert.assertTrue(
        columnSuggestions.getSuggestionsType().equals(SuggestionsType.COLUMN.getType()));
    Assert.assertArrayEquals(expected.toArray(), columnSuggestions.getSuggestions().toArray());
  }

  @Test
  public void testColumnsInNonDatasetContainer() {
    final String prefix = "";
    final SuggestionsType type = SuggestionsType.COLUMN;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Arrays.asList("INFORMATION_SCHEMA"));
    final List<String> queryContext = Collections.EMPTY_LIST;
    final List<SuggestionEntity> expected = Collections.EMPTY_LIST;
    AutocompleteResponse columnSuggestions =
        testAutocompleteSuccess(
            prefix, type, catalogEntityKeys, queryContext, AutocompleteResponse.class);
    Assert.assertNotNull(columnSuggestions);
    Assert.assertTrue(
        columnSuggestions.getSuggestionsType().equals(SuggestionsType.COLUMN.getType()));
    Assert.assertArrayEquals(expected.toArray(), columnSuggestions.getSuggestions().toArray());
  }

  private void testAutocompleteError(
      String prefix,
      SuggestionsType type,
      List<List<String>> catalogEntityKeys,
      List<String> queryContext) {
    final String endpoint = "/sql/autocomplete";

    expectError(
        FamilyExpectation.CLIENT_ERROR,
        getBuilder(getHttpClient().getAPIv2().path(endpoint))
            .buildPost(
                Entity.entity(
                    new AutocompleteRequest(
                        prefix, type, catalogEntityKeys, queryContext, null, null),
                    MediaType.APPLICATION_JSON_TYPE)),
        GenericErrorMessage.class);
  }

  private <T> T testAutocompleteSuccess(
      String prefix,
      SuggestionsType type,
      List<List<String>> catalogEntityKeys,
      List<String> queryContext,
      Class<T> entityType) {
    final String endpoint = "/sql/autocomplete";

    Response response =
        expectSuccess(
            getBuilder(getHttpClient().getAPIv2().path(endpoint))
                .buildPost(
                    Entity.entity(
                        new AutocompleteRequest(
                            prefix, type, catalogEntityKeys, queryContext, null, null),
                        MediaType.APPLICATION_JSON_TYPE)));
    T suggestions = response.readEntity(entityType);
    return suggestions;
  }
}
