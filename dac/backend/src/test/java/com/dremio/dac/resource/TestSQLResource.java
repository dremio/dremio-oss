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

import java.util.ArrayList;
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
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.dac.api.Folder;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.sources.SourceUI;
import com.dremio.dac.model.sources.UIMetadataPolicy;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.server.FamilyExpectation;
import com.dremio.dac.server.GenericErrorMessage;
import com.dremio.dac.service.source.SourceService;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.NASConf;
import com.dremio.service.autocomplete.AutocompleteRequestImplementation;
import com.dremio.service.autocomplete.AutocompleteV2Request;
import com.dremio.service.autocomplete.AutocompleteV2RequestType;
import com.dremio.service.autocomplete.ColumnSuggestions;
import com.dremio.service.autocomplete.ContainerSuggestions;
import com.dremio.service.autocomplete.SuggestionEntity;
import com.dremio.service.autocomplete.SuggestionsType;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Tests {@link com.dremio.dac.resource.SQLResource} API
 */
public class TestSQLResource extends BaseTestServer {
  private static final String SOURCE_NAME = "mysrc";
  private static final long DEFAULT_REFRESH_PERIOD = TimeUnit.HOURS.toMillis(4);
  private static final long DEFAULT_GRACE_PERIOD = TimeUnit.HOURS.toMillis(12);
  private static final DatasetPath DATASET_PATH_ONE = new DatasetPath(ImmutableList.of(SOURCE_NAME, "ds1"));
  private static final DatasetPath DATASET_PATH_TWO = new DatasetPath(ImmutableList.of(SOURCE_NAME, "ds2"));
  private static final DatasetPath DATASET_PATH_THREE = new DatasetPath(ImmutableList.of(SOURCE_NAME, "ds3"));

  @Rule
  public final TemporaryFolder folder = new TemporaryFolder();

  protected NamespaceService getNamespaceService() {
    final NamespaceService service = newNamespaceService();
    return Preconditions.checkNotNull(service, "ns service is required");
  }

  protected SourceService getSourceService() {
    final SourceService service = newSourceService();
    return Preconditions.checkNotNull(service, "source service is required");
  }

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
    // Please note: if this source is ever refreshed, the physical dataset added below will disappear
    source.setMetadataPolicy(UIMetadataPolicy.of(CatalogService.NEVER_REFRESH_POLICY));
    source.setConfig(nas);
    getSourceService().registerSourceWithRuntime(source);
    addPhysicalDataset(DATASET_PATH_ONE, DatasetType.PHYSICAL_DATASET);
    addPhysicalDataset(DATASET_PATH_TWO, DatasetType.PHYSICAL_DATASET);
    addPhysicalDataset(DATASET_PATH_THREE, DatasetType.PHYSICAL_DATASET);

    expectSuccess(getBuilder(getPublicAPI(3).path("/catalog/")).buildPost(Entity.json(new com.dremio.dac.api.Space(null, "testSpace", null, null, null))), new GenericType<com.dremio.dac.api.Space>() {});

    DatasetPath d1Path = new DatasetPath("testSpace.supplier");
    createDatasetFromSQLAndSave(d1Path, "select s_name, s_phone from cp.\"tpch/supplier.parquet\"", asList("cp"));

    Folder newFolder = new Folder(null, Arrays.asList("testSpace", "myFolder"), null, null);
    expectSuccess(getBuilder(getPublicAPI(3).path("/catalog/")).buildPost(Entity.json(newFolder)), new GenericType<Folder>() {
    });

    DatasetPath d2Path = new DatasetPath("testSpace.myFolder.supplier");
    createDatasetFromSQLAndSave(d2Path, "select s_name, s_phone from cp.\"tpch/supplier.parquet\"", asList("cp"));
  }

  @After
  public void clear() throws Exception {
    clearAllDataExceptUser();
  }

  @Test
  public void testBasicAutocomplete() throws Exception {
    String query = "SELECT ";
    String autocompleteResponse = testAutocomplete(query, query.length(), new ArrayList<>());
    Assert.assertNotNull(autocompleteResponse);
  }

  @Test
  public void testCatalogEntryCompletionWithContext() {
    String query = "SELECT * FROM ";
    String autocompleteResponse = testAutocomplete(query, query.length(), ImmutableList.of("testSpace"));
    Assert.assertNotNull(autocompleteResponse);
    Assert.assertTrue(autocompleteResponse.contains("CatalogEntry"));
    Assert.assertTrue(autocompleteResponse.contains("supplier"));
  }

  @Test
  public void testColumnCompletion() {
    String query = "SELECT  FROM testSpace.supplier";
    String autocompleteResponse = testAutocomplete(query, 7, new ArrayList<>());
    Assert.assertNotNull(autocompleteResponse);
    Assert.assertTrue(autocompleteResponse.contains("Column"));
  }

  @Test
  @Ignore
  public void testColumnCompletionWithNessie() {
    String query = "SELECT  FROM testSpace.supplier AT BRANCH branchA";
    String autocompleteResponse = testAutocomplete(query, 7, new ArrayList<>());
    Assert.assertNotNull(autocompleteResponse);
    Assert.assertTrue(autocompleteResponse.contains("Column"));
  }

  @Test
  public void testColumnCompletionWithContext() throws Exception {
    String query = "SELECT  FROM supplier";
    String autocompleteResponse = testAutocomplete(query, 7, ImmutableList.of("testSpace", "myFolder"));
    Assert.assertNotNull(autocompleteResponse);
    Assert.assertTrue(autocompleteResponse.contains("Column"));
    Assert.assertTrue(autocompleteResponse.contains("supplier"));
    Assert.assertTrue(autocompleteResponse.contains("s_name"));
  }

  @Test
  public void testFunctionCompletion() throws Exception {
    String query = "SELECT * FROM testSpace.supplier WHERE AB";
    String autocompleteResponse = testAutocomplete(query, query.length(), new ArrayList<>());
    Assert.assertNotNull(autocompleteResponse);
    Assert.assertTrue(autocompleteResponse.contains("Function"));
  }

  @Test
  public void testFunctionCompletion2() throws Exception {
    String query = "SELECT * FROM testSpace.supplier WHERE ASCII(";
    String autocompleteResponse = testAutocomplete(query, query.length(), new ArrayList<>());
    Assert.assertNotNull(autocompleteResponse);
    Assert.assertTrue(autocompleteResponse.contains("Column"));
  }

  @Test
  public void testFolderRegression() throws Exception {
    String query = "SELECT * FROM testSpace.";
    String autocompleteResponse = testAutocomplete(query, query.length(), new ArrayList<>());
    Assert.assertNotNull(autocompleteResponse);
    Assert.assertTrue(autocompleteResponse.contains("CatalogEntry"));
    Assert.assertTrue(autocompleteResponse.contains("myFolder"));
    Assert.assertTrue(autocompleteResponse.contains("Folder"));
  }

  @Test
  public void testNullCatalogEntityKeys() throws Exception {
    final String prefix = "";
    final AutocompleteV2RequestType type = AutocompleteV2RequestType.CONTAINER;
    final List<List<String>> catalogEntityKeys = null;
    final List<String> queryContext = Collections.emptyList();
    testAutocompleteV2Error(prefix, type, catalogEntityKeys, queryContext);
  }

  @Test
  public void testCatalogEntityKeysForContainerTypeIsSize2() throws Exception {
    final String prefix = "";
    final AutocompleteV2RequestType type = AutocompleteV2RequestType.CONTAINER;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Collections.EMPTY_LIST, Arrays.asList("@dremio", "mySpace"));
    final List<String> queryContext = Collections.emptyList();
    testAutocompleteV2Error(prefix, type, catalogEntityKeys, queryContext);
  }

  @Test
  public void testCatalogEntityKeysForColumnTypeHasEmpty() throws Exception {
    final String prefix = "";
    final AutocompleteV2RequestType type = AutocompleteV2RequestType.COLUMN;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Collections.EMPTY_LIST);
    final List<String> queryContext = Collections.emptyList();
    testAutocompleteV2Error(prefix, type, catalogEntityKeys, queryContext);
  }

  @Test
  public void testCatalogEntityKeysForBranchTypeHasEmpty() throws Exception {
    final String prefix = "foo";
    final AutocompleteV2RequestType type = AutocompleteV2RequestType.REFERENCE;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Collections.EMPTY_LIST);
    final List<String> queryContext = Collections.emptyList();
    testAutocompleteV2Error(prefix, type, catalogEntityKeys, queryContext);
  }

  @Test
  public void testTopLevelContainersWithoutPrefix() {
    final String prefix = "";
    final AutocompleteV2RequestType type = AutocompleteV2RequestType.CONTAINER;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Collections.EMPTY_LIST);
    final List<String> queryContext = Arrays.asList("testSpace");
    final List<SuggestionEntity> expected = Arrays.asList(
      new SuggestionEntity("[@dremio]","home"),
      new SuggestionEntity("[testSpace]", "space"),
      new SuggestionEntity("[cp]","source"),
      new SuggestionEntity("[mysrc]", "source"),
      new SuggestionEntity("[INFORMATION_SCHEMA]","source"),
      new SuggestionEntity("[sys]","source"),
      new SuggestionEntity("[testSpace, myFolder]","folder"),
      new SuggestionEntity("[testSpace, supplier]","virtual"));
    ContainerSuggestions containerSuggestions = testAutocompleteV2Success(prefix, type, catalogEntityKeys, queryContext, ContainerSuggestions.class);
    Assert.assertNotNull(containerSuggestions);
    Assert.assertTrue(containerSuggestions.getSuggestionsType().equals(SuggestionsType.CONTAINER.getType()));
    Assert.assertArrayEquals(expected.toArray(), containerSuggestions.getContainers().toArray());
  }

  @Test
  public void testTopLevelContainersWithPrefix() {
    final String prefix = "s";
    final AutocompleteV2RequestType type = AutocompleteV2RequestType.CONTAINER;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Collections.EMPTY_LIST);
    final List<String> queryContext = Arrays.asList("testSpace");
    final List<SuggestionEntity> expected = Arrays.asList(
      new SuggestionEntity("[sys]","source"),
      new SuggestionEntity("[testSpace, supplier]","virtual"));
    ContainerSuggestions containerSuggestions = testAutocompleteV2Success(prefix, type, catalogEntityKeys, queryContext, ContainerSuggestions.class);
    Assert.assertNotNull(containerSuggestions);
    Assert.assertTrue(containerSuggestions.getSuggestionsType().equals(SuggestionsType.CONTAINER.getType()));
    Assert.assertArrayEquals(expected.toArray(), containerSuggestions.getContainers().toArray());
  }

  @Test
  public void testTopLevelContainersWithPrefixMatchingQueryContext() {
    final String prefix = "test";
    final AutocompleteV2RequestType type = AutocompleteV2RequestType.CONTAINER;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Collections.EMPTY_LIST);
    final List<String> queryContext = Arrays.asList("testSpace");
    final List<SuggestionEntity> expected = Arrays.asList(
      new SuggestionEntity("[testSpace]", "space"));
    ContainerSuggestions containerSuggestions = testAutocompleteV2Success(prefix, type, catalogEntityKeys, queryContext, ContainerSuggestions.class);
    Assert.assertNotNull(containerSuggestions);
    Assert.assertTrue(containerSuggestions.getSuggestionsType().equals(SuggestionsType.CONTAINER.getType()));
    Assert.assertArrayEquals(expected.toArray(), containerSuggestions.getContainers().toArray());
  }

  @Test
  public void testTopLevelContainersWithPrefixHappenToBeQueryContext() {
    final String prefix = "testSpace";
    final AutocompleteV2RequestType type = AutocompleteV2RequestType.CONTAINER;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Collections.EMPTY_LIST);
    final List<String> queryContext = Arrays.asList("testSpace");
    final List<SuggestionEntity> expected = Arrays.asList(
      new SuggestionEntity("[testSpace]", "space"));
    ContainerSuggestions containerSuggestions = testAutocompleteV2Success(prefix, type, catalogEntityKeys, queryContext, ContainerSuggestions.class);
    Assert.assertNotNull(containerSuggestions);
    Assert.assertTrue(containerSuggestions.getSuggestionsType().equals(SuggestionsType.CONTAINER.getType()));
    Assert.assertArrayEquals(expected.toArray(), containerSuggestions.getContainers().toArray());
  }

  @Test
  public void testTopLevelContainersWithPrefixIgnoredCase() {
    final String prefix = "info";
    final AutocompleteV2RequestType type = AutocompleteV2RequestType.CONTAINER;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Collections.EMPTY_LIST);
    final List<String> queryContext = Arrays.asList("testSpace");
    final List<SuggestionEntity> expected = Arrays.asList(
      new SuggestionEntity("[INFORMATION_SCHEMA]","source"));
    ContainerSuggestions containerSuggestions = testAutocompleteV2Success(prefix, type, catalogEntityKeys, queryContext, ContainerSuggestions.class);
    Assert.assertNotNull(containerSuggestions);
    Assert.assertTrue(containerSuggestions.getSuggestionsType().equals(SuggestionsType.CONTAINER.getType()));
    Assert.assertArrayEquals(expected.toArray(), containerSuggestions.getContainers().toArray());
  }

  @Test
  public void testTopLevelContainersWithoutMatch() {
    final String prefix = "dremio";
    final AutocompleteV2RequestType type = AutocompleteV2RequestType.CONTAINER;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Collections.EMPTY_LIST);
    final List<String> queryContext = Arrays.asList("testSpace");
    final List<SuggestionEntity> expected = Collections.EMPTY_LIST;
    ContainerSuggestions containerSuggestions = testAutocompleteV2Success(prefix, type, catalogEntityKeys, queryContext, ContainerSuggestions.class);
    Assert.assertNotNull(containerSuggestions);
    Assert.assertTrue(containerSuggestions.getSuggestionsType().equals(SuggestionsType.CONTAINER.getType()));
    Assert.assertArrayEquals(expected.toArray(), containerSuggestions.getContainers().toArray());
  }

  /**
   * Home space @dremio is empty
   */
  @Test
  public void testContainersInHomeWithoutPrefix() {
    final String prefix = "";
    final AutocompleteV2RequestType type = AutocompleteV2RequestType.CONTAINER;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Arrays.asList("@dremio"));
    final List<String> queryContext = Collections.EMPTY_LIST;
    final List<SuggestionEntity> expected = Collections.EMPTY_LIST;
    ContainerSuggestions containerSuggestions = testAutocompleteV2Success(prefix, type, catalogEntityKeys, queryContext, ContainerSuggestions.class);
    Assert.assertNotNull(containerSuggestions);
    Assert.assertTrue(containerSuggestions.getSuggestionsType().equals(SuggestionsType.CONTAINER.getType()));
    Assert.assertArrayEquals(expected.toArray(), containerSuggestions.getContainers().toArray());
  }

  @Test
  public void testContainersInHomeWithPrefix() {
    final String prefix = "no-match";
    final AutocompleteV2RequestType type = AutocompleteV2RequestType.CONTAINER;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Arrays.asList("@dremio"));
    final List<String> queryContext = Collections.EMPTY_LIST;
    final List<SuggestionEntity> expected = Collections.EMPTY_LIST;
    ContainerSuggestions containerSuggestions = testAutocompleteV2Success(prefix, type, catalogEntityKeys, queryContext, ContainerSuggestions.class);
    Assert.assertNotNull(containerSuggestions);
    Assert.assertTrue(containerSuggestions.getSuggestionsType().equals(SuggestionsType.CONTAINER.getType()));
    Assert.assertArrayEquals(expected.toArray(), containerSuggestions.getContainers().toArray());
  }

  @Test
  public void testContainersInSpaceWithoutPrefix() {
    final String prefix = "";
    final AutocompleteV2RequestType type = AutocompleteV2RequestType.CONTAINER;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Arrays.asList("testSpace"));
    final List<String> queryContext = Collections.EMPTY_LIST;
    final List<SuggestionEntity> expected = Arrays.asList(
      new SuggestionEntity("[testSpace, myFolder]","folder"),
      new SuggestionEntity("[testSpace, supplier]","virtual"));
    ContainerSuggestions containerSuggestions = testAutocompleteV2Success(prefix, type, catalogEntityKeys, queryContext, ContainerSuggestions.class);
    Assert.assertNotNull(containerSuggestions);
    Assert.assertTrue(containerSuggestions.getSuggestionsType().equals(SuggestionsType.CONTAINER.getType()));
    Assert.assertArrayEquals(expected.toArray(), containerSuggestions.getContainers().toArray());
  }

  @Test
  public void testContainersInSpaceWithPrefix() {
    final String prefix = "My";
    final AutocompleteV2RequestType type = AutocompleteV2RequestType.CONTAINER;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Arrays.asList("testSpace"));
    final List<String> queryContext = Collections.EMPTY_LIST;
    final List<SuggestionEntity> expected = Arrays.asList(
      new SuggestionEntity("[testSpace, myFolder]","folder"));
    ContainerSuggestions containerSuggestions = testAutocompleteV2Success(prefix, type, catalogEntityKeys, queryContext, ContainerSuggestions.class);
    Assert.assertNotNull(containerSuggestions);
    Assert.assertTrue(containerSuggestions.getSuggestionsType().equals(SuggestionsType.CONTAINER.getType()));
    Assert.assertArrayEquals(expected.toArray(), containerSuggestions.getContainers().toArray());
  }

  @Test
  public void testContainersInSpaceFolderWithoutPrefix() {
    final String prefix = "";
    final AutocompleteV2RequestType type = AutocompleteV2RequestType.CONTAINER;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Arrays.asList("testSpace", "myFolder"));
    final List<String> queryContext = Collections.EMPTY_LIST;
    final List<SuggestionEntity> expected = Arrays.asList(
      new SuggestionEntity("[testSpace, myFolder, supplier]","virtual"));
    ContainerSuggestions containerSuggestions = testAutocompleteV2Success(prefix, type, catalogEntityKeys, queryContext, ContainerSuggestions.class);
    Assert.assertNotNull(containerSuggestions);
    Assert.assertTrue(containerSuggestions.getSuggestionsType().equals(SuggestionsType.CONTAINER.getType()));
    Assert.assertArrayEquals(expected.toArray(), containerSuggestions.getContainers().toArray());
  }

  @Test
  public void testContainersInSourceWithoutPrefix() {
    final String prefix = "";
    final AutocompleteV2RequestType type = AutocompleteV2RequestType.CONTAINER;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Arrays.asList("INFORMATION_SCHEMA"));
    final List<String> queryContext = Collections.EMPTY_LIST;
    final List<SuggestionEntity> expected = Arrays.asList(
      new SuggestionEntity("[INFORMATION_SCHEMA, CATALOGS]", "direct"),
      new SuggestionEntity("[INFORMATION_SCHEMA, COLUMNS]", "direct"),
      new SuggestionEntity("[INFORMATION_SCHEMA, SCHEMATA]", "direct"),
      new SuggestionEntity("[INFORMATION_SCHEMA, TABLES]", "direct"),
      new SuggestionEntity("[INFORMATION_SCHEMA, VIEWS]", "direct"));
    ContainerSuggestions containerSuggestions = testAutocompleteV2Success(prefix, type, catalogEntityKeys, queryContext, ContainerSuggestions.class);
    Assert.assertNotNull(containerSuggestions);
    Assert.assertTrue(containerSuggestions.getSuggestionsType().equals(SuggestionsType.CONTAINER.getType()));
    Assert.assertArrayEquals(expected.toArray(), containerSuggestions.getContainers().toArray());
  }

  @Test
  public void testContainersInSourceWithPrefix() {
    final String prefix = "c";
    final AutocompleteV2RequestType type = AutocompleteV2RequestType.CONTAINER;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Arrays.asList("INFORMATION_SCHEMA"));
    final List<String> queryContext = Collections.EMPTY_LIST;
    final List<SuggestionEntity> expected = Arrays.asList(
      new SuggestionEntity("[INFORMATION_SCHEMA, CATALOGS]", "direct"),
      new SuggestionEntity("[INFORMATION_SCHEMA, COLUMNS]", "direct"));
    ContainerSuggestions containerSuggestions = testAutocompleteV2Success(prefix, type, catalogEntityKeys, queryContext, ContainerSuggestions.class);
    Assert.assertNotNull(containerSuggestions);
    Assert.assertTrue(containerSuggestions.getSuggestionsType().equals(SuggestionsType.CONTAINER.getType()));
    Assert.assertArrayEquals(expected.toArray(), containerSuggestions.getContainers().toArray());
  }

  @Test
  public void testColumnsWithoutPrefix() {
    final String prefix = "";
    final AutocompleteV2RequestType type = AutocompleteV2RequestType.COLUMN;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Arrays.asList("testSpace", "supplier"));
    final List<String> queryContext = Collections.EMPTY_LIST;
    final List<SuggestionEntity> expected = Arrays.asList(
      new SuggestionEntity("[testSpace, supplier, s_name]", "TEXT"),
      new SuggestionEntity("[testSpace, supplier, s_phone]", "TEXT"));
    ColumnSuggestions columnSuggestions = testAutocompleteV2Success(prefix, type, catalogEntityKeys, queryContext, ColumnSuggestions.class);
    Assert.assertNotNull(columnSuggestions);
    Assert.assertTrue(columnSuggestions.getSuggestionsType().equals(SuggestionsType.COLUMN.getType()));
    Assert.assertArrayEquals(expected.toArray(), columnSuggestions.getColumns().toArray());
  }

  @Test
  public void testColumnsWithPrefix() {
    final String prefix = "S_n";
    final AutocompleteV2RequestType type = AutocompleteV2RequestType.COLUMN;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Arrays.asList("testSpace", "supplier"));
    final List<String> queryContext = Collections.EMPTY_LIST;
    final List<SuggestionEntity> expected = Arrays.asList(
      new SuggestionEntity("[testSpace, supplier, s_name]", "TEXT"));
    ColumnSuggestions columnSuggestions = testAutocompleteV2Success(prefix, type, catalogEntityKeys, queryContext, ColumnSuggestions.class);
    Assert.assertNotNull(columnSuggestions);
    Assert.assertTrue(columnSuggestions.getSuggestionsType().equals(SuggestionsType.COLUMN.getType()));
    Assert.assertArrayEquals(expected.toArray(), columnSuggestions.getColumns().toArray());
  }

  @Test
  public void testColumnsWithQueryContextIgnored() {
    final String prefix = "S_n";
    final AutocompleteV2RequestType type = AutocompleteV2RequestType.COLUMN;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Arrays.asList("testSpace", "supplier"));
    final List<String> queryContext = Arrays.asList("testSpace");
    final List<SuggestionEntity> expected = Arrays.asList(
      new SuggestionEntity("[testSpace, supplier, s_name]", "TEXT"));
    ColumnSuggestions columnSuggestions = testAutocompleteV2Success(prefix, type, catalogEntityKeys, queryContext, ColumnSuggestions.class);
    Assert.assertNotNull(columnSuggestions);
    Assert.assertTrue(columnSuggestions.getSuggestionsType().equals(SuggestionsType.COLUMN.getType()));
    Assert.assertArrayEquals(expected.toArray(), columnSuggestions.getColumns().toArray());
  }

  @Test
  public void testColumnsInInformationSchemaSourceWithoutPrefix() {
    final String prefix = "";
    final AutocompleteV2RequestType type = AutocompleteV2RequestType.COLUMN;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Arrays.asList("INFORMATION_SCHEMA", "CATALOGS"));
    final List<String> queryContext = Collections.EMPTY_LIST;
    final List<SuggestionEntity> expected = Arrays.asList(
      new SuggestionEntity("[INFORMATION_SCHEMA, CATALOGS, CATALOG_NAME]", "TEXT"),
      new SuggestionEntity("[INFORMATION_SCHEMA, CATALOGS, CATALOG_DESCRIPTION]", "TEXT"),
      new SuggestionEntity("[INFORMATION_SCHEMA, CATALOGS, CATALOG_CONNECT]", "TEXT"));
    ColumnSuggestions columnSuggestions = testAutocompleteV2Success(prefix, type, catalogEntityKeys, queryContext, ColumnSuggestions.class);
    Assert.assertNotNull(columnSuggestions);
    Assert.assertTrue(columnSuggestions.getSuggestionsType().equals(SuggestionsType.COLUMN.getType()));
    Assert.assertArrayEquals(expected.toArray(), columnSuggestions.getColumns().toArray());
  }

  @Test
  public void testColumnsInInformationSchemaSourceWithPrefix() {
    final String prefix = "catalog_con";
    final AutocompleteV2RequestType type = AutocompleteV2RequestType.COLUMN;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Arrays.asList("INFORMATION_SCHEMA", "CATALOGS"));
    final List<String> queryContext = Collections.EMPTY_LIST;
    final List<SuggestionEntity> expected = Arrays.asList(
      new SuggestionEntity("[INFORMATION_SCHEMA, CATALOGS, CATALOG_CONNECT]", "TEXT"));
    ColumnSuggestions columnSuggestions = testAutocompleteV2Success(prefix, type, catalogEntityKeys, queryContext, ColumnSuggestions.class);
    Assert.assertNotNull(columnSuggestions);
    Assert.assertTrue(columnSuggestions.getSuggestionsType().equals(SuggestionsType.COLUMN.getType()));
    Assert.assertArrayEquals(expected.toArray(), columnSuggestions.getColumns().toArray());
  }

  @Test
  public void testColumnsInNonDatasetContainer() {
    final String prefix = "";
    final AutocompleteV2RequestType type = AutocompleteV2RequestType.COLUMN;
    final List<List<String>> catalogEntityKeys = Arrays.asList(Arrays.asList("INFORMATION_SCHEMA"));
    final List<String> queryContext = Collections.EMPTY_LIST;
    final List<SuggestionEntity> expected = Collections.EMPTY_LIST;
    ColumnSuggestions columnSuggestions = testAutocompleteV2Success(prefix, type, catalogEntityKeys, queryContext, ColumnSuggestions.class);
    Assert.assertNotNull(columnSuggestions);
    Assert.assertTrue(columnSuggestions.getSuggestionsType().equals(SuggestionsType.COLUMN.getType()));
    Assert.assertArrayEquals(expected.toArray(), columnSuggestions.getColumns().toArray());
  }

  private String testAutocomplete(String queryString, int cursorPos, List<String> context) {
    final String endpoint = "/sql/autocomplete";

    Response response = expectSuccess(
      getBuilder(getAPIv2().path(endpoint))
          .buildPost(
            Entity.entity(
              new AutocompleteRequestImplementation(queryString, context, cursorPos),
              MediaType.APPLICATION_JSON_TYPE)));
    String json = response.readEntity(String.class);
    return json;
  }

  private void testAutocompleteV2Error(
    String prefix,
    AutocompleteV2RequestType type,
    List<List<String>> catalogEntityKeys,
    List<String> queryContext) {
    final String endpoint = "/sql/autocomplete/v2";

    expectError(
      FamilyExpectation.CLIENT_ERROR,
      getBuilder(getAPIv2().path(endpoint))
        .buildPost(
          Entity.entity(new AutocompleteV2Request(prefix, type, catalogEntityKeys, queryContext, null, null), MediaType.APPLICATION_JSON_TYPE)),
      GenericErrorMessage.class);
  }

  private <T> T testAutocompleteV2Success(
    String prefix,
    AutocompleteV2RequestType type,
    List<List<String>> catalogEntityKeys,
    List<String> queryContext,
    Class<T> entityType) {
    final String endpoint = "/sql/autocomplete/v2";

    Response response = expectSuccess(
      getBuilder(getAPIv2().path(endpoint))
        .buildPost(
          Entity.entity(new AutocompleteV2Request(prefix, type, catalogEntityKeys, queryContext, null, null), MediaType.APPLICATION_JSON_TYPE)));
    T suggestions = response.readEntity(entityType);
    return suggestions;
  }
}
