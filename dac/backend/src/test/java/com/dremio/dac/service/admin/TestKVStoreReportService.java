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
package com.dremio.dac.service.admin;

import static com.dremio.dac.api.TestCatalogResource.getFolderIdByName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.dremio.common.util.TestTools;
import com.dremio.common.utils.PathUtils;
import com.dremio.dac.api.CatalogItem;
import com.dremio.dac.api.Dataset;
import com.dremio.dac.api.Dataset.RefreshSettings;
import com.dremio.dac.api.Folder;
import com.dremio.dac.api.MetadataPolicy;
import com.dremio.dac.api.Source;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.service.admin.KVStoreReportService.KVStoreNotSupportedException;
import com.dremio.dac.service.catalog.CatalogServiceHelper;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.NASConf;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.PartitionChunkId;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.MultiSplit;
import com.dremio.service.namespace.dataset.proto.RefreshMethod;
import com.dremio.service.namespace.file.FileFormat;
import com.dremio.service.namespace.file.proto.JsonFileConfig;
import com.dremio.service.namespace.proto.RefreshPolicyType;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationId;
import com.dremio.service.reflection.proto.MaterializationState;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionGoalState;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.ReflectionState;
import com.dremio.service.reflection.store.MaterializationStore;
import com.dremio.service.reflection.store.ReflectionEntriesStore;
import com.dremio.service.reflection.store.ReflectionGoalsStore;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import javax.inject.Provider;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Test kv store report content. */
public class TestKVStoreReportService extends BaseTestServer {
  private static final String CATALOG_PATH = "/catalog/";

  private static Provider<LegacyKVStoreProvider> legacyKVStoreProviderProvider;
  private static Provider<KVStoreProvider> kvStoreProviderProvider;
  private static KVStoreReportService service;
  private static final ListeningExecutorService executorService =
      MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
  private static int sourcesCount;

  private String partitionChunkId;
  private Dataset createDataset;
  private ReflectionId reflectionId;

  @ClassRule public static final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @BeforeClass
  public static void init() throws Exception {
    Assume.assumeFalse(
        BaseTestServer.isMultinode()); // the api can only run on master node with a local kvstore
    BaseTestServer.init();
    legacyKVStoreProviderProvider = p(LegacyKVStoreProvider.class);
    kvStoreProviderProvider = p(KVStoreProvider.class);
    service =
        new KVStoreReportService(
            legacyKVStoreProviderProvider,
            kvStoreProviderProvider,
            p(NamespaceService.class),
            () -> executorService);
    service.start();
    sourcesCount = getNamespaceService().getSources().size();
  }

  /** prepare the env (source, datasets) for the interested kvstores to have some contents */
  private void createSplitReflectionEntriesInStores() {
    NASConf nasConf = new NASConf();
    nasConf.path = TestTools.getWorkingPath() + "/src/test/resources";

    Source source = new Source();
    source.setName("catalog-test");
    source.setType("NAS");
    source.setConfig(nasConf);
    source.setMetadataPolicy(new MetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY));
    source.setAccelerationRefreshPeriodMs(9800000L);

    // create the source
    source =
        expectSuccess(
            getBuilder(getPublicAPI(3).path(CATALOG_PATH)).buildPost(Entity.json(source)),
            new GenericType<Source>() {});

    assertFalse(
        "check auto-promotion is disabled", source.getMetadataPolicy().isAutoPromoteDatasets());

    doc("browse to the json directory");
    String id = getFolderIdByName(source.getChildren(), "json");
    assertNotNull("Failed to find json directory", id);

    doc("load the json dir");
    Folder folder =
        expectSuccess(
            getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(PathUtils.encodeURIComponent(id)))
                .buildGet(),
            new GenericType<Folder>() {});
    assertEquals(folder.getChildren().size(), 19);

    String fileId = null;

    for (CatalogItem item : folder.getChildren()) {
      List<String> path = item.getPath();
      // get the numbers.json file
      if (item.getType() == CatalogItem.CatalogItemType.FILE
          && path.get(path.size() - 1).equals("numbers.json")) {
        fileId = item.getId();
        break;
      }
    }

    assertNotNull("Failed to find numbers.json file", fileId);

    doc("load the file");
    final com.dremio.dac.api.File file =
        expectSuccess(
            getBuilder(
                    getPublicAPI(3).path(CATALOG_PATH).path(PathUtils.encodeURIComponent(fileId)))
                .buildGet(),
            new GenericType<com.dremio.dac.api.File>() {});

    doc("promote the file (dac/backend/src/test/resources/json/numbers.json)");
    createDataset =
        createPDS(
            UUID.randomUUID().toString(),
            CatalogServiceHelper.getPathFromInternalId(file.getId()),
            new JsonFileConfig());

    createDataset =
        expectSuccess(
            getBuilder(
                    getPublicAPI(3).path(CATALOG_PATH).path(PathUtils.encodeURIComponent(fileId)))
                .buildPost(Entity.json(createDataset)),
            new GenericType<Dataset>() {});

    doc("load the dataset");
    createDataset =
        expectSuccess(
            getBuilder(getPublicAPI(3).path(CATALOG_PATH).path(createDataset.getId())).buildGet(),
            new GenericType<Dataset>() {});

    KVStore<PartitionChunkId, MultiSplit> multiSplitsStore =
        kvStoreProviderProvider.get().getStore(NamespaceServiceImpl.MultiSplitStoreCreator.class);

    partitionChunkId = String.format("%s_%d_%d", UUID.randomUUID(), System.currentTimeMillis(), 0);
    PartitionChunkId chunkId = PartitionChunkId.of(partitionChunkId);

    MultiSplit multiSplit =
        MultiSplit.newBuilder()
            .setMultiSplitKey("10")
            .setSplitCount(5)
            .setSplitData(ByteString.copyFromUtf8("test"))
            .build();
    multiSplitsStore.put(chunkId, multiSplit);

    ReflectionEntriesStore reflectionEntriesStore =
        new ReflectionEntriesStore(legacyKVStoreProviderProvider);
    ReflectionGoalsStore reflectionGoalsStore =
        new ReflectionGoalsStore(legacyKVStoreProviderProvider);
    MaterializationStore materializationStore =
        new MaterializationStore(legacyKVStoreProviderProvider);

    reflectionId = new ReflectionId(UUID.randomUUID().toString());
    ReflectionEntry reflectionEntry =
        new ReflectionEntry()
            .setId(reflectionId)
            .setDatasetId(createDataset.getId())
            .setName("raw-test")
            .setState(ReflectionState.ACTIVE)
            .setGoalVersion(UUID.randomUUID().toString())
            .setLastSuccessfulRefresh(System.currentTimeMillis());
    reflectionEntriesStore.save(reflectionEntry);

    ReflectionGoal reflectionGoal =
        new ReflectionGoal()
            .setId(reflectionId)
            .setDatasetId(createDataset.getId())
            .setName("test-goal-name")
            .setState(ReflectionGoalState.ENABLED)
            .setVersion(System.currentTimeMillis());
    reflectionGoalsStore.save(reflectionGoal);

    final Materialization newMaterialization =
        new Materialization()
            .setId(new MaterializationId(UUID.randomUUID().toString()))
            .setReflectionId(reflectionEntry.getId())
            .setState(MaterializationState.RUNNING)
            .setExpiration(System.currentTimeMillis() + 360_000)
            .setInitRefreshSubmit(System.currentTimeMillis());
    materializationStore.save(newMaterialization);
  }

  private Dataset createPDS(String id, List<String> path, FileFormat format) {
    RefreshSettings refreshPolicy =
        new RefreshSettings(
            RefreshPolicyType.PERIOD,
            "someField",
            3600000L,
            null,
            10800000L,
            RefreshMethod.FULL,
            false,
            false,
            false);
    return new Dataset(
        id,
        Dataset.DatasetType.PHYSICAL_DATASET,
        path,
        null,
        System.currentTimeMillis(),
        null,
        refreshPolicy,
        null,
        null,
        format,
        null);
  }

  /** mock {@link com.dremio.dac.resource.KVStoreReportResource#doDownload(List)} method */
  private void doDownload(List<String> storeNames, String filename)
      throws IOException, KVStoreNotSupportedException, ExecutionException, InterruptedException {
    BufferedInputStream pipeIs = new BufferedInputStream(service.getSplitReport(storeNames));

    ListenableFuture<Object> future =
        executorService.submit(
            () -> {
              try (FileOutputStream fos = new FileOutputStream(temporaryFolder.newFile(filename));
                  BufferedOutputStream output = new BufferedOutputStream(fos);
                  BufferedInputStream toClose2 = pipeIs) {
                byte[] buf = new byte[KVStoreReportService.BUFFER_SIZE];
                int len;
                while ((len = pipeIs.read(buf)) > -1) {
                  if (len < KVStoreReportService.BUFFER_SIZE) {
                    output.write(Arrays.copyOf(buf, len));
                  } else {
                    output.write(buf);
                  }
                }
              } catch (IOException e) {
                Assert.fail("Failed to write to output." + e.toString());
              }
              return null;
            });
    // wait for the zip to complete
    future.get();
  }

  /**
   * verify the number of values on the second line matches the number of column names from the
   * first line return the second line
   */
  private String checksecondLineMatchColumnsCount(BufferedReader bufferedReader)
      throws IOException {
    String firstLine = bufferedReader.readLine();
    assertNotNull("File shouldn't be empty.", firstLine);
    String secondLine = bufferedReader.readLine();
    assertNotNull("Table shouldn't be empty.", secondLine);
    assertEquals(
        "Number of cells in second line does not match number of columns.",
        firstLine.split(",").length,
        secondLine.split(",").length);
    return secondLine;
  }

  /**
   * Check if the zip contains the right entries. Ignore each entry's content.
   *
   * @param expectedEntries a list of expected entries name
   * @param storeNames a list of store names (inputs from query params)
   * @param zipFileName downloaded zip filename to distinguish the resulting zip file from other
   *     tests
   */
  private void checkEntriesOnly(
      List<String> expectedEntries, List<String> storeNames, String zipFileName)
      throws IOException, KVStoreNotSupportedException, ExecutionException, InterruptedException {
    doDownload(storeNames, zipFileName);
    File file =
        Objects.requireNonNull(
            temporaryFolder.getRoot().listFiles((dir, name) -> name.equals(zipFileName)))[0];
    try (ZipFile zipFile = new ZipFile(file)) {
      Enumeration<? extends ZipEntry> entries = zipFile.entries();
      int entriesCount = 0;
      while (entries.hasMoreElements()) {
        entriesCount++;
        String entryName = entries.nextElement().getName();
        assertTrue(
            String.format(
                "Entry %s is not part of %s", entryName, String.join(",", expectedEntries)),
            expectedEntries.contains(entryName));
      }
      assertEquals(
          String.format(
              "The zip should have %d files but have %d files",
              expectedEntries.size(), entriesCount),
          expectedEntries.size(),
          entriesCount);
    }
  }

  @Test
  public void testAllStoresReport() throws Exception {
    // prepare the env
    createSplitReflectionEntriesInStores();

    // generate the zip file
    doDownload(ImmutableList.of(), "all-kv-store.zip");

    // verify the zip content
    JSONParser jsonParser = new JSONParser();
    File file =
        Objects.requireNonNull(
            temporaryFolder.getRoot().listFiles((dir, name) -> name.equals("all-kv-store.zip")))[0];
    try (ZipFile zipFile = new ZipFile(file)) {
      verifyZipEntries(zipFile, jsonParser);
    }
  }

  private void verifyZipEntries(ZipFile zipFile, JSONParser jsonParser) throws Exception {
    Enumeration<? extends ZipEntry> entries = zipFile.entries();
    int entriesCount = 0;
    while (entries.hasMoreElements()) {
      entriesCount++;
      ZipEntry entry = entries.nextElement();
      switch (entry.getName()) {
        case "sources.json":
          // verify number of sources and the created source with correct value
          try (BufferedInputStream bufferedInputStream =
              new BufferedInputStream(zipFile.getInputStream(entry)); ) {
            Object obj = jsonParser.parse(bufferedInputStream);
            JSONArray sourcesList = (JSONArray) obj;
            assertEquals("Number of sources do not match.", sourcesCount + 1, sourcesList.size());

            Optional<Object> targetObj =
                sourcesList.stream()
                    .filter(o -> ((JSONObject) o).get("name").equals("catalog-test"))
                    .findFirst();
            assertTrue(
                "Source catalog-test does not exist in the sources.json", targetObj.isPresent());
            assertEquals(
                "Source reflection refresh policy does not match.",
                9800000,
                ((JSONObject) targetObj.get()).get("accelerationRefreshPeriod"));
          }
          break;

        case "kvstores_stats.log":
          // verify kvstore stats is not empty
          try (BufferedInputStream bufferedInputStream =
              new BufferedInputStream(zipFile.getInputStream(entry)); ) {
            assertTrue("kvstores_stats.log shouldn't be empty.", bufferedInputStream.read() > -1);
          }
          break;

        case KVStoreReportService.MULTI_SPLITS + ".csv":
          // verify the row is the same as the one inserted in multi-split store when preparing the
          // env
          try (BufferedReader bufferedReader =
              new BufferedReader(new InputStreamReader(zipFile.getInputStream(entry))); ) {
            String secondLine = checksecondLineMatchColumnsCount(bufferedReader);
            List<String> expected =
                ImmutableList.of(
                    partitionChunkId.split("_")[0],
                    partitionChunkId.split("_")[1],
                    partitionChunkId.split("_")[2],
                    "12",
                    "4",
                    "5",
                    "10");
            assertEquals(
                "Values of second line are incorrect.",
                expected,
                Arrays.asList(secondLine.split(",")));
          }
          break;

        case KVStoreReportService.DATASET_SPLITS + ".csv":
          // verify if the row with the created dataset id exist in this zip entry content
          try (BufferedReader bufferedReader =
              new BufferedReader(new InputStreamReader(zipFile.getInputStream(entry))); ) {
            String line = checksecondLineMatchColumnsCount(bufferedReader);
            String createDatasetId = createDataset.getId();
            while (line != null && !line.split(",")[0].equals(createDatasetId)) {
              line = bufferedReader.readLine();
            }
            assertNotNull(
                String.format(
                    "Could not find the created dataset in %s.csv",
                    KVStoreReportService.DATASET_SPLITS),
                line);
          }
          break;

        case KVStoreReportService.NAMESPACE + ".csv":
          // verify if the row with the created dataset path exist in this zip entry
          try (BufferedReader bufferedReader =
              new BufferedReader(new InputStreamReader(zipFile.getInputStream(entry))); ) {
            String line = checksecondLineMatchColumnsCount(bufferedReader);
            String createDatasetPath = String.join("/", createDataset.getPath());
            while (line != null && !line.split(",")[0].equals(createDatasetPath)) {
              line = bufferedReader.readLine();
            }
            assertNotNull(
                String.format(
                    "Could not find the created dataset in %s.csv", KVStoreReportService.NAMESPACE),
                line);
          }
          break;

        case KVStoreReportService.REFLECTION_ENTRIES + ".csv":
          // verify if the single row in this zip entry contains the correct reflection id and
          // reflection entry name
          try (BufferedReader bufferedReader =
              new BufferedReader(new InputStreamReader(zipFile.getInputStream(entry))); ) {
            String secondLine = checksecondLineMatchColumnsCount(bufferedReader);
            String[] secondLineValues = secondLine.split(",");
            assertEquals("reflection id is incorrect.", reflectionId.getId(), secondLineValues[0]);
            assertEquals("reflection entry name is incorrect.", "raw-test", secondLineValues[2]);
          }
          break;

        case KVStoreReportService.REFLECTION_GOALS + ".csv":
          // verify if the single row in this zip entry contains the correct reflection id and
          // reflection goal name
          try (BufferedReader bufferedReader =
              new BufferedReader(new InputStreamReader(zipFile.getInputStream(entry))); ) {
            String secondLine = checksecondLineMatchColumnsCount(bufferedReader);
            String[] secondLineValues = secondLine.split(",");
            assertEquals("reflection id is incorrect.", reflectionId.getId(), secondLineValues[0]);
            assertEquals(
                "reflection goal name is incorrect.", "test-goal-name", secondLineValues[2]);
          }
          break;

        case KVStoreReportService.MATERIALIZATION + ".csv":
          // verify if the single row in this zip entry contains the correct reflection id and
          // number_partitions
          try (BufferedReader bufferedReader =
              new BufferedReader(new InputStreamReader(zipFile.getInputStream(entry))); ) {
            String secondLine = checksecondLineMatchColumnsCount(bufferedReader);
            String[] secondLineValues = secondLine.split(",");
            assertEquals("reflection id is incorrect.", reflectionId.getId(), secondLineValues[1]);
            assertEquals("num_partitions is incorrect.", "0", secondLineValues[9]);
          }
          break;

        default:
          assertNotEquals("Zip doesn't expect error", "error_report.log", entry.getName());
          Assert.fail("Invalid entry in zip: " + entry.getName());
      }
    }
    assertEquals("The zip should have 8 files", 8, entriesCount);
  }

  @Test
  public void testDuplicateStoreNames() throws IOException {
    List<String> expectedEntries =
        ImmutableList.of(
            KVStoreReportService.REFLECTION_ENTRIES + ".csv", "kvstores_stats.log", "sources.json");
    List<String> inputs =
        ImmutableList.of(
            KVStoreReportService.REFLECTION_ENTRIES, KVStoreReportService.REFLECTION_ENTRIES);
    try {
      checkEntriesOnly(expectedEntries, inputs, "test_duplicate_store_names-report.zip");
    } catch (KVStoreNotSupportedException e) {
      Assert.fail(String.format("input %s should be supported.", String.join(",", inputs)));
    } catch (ExecutionException | InterruptedException e) {
      Assert.fail("Failed to generate the zip. " + e.toString());
    }
  }

  @Test
  public void testMultipleStoreNames() throws IOException {
    List<String> expectedEntries =
        ImmutableList.of(
            KVStoreReportService.REFLECTION_ENTRIES + ".csv",
            KVStoreReportService.MATERIALIZATION + ".csv",
            "kvstores_stats.log",
            "sources.json");
    List<String> inputs =
        ImmutableList.of(
            KVStoreReportService.REFLECTION_ENTRIES,
            KVStoreReportService.REFLECTION_ENTRIES,
            KVStoreReportService.MATERIALIZATION);
    try {
      checkEntriesOnly(expectedEntries, inputs, "test_multiple_stores-report.zip");
    } catch (KVStoreNotSupportedException e) {
      Assert.fail(String.format("input %s should be supported.", String.join(",", inputs)));
    } catch (ExecutionException | InterruptedException e) {
      Assert.fail("Failed to generate the zip. " + e.toString());
    }
  }

  @Test
  public void testNoAnalysis() throws IOException {
    List<String> expectedEntries = ImmutableList.of("kvstores_stats.log", "sources.json");
    List<String> inputs = ImmutableList.of(KVStoreReportService.NO_ANALYSIS);
    try {
      checkEntriesOnly(expectedEntries, inputs, "test_none-report.zip");
    } catch (KVStoreNotSupportedException e) {
      Assert.fail(String.format("input %s should be supported.", String.join(",", inputs)));
    } catch (ExecutionException | InterruptedException e) {
      Assert.fail("Failed to generate the zip. " + e.toString());
    }
  }

  @Test
  public void testInputwithNoAnalysisAndOtherStores() throws IOException {
    List<String> expectedEntries =
        ImmutableList.of(
            KVStoreReportService.MATERIALIZATION + ".csv", "kvstores_stats.log", "sources.json");
    List<String> inputs =
        ImmutableList.of(KVStoreReportService.NO_ANALYSIS, KVStoreReportService.MATERIALIZATION);
    try {
      checkEntriesOnly(
          expectedEntries, inputs, "test_input_with_none_and_materialization_sources-report.zip");
    } catch (KVStoreNotSupportedException e) {
      Assert.fail(String.format("input %s should be supported.", String.join(",", inputs)));
    } catch (ExecutionException | InterruptedException e) {
      Assert.fail("Failed to generate the zip. " + e.toString());
    }
  }

  @Test
  public void testInvalidName() throws IOException {
    try {
      checkEntriesOnly(
          ImmutableList.of(),
          ImmutableList.of("invalid-name", KVStoreReportService.MATERIALIZATION),
          "test_invalid_input-report.zip");
    } catch (KVStoreNotSupportedException e) {
      return;
    } catch (ExecutionException | InterruptedException e) {
      Assert.fail("Failed to generate the zip. " + e.toString());
    }
    Assert.fail("Invalid input. Should throw.");
  }
}
