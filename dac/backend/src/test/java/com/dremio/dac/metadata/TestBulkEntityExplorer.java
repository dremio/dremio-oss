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
package com.dremio.dac.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.common.concurrent.bulk.BulkRequest;
import com.dremio.common.concurrent.bulk.BulkResponse;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.Optional;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.junit.BeforeClass;
import org.junit.Test;

public abstract class TestBulkEntityExplorer extends BaseTestServer {

  protected static final String SOURCE_1 = "source1";
  protected static final String SOURCE_2 = "source2";
  protected static final String SOURCE_1_EXTERNAL = "source1_external";
  protected static final String SOURCE_2_EXTERNAL = "source2_external";
  private static final String TABLE_NAME_PREFIX = "table_";

  private static Catalog catalog;

  private static final PrimitiveIterator.OfInt random =
      ThreadLocalRandom.current().ints(10_000, 1_000, 1_000_000).distinct().iterator();

  @BeforeClass
  public static void init() throws Exception {
    BaseTestServer.init();
    BaseTestServer.getPopulator().populateTestUsers();
    catalog = getCatalogService().getSystemUserCatalog();
  }

  @Test
  public void testSingleTableLookupUnpromoted() throws Exception {
    String tableName = TABLE_NAME_PREFIX + random.nextInt();
    ExpectedIcebergMetadata expected = createIcebergTable(SOURCE_1, tableName);

    NamespaceKey key = new NamespaceKey(ImmutableList.of(SOURCE_1, tableName));
    BulkRequest<NamespaceKey> request = BulkRequest.<NamespaceKey>builder().add(key).build();
    BulkResponse<NamespaceKey, Optional<DremioTable>> response = catalog.bulkGetTables(request);
    validate(response, expected);
  }

  @Test
  public void testSingleTableLookupCached() throws Exception {
    String tableName = TABLE_NAME_PREFIX + random.nextInt();
    ExpectedIcebergMetadata expected = createIcebergTable(SOURCE_1, tableName);

    NamespaceKey key = new NamespaceKey(ImmutableList.of(SOURCE_1, tableName));
    DremioTable dremioTable = catalog.getTable(key);
    assertThat(dremioTable).isNotNull();

    BulkRequest<NamespaceKey> request = BulkRequest.<NamespaceKey>builder().add(key).build();
    BulkResponse<NamespaceKey, Optional<DremioTable>> response = catalog.bulkGetTables(request);
    validate(response, expected);
  }

  @Test
  public void testMultiSourceMultiTableLookup() throws Exception {
    String table1 = TABLE_NAME_PREFIX + random.nextInt();
    String table2 = TABLE_NAME_PREFIX + random.nextInt();
    String table3 = TABLE_NAME_PREFIX + random.nextInt();
    String table4 = TABLE_NAME_PREFIX + random.nextInt();
    String table5 = TABLE_NAME_PREFIX + random.nextInt();
    String table6 = TABLE_NAME_PREFIX + random.nextInt();

    ExpectedIcebergMetadata expected1 = createIcebergTable(SOURCE_1, table1);
    ExpectedIcebergMetadata expected2 = createIcebergTable(SOURCE_1, table2);
    ExpectedIcebergMetadata expected3 = createIcebergTable(SOURCE_1, table3);
    ExpectedIcebergMetadata expected4 = createIcebergTable(SOURCE_2, table4);
    ExpectedIcebergMetadata expected5 = createIcebergTable(SOURCE_2, table5);
    ExpectedIcebergMetadata expected6 = createIcebergTable(SOURCE_2, table6);

    NamespaceKey key1 = new NamespaceKey(ImmutableList.of(SOURCE_1, table1));
    NamespaceKey key2 = new NamespaceKey(ImmutableList.of(SOURCE_1, table2));
    NamespaceKey key3 = new NamespaceKey(ImmutableList.of(SOURCE_1, table3));
    NamespaceKey key4 = new NamespaceKey(ImmutableList.of(SOURCE_2, table4));
    NamespaceKey key5 = new NamespaceKey(ImmutableList.of(SOURCE_2, table5));
    NamespaceKey key6 = new NamespaceKey(ImmutableList.of(SOURCE_2, table6));

    // fetch 4 tables with autopromote, and 2 more from cache
    DremioTable dremioTable1 = catalog.getTable(key1);
    DremioTable dremioTable4 = catalog.getTable(key4);
    assertThat(dremioTable1).isNotNull();
    assertThat(dremioTable4).isNotNull();
    BulkRequest<NamespaceKey> request =
        BulkRequest.<NamespaceKey>builder()
            .add(key1)
            .add(key2)
            .add(key3)
            .add(key4)
            .add(key5)
            .add(key6)
            .build();
    BulkResponse<NamespaceKey, Optional<DremioTable>> response = catalog.bulkGetTables(request);
    validate(response, expected1, expected2, expected3, expected4, expected5, expected6);
  }

  @Test
  public void testInvalidKeyLookup() throws Exception {
    String invalidTable = TABLE_NAME_PREFIX + random.nextInt();
    NamespaceKey key = new NamespaceKey(ImmutableList.of(SOURCE_1, invalidTable));

    BulkRequest<NamespaceKey> request = BulkRequest.<NamespaceKey>builder().add(key).build();
    BulkResponse<NamespaceKey, Optional<DremioTable>> response = catalog.bulkGetTables(request);
    validate(response, ExpectedInvalidKey.of(key));
  }

  @Test
  public void testValidAndInvalidKeyLookup() throws Exception {
    String invalidTable = TABLE_NAME_PREFIX + random.nextInt();
    NamespaceKey invalidKey = new NamespaceKey(ImmutableList.of(SOURCE_1, invalidTable));

    String table1 = TABLE_NAME_PREFIX + random.nextInt();
    String table2 = TABLE_NAME_PREFIX + random.nextInt();
    String table3 = TABLE_NAME_PREFIX + random.nextInt();
    String table4 = TABLE_NAME_PREFIX + random.nextInt();
    String table5 = TABLE_NAME_PREFIX + random.nextInt();
    String table6 = TABLE_NAME_PREFIX + random.nextInt();

    ExpectedIcebergMetadata expected1 = createIcebergTable(SOURCE_1, table1);
    ExpectedIcebergMetadata expected2 = createIcebergTable(SOURCE_1, table2);
    ExpectedIcebergMetadata expected3 = createIcebergTable(SOURCE_1, table3);
    ExpectedIcebergMetadata expected4 = createIcebergTable(SOURCE_2, table4);
    ExpectedIcebergMetadata expected5 = createIcebergTable(SOURCE_2, table5);
    ExpectedIcebergMetadata expected6 = createIcebergTable(SOURCE_2, table6);

    NamespaceKey key1 = new NamespaceKey(ImmutableList.of(SOURCE_1, table1));
    NamespaceKey key2 = new NamespaceKey(ImmutableList.of(SOURCE_1, table2));
    NamespaceKey key3 = new NamespaceKey(ImmutableList.of(SOURCE_1, table3));
    NamespaceKey key4 = new NamespaceKey(ImmutableList.of(SOURCE_2, table4));
    NamespaceKey key5 = new NamespaceKey(ImmutableList.of(SOURCE_2, table5));
    NamespaceKey key6 = new NamespaceKey(ImmutableList.of(SOURCE_2, table6));

    BulkRequest<NamespaceKey> request =
        BulkRequest.<NamespaceKey>builder()
            .add(invalidKey)
            .add(key1)
            .add(key2)
            .add(key3)
            .add(key4)
            .add(key5)
            .add(key6)
            .build();
    BulkResponse<NamespaceKey, Optional<DremioTable>> response = catalog.bulkGetTables(request);
    validate(
        response,
        ExpectedInvalidKey.of(invalidKey),
        expected1,
        expected2,
        expected3,
        expected4,
        expected5,
        expected6);
  }

  private ExpectedIcebergMetadata createIcebergTable(String source, String name) {
    String external = source + "_external";
    submitJobAndWaitUntilCompletion(
        JobRequest.newBuilder()
            .setSqlQuery(
                getQueryFromSQL(
                    String.format(
                        "create table %s.%s as select * from (values ('a', 'b', 'c'))",
                        external, name)))
            .build());
    DremioTable table = catalog.getTable(new NamespaceKey(ImmutableList.of(external, name)));
    long snapshotId = getSnapshotId(table);

    NamespaceKey expectedKey = new NamespaceKey(ImmutableList.of(source, name));
    NamespaceKey expectedPath =
        new NamespaceKey(
            table.getPath().getPathComponents().stream()
                .map(s -> s.equalsIgnoreCase(external) ? source : s)
                .collect(ImmutableList.toImmutableList()));
    return ExpectedIcebergMetadata.of(expectedKey, expectedPath, snapshotId);
  }

  private void validate(
      BulkResponse<NamespaceKey, Optional<DremioTable>> responses,
      ExpectedTableMetadata... expectedTables)
      throws Exception {
    Set<NamespaceKey> expectedKeys =
        Arrays.stream(expectedTables).map(ExpectedTableMetadata::key).collect(Collectors.toSet());
    assertThat(responses.keys()).containsExactlyInAnyOrderElementsOf(expectedKeys);

    for (ExpectedTableMetadata expectedMetadata : expectedTables) {
      expectedMetadata.validate(responses.get(expectedMetadata.key()));
    }
  }

  private static long getSnapshotId(DremioTable table) {
    return Optional.ofNullable(table)
        .map(DremioTable::getDatasetConfig)
        .map(DatasetConfig::getPhysicalDataset)
        .map(PhysicalDataset::getIcebergMetadata)
        .map(IcebergMetadata::getSnapshotId)
        .orElseThrow();
  }

  private abstract static class ExpectedTableMetadata {
    private final NamespaceKey key;
    private final NamespaceKey path;

    private ExpectedTableMetadata(NamespaceKey key, NamespaceKey path) {
      this.key = key;
      this.path = path;
    }

    public NamespaceKey key() {
      return key;
    }

    public void validate(BulkResponse.Response<NamespaceKey, Optional<DremioTable>> response)
        throws Exception {
      assertThat(response.key()).isEqualTo(key);

      Optional<DremioTable> optTable = response.response().toCompletableFuture().get();
      optTable.ifPresent(table -> assertThat(table.getPath()).isEqualTo(path));
    }
  }

  private static final class ExpectedIcebergMetadata extends ExpectedTableMetadata {
    private final long snapshotId;

    private ExpectedIcebergMetadata(NamespaceKey key, NamespaceKey fullPath, long snapshotId) {
      super(key, fullPath);
      this.snapshotId = snapshotId;
    }

    public static ExpectedIcebergMetadata of(NamespaceKey key, NamespaceKey path, long snapshotId) {
      return new ExpectedIcebergMetadata(key, path, snapshotId);
    }

    @Override
    public void validate(BulkResponse.Response<NamespaceKey, Optional<DremioTable>> response)
        throws Exception {
      super.validate(response);

      Optional<DremioTable> optTable = response.response().toCompletableFuture().get();
      assertThat(optTable).isPresent();
      DatasetConfig datasetConfig = optTable.get().getDatasetConfig();
      assertThat(datasetConfig.getPhysicalDataset()).isNotNull();
      assertThat(datasetConfig.getPhysicalDataset().getIcebergMetadata()).isNotNull();

      IcebergMetadata icebergMetadata = datasetConfig.getPhysicalDataset().getIcebergMetadata();
      assertThat(icebergMetadata.getSnapshotId()).isEqualTo(snapshotId);
    }
  }

  private static final class ExpectedInvalidKey extends ExpectedTableMetadata {

    private ExpectedInvalidKey(NamespaceKey key) {
      super(key, null);
    }

    public static ExpectedInvalidKey of(NamespaceKey key) {
      return new ExpectedInvalidKey(key);
    }

    @Override
    public void validate(BulkResponse.Response<NamespaceKey, Optional<DremioTable>> response)
        throws Exception {
      Optional<DremioTable> optTable = response.response().toCompletableFuture().get();
      assertThat(optTable).isEmpty();
    }
  }
}
