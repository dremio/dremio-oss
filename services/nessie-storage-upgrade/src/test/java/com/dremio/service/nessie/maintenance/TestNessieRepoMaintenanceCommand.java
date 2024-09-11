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
package com.dremio.service.nessie.maintenance;

import static com.dremio.test.DremioTest.CLASSPATH_SCAN_RESULT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.common.SuppressForbidden;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.api.IndexedStore;
import com.dremio.service.embedded.catalog.EmbeddedUnversionedStore;
import com.dremio.service.namespace.NamespaceServiceImpl.NamespaceStoreCreator;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.nessie.maintenance.NessieRepoMaintenanceCommand.Options;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Put;
import org.slf4j.helpers.MessageFormatter;

class TestNessieRepoMaintenanceCommand {

  private LocalKVStoreProvider storeProvider;
  private EmbeddedUnversionedStore store;

  @BeforeEach
  void createKVStore() throws Exception {
    storeProvider = new LocalKVStoreProvider(CLASSPATH_SCAN_RESULT, null, true, false); // in-memory
    storeProvider.start();

    store = new EmbeddedUnversionedStore(() -> storeProvider);
  }

  @AfterEach
  void stopKVStore() throws Exception {
    if (storeProvider != null) {
      storeProvider.close();
    }
  }

  @Test
  void testListKeys() throws Exception {
    // Just a smoke test. This option is not meant for production use.
    NessieRepoMaintenanceCommand.execute(
        storeProvider, Options.parse(new String[] {"--list-keys"}));
  }

  @Test
  @SuppressForbidden // Nessie's relocated ByteString is required to interface with Nessie Database
  // Adapters.
  void testListObsoleteInternalKeys() throws Exception {
    String tableId2 = "8ec5373f-d2a6-4b1a-a870-e18046bbd6ae";
    String tableId3 = "34dadc3a-ae44-4e61-b78e-0295c089df70";
    store.commit(
        BranchName.of("main"),
        Optional.empty(),
        CommitMeta.fromMessage("test-meta"),
        ImmutableList.of(
            Put.of(ContentKey.of("test1"), IcebergTable.of("t1", 0, 0, 0, 0)),
            Put.of(
                ContentKey.of("dremio.internal", "test2/" + tableId2),
                IcebergTable.of("t2", 0, 0, 0, 0)),
            Put.of(
                ContentKey.of("dremio.internal", "test3/" + tableId3),
                IcebergTable.of("t3", 0, 0, 0, 0))));

    IndexedStore<String, NameSpaceContainer> namespace =
        storeProvider.getStore(NamespaceStoreCreator.class);
    IcebergMetadata metadata = new IcebergMetadata();
    metadata.setTableUuid(tableId3);
    metadata.setMetadataFileLocation("test-location");
    namespace.put(
        "dataset3",
        new NameSpaceContainer()
            .setFullPathList(ImmutableList.of("ns", "dataset"))
            .setType(NameSpaceContainer.Type.DATASET)
            .setDataset(
                new DatasetConfig()
                    .setType(DatasetType.PHYSICAL_DATASET_SOURCE_FILE)
                    .setId(new EntityId("ds-id3"))
                    .setPhysicalDataset(new PhysicalDataset().setIcebergMetadata(metadata))));

    // Note: This DataSet has null TableUuid in IcebergMetadata, which represents a first-class
    // Iceberg table,
    // not a DataSet promoted from plain Parquet files.
    String dataset4Tag =
        namespace
            .put(
                "dataset4",
                new NameSpaceContainer()
                    .setFullPathList(ImmutableList.of("ns", "dataset4"))
                    .setType(NameSpaceContainer.Type.DATASET)
                    .setDataset(
                        new DatasetConfig()
                            .setType(DatasetType.PHYSICAL_DATASET_SOURCE_FILE)
                            .setId(new EntityId("ds-id4"))
                            .setPhysicalDataset(
                                new PhysicalDataset().setIcebergMetadata(new IcebergMetadata()))))
            .getTag();

    List<String> log = new ArrayList<>();
    NessieRepoMaintenanceCommand.execute(
        storeProvider,
        Options.parse(new String[] {"--list-obsolete-internal-keys"}),
        (msg, args) -> log.add(MessageFormatter.arrayFormat(msg, args).getMessage()));

    // Note: "test1" is not an "internal" key, so it is not reported
    assertThat(log).containsExactly("dremio.internal|test2/8ec5373f-d2a6-4b1a-a870-e18046bbd6ae");

    String tableId4 = "e14e4ecb-d39c-4311-84fc-2127cc11f195";
    IcebergMetadata metadata4 = new IcebergMetadata();
    metadata4.setTableUuid(tableId4);
    metadata4.setMetadataFileLocation("test-location");
    namespace.put(
        "dataset4",
        new NameSpaceContainer()
            .setFullPathList(ImmutableList.of("ns", "dataset4"))
            .setType(NameSpaceContainer.Type.DATASET)
            .setDataset(
                new DatasetConfig()
                    .setType(DatasetType.PHYSICAL_DATASET_SOURCE_FILE)
                    .setId(new EntityId("ds-id4"))
                    .setTag(dataset4Tag)
                    .setPhysicalDataset(new PhysicalDataset().setIcebergMetadata(metadata4))));

    log.clear();
    assertThatThrownBy(
            () ->
                NessieRepoMaintenanceCommand.execute(
                    storeProvider,
                    Options.parse(new String[] {"--list-obsolete-internal-keys"}),
                    (msg, args) -> log.add(MessageFormatter.arrayFormat(msg, args).getMessage())))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Keys for some table IDs were not found");

    assertThat(log)
        .containsExactly(
            "dremio.internal|test2/8ec5373f-d2a6-4b1a-a870-e18046bbd6ae",
            "Live metadata table ID: e14e4ecb-d39c-4311-84fc-2127cc11f195 does not have a corresponding Nessie key");
  }
}
