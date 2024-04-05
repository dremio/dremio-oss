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
package com.dremio.service.embedded.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.api.KVStore;
import com.dremio.services.nessie.grpc.client.GrpcClientBuilder;
import com.dremio.test.DremioTest;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.NessieBadRequestException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;

class TestEmbeddedMetadataPointerService {

  private static LocalKVStoreProvider storeProvider;
  private static EmbeddedMetadataPointerService service;
  private static Server server;
  private static ManagedChannel channel;
  private static NessieApiV2 api;

  @BeforeAll
  static void setup() throws Exception {
    storeProvider =
        new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT, null, true, false); // in-memory
    storeProvider.start();

    service = new EmbeddedMetadataPointerService(() -> storeProvider);
    service.start();

    String serverName = InProcessServerBuilder.generateName();
    InProcessServerBuilder serverBuilder =
        InProcessServerBuilder.forName(serverName).directExecutor();
    service.getGrpcServices().forEach(serverBuilder::addService);
    server = serverBuilder.build().start();
    channel = InProcessChannelBuilder.forName(serverName).build();

    api = GrpcClientBuilder.builder().withChannel(channel).build(NessieApiV2.class);
  }

  @AfterAll
  static void shutdown() throws Exception {
    channel.shutdown();
    server.shutdown();
    service.close();
    storeProvider.close();
  }

  @BeforeEach
  void wipeKVStore() {
    KVStore<String, String> kvStore = storeProvider.getStore(EmbeddedPointerStoreBuilder.class);
    kvStore.find().forEach(doc -> kvStore.delete(doc.getKey()));
  }

  @Test
  void getDefaultBranch() throws NessieNotFoundException {
    assertThat(api.getDefaultBranch().getHash()).isNotBlank();
    assertThat(api.getDefaultBranch().getName()).isEqualTo("main");
  }

  @Test
  void createTable() throws NessieNotFoundException, NessieConflictException {
    ContentKey key1 = ContentKey.of("test.ns", "table1");
    ContentKey key2 = ContentKey.of("test.ns", "table2");
    Branch branch =
        api.commitMultipleOperations()
            .branch(api.getDefaultBranch())
            .commitMeta(CommitMeta.fromMessage("test"))
            .operation(Operation.Unchanged.of(key1)) // ignored
            .operation(Operation.Put.of(key1, IcebergTable.of("loc111", 0, 0, 0, 0)))
            .operation(Operation.Put.of(key2, IcebergTable.of("loc222", 0, 0, 0, 0)))
            .commit();
    assertThat(branch.getHash()).isNotBlank();
    assertThat(branch.getName()).isEqualTo("main");

    assertThat(api.getContent().refName("main").getSingle(key1).getContent())
        .asInstanceOf(type(IcebergTable.class))
        .satisfies(t -> assertThat(t.getMetadataLocation()).isEqualTo("loc111"))
        .satisfies(t -> assertThat(t.getId()).isNotBlank());
    assertThat(api.getContent().refName("main").getSingle(key2).getContent())
        .asInstanceOf(type(IcebergTable.class))
        .satisfies(t -> assertThat(t.getMetadataLocation()).isEqualTo("loc222"))
        .satisfies(t -> assertThat(t.getId()).isNotBlank());

    assertThat(api.getContent().key(key1).key(key2).get())
        .extracting(r -> r.get(key1), r -> r.get(key2))
        .asInstanceOf(list(IcebergTable.class))
        .extracting(IcebergTable::getMetadataLocation)
        .containsExactlyInAnyOrder("loc111", "loc222");

    assertThat(api.getEntries().refName("main").key(key1).key(key2).get().getEntries())
        .extracting(EntriesResponse.Entry::getName)
        .containsExactlyInAnyOrder(key1, key2);
    assertThat(api.getEntries().refName("main").key(key1).key(key2).get().getEntries())
        .extracting(EntriesResponse.Entry::getContent)
        .asInstanceOf(list(IcebergTable.class))
        .extracting(IcebergTable::getMetadataLocation)
        .containsExactlyInAnyOrder("loc111", "loc222");
  }

  @Test
  void updateTable() throws NessieNotFoundException, NessieConflictException {
    ContentKey key1 = ContentKey.of("test.ns", "table1");
    Branch branch =
        api.commitMultipleOperations()
            .branch(api.getDefaultBranch())
            .commitMeta(CommitMeta.fromMessage("test"))
            .operation(Operation.Put.of(key1, IcebergTable.of("loc111", 0, 0, 0, 0)))
            .commit();
    assertThat(branch.getHash()).isNotBlank();
    assertThat(branch.getName()).isEqualTo("main");

    Content table = api.getContent().refName("main").getSingle(key1).getContent();
    assertThat(table)
        .asInstanceOf(type(IcebergTable.class))
        .extracting(IcebergTable::getMetadataLocation)
        .isEqualTo("loc111");

    branch =
        api.commitMultipleOperations()
            .branch(api.getDefaultBranch())
            .commitMeta(CommitMeta.fromMessage("test"))
            .operation(Operation.Put.of(key1, IcebergTable.of("loc222", 0, 0, 0, 0, table.getId())))
            .commit();
    assertThat(branch.getHash()).isNotBlank();
    assertThat(branch.getName()).isEqualTo("main");

    Content table2 = api.getContent().refName("main").getSingle(key1).getContent();
    assertThat(table2)
        .asInstanceOf(type(IcebergTable.class))
        .extracting(IcebergTable::getMetadataLocation)
        .isEqualTo("loc222");
  }

  @Test
  void updateConflict() throws NessieNotFoundException, NessieConflictException {
    ContentKey key1 = ContentKey.of("test.ns", "table1");
    Branch branch = api.getDefaultBranch();
    api.commitMultipleOperations()
        .branch(branch)
        .commitMeta(CommitMeta.fromMessage("test"))
        .operation(Operation.Put.of(key1, IcebergTable.of("loc111", 0, 0, 0, 0)))
        .commit();

    Content table = api.getContent().refName("main").getSingle(key1).getContent();
    assertThat(table)
        .asInstanceOf(type(IcebergTable.class))
        .extracting(IcebergTable::getMetadataLocation)
        .isEqualTo("loc111");

    api.commitMultipleOperations()
        .branch(branch)
        .commitMeta(CommitMeta.fromMessage("test"))
        .operation(Operation.Put.of(key1, IcebergTable.of("loc222", 0, 0, 0, 0, table.getId())))
        .commit();

    // emulate concurrent update
    assertThatThrownBy(
            () ->
                api.commitMultipleOperations()
                    .branch(branch)
                    .commitMeta(CommitMeta.fromMessage("test"))
                    .operation(
                        Operation.Put.of(
                            key1, IcebergTable.of("loc333", 0, 0, 0, 0, table.getId())))
                    .commit())
        .isInstanceOf(NessieBadRequestException.class); // table ID mismatch

    Content table2 = api.getContent().refName("main").getSingle(key1).getContent();
    assertThat(table2)
        .asInstanceOf(type(IcebergTable.class))
        .extracting(IcebergTable::getMetadataLocation)
        .isEqualTo("loc222"); // first update
  }

  @Test
  void deleteTable() throws NessieNotFoundException, NessieConflictException {
    ContentKey key1 = ContentKey.of("test.ns", "table1");
    Branch branch =
        api.commitMultipleOperations()
            .branch(api.getDefaultBranch())
            .commitMeta(CommitMeta.fromMessage("test"))
            .operation(Operation.Put.of(key1, IcebergTable.of("loc111", 0, 0, 0, 0)))
            .commit();
    assertThat(branch.getHash()).isNotBlank();
    assertThat(branch.getName()).isEqualTo("main");

    Content table = api.getContent().refName("main").getSingle(key1).getContent();
    assertThat(table.getId()).isNotBlank();

    branch =
        api.commitMultipleOperations()
            .branch(api.getDefaultBranch())
            .commitMeta(CommitMeta.fromMessage("test"))
            .operation(Operation.Delete.of(key1))
            .commit();
    assertThat(branch.getHash()).isNotBlank();
    assertThat(branch.getName()).isEqualTo("main");

    assertThat(api.getContent().refName("main").key(key1).get()).isEmpty();
    assertThat(api.getEntries().refName("main").key(key1).get().getEntries()).isEmpty();
  }

  @Test
  void createNamespace() throws NessieNotFoundException, NessieConflictException {
    Namespace ns = Namespace.of("test", "ns");
    Branch branch =
        api.commitMultipleOperations()
            .branch(api.getDefaultBranch())
            .commitMeta(CommitMeta.fromMessage("test"))
            .operation(Operation.Put.of(ns.toContentKey(), ns))
            .commit();
    assertThat(branch.getHash()).isNotBlank();
    assertThat(branch.getName()).isEqualTo("main");

    assertThat(api.getContent().refName("main").getSingle(ns.toContentKey()).getContent())
        .asInstanceOf(type(Namespace.class))
        .extracting(Namespace::toContentKey)
        .isEqualTo(ns.toContentKey());

    assertThat(api.getEntries().refName("main").key(ns.toContentKey()).get().getEntries())
        .extracting(EntriesResponse.Entry::getContent)
        .asInstanceOf(list(Namespace.class))
        .extracting(Namespace::toContentKey)
        .containsExactly(ns.toContentKey());
  }

  @Test
  void getAllReferences() {
    assertThat(api.getAllReferences().get().getReferences())
        .extracting(Reference::getName)
        .containsExactly("main");
  }
}
