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
package com.dremio.plugins;

import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.api.KVStore;
import com.dremio.service.embedded.catalog.EmbeddedMetadataPointerService;
import com.dremio.service.embedded.catalog.EmbeddedPointerStoreBuilder;
import com.dremio.services.nessie.grpc.client.GrpcClientBuilder;
import com.dremio.test.DremioTest;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.projectnessie.client.api.NessieApiV2;

public class TestNessieClientImplWithEmbeddedPointerService
    extends AbstractNessieClientImplTestWithServer {

  private static LocalKVStoreProvider storeProvider;
  private static Server server;
  private static ManagedChannel channel;
  private static NessieApiV2 api;

  @BeforeAll
  static void setup() throws Exception {
    storeProvider =
        new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT, null, true, false); // in-memory
    storeProvider.start();

    //noinspection resource
    EmbeddedMetadataPointerService service =
        new EmbeddedMetadataPointerService(() -> storeProvider);
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
  static void shutdown() {
    channel.shutdown();
    server.shutdown();
  }

  @BeforeEach
  void wipeKVStore() {
    KVStore<String, String> kvStore = storeProvider.getStore(EmbeddedPointerStoreBuilder.class);
    kvStore.find().forEach(doc -> kvStore.delete(doc.getKey()));

    init(api);
  }
}
