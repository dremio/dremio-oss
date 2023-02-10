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
package com.dremio.service.nessie;

import java.util.List;
import java.util.function.Supplier;

import javax.inject.Provider;

import org.projectnessie.server.store.TableCommitMetaStoreWorker;
import org.projectnessie.services.impl.ConfigApiImpl;
import org.projectnessie.services.impl.ContentApiImpl;
import org.projectnessie.services.impl.TreeApiImpl;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.inmem.InmemoryDatabaseAdapterFactory;
import org.projectnessie.versioned.persist.inmem.InmemoryStore;
import org.projectnessie.versioned.persist.store.PersistVersionStore;

import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.options.OptionManager;
import com.dremio.service.Service;
import com.dremio.service.scheduler.SchedulerService;
import com.dremio.services.nessie.grpc.server.ConfigService;
import com.dremio.services.nessie.grpc.server.ContentService;
import com.dremio.services.nessie.grpc.server.TreeService;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;

import io.grpc.BindableService;

/**
 * Class that embeds Nessie into Dremio coordinator.
 */
public class NessieService implements Service {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NessieService.class);

  private final Provider<KVStoreProvider> kvStoreProvider;
  private final Provider<SchedulerService> schedulerServiceProvider;
  private final NessieConfig serverConfig;
  private final Supplier<DatabaseAdapter> adapter;
  private final Supplier<VersionStore> versionStoreSupplier;
  private final Supplier<org.projectnessie.api.TreeApi> treeApi;
  private final TreeService treeService;
  private final ContentService contentService;
  private final ConfigService configService;
  private final Provider<OptionManager> optionManagerProvider;
  private final Supplier<Boolean> isMaster;

  public NessieService(Provider<KVStoreProvider> kvStoreProvider,
                       Provider<OptionManager> optionManagerProvider,
                       Provider<SchedulerService> schedulerServiceProvider,
                       boolean inMemoryBackend,
                       Supplier<Boolean> isMaster) {
    this.kvStoreProvider = kvStoreProvider;
    this.schedulerServiceProvider = schedulerServiceProvider;
    this.serverConfig = new NessieConfig();
    this.optionManagerProvider = optionManagerProvider;

    final TableCommitMetaStoreWorker worker = new TableCommitMetaStoreWorker();
    this.adapter = Suppliers.memoize(() -> createAdapter(inMemoryBackend));
    this.versionStoreSupplier = Suppliers.memoize(() -> new PersistVersionStore(adapter.get(), worker));

    this.treeApi = Suppliers.memoize(() -> new TreeApiImpl(serverConfig, versionStoreSupplier.get(), null, null));
    this.treeService = new TreeService(treeApi);
    this.contentService = new ContentService(Suppliers.memoize(() -> new ContentApiImpl(serverConfig, versionStoreSupplier.get(), null, null)));
    this.configService = new ConfigService(Suppliers.memoize(() -> new ConfigApiImpl(serverConfig)));

    this.isMaster = isMaster;
  }

  public List<BindableService> getGrpcServices() {
    return Lists.newArrayList(treeService, contentService, configService);
  }

  @Override
  @SuppressWarnings("ReturnValueIgnored")
  public void start() throws Exception {
    logger.info("Starting Nessie gRPC Services.");

    // Note: Nessie Service is also started on "scale-out" coordinators, but those nodes do not have direct
    // access to the KVStore, so we skip repo init and maintenance work on those nodes.
    // TODO: it probably makes sense to start Nessie Service only on master.
    if (isMaster.get()) {
      // Initialize the repository, so it is ready before the first request is received
      adapter.get().initializeRepo(serverConfig.getDefaultBranch());

      NessieRepoMaintenanceTask repoMaintenanceTask = new NessieRepoMaintenanceTask(
        adapter.get(), optionManagerProvider.get());
      repoMaintenanceTask.schedule(schedulerServiceProvider.get());
    }

    logger.info("Started Nessie gRPC Services.");
  }

  @Override
  public void close() throws Exception {
    logger.info("Stopping Nessie gRPC Service: Nothing to do");
  }

  private DatabaseAdapter createAdapter(boolean inMemoryBackend) {
    final NessieDatabaseAdapterConfig adapterCfg = new NessieDatabaseAdapterConfig(optionManagerProvider);
    DatabaseAdapter adapter;
    if (inMemoryBackend) {
      logger.debug("Using in-memory backing store for nessie...");
      adapter = new InmemoryDatabaseAdapterFactory().newBuilder()
        .withConfig(adapterCfg)
        .withConnector(new InmemoryStore())
        .build();
    } else {
      logger.debug("Using persistent backing store for nessie...");

      NessieDatastoreInstance store = new NessieDatastoreInstance();
      store.configure(new ImmutableDatastoreDbConfig.Builder().setStoreProvider(kvStoreProvider).build());
      store.initialize();

      adapter = new DatastoreDatabaseAdapterFactory().newBuilder()
        .withConnector(store)
        .withConfig(adapterCfg)
        .build();
    }

    return adapter;
  }
}
