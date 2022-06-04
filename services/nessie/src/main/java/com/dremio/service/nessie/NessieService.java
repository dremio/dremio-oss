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

import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
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
import com.dremio.options.Options;
import com.dremio.options.TypeValidators;
import com.dremio.service.Service;
import com.dremio.services.nessie.grpc.server.ConfigService;
import com.dremio.services.nessie.grpc.server.ContentService;
import com.dremio.services.nessie.grpc.server.TreeService;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;

import io.grpc.BindableService;

/**
 * Class that embeds Nessie into Dremio coordinator.
 */
@Options
public class NessieService implements Service {
  private static final long COMMIT_TIMEOUT_MS_DEFAULT = 600000L; // 10 min

  // A support option to override the maximum time when using NessieKVVersionStore.
  // If this is not set, use the timeout supplied in the constructor.
  public static final TypeValidators.PositiveLongValidator COMMIT_TIMEOUT_MS =
    new TypeValidators.PositiveLongValidator("nessie.kvversionstore.commit_timeout_ms", Integer.MAX_VALUE, COMMIT_TIMEOUT_MS_DEFAULT);

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NessieService.class);

  private final Provider<KVStoreProvider> kvStoreProvider;
  private final NessieConfig serverConfig;
  private final Supplier<VersionStore<Content, CommitMeta, Content.Type>> versionStoreSupplier;
  private final Supplier<org.projectnessie.api.TreeApi> treeApi;
  private final TreeService treeService;
  private final ContentService contentService;
  private final ConfigService configService;
  private final Supplier<Long> kvStoreCommitTimeoutMsSupplier;
  private final Supplier<Boolean> isMaster;

  public NessieService(Provider<KVStoreProvider> kvStoreProvider,
                       Provider<OptionManager> optionManagerProvider,
                       boolean inMemoryBackend,
                       long defaultKvStoreCommitTimeoutMs,
                       Supplier<Boolean> isMaster) {
    this.kvStoreProvider = kvStoreProvider;
    this.serverConfig = new NessieConfig();
    this.kvStoreCommitTimeoutMsSupplier = () ->  {
      final long overriddenTimeout = optionManagerProvider.get().getOption(COMMIT_TIMEOUT_MS);
      return overriddenTimeout != 0 ? overriddenTimeout : defaultKvStoreCommitTimeoutMs;
    };

    this.versionStoreSupplier = Suppliers.memoize(() -> getVersionStore(inMemoryBackend));
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

    // Get the version store here, so it is ready before the first request is received
    versionStoreSupplier.get();
    logger.info("Started Nessie gRPC Services.");
  }

  @Override
  public void close() throws Exception {
    logger.info("Stopping Nessie gRPC Service: Nothing to do");
  }

  private VersionStore<Content, CommitMeta, Content.Type> getVersionStore(boolean inMemoryBackend) {
    final TableCommitMetaStoreWorker worker = new TableCommitMetaStoreWorker();
    final NessieDatabaseAdapterConfig adapterCfg = new ImmutableNessieDatabaseAdapterConfig.Builder().setCommitTimeout(kvStoreCommitTimeoutMsSupplier.get()).build();
    DatabaseAdapter adapter;
    if (inMemoryBackend) {
      logger.debug("Using in-memory backing store for nessie...");
      adapter = new InmemoryDatabaseAdapterFactory().newBuilder().withConfig(adapterCfg)
        .withConnector(new InmemoryStore()).build();
    } else {
      logger.debug("Using persistent backing store for nessie...");
      NessieDatastoreInstance store = new NessieDatastoreInstance();
      store.configure(new ImmutableDatastoreDbConfig.Builder().setStoreProvider(kvStoreProvider).build());
      store.initialize();
      adapter = new DatastoreDatabaseAdapter(adapterCfg, store);
    }
    adapter.initializeRepo(serverConfig.getDefaultBranch());
    return new PersistVersionStore<>(adapter, worker);
  }
}
