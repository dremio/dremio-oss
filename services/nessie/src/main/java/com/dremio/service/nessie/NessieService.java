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
import org.projectnessie.model.Contents;
import org.projectnessie.server.store.TableCommitMetaStoreWorker;
import org.projectnessie.services.impl.ConfigApiImpl;
import org.projectnessie.services.impl.ContentsApiImpl;
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
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;

import io.grpc.BindableService;

/**
 * Class that embeds Nessie into Dremio coordinator.
 */
@Options
public class NessieService implements Service {
  private static final long NO_RETRY_LIMIT_OVERRIDE = 0L;

  // An support option to override the maximum number of retries at run time when using NessieKVVerssionStore.
  // If this is set to NO_RETRY_LIMIT_OVERRIDE, use the limit supplied in the constructor.
  public static final TypeValidators.PositiveLongValidator RETRY_LIMIT =
    new TypeValidators.PositiveLongValidator("nessie.kvversionstore.max_retries", Integer.MAX_VALUE, NO_RETRY_LIMIT_OVERRIDE);

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NessieService.class);

  private final Provider<KVStoreProvider> kvStoreProvider;
  private final NessieConfig serverConfig;
  private final Supplier<VersionStore<Contents, CommitMeta, Contents.Type>> versionStoreSupplier;
  private final TreeApiService treeApiService;
  private final ContentsApiService contentsApiService;
  private final ConfigApiService configApiService;
  private final Supplier<Integer> kvStoreMaxCommitRetriesSupplier;

  public NessieService(Provider<KVStoreProvider> kvStoreProvider,
                       Provider<OptionManager> optionManagerProvider,
                       boolean inMemoryBackend,
                       int defaultKvStoreMaxCommitRetries) {
    this.kvStoreProvider = kvStoreProvider;
    this.serverConfig = new NessieConfig();
    this.kvStoreMaxCommitRetriesSupplier = () ->  {
      final int overriddenLimit = Ints.saturatedCast(optionManagerProvider.get().getOption(RETRY_LIMIT));
      return overriddenLimit != 0 ? overriddenLimit : defaultKvStoreMaxCommitRetries;
    };

    this.versionStoreSupplier = Suppliers.memoize(() -> getVersionStore(inMemoryBackend));
    this.treeApiService = new TreeApiService(Suppliers.memoize(() ->
      new TreeApiImpl(serverConfig, versionStoreSupplier.get(), null, null)));
    this.contentsApiService = new ContentsApiService(Suppliers.memoize(() ->
        new ContentsApiImpl(serverConfig, versionStoreSupplier.get(), null, null))
    );
    this.configApiService = new ConfigApiService(Suppliers.memoize(() -> new ConfigApiImpl(serverConfig)));
  }

  public List<BindableService> getGrpcServices() {
    return Lists.newArrayList(treeApiService, contentsApiService, configApiService);
  }

  @Override
  @SuppressWarnings("ReturnValueIgnored")
  public void start() throws Exception {
    logger.info("Starting Nessie gRPC Services.");

    // Get the version store here so it is ready before the first request is received
    versionStoreSupplier.get();

    logger.info("Started Nessie gRPC Services.");
  }

  @Override
  public void close() throws Exception {
    logger.info("Stopping Nessie gRPC Service: Nothing to do");
  }

  private final VersionStore<Contents, CommitMeta, Contents.Type> getVersionStore(boolean inMemoryBackend) {
    final TableCommitMetaStoreWorker worker = new TableCommitMetaStoreWorker();
    final NessieDatabaseAdapterConfig adapterCfg = new NessieDatabaseAdapterConfig(kvStoreMaxCommitRetriesSupplier.get());
    DatabaseAdapter adapter;
    if (inMemoryBackend) {
      logger.debug("Using in-memory backing store for nessie...");
      adapter = new InmemoryDatabaseAdapterFactory().newBuilder().withConfig(adapterCfg)
        .withConnector(new InmemoryStore()).build();
    } else {
      logger.debug("Using persistent backing store for nessie...");
      adapter = new DatastoreDatabaseAdapter(adapterCfg, new NessieDatastoreInstance(kvStoreProvider));
    }
    adapter.initializeRepo(serverConfig.getDefaultBranch());
    return new PersistVersionStore<>(adapter, worker);
  }
}
