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
package com.dremio.exec.catalog;

import static com.dremio.exec.catalog.CatalogOptions.SOURCE_SECRETS_ENCRYPTION_ENABLED;

import com.dremio.catalog.exception.SourceAlreadyExistsException;
import com.dremio.common.AutoCloseables;
import com.dremio.common.VM;
import com.dremio.common.concurrent.CloseableThreadPool;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.DremioCollectors;
import com.dremio.concurrent.Runnables;
import com.dremio.config.DremioConfig;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.server.JobResultInfoProvider;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.options.OptionManager;
import com.dremio.service.coordinator.ClusterCoordinator.Role;
import com.dremio.service.listing.DatasetListingService;
import com.dremio.service.namespace.NamespaceAttribute;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.SourceState.SourceStatus;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.source.proto.SourceInternalData;
import com.dremio.service.orphanage.Orphanage;
import com.dremio.service.scheduler.Cancellable;
import com.dremio.service.scheduler.ModifiableSchedulerService;
import com.dremio.service.scheduler.Schedule;
import com.dremio.service.scheduler.SchedulerService;
import com.dremio.service.users.SystemUser;
import com.dremio.services.configuration.ConfigurationStore;
import com.dremio.services.configuration.proto.ConfigurationEntry;
import com.dremio.services.credentials.CredentialsServiceUtils;
import com.dremio.services.credentials.NoopSecretsCreator;
import com.dremio.services.credentials.SecretsCreator;
import com.dremio.services.credentials.SystemSecretCredentialsProvider;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import io.protostuff.ByteString;
import java.net.URI;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages the creation, deletion and retrieval of storage plugins. */
public class PluginsManager implements AutoCloseable, Iterable<StoragePlugin> {

  private static final Logger logger = LoggerFactory.getLogger(PluginsManager.class);
  private static final String SOURCE_SECRET_MIGRATION_CONFIG_KEY =
      "SourceSecretMigrationLastCompleteTimeInMs";

  protected final SabotContext sabotContext;
  protected final OptionManager optionManager;
  private final DremioConfig config;
  protected final ConnectionReader reader;
  protected final SchedulerService scheduler;
  protected final CloseableThreadPool executor = new CloseableThreadPool("source-management");
  private final DatasetListingService datasetListing;
  private final ConcurrentHashMap<String, ManagedStoragePlugin> plugins = new ConcurrentHashMap<>();
  private final long startupWait;
  protected final LegacyKVStore<NamespaceKey, SourceInternalData> sourceDataStore;
  protected final CatalogServiceMonitor monitor;
  private Cancellable refresher;
  protected final NamespaceService systemNamespace;
  private final Orphanage orphanage;
  protected final Provider<MetadataRefreshInfoBroadcaster> broadcasterProvider;
  private final Predicate<String> influxSourcePred;
  protected final ModifiableSchedulerService modifiableScheduler;
  private final Provider<LegacyKVStoreProvider> legacyKvStoreProvider;

  public PluginsManager(
      SabotContext sabotContext,
      NamespaceService systemNamespace,
      Orphanage orphanage,
      DatasetListingService datasetListingService,
      OptionManager optionManager,
      DremioConfig config,
      LegacyKVStore<NamespaceKey, SourceInternalData> sourceDataStore,
      SchedulerService scheduler,
      ConnectionReader reader,
      CatalogServiceMonitor monitor,
      Provider<MetadataRefreshInfoBroadcaster> broadcasterProvider,
      Predicate<String> influxSourcePred,
      ModifiableSchedulerService modifiableScheduler,
      Provider<LegacyKVStoreProvider> legacyKvStoreProvider) {
    this.sabotContext = sabotContext;
    this.optionManager = optionManager;
    this.config = config;
    this.reader = reader;
    this.sourceDataStore = sourceDataStore;
    this.systemNamespace = systemNamespace;
    this.orphanage = orphanage;
    this.scheduler = scheduler;
    this.datasetListing = datasetListingService;
    this.startupWait =
        VM.isDebugEnabled()
            ? TimeUnit.DAYS.toMillis(365)
            : optionManager.getOption(CatalogOptions.STARTUP_WAIT_MAX);
    this.monitor = monitor;
    this.broadcasterProvider = broadcasterProvider;
    this.influxSourcePred = influxSourcePred;
    this.modifiableScheduler = modifiableScheduler;
    this.legacyKvStoreProvider = legacyKvStoreProvider;
  }

  ConnectionReader getReader() {
    return reader;
  }

  public Set<String> getSourceNameSet() {
    return plugins.values().stream()
        .map(input -> input.getName().getRoot())
        .collect(Collectors.toSet());
  }

  public Stream<VersionedPlugin> getAllVersionedPlugins() {
    return plugins.values().stream()
        .filter(
            input ->
                input.getPlugin() != null && input.getPlugin().isWrapperFor(VersionedPlugin.class))
        .map(entry -> entry.getPlugin())
        .map(storagePlugin -> storagePlugin.unwrap(VersionedPlugin.class));
  }

  /** Automatically synchronizing sources regularily */
  private final class Refresher implements Runnable {

    @Override
    public void run() {
      try {
        synchronizeSources(influxSourcePred);
      } catch (Exception ex) {
        logger.warn("Failure while synchronizing sources.");
      }
    }

    @Override
    public String toString() {
      return "catalog-source-synchronization";
    }
  }

  Iterable<ManagedStoragePlugin> managed() {
    return plugins.values();
  }

  /**
   * Create a new managed storage plugin. Requires the PluginManager.writeLock() to be held.
   *
   * @param config The configuration to create.
   * @return The newly created managed storage plugin. If a plugin with the provided name already
   *     exists, does nothing and returns null.
   * @throws Exception
   * @throws TimeoutException
   */
  public ManagedStoragePlugin create(
      SourceConfig config, String userName, NamespaceAttribute... attributes)
      throws TimeoutException, Exception {
    if (hasPlugin(config.getName())) {
      throw new SourceAlreadyExistsException(
          String.format("Source [%s] already exists.", config.getName()));
    }

    ManagedStoragePlugin plugin = newPlugin(config);
    try {
      plugin.createSource(config, userName, attributes);
    } catch (UserException e) {
      // The creation of Source can fail due to various reasons.
      // In case of failure, we need to cleanup the in-memory state of the source. Hence closing the
      // plugin.
      logger.error("Exception while creating source.", e);
      try {
        plugin.close();
      } catch (Exception ex) {
        e.addSuppressed(ex);
      }
      throw e;
    }

    // use concurrency features of concurrent hash map to avoid locking.
    ManagedStoragePlugin existing = plugins.putIfAbsent(c(config.getName()), plugin);

    if (existing == null) {
      return plugin;
    }

    // This means it  has been added by a concurrent thread doing create with the same name
    final SourceAlreadyExistsException e =
        new SourceAlreadyExistsException(
            String.format("Source [%s] already exists.", config.getName()));
    try {
      // this happened in time with someone else.
      plugin.close();
    } catch (Exception ex) {
      e.addSuppressed(ex);
    }
    throw e;
  }

  private void migrateSourceSecrets() throws NamespaceException {
    final SecretsCreator secretsCreator = sabotContext.getSecretsCreator().get();
    // Short-circuit early if encryption is disabled via binding
    if (secretsCreator instanceof NoopSecretsCreator) {
      return;
    }

    // Migration runs only once on master coordinator.
    final ConfigurationStore configurationStore =
        new ConfigurationStore(legacyKvStoreProvider.get());
    final Optional<ConfigurationEntry> lastMigrationTime =
        Optional.ofNullable(configurationStore.get(SOURCE_SECRET_MIGRATION_CONFIG_KEY));
    if (lastMigrationTime.isPresent()) {
      return;
    }

    final Stopwatch stopwatchForAllPlugins = Stopwatch.createStarted();
    for (SourceConfig source : datasetListing.getSources(SystemUser.SYSTEM_USERNAME)) {
      final Stopwatch stopwatchForEachPlugin = Stopwatch.createStarted();
      final String sourceName = source.getName();
      final String sourceType = source.getType();
      if (isSourceTypeToSkipMigration(sourceType)) {
        logger.debug(
            "Skip source migration for [{}] based on type [{}]. Took {} milliseconds.",
            sourceName,
            sourceType,
            stopwatchForEachPlugin.elapsed(TimeUnit.MILLISECONDS));
        continue;
      }
      final boolean didEncryptionHappen;
      try {
        final ConnectionConf<?, ?> connectionConf = source.getConnectionConf(reader);
        Predicate<String> isNotSystemEncryptedFilter =
            secret -> {
              String scheme;
              try {
                final URI uri = CredentialsServiceUtils.safeURICreate(secret);
                scheme = uri.getScheme();
                if (!SystemSecretCredentialsProvider.SECRET_PROVIDER_SCHEME.equals(scheme)) {
                  // Scheme is not system
                  return true;
                }
                return !secretsCreator.isEncrypted(uri.getSchemeSpecificPart());
              } catch (IllegalArgumentException ignored) {
                // Not a URI
                return true;
              }
            };
        didEncryptionHappen =
            connectionConf.encryptSecrets(secretsCreator, isNotSystemEncryptedFilter);
        if (!didEncryptionHappen) {
          logger.info(
              "Did not need to migrate the source [{}]. Took {} milliseconds.",
              sourceName,
              stopwatchForEachPlugin.elapsed(TimeUnit.MILLISECONDS));
          continue;
        }
        source.setConnectionConf(connectionConf);
        // Update KVStore.
        try {
          systemNamespace.addOrUpdateSource(source.getKey(), source);
        } catch (ConcurrentModificationException cme) {
          // Retry one more time.
          systemNamespace.addOrUpdateSource(source.getKey(), source);
        }
        logger.info(
            "Successfully migrate the source [{}]. Took {} milliseconds.",
            sourceName,
            stopwatchForEachPlugin.elapsed(TimeUnit.MILLISECONDS));

      } catch (Exception e) {
        logger.error(
            "Failed to migrate the source [{}]. Reason: {} Took {} milliseconds.",
            sourceName,
            e.getMessage(),
            stopwatchForEachPlugin.elapsed(TimeUnit.MILLISECONDS),
            e);
        // Error here should not block Dremio from starting up.
      }
      stopwatchForEachPlugin.stop();
    }
    // Store the timestamp when the migration is finished.
    configurationStore.put(
        SOURCE_SECRET_MIGRATION_CONFIG_KEY,
        new ConfigurationEntry()
            .setValue(ByteString.copyFromUtf8(String.valueOf(System.currentTimeMillis()))));
    stopwatchForAllPlugins.stop();
    logger.info(
        "Completed sources migration. Total: {} milliseconds.",
        stopwatchForAllPlugins.elapsed(TimeUnit.MILLISECONDS));
  }

  /**
   * @throws NamespaceException
   */
  public void start() throws NamespaceException {

    // Encryption starts.
    if ((sabotContext.isMaster()
            || (this.config.isMasterlessEnabled() && sabotContext.isCoordinator()))
        && this.optionManager.getOption(SOURCE_SECRETS_ENCRYPTION_ENABLED)) {
      migrateSourceSecrets();
    }
    // Encryption ends.

    // Since this is run inside the system startup, no one should be able to interact with it until
    // we've already
    // started everything. Thus no locking is necessary.

    ImmutableMap.Builder<String, CompletableFuture<SourceState>> futuresBuilder =
        ImmutableMap.builder();
    for (SourceConfig source : datasetListing.getSources(SystemUser.SYSTEM_USERNAME)) {
      ManagedStoragePlugin plugin = newPlugin(source);

      futuresBuilder.put(source.getName(), plugin.startAsync());
      plugins.put(c(source.getName()), plugin);
    }

    Map<String, CompletableFuture<SourceState>> futures = futuresBuilder.build();
    final CompletableFuture<Void> futureWait =
        CompletableFuture.allOf(futures.values().toArray(new CompletableFuture[futures.size()]));
    try {
      // wait STARTUP_WAIT_MILLIS or until all plugins have started/failed to start.
      futureWait.get(startupWait, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      // ignore since we're going to evaluate individually below.
    }

    final StringBuilder sb = new StringBuilder();

    int count = 0;
    sb.append("Result of storage plugin startup: \n");
    for (final ManagedStoragePlugin p : plugins.values()) {
      count++;
      String name = p.getName().getRoot();
      final CompletableFuture<SourceState> future = futures.get(name);
      Preconditions.checkNotNull(
          future,
          "Unexpected failure to retrieve source %s from available futures %s.",
          name,
          futures.keySet());
      if (future.isDone()) {
        try {
          SourceState state = future.get();
          String result =
              state.getStatus() == SourceStatus.bad ? "started in bad state" : "success";
          sb.append(
              String.format("\t%s: %s (%dms). %s\n", name, result, p.getStartupTime(), state));
        } catch (Exception ex) {
          logger.error(
              "Failure while starting plugin {} after {}ms.", p.getName(), p.getStartupTime(), ex);
          sb.append(
              String.format("\t%s: failed (%dms). %s\n", name, p.getStartupTime(), p.getState()));
          p.initiateFixFailedStartTask();
        }
      } else {
        // not finished, let's get a log entry later.
        future.thenRun(Runnables.combo(new LateSourceRunnable(future, p)));
        sb.append(String.format("\t%s: pending.\n", name));
      }
    }

    // for coordinator, ensure catalog synchronization. Don't start this until the plugins manager
    // is started.
    if (sabotContext.getRoles().contains(Role.COORDINATOR)) {
      refresher =
          scheduler.schedule(
              Schedule.Builder.everyMillis(CatalogServiceImpl.CATALOG_SYNC).build(),
              Runnables.combo(new Refresher()));
    }

    if (count > 0) {
      logger.info(sb.toString());
    }
  }

  private ManagedStoragePlugin newPlugin(SourceConfig config) {
    final boolean isVirtualMaster =
        sabotContext.isMaster()
            || (this.config.isMasterlessEnabled() && sabotContext.isCoordinator());
    return newManagedStoragePlugin(config, isVirtualMaster);
  }

  protected ManagedStoragePlugin newManagedStoragePlugin(
      SourceConfig config, boolean isVirtualMaster) {
    return new ManagedStoragePlugin(
        sabotContext,
        executor,
        isVirtualMaster,
        modifiableScheduler,
        systemNamespace,
        orphanage,
        sourceDataStore,
        config,
        optionManager,
        reader,
        monitor.forPlugin(config.getName()),
        broadcasterProvider,
        influxSourcePred);
  }

  /**
   * Runnable that is used to finish startup for sources that aren't completed starting up within
   * the initial startup time.
   */
  private final class LateSourceRunnable implements Runnable {

    private final CompletableFuture<SourceState> future;
    private final ManagedStoragePlugin plugin;

    public LateSourceRunnable(CompletableFuture<SourceState> future, ManagedStoragePlugin plugin) {
      this.future = future;
      this.plugin = plugin;
    }

    @Override
    public void run() {
      try {
        SourceState state = future.get();
        String result =
            state.getStatus() == SourceStatus.bad ? "started in bad state" : "started sucessfully";
        logger.info(
            "Plugin {} {} after {}ms. Current status: {}",
            plugin.getName(),
            result,
            plugin.getStartupTime(),
            state);
      } catch (Exception ex) {
        logger.error(
            "Failure while starting plugin {} after {}ms.",
            plugin.getName(),
            plugin.getStartupTime(),
            ex);
        plugin.initiateFixFailedStartTask();
      }
    }

    @Override
    public String toString() {
      return "late-load-" + plugin.getName().getRoot();
    }
  }

  /**
   * Canonicalize storage plugin name.
   *
   * @param pluginName
   * @return a canonicalized version of the key.
   */
  private String c(String pluginName) {
    return pluginName.toLowerCase();
  }

  /** Iterator only returns non-bad state plugins. */
  @Override
  public Iterator<StoragePlugin> iterator() {
    return plugins.values().stream()
        .map(input -> input.unwrap(StoragePlugin.class))
        .filter(input -> input.getState().getStatus() != SourceStatus.bad)
        .iterator();
  }

  public boolean hasPlugin(String name) {
    return plugins.containsKey(c(name));
  }

  public ConcurrentHashMap<String, ManagedStoragePlugin> getPlugins() {
    return plugins;
  }

  @WithSpan
  public ManagedStoragePlugin getSynchronized(
      SourceConfig pluginConfig, java.util.function.Predicate<String> influxSourcePred)
      throws Exception {
    while (true) {
      ManagedStoragePlugin plugin = plugins.get(c(pluginConfig.getName()));

      if (plugin != null) {
        plugin.synchronizeSource(pluginConfig);
        return plugin;
      }
      // Try to create the plugin to synchronize.
      plugin = newPlugin(pluginConfig);
      plugin.replacePluginWithLock(pluginConfig, createWaitMillis(), true);

      // If this is a coordinator and a plugin is missing, it's probably been deleted from the CHM
      // by a
      // concurrent thread or a create operation may be in progress (check if it's in flux) and has
      // not
      // yet added it to the CHM.
      // So lets skip it and allow this to be picked up in the next refresher run.
      // For an executor, there should be no clashes with any mutation.
      if (influxSourcePred.test(pluginConfig.getName())
          || (sabotContext.isCoordinator()
              && !systemNamespace.exists(new NamespaceKey(pluginConfig.getName())))) {
        throw new ConcurrentModificationException(
            String.format(
                "Source [%s] is being modified. Will refresh this source in next refresh cycle. ",
                pluginConfig.getName()));
      }
      // Note: there is known window between the above "if" check and the next statement. This
      // cannot be eliminated unless we
      // get a distributed lock just before the if check above. So in theory there is a race
      // possible if
      // another thread managed to get a distributed lock, set the influxSources, get a reentrant
      // write lock, creates/deletes a source
      // all between the above if condition and the following call. But we are avoiding locks in the
      // GetSources and Refresher thread
      // (as was the design goal) Practically, very unlikely window for all the above to slip in.

      ManagedStoragePlugin existing = plugins.putIfAbsent(c(pluginConfig.getName()), plugin);
      if (existing == null) {
        return plugin;
      }
      try {
        // this happened in time with someone else.
        plugin.close();
      } catch (Exception ex) {
        logger.debug("Exception while closing concurrently created plugin.", ex);
      }
    }
  }

  private long createWaitMillis() {
    if (VM.isDebugEnabled()) {
      return TimeUnit.DAYS.toMillis(365);
    }
    return optionManager.getOption(CatalogOptions.STORAGE_PLUGIN_CREATE_MAX);
  }

  /**
   * Remove a source by grabbing the write lock and then removing from the map
   *
   * @param config The config of the source to dete.
   * @return True if the source matched and was removed.
   */
  public boolean closeAndRemoveSource(SourceConfig config) {
    final String name = config.getName();
    logger.debug("Deleting source [{}]", name);

    ManagedStoragePlugin plugin = plugins.get(c(name));

    if (plugin == null) {
      return true;
    }

    try {
      final boolean removed = plugin.close(config, s -> plugins.remove(c(name)));
      plugin.deleteServiceSet();
      return removed;
    } catch (Exception ex) {
      logger.error("Exception while shutting down source {}.", name, ex);
      return true;
    }
  }

  public ManagedStoragePlugin get(String name) {
    return plugins.get(c(name));
  }

  protected Predicate<String> getInfluxSourcePred() {
    return influxSourcePred;
  }

  /** For each source, synchronize the sources definition to the namespace. */
  @VisibleForTesting
  void synchronizeSources(java.util.function.Predicate<String> influxSourcePred) {
    // first collect up all the current source configs.
    final Map<String, SourceConfig> configs =
        plugins.values().stream()
            .map(ManagedStoragePlugin::getConfig)
            .collect(DremioCollectors.uniqueGrouping(SourceConfig::getName));

    // second, for each source, synchronize to latest state
    final Set<String> names = getSourceNameSet();
    for (SourceConfig config : systemNamespace.getSources()) {
      names.remove(config.getName());

      try {
        // if an active modification is happening, don't synchronize this source now
        if (influxSourcePred.test(config.getName())) {
          logger.warn(
              "Skipping synchronizing source {} since it's being modified", config.getName());
          continue;
        }
        getSynchronized(config, influxSourcePred);
      } catch (Exception ex) {
        logger.warn("Failure updating source [{}] during scheduled updates.", config, ex);
      }
    }

    // third, delete everything that wasn't found, assuming it matches what we originally searched.
    for (String name : names) {
      SourceConfig originalConfig = configs.get(name);
      if (originalConfig != null) {
        try {
          this.closeAndRemoveSource(originalConfig);
        } catch (Exception e) {
          logger.warn(
              "Failure while deleting source [{}] during source synchronization.",
              originalConfig.getName(),
              e);
        }
      }
    }
  }

  @Override
  public void close() throws Exception {
    if (refresher != null) {
      refresher.cancel(false);
    }

    AutoCloseables.close(Iterables.concat(Collections.singleton(executor), plugins.values()));
  }

  SabotConfig getSabotConfig() {
    return sabotContext.getConfig();
  }

  OptionManager getOptionManager() {
    return sabotContext.getOptionManager();
  }

  JobResultInfoProvider getJobResultInfoProvider() {
    return sabotContext.getJobResultInfoProvider();
  }

  /**
   * Certain sources do not contain any plain-text secrets based on source type. Hence, we want to
   * exit migration early before the need to validate all fields in the source config. Even if there
   * is a miss on the source type here that doesn't need secret migration, migration shouldn't cause
   * a regression on the source.
   *
   * <p>TODO: Some of these contain secrets in cloud. Cloud impl will need to overwrite this list.
   *
   * @param srcType can be any string value set for {@link com.dremio.exec.catalog.conf.SourceType}
   * @return true if the source should skip migration
   */
  public static boolean isSourceTypeToSkipMigration(String srcType) {
    return ("HOME".equalsIgnoreCase(srcType))
        || ("INTERNAL".equalsIgnoreCase(srcType))
        || ("ACCELERATION".equalsIgnoreCase(srcType))
        || ("INFORMATION_SCHEMA".equalsIgnoreCase(srcType))
        || ("METADATA".equalsIgnoreCase(srcType))
        || ("SYS".equalsIgnoreCase(srcType))
        || ("GANDIVA_CACHE".equalsIgnoreCase(srcType))
        || ("ESYSFLIGHT".equalsIgnoreCase(srcType))
        || ("SYSTEMICEBERGTABLES".equalsIgnoreCase(srcType));
  }
}
