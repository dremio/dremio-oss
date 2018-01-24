/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.store;

import static com.dremio.service.namespace.proto.NameSpaceContainer.Type;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.common.store.StoragePluginConfig;
import com.dremio.datastore.KVStore;
import com.dremio.datastore.PassThroughSerializer;
import com.dremio.datastore.StoreBuildingFactory;
import com.dremio.datastore.StringSerializer;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.exception.NodeStartupException;
import com.dremio.exec.exception.StoreException;
import com.dremio.exec.planner.logical.StoragePlugins;
import com.dremio.exec.serialization.JacksonSerializer;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.StoragePluginStarter.Success;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FormatPlugin;
import com.dremio.exec.store.ischema.InfoSchemaConfig;
import com.dremio.exec.store.sys.PersistentStore;
import com.dremio.exec.store.sys.PersistentStoreProvider;
import com.dremio.exec.store.sys.SystemTablePluginConfigProvider;
import com.dremio.exec.store.sys.store.KVPersistentStore.PersistentStoreCreator;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.StoragePluginId;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.source.proto.SourceType;
import com.dremio.service.namespace.source.proto.UpdateMode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;

public class StoragePluginRegistryImpl implements StoragePluginRegistry, AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StoragePluginRegistryImpl.class);
  private static final long DEFAULT_REFRESH_PERIOD = TimeUnit.HOURS.toMillis(2);
  private static final long DEFAULT_GRACE_PERIOD = TimeUnit.HOURS.toMillis(6);
  private static final long STORAGE_PLUGIN_STARTUP_WAIT_MILLIS = 15_000;

  private static final long ONEYEAR = TimeUnit.DAYS.toMillis(365);
  private static final MetadataPolicy ONE_YEAR_POLICY = new MetadataPolicy()
      .setAuthTtlMs(ONEYEAR)
      .setDatasetDefinitionExpireAfterMs(ONEYEAR)
      .setDatasetDefinitionRefreshAfterMs(ONEYEAR)
      .setDatasetUpdateMode(UpdateMode.PREFETCH)
      .setNamesRefreshMs(ONEYEAR);

  private Map<Class<?>, Constructor<? extends StoragePlugin>> availablePlugins = Collections.emptyMap();
  private final StoragePluginMap plugins = new StoragePluginMap();

  private final SabotContext context;
  private final CatalogService catalog;
  private final PersistentStore<StoragePluginConfig> pluginSystemTable;
  private final LogicalPlanPersistence lpPersistence;
  private final ScanResult classpathScan;
  private final SystemTablePluginConfigProvider sysPluginProvider;

  private final ConcurrentHashMap<String, Integer> manuallyAddedPlugins = new ConcurrentHashMap<>();
  private final LoadingCache<StoragePluginConfig, StoragePlugin> ephemeralPlugins;

  //TODO do we actually need to pass SabotContext ?
  public StoragePluginRegistryImpl(final SabotContext context,
                                   final CatalogService catalog,
                                   final PersistentStoreProvider provider,
                                   final SystemTablePluginConfigProvider sysPluginProvider) {
    this.context = checkNotNull(context);
    this.catalog = checkNotNull(catalog);
    this.lpPersistence = checkNotNull(context.getLpPersistence());
    this.classpathScan = checkNotNull(context.getClasspathScan());
    this.sysPluginProvider = sysPluginProvider;
    try {
      this.pluginSystemTable = provider.getOrCreateStore(PSTORE_NAME, StoragePluginCreator.class,
        new JacksonSerializer<>(lpPersistence.getMapper(), StoragePluginConfig.class));
    } catch (StoreException | RuntimeException e) {
      logger.error("Failure while loading storage plugin registry.", e);
      throw new RuntimeException("Failure while reading and loading storage plugin configuration.", e);
    }

    ephemeralPlugins = CacheBuilder.newBuilder()
        .expireAfterAccess(24, TimeUnit.HOURS)
        .maximumSize(250)
        .removalListener(new RemovalListener<StoragePluginConfig, StoragePlugin>() {
          @Override
          public void onRemoval(RemovalNotification<StoragePluginConfig, StoragePlugin> notification) {
            closePlugin(notification.getValue());
          }
        })
        .build(new CacheLoader<StoragePluginConfig, StoragePlugin>() {
          @Override
          public StoragePlugin load(StoragePluginConfig config) throws Exception {
            return create(null, config, true);
          }
        });
  }

  public static class StoragePluginCreator implements PersistentStoreCreator {
    @Override
    public KVStore<String, byte[]> build(StoreBuildingFactory factory) {
      return factory.<String, byte[]>newStore()
        .name(PSTORE_NAME)
        .keySerializer(StringSerializer.class)
        .valueSerializer(PassThroughSerializer.class)
        .build();
    }
  }

  public void init() throws NodeStartupException {
    availablePlugins = findAvailablePlugins(classpathScan);

    // create registered plugins defined in "storage-plugins.json"
    this.plugins.putAll(createPlugins());
  }

  private Map<String, StoragePlugin> createPlugins() throws NodeStartupException {
    try {
      /*
       * Check if the storage plugins system table has any entries. If not, load the boostrap-storage-plugin file into
       * the system table.
       */
      if (!pluginSystemTable.getAll().hasNext()) {
        // bootstrap load the config since no plugins are stored.
        logger.info("No storage plugin instances configured in persistent store, loading bootstrap configuration.");
        Collection<URL> urls = ClassPathScanner.forResource(ExecConstants.BOOTSTRAP_STORAGE_PLUGINS_FILE, false);
        if (urls != null && !urls.isEmpty()) {
          logger.info("Loading the storage plugin configs from URLs {}.", urls);
          Map<String, URL> pluginURLMap = Maps.newHashMap();
          for (URL url : urls) {
            String pluginsData = Resources.toString(url, Charsets.UTF_8);
            StoragePlugins plugins = lpPersistence.getMapper().readValue(pluginsData, StoragePlugins.class);
            for (Map.Entry<String, StoragePluginConfig> config : plugins) {
              if (!pluginSystemTable.putIfAbsent(config.getKey(), config.getValue())) {
                logger.warn("Duplicate plugin instance '{}' defined in [{}, {}], ignoring the later one.",
                    config.getKey(), pluginURLMap.get(config.getKey()), url);
                continue;
              }
              // Pls note (wrt DX-7484): the bootstrap file does not contain any plugin source types. Since it's used
              // only in tests, ignoring the source types of any sources in a bootstrap file
              pluginURLMap.put(config.getKey(), url);
            }
          }
        } else {
          logger.debug("Failure finding " + ExecConstants.BOOTSTRAP_STORAGE_PLUGINS_FILE);
        }
      }


      Map<String, StoragePlugin> activePlugins = new HashMap<>();

      activePlugins.put(INFORMATION_SCHEMA_PLUGIN, registerInternal(INFORMATION_SCHEMA_PLUGIN, new InfoSchemaConfig(), SourceType.INFORMATION_SCHEMA));
      activePlugins.put(SYS_PLUGIN, registerInternal(SYS_PLUGIN, sysPluginProvider.get(), SourceType.SYS));

      StoragePluginStarter starter = new StoragePluginStarter(pluginSystemTable, STORAGE_PLUGIN_STARTUP_WAIT_MILLIS, new InitTimeCreator());
      for (Map.Entry<String, StoragePluginConfig> entry : Lists.newArrayList(pluginSystemTable.getAll())) {
        starter.add(entry.getKey(), entry.getValue());
      }

      for(Success success : starter.start()){
        activePlugins.put(success.getName(), success.getPlugin());
      }

      return activePlugins;
    } catch (IOException e) {
      logger.error("Failure setting up storage plugins.  SabotNode exiting.", e);
      throw new IllegalStateException(e);
    }
  }

  private class InitTimeCreator implements StoragePluginStarter.StoragePluginCreator {
    @Override
    public StoragePlugin create(String name, StoragePluginConfig pluginConfig) throws Exception {
        return StoragePluginRegistryImpl.this.create(name, pluginConfig, true);
    }

    @Override
    public void informLateCreate(String name, StoragePlugin plugin) {
      plugins.put(name, plugin);
    }
  }

  @Override
  public void addPlugin(String name, StoragePlugin plugin) {
    plugins.put(name, plugin);
    manuallyAddedPlugins.put(name, 0);
  }

  @Override
  public void deletePlugin(String name) {
    StoragePlugin plugin = plugins.remove(name);
    closePlugin(plugin);
    if(manuallyAddedPlugins.remove(name) == null){
      pluginSystemTable.delete(name);
    }
    catalog.unregisterSource(new NamespaceKey(name));
  }

  private void closePlugin(StoragePlugin plugin) {
    if (plugin == null) {
      return;
    }

    try {
      plugin.close();
    } catch (Exception e) {
      logger.warn("Exception while shutting down storage plugin.");
    }
  }

  @Override
  public StoragePlugin createOrUpdate(String name, StoragePluginConfig config, boolean persist) throws ExecutionSetupException {
    return createOrUpdate(name, config, null, persist);
  }

  @Override
  public StoragePlugin createOrUpdate(String name, StoragePluginConfig config, SourceConfig sourceConfig, boolean persist)
      throws ExecutionSetupException {
    for (;;) {
      final StoragePlugin oldPlugin = plugins.get(name);
      final StoragePlugin newPlugin = create(name, config, sourceConfig, false);
      boolean done = false;
      try {
        if (oldPlugin != null) {
          done = plugins.replace(name, oldPlugin, newPlugin);
          if (done) {
            closePlugin(oldPlugin);
          }
        }else {
          done = (null == plugins.putIfAbsent(name, newPlugin));
        }
      } finally {
        if (!done) {
          closePlugin(newPlugin);
        }
      }

      if (done) {
        if (persist) {
          pluginSystemTable.put(name, config);
        }

        if (name != null) {
          if (sourceConfig == null) {
            sourceConfig = decorate(new SourceConfig().setType(SourceType.UNKNOWN).setName(name));
          }
          catalog.scheduleMetadataRefresh(new NamespaceKey(name), sourceConfig);
        }
        return newPlugin;
      }
    }
  }

  @Override
  public StoragePlugin getPlugin(String name) throws ExecutionSetupException {
    StoragePlugin plugin = plugins.get(name);
    if (manuallyAddedPlugins.containsKey(name)) {
      return plugin;
    }

    // since we lazily manage the list of plugins per server, we need to update this once we know that it is time.
    StoragePluginConfig config = this.pluginSystemTable.get(name);
    if (config == null) {
      if (plugin != null) {
        plugins.remove(name);
      }
      return null;
    } else {
      if (plugin == null || !plugin.getId().getConfig().equals(config)) {
        plugin = createOrUpdate(name, config, false);
      }
      return plugin;
    }
  }

  @Override
  public StoragePlugin getPlugin(StoragePluginId pluginId) throws ExecutionSetupException {
    //TODO: this should be smarted (checking name, etc)
    StoragePlugin plugin = getPlugin(pluginId.getName());
    if(plugin != null && plugin.getId().equals(pluginId)){
      return plugin;
    }
    return getPlugin(pluginId.getConfig());
  }

  public StoragePlugin getPlugin(final StoragePluginConfig config) throws ExecutionSetupException {
    // try to lookup plugin by configuration
    StoragePlugin plugin = plugins.get(config);
    if (plugin != null) {
      return plugin;
    }

    // no named plugin matches the desired configuration, let's create an
    // ephemeral storage plugin (or get one from the cache)
    try {
      return ephemeralPlugins.get(config);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof ExecutionSetupException) {
        throw (ExecutionSetupException) cause;
      } else {
        // this shouldn't happen. here for completeness.
        throw new ExecutionSetupException("Failure while trying to create ephemeral plugin.", cause);
      }
    }
  }

  @Override
  public FormatPlugin getFormatPlugin(StoragePluginConfig storageConfig, FormatPluginConfig formatConfig)
      throws ExecutionSetupException {
    StoragePlugin p = getPlugin(storageConfig);
    if (!(p instanceof FileSystemPlugin)) {
      throw new ExecutionSetupException(
          String.format("You tried to request a format plugin for a storage plugin that wasn't of type "
              + "FileSystemPlugin. The actual type of plugin was %s.", p.getClass().getName()));
    }
    FileSystemPlugin storage = (FileSystemPlugin) p;
    return storage.getFormatPlugin(formatConfig);
  }

  private SourceConfig decorate(SourceConfig sourceConfig) {
    return sourceConfig
      .setAccelerationRefreshPeriod(Optional.fromNullable(sourceConfig.getAccelerationRefreshPeriod()).or(DEFAULT_REFRESH_PERIOD))
      .setAccelerationGracePeriod(Optional.fromNullable(sourceConfig.getAccelerationGracePeriod()).or(DEFAULT_GRACE_PERIOD))
      .setCtime(System.currentTimeMillis())
      .setDescription("")
      .setImg("");
  }

  public static boolean isInternal(SourceConfig sourceConfig) {
    return isInternal(sourceConfig.getName());
  }

  public static boolean isInternal(String name) {
    return name.startsWith("__") || name.startsWith("$");
  }

  private StoragePlugin create(String name, StoragePluginConfig pluginConfig, boolean atRestart) throws ExecutionSetupException {
    return create(name, pluginConfig, null, atRestart);
  }

  /**
   * Create an internal source.
   *
   * Used for sources that have static metadata and are not managed by end users (such as system tables).
   *
   * Will create the source and persist it in Namespace but does not create/exist in plugin system table.
   *
   * Also schedules metadata refresh with Catalog service once per year as an UNKNOWN type source.
   *
   * @param name
   * @param pluginConfig
   * @param type
   * @return The create plugin.
   */
  private StoragePlugin registerInternal(String name, StoragePluginConfig pluginConfig, SourceType type) {
    final NamespaceService ns = context.getNamespaceService(SYSTEM_USERNAME);
    final NamespaceKey key = new NamespaceKey(name);

    if(!ns.exists(key, Type.SOURCE)) {
      final SourceConfig config = decorate(new SourceConfig()
          .setType(type).setName(name)
          .setAccelerationRefreshPeriod(DEFAULT_REFRESH_PERIOD)
          .setAccelerationGracePeriod(DEFAULT_GRACE_PERIOD)
          .setMetadataPolicy(ONE_YEAR_POLICY));

      try {
        ns.addOrUpdateSource(key, config);
      } catch (Exception ex) {
        // race condition, ignore.
      }
    }

    try {
      Constructor<? extends StoragePlugin> c = availablePlugins.get(pluginConfig.getClass());
      if(c == null) {
        throw new IllegalStateException(String.format("Unknown plugin config: %s. Known Plugin Configurations: %s.", pluginConfig.getClass().getName(), availablePlugins.keySet()));
      }
      StoragePlugin plugin = c.newInstance(pluginConfig, context, name);
      manuallyAddedPlugins.put(name, 0);
      catalog.registerSource(key, plugin);
      catalog.refreshSource(key, ONE_YEAR_POLICY);
      return plugin;
    } catch (Exception ex) {
      throw Throwables.propagate(ex);
    }
  }

  private StoragePlugin create(String name, StoragePluginConfig pluginConfig, SourceConfig sourceConfig, boolean atRestart) throws ExecutionSetupException {
    // name could be null if this is a ephemeral storage plugin.

    StoragePlugin plugin;
    boolean updateSourceInNamespace = false;
    // if sourceConfig is null, that means
    if (sourceConfig != null) {
      sourceConfig
        .setAccelerationRefreshPeriod(Optional.fromNullable(sourceConfig.getAccelerationRefreshPeriod()).or(DEFAULT_REFRESH_PERIOD))
        .setAccelerationGracePeriod(Optional.fromNullable(sourceConfig.getAccelerationGracePeriod()).or(DEFAULT_GRACE_PERIOD));
      updateSourceInNamespace = true;
    }
    Constructor<? extends StoragePlugin> c = availablePlugins.get(pluginConfig.getClass());
    if (c == null) {
      throw new ExecutionSetupException(String.format("Failure finding StoragePlugin constructor for config %s",
          pluginConfig));
    }
    final NamespaceKey sourceKey = (name == null ? null : new NamespaceKey(name));

    try {
      plugin = c.newInstance(pluginConfig, context, name);
      plugin.start();
      if (plugin.getState().getStatus() == SourceState.SourceStatus.bad
          && context.getOptionManager().getOption(ExecConstants.STORAGE_PLUGIN_CHECK_STATE)
          && !atRestart) {
        // Unable to start the plugin
        throw UserException.dataReadError()
          .message("Unable to connect to source %s", name)
          .build(logger);
      }
      try {
        if (name != null) {
          final NamespaceService ns = context.getNamespaceService(SYSTEM_USERNAME);
          if (ns != null) {
            // At restart, the source configuration is not passed into source creation. Instead, we use the
            // configuration that's already stored in the namespace service
            if (sourceConfig == null && atRestart && ns.exists(sourceKey, Type.SOURCE)) {
              sourceConfig = ns.getSource(sourceKey);
            }
            if (sourceConfig == null) {
              sourceConfig = decorate(new SourceConfig().setType(SourceType.UNKNOWN).setName(name));
            }
            if (updateSourceInNamespace || !ns.exists(sourceKey, Type.SOURCE)) {
              updateSourceInNamespace = true;
              ns.addOrUpdateSource(sourceKey, sourceConfig);
            }
          }
          catalog.registerSource(sourceKey, plugin);
        }
      } catch (ConcurrentModificationException ce) {
        if (sourceConfig == null) {
          logger.debug("Exception", ce);
        } else {
          throw UserException.concurrentModificationError()
            .message("Source updated concurrently either by another user, or by a background refresh. Please refresh and try again.")
            .build(logger);
        }
      } catch (NamespaceException ne) {
        if (sourceConfig == null) {
          logger.debug("Exception", ne);
        } else {
          throw ne;
        }
      }

      if (updateSourceInNamespace) {
        catalog.refreshSourceNames(sourceKey, sourceConfig.getMetadataPolicy() == null ? CatalogService.DEFAULT_METADATA_POLICY : sourceConfig.getMetadataPolicy());
        catalog.scheduleMetadataRefresh(sourceKey, sourceConfig);
      }
      return plugin;
    } catch (IOException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NamespaceException e) {
      Throwable t = e instanceof InvocationTargetException ? ((InvocationTargetException) e).getTargetException() : e;
      if (t instanceof ExecutionSetupException) {
        throw ((ExecutionSetupException) t);
      }
      throw new ExecutionSetupException(String.format("Failure setting up new storage plugin configuration for config %s", pluginConfig), t);
    }
  }

  @Override
  public Iterator<Entry<String, StoragePlugin>> iterator() {
    return plugins.iterator();
  }

  public synchronized void close() throws Exception {
    ephemeralPlugins.invalidateAll();
    plugins.close();
    pluginSystemTable.close();
  }

  /**
   * Get a list of all available storage plugin class constructors.
   * @param classpathScan
   *          A classpath scan to use.
   * @return A Map of StoragePluginConfig => StoragePlugin.<init>() constructors.
   */
  @SuppressWarnings("unchecked")
  public static Map<Class<?>, Constructor<? extends StoragePlugin>> findAvailablePlugins(final ScanResult classpathScan) {
    Map<Class<?>, Constructor<? extends StoragePlugin>> availablePlugins = new HashMap<>();
    final Set<Class<? extends StoragePlugin>> pluginClasses = (Set<Class<? extends StoragePlugin>>) (Object) classpathScan.getImplementations(StoragePlugin.class);
    final String lineBrokenList = pluginClasses.size() == 0 ? "" : "\n\t- " + Joiner.on("\n\t- ").join(pluginClasses);
    logger.debug("Found {} storage plugin configuration classes: {}.", pluginClasses.size(), lineBrokenList);
    for (Class<? extends StoragePlugin> plugin : pluginClasses) {
      int i = 0;
      for (Constructor<?> c : plugin.getConstructors()) {
        Class<?>[] params = c.getParameterTypes();
        if (params.length != 3
            || params[1] != SabotContext.class
            || !StoragePluginConfig.class.isAssignableFrom(params[0])
            || params[2] != String.class) {
          logger.info("Skipping StoragePlugin constructor {} for plugin class {} since it doesn't implement a "
              + "[constructor(StoragePluginConfig, SabotContext, String)]", c, plugin);
          continue;
        }
        availablePlugins.put(params[0], (Constructor<? extends StoragePlugin>) c);
        i++;
      }
      if (i == 0) {
        logger.debug("Skipping registration of StoragePlugin {} as it doesn't have a constructor with the parameters "
            + "of (StorangePluginConfig, Config)", plugin.getCanonicalName());
      }
    }
    return availablePlugins;
  }

  @VisibleForTesting
  public void refreshSourceMetadataInNamespace(final String pluginName, MetadataPolicy metadataPolicy)
    throws NamespaceException {
    StoragePlugin plugin2 = catalog.getStoragePlugin(pluginName);
    if(plugin2 != null){
      catalog.refreshSource(new NamespaceKey(pluginName), metadataPolicy);
    }
  }

  @Override
  @VisibleForTesting
  public void updateNamespace(Set<String> pluginNames, MetadataPolicy policy) {
    for (Entry<String,StoragePlugin> entry : plugins) {
      if (pluginNames.contains(entry.getKey())) {
        try {
          try {
            context.getNamespaceService(SYSTEM_USERNAME).getSource(new NamespaceKey(Collections.singletonList(entry.getKey())));
          } catch (NamespaceNotFoundException e) {
            try {
              createOrUpdate(entry.getKey(), entry.getValue().getId().getConfig(), true);
            } catch (ExecutionSetupException e1) {
              throw new RuntimeException(e1);
            }
          }

          StoragePlugin plugin2 = catalog.getStoragePlugin(entry.getKey());
          if(plugin2 != null){
            catalog.refreshSource(new NamespaceKey(entry.getKey()), policy);
          }
        } catch (NamespaceException e) {
          logger.warn("exception", e);
        }
      }
    }
  }
}
