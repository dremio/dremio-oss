/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.dac.daemon;

import static com.dremio.dac.service.datasets.DatasetDownloadManager.DATASET_DOWNLOAD_STORAGE_PLUGIN;
import static com.dremio.dac.support.SupportService.DREMIO_LOG_PATH_PROPERTY;
import static com.dremio.dac.support.SupportService.LOCAL_STORAGE_PLUGIN;
import static com.dremio.dac.support.SupportService.LOGS_STORAGE_PLUGIN;
import static com.dremio.dac.support.SupportService.TEMPORARY_SUPPORT_PATH;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ConcurrentModificationException;

import com.dremio.config.DremioConfig;
import com.dremio.dac.homefiles.HomeFileConf;
import com.dremio.dac.homefiles.HomeFileSystemStoragePlugin;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.InternalFileConf;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.dremio.service.BindingProvider;
import com.dremio.service.Initializer;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.reflection.materialization.AccelerationStoragePluginConfig;
import com.google.common.annotations.VisibleForTesting;

/**
 * Create all the system storage plugins, such as results, accelerator, etc.
 * Also creates the backing directories for each of these plugins
 */
@SuppressWarnings("unused") // found through reflection search and executed by InitializerRegistry
public class SystemStoragePluginInitializer implements Initializer<Void> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SystemStoragePluginInitializer.class);

  @Override
  public Void initialize(BindingProvider provider) throws Exception {
    final SabotContext sabotContext = provider.lookup(SabotContext.class);
    boolean isMaster = sabotContext.getRoles().contains(ClusterCoordinator.Role.MASTER);
    if (!isMaster) {
      logger.debug("System storage plugins will be created only on master coordinator");
      return null;
    }
    final DremioConfig config = provider.lookup(DremioConfig.class);
    final CatalogService catalogService = provider.lookup(CatalogService.class);
    final NamespaceService ns = provider.lookup(SabotContext.class).getNamespaceService(SYSTEM_USERNAME);

    final Path supportPath = Paths.get(sabotContext.getOptionManager().getOption(TEMPORARY_SUPPORT_PATH));
    final Path logPath = Paths.get(System.getProperty(DREMIO_LOG_PATH_PROPERTY, "/var/log/dremio"));

    final URI homePath = config.getURI(DremioConfig.UPLOADS_PATH_STRING);
    final URI accelerationPath = config.getURI(DremioConfig.ACCELERATOR_PATH_STRING);
    final URI resultsPath = config.getURI(DremioConfig.RESULTS_PATH_STRING);
    final URI scratchPath = config.getURI(DremioConfig.SCRATCH_PATH_STRING);
    final URI downloadPath = config.getURI(DremioConfig.DOWNLOADS_PATH_STRING);
    // Do not construct URI simply by concatenating, as it might not be encoded properly
    final URI logsPath = new URI("pdfs", "///" + logPath.toUri().getPath(), null);
    final URI supportURI = supportPath.toUri();

    SourceConfig home = new SourceConfig();
    HomeFileConf hfc = new HomeFileConf(config);
    home.setConnectionConf(hfc);
    home.setName(HomeFileSystemStoragePlugin.HOME_PLUGIN_NAME);
    home.setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY);

    createOrUpdateSystemSource(catalogService, ns, home);

    createOrUpdateSystemSource(catalogService, ns, AccelerationStoragePluginConfig.create(accelerationPath));

    createOrUpdateSystemSource(catalogService, ns,
      InternalFileConf.create(DACDaemonModule.JOBS_STORAGEPLUGIN_NAME, resultsPath, SchemaMutability.SYSTEM_TABLE,
          CatalogService.NEVER_REFRESH_POLICY_WITH_AUTO_PROMOTE));

    createOrUpdateSystemSource(catalogService, ns,
      InternalFileConf.create(DACDaemonModule.SCRATCH_STORAGEPLUGIN_NAME, scratchPath, SchemaMutability.USER_TABLE,
          CatalogService.NEVER_REFRESH_POLICY_WITH_AUTO_PROMOTE));

    createOrUpdateSystemSource(catalogService, ns,
      InternalFileConf.create(DATASET_DOWNLOAD_STORAGE_PLUGIN, downloadPath, SchemaMutability.USER_TABLE,
          CatalogService.NEVER_REFRESH_POLICY));

    createOrUpdateSystemSource(catalogService, ns,
      InternalFileConf.create(LOGS_STORAGE_PLUGIN, logsPath, SchemaMutability.NONE,
          CatalogService.DEFAULT_METADATA_POLICY));

    createOrUpdateSystemSource(catalogService, ns,
      InternalFileConf.create(LOCAL_STORAGE_PLUGIN, supportURI, SchemaMutability.SYSTEM_TABLE,
          CatalogService.NEVER_REFRESH_POLICY));

    return null;
  }

  /**
   * Create provided source if does not exist
   * or update if does exist
   * used for Such internal sources that can change based on the external configuration
   * such as hdfs to pdfs, directory structures
   * @param catalogService
   * @param ns
   * @param config
   */
  @VisibleForTesting
  void createOrUpdateSystemSource(final CatalogService catalogService, final NamespaceService ns, final
  SourceConfig config) throws Exception {
    try {
      final boolean isCreated = catalogService.createSourceIfMissingWithThrow(config);
      if (isCreated) {
        return;
      }
    } catch (ConcurrentModificationException ex) {
      // someone else got there first, ignore this failure.
      logger.info("Two source creations occurred simultaneously, ignoring the failed one.", ex);
      // proceed with update
    }
    final NamespaceKey nsKey = new NamespaceKey(config.getName());
    final SourceConfig oldConfig = ns.getSource(nsKey);
    final SourceConfig updatedConfig = config;
    // make incoming config match existing config to be used in comparison
    updatedConfig
      .setId(oldConfig.getId())
      .setCtime(oldConfig.getCtime())
      .setVersion(oldConfig.getVersion());
    // if old and new configs match don't update
    if (oldConfig.equals(updatedConfig)) {
      return;
    }
    ((CatalogServiceImpl) catalogService).getSystemUserCatalog().updateSource(updatedConfig);
  }
}
