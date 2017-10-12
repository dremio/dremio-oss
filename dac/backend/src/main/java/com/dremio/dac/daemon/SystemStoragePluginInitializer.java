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
package com.dremio.dac.daemon;

import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.dremio.config.DremioConfig;
import com.dremio.dac.homefiles.HomeFileConfig;
import com.dremio.dac.homefiles.HomeFileSystemPluginConfig;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.StoragePluginRegistry;
import com.dremio.exec.store.dfs.FileSystemConfig;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.dremio.service.BindingProvider;
import com.dremio.service.Initializer;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.source.proto.SourceConfig;

/**
 * Create all the system storage plugins, such as results, accelerator, etc.
 * Also creates the backing directories for each of these plugins
 */
@SuppressWarnings("unused") // found through reflection search and executed by InitializerRegistry
public class SystemStoragePluginInitializer implements Initializer<Void> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SystemStoragePluginInitializer.class);

  @Override
  public Void initialize(BindingProvider provider) throws Exception {
    final DremioConfig config = provider.lookup(DremioConfig.class);
    final StoragePluginRegistry storagePluginRegistry = provider.lookup(StoragePluginRegistry.class);
    final NamespaceService ns = provider.lookup(SabotContext.class).getNamespaceService(SYSTEM_USERNAME);

    final URI homePath = config.getURI(DremioConfig.UPLOADS_PATH_STRING);
    final URI accelerationPath = config.getURI(DremioConfig.ACCELERATOR_PATH_STRING);
    final URI resultsPath = config.getURI(DremioConfig.RESULTS_PATH_STRING);
    final URI scratchPath = config.getURI(DremioConfig.SCRATCH_PATH_STRING);
    final Configuration fsConf = FileSystemPlugin.getNewFsConf();

    FileSystemWrapper.get(homePath, fsConf).mkdirs(new Path(homePath.getPath()));
    storagePluginRegistry.createOrUpdate(HomeFileConfig.HOME_PLUGIN_NAME,
      new HomeFileSystemPluginConfig(provider.lookup(HomeFileConfig.class)), true);

    FileSystemWrapper.get(accelerationPath, fsConf).mkdirs(new Path(accelerationPath.getPath()));
    storagePluginRegistry.createOrUpdate(DACDaemonModule.ACCELERATOR_STORAGEPLUGIN_NAME,
      new FileSystemConfig(accelerationPath, SchemaMutability.SYSTEM_TABLE), true);
    try {
      final NamespaceKey sourceKey = new NamespaceKey(DACDaemonModule.ACCELERATOR_STORAGEPLUGIN_NAME);
      SourceConfig sourceConfig = ns.getSource(sourceKey);
      if (sourceConfig.getMetadataPolicy() == null) {
        sourceConfig.setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY);
        ns.addOrUpdateSource(sourceKey, sourceConfig);
      }
    } catch (Throwable e) {
      logger.warn("Couldn't set metadata policy on the accelerator storage plugin", e);
    }

    FileSystemWrapper.get(resultsPath, fsConf).mkdirs(new Path(resultsPath.getPath()));
    storagePluginRegistry.createOrUpdate(DACDaemonModule.JOBS_STORAGEPLUGIN_NAME,
      new FileSystemConfig(resultsPath, SchemaMutability.SYSTEM_TABLE), true);

    FileSystemWrapper.get(scratchPath, fsConf).mkdirs(new Path(scratchPath.getPath()));
    storagePluginRegistry.createOrUpdate(DACDaemonModule.SCRATCH_STORAGEPLUGIN_NAME,
      new FileSystemConfig(scratchPath, SchemaMutability.USER_TABLE), true);
    return null;
  }
}
