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

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.dremio.config.DremioConfig;
import com.dremio.dac.homefiles.HomeFileConfig;
import com.dremio.dac.homefiles.HomeFileSystemPluginConfig;
import com.dremio.exec.store.StoragePluginRegistry;
import com.dremio.exec.store.dfs.FileSystemConfig;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.dremio.service.BindingProvider;
import com.dremio.service.Initializer;

/**
 * Create all the system storage plugins, such as results, accelerator, etc.
 * Also creates the backing directories for each of these plugins
 */
@SuppressWarnings("unused") // found through reflection search and executed by InitializerRegistry
public class SystemStoragePluginInitializer implements Initializer<Void> {
  @Override
  public Void initialize(BindingProvider provider) throws Exception {
    final DremioConfig config = provider.lookup(DremioConfig.class);
    StoragePluginRegistry storagePluginRegistry = provider.lookup(StoragePluginRegistry.class);
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

    FileSystemWrapper.get(resultsPath, fsConf).mkdirs(new Path(resultsPath.getPath()));
    storagePluginRegistry.createOrUpdate(DACDaemonModule.JOBS_STORAGEPLUGIN_NAME,
      new FileSystemConfig(resultsPath, SchemaMutability.SYSTEM_TABLE), true);

    FileSystemWrapper.get(scratchPath, fsConf).mkdirs(new Path(scratchPath.getPath()));
    storagePluginRegistry.createOrUpdate(DACDaemonModule.SCRATCH_STORAGEPLUGIN_NAME,
      new FileSystemConfig(scratchPath, SchemaMutability.USER_TABLE), true);
    return null;
  }
}
