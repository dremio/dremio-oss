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
package com.dremio.dac.scratch;

import java.net.URI;

import javax.inject.Provider;

import com.dremio.config.DremioConfig;
import com.dremio.exec.store.StoragePluginRegistry;
import com.dremio.exec.store.dfs.FileSystemConfig;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.dremio.service.Service;

/**
 * Creates a scratch storage spaces.
 */
public class ScratchService implements Service {

  public static final String NAME = "$scratch";

  private final Provider<StoragePluginRegistry> registry;
  private final URI scratchPath;

  public ScratchService(Provider<StoragePluginRegistry> pluginRegistry, DremioConfig config){
    this.registry = pluginRegistry;
    this.scratchPath = config.getURI(DremioConfig.SCRATCH_PATH_STRING);
  }

  @Override
  public void start() throws Exception {
    FileSystemConfig config = new FileSystemConfig(scratchPath, SchemaMutability.USER_TABLE);
    StoragePluginRegistry storagePluginRegistry = registry.get();
    storagePluginRegistry.createOrUpdate(NAME, config, true);
  }

  @Override
  public void close() {
  }

}
