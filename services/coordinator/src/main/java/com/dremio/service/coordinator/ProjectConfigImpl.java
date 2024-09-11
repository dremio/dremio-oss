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
package com.dremio.service.coordinator;

import com.dremio.config.DremioConfig;
import com.dremio.service.coordinator.proto.DataCredentials;
import java.net.URI;
import java.net.URISyntaxException;
import javax.inject.Provider;

/**
 * merges info from file based config & store based config if attribute present in store, use that.
 * else, fallback to file.
 */
public class ProjectConfigImpl implements ProjectConfig {
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(ProjectConfigImpl.class);

  public static final String ACCELERATION_PLUGIN_SUB_PATH = "/accelerator";
  public static final String UPLOADS_PLUGIN_SUB_PATH = "/uploads";
  public static final String SCRATCH_PLUGIN_SUB_PATH = "/scratch";
  public static final String METADATA_PLUGIN_SUB_PATH = "/metadata";
  public static final String GANDIVA_PERSISTENT_CACHE_PLUGIN_SUB_PATH = "/gandiva";
  public static final String SYSTEM_ICEBERG_TABLES_PLUGIN_SUB_PATH = "/system_iceberg_tables";
  public static final String NODE_HISTORY_PLUGIN_SUB_PATH = "/node_history";

  private final Provider<DremioConfig> fileProvider;
  private final Provider<ProjectConfigStore> storeProvider;

  public ProjectConfigImpl(
      Provider<DremioConfig> fileProvider, Provider<ProjectConfigStore> storeProvider) {
    this.fileProvider = fileProvider;
    this.storeProvider = storeProvider;
  }

  @Override
  public DistPathConfig getAcceleratorConfig() {
    return getDistPathConfig(DremioConfig.ACCELERATOR_PATH_STRING, ACCELERATION_PLUGIN_SUB_PATH);
  }

  @Override
  public DistPathConfig getScratchConfig() {
    return getDistPathConfig(DremioConfig.SCRATCH_PATH_STRING, SCRATCH_PLUGIN_SUB_PATH);
  }

  @Override
  public DistPathConfig getUploadsConfig() {
    return getDistPathConfig(DremioConfig.UPLOADS_PATH_STRING, UPLOADS_PLUGIN_SUB_PATH);
  }

  @Override
  public DistPathConfig getMetadataConfig() {
    return getDistPathConfig(DremioConfig.METADATA_PATH_STRING, METADATA_PLUGIN_SUB_PATH);
  }

  @Override
  public DistPathConfig getGandivaPersistentCacheConfig() {
    return getDistPathConfig(
        DremioConfig.GANDIVA_CACHE_PATH_STRING, GANDIVA_PERSISTENT_CACHE_PLUGIN_SUB_PATH);
  }

  @Override
  public DistPathConfig getSystemIcebergTablesConfig() {
    return getDistPathConfig(
        DremioConfig.SYSTEM_ICEBERG_TABLES_PATH_STRING, SYSTEM_ICEBERG_TABLES_PLUGIN_SUB_PATH);
  }

  @Override
  public DistPathConfig getNodeHistoryConfig() {
    return getDistPathConfig(DremioConfig.NODE_HISTORY_PATH_STRING, NODE_HISTORY_PLUGIN_SUB_PATH);
  }

  protected DistPathConfig getDistPathConfig(String pathString, String subPath) {
    URI path;
    ProjectConfigStore store = storeProvider.get();
    DataCredentials dataCredentials = null;
    if (store.get() == null || !store.get().hasDistStoreConfig()) {
      path = fileProvider.get().getURI(pathString);
    } else {
      try {
        path = new URI(store.get().getDistStoreConfig().getPath() + subPath);
        LOGGER.info(
            "Got dist path for {} from store {}. Uri {}",
            pathString,
            store.get().getDistStoreConfig().getPath(),
            path);
      } catch (URISyntaxException e) {
        path = fileProvider.get().getURI(pathString);
        LOGGER.error(
            "Invalid dist path for {} in store {}. Uri {}",
            pathString,
            store.get().getDistStoreConfig().getPath(),
            path);
      }
      if (store.get().getDataCredentials().hasKeys()) {
        dataCredentials =
            DataCredentials.newBuilder()
                .setKeys(store.get().getDataCredentials().getKeys())
                .build();
      } else if (store.get().getDataCredentials().hasDataRole()) {
        dataCredentials =
            DataCredentials.newBuilder()
                .setDataRole(store.get().getDataCredentials().getDataRole())
                .build();
      } else if (store.get().getDataCredentials().hasClientAccess()) {
        dataCredentials =
            DataCredentials.newBuilder()
                .setClientAccess(store.get().getDataCredentials().getClientAccess())
                .build();
      }
    }
    return new DistPathConfig(path, dataCredentials);
  }

  @Override
  public String getOrgId() {
    if (storeProvider.get().get() != null) {
      return storeProvider.get().get().getOrgId();
    }
    return null;
  }
}
