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

import java.net.URI;
import java.net.URISyntaxException;

import javax.inject.Provider;

import com.dremio.config.DremioConfig;
import com.dremio.service.coordinator.proto.AwsKeys;

/**
 * merges info from file based config & store based config
 * if attribute present in store, use that. else, fallback to file.
 */
public class ProjectConfigImpl implements ProjectConfig {
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(ProjectConfigImpl.class);

  private static final String ACCELERATION_PLUGIN_SUB_PATH = "/accelerator";
  private static final String DOWNLOADS_PLUGIN_SUB_PATH = "/downloads";
  private static final String SCRATCH_PLUGIN_SUB_PATH = "/scratch";

  private final Provider<DremioConfig> fileProvider;
  private final Provider<ProjectConfigStore> storeProvider;

  public ProjectConfigImpl(Provider<DremioConfig> fileProvider, Provider<ProjectConfigStore> storeProvider) {
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
  public DistPathConfig getDownloadsConfig() {
    return getDistPathConfig(DremioConfig.DOWNLOADS_PATH_STRING, DOWNLOADS_PLUGIN_SUB_PATH);
  }

  private DistPathConfig getDistPathConfig(String pathString, String subPath) {
    URI path;
    String accessKey = null;
    String secretKey = null;
    ProjectConfigStore store = storeProvider.get();
    AwsKeys.Builder builder1 = AwsKeys.newBuilder();
    if (store.get() == null || !store.get().hasDistStoreConfig()) {
      path = fileProvider.get().getURI(pathString);
    } else {
      try {
        path = new URI(store.get().getDistStoreConfig().getPath() + subPath);
        LOGGER.info("Got dist path for {} from store {}. Uri {}", store.get().getDistStoreConfig().getPath(), path);
      } catch (URISyntaxException e) {
        path = fileProvider.get().getURI(pathString);
        LOGGER.error("Invalid dist path for {} in store {}. Uri {}", pathString, store.get().getDistStoreConfig().getPath(), path);
      }
      accessKey = store.get().getDistStoreConfig().getAwsKeys().getAccessKey();
      secretKey = store.get().getDistStoreConfig().getAwsKeys().getSecretKey();
      builder1.setSecretKey(secretKey).setAccessKey(accessKey);
    }
    return new DistPathConfig(path, builder1.build());
  }

  @Override
  public String getOrgId() {
    if (storeProvider.get().get() != null) {
      return storeProvider.get().get().getOrgId();
    }
    return null;
  }
}
