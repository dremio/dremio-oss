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
package com.dremio.dac.cmd.upgrade;

import java.util.List;

import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.service.DirectProvider;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.RefreshMethod;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.reflection.ReflectionSettings;
import com.dremio.service.reflection.ReflectionUtils;

/**
 * Migrates pre-1.5 acceleration settings on sources and datasets to the new ReflectionSettings store.
 */
public class MoveFromAccelerationSettingsToReflectionSettings extends UpgradeTask {
  public MoveFromAccelerationSettingsToReflectionSettings() {
    super("Migrate from acceleration settings to reflection settings", VERSION_106, VERSION_150);
  }

  @Override
  public void upgrade(UpgradeContext context) {
    final NamespaceService namespaceService = new NamespaceServiceImpl(context.getKVStoreProvider().get());
    final ReflectionSettings reflectionSettings = new ReflectionSettings(DirectProvider.wrap(namespaceService), context.getKVStoreProvider());

    List<SourceConfig> sources = namespaceService.getSources();
    for (SourceConfig sourceConfig : sources) {
      try {
        ConnectionConf<?, ?> connectionConf = context.getConnectionReader().getConnectionConf(sourceConfig);
        if (connectionConf == null || connectionConf.isInternal()) {
          continue;
        }
      } catch (Exception e) {
        System.out.printf("  Could not get connection config of source [%s] with exception [%s]\n", sourceConfig.getName(), e);
        continue;
      }

      System.out.printf("  Migrating source [%s]\n", sourceConfig.getName());

      AccelerationSettings settings = new AccelerationSettings();
      settings.setRefreshPeriod(sourceConfig.getAccelerationRefreshPeriod());
      settings.setGracePeriod(sourceConfig.getAccelerationGracePeriod());
      settings.setMethod(RefreshMethod.FULL);

      reflectionSettings.setReflectionSettings(sourceConfig.getKey(), settings);

      // migrate all datasets under source
      try {
        List<NamespaceKey> allDatasets = namespaceService.getAllDatasets(sourceConfig.getKey());

        for (NamespaceKey key : allDatasets) {
          try {
            DatasetConfig datasetConfig = namespaceService.getDataset(key);

            // only PDS have settings, and we don't need to handle home datasets since they have non-changable settings
            if (ReflectionUtils.isPhysicalDataset(datasetConfig.getType()) && !ReflectionUtils.isHomeDataset(datasetConfig.getType())) {
              AccelerationSettings datasetAccelerationSettings = datasetConfig.getPhysicalDataset().getAccelerationSettings();

              // if the settings of the dataset match the source settings, no need to store them
              if (!datasetAccelerationSettings.equals(settings)) {
                reflectionSettings.setReflectionSettings(key, datasetAccelerationSettings);
              }
            }
          } catch (NamespaceException e) {
            System.out.printf("  Could not migrate settings of dataset [%s]\n", key.getName());
          }
        }
      } catch (NamespaceException e) {
        System.out.printf("  Could not migrate settings of datasets belonging to source [%s]\n", sourceConfig.getName());
      }

    }
  }
}
