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

import com.dremio.common.Version;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.google.common.collect.ImmutableList;

/**
 * In Dremio 2.0.10 and 2.1.0 we upgraded the Hive client version to 2.1.1 from 1.2.1. We store the InputSplits
 * in Dataset to avoid getting the splits every time the table is queried. After the Hive client upgrade the InputSplits
 * may not be deserializable with Hive 2.1.1 client. This upgrade task is to delete such InputSplits so that we
 * find the splits based on the new Hive client classes when the table is queried or when the metadata refresh happens
 */
public class DeleteHive121BasedInputSplits extends UpgradeTask implements LegacyUpgradeTask {

  //DO NOT MODIFY
  static final String taskUUID = "a5d23112-f354-42fe-bdeb-b024d8d5fb1b";

  public DeleteHive121BasedInputSplits() {
    super("Deleting Hive 1.2.1 based InputSplits", ImmutableList.of(DeleteHistoryOfRenamedDatasets.taskUUID));
  }

  @Override
  public Version getMaxVersion() {
    return VERSION_2010;
  }

  @Override
  public String getTaskUUID() {
    return taskUUID;
  }

  @Override
  public void upgrade(UpgradeContext context) throws Exception {
    final NamespaceService namespaceService = new NamespaceServiceImpl(context.getKVStoreProvider());

    try {
      for (SourceConfig source : namespaceService.getSources()) {
        if (!"HIVE".equalsIgnoreCase(ConnectionReader.toType(source))) {
          continue;
        }

        System.out.printf("  Handling Hive source %s%n", source.getName());
        for (NamespaceKey datasetPath : namespaceService.getAllDatasets(new NamespaceKey(source.getName()))) {
          final DatasetConfig datasetConfig = namespaceService.getDataset(datasetPath);

          if (datasetConfig.getReadDefinition() == null || datasetConfig.getReadDefinition().getExtendedProperty() == null) {
            continue;
          }

          System.out.printf("    Clearing read definition of table %s%n", datasetPath.getSchemaPath());
          datasetConfig.setReadDefinition(null);
          namespaceService.addOrUpdateDataset(datasetPath, datasetConfig);
        }
      }
    } catch (NamespaceException e) {
      throw new RuntimeException("Hive121BasedInputSplits failed", e);
    }
  }

  @Override
  public String toString() {
    return String.format("'%s' up to %s)", getDescription(), getMaxVersion());
  }
}
