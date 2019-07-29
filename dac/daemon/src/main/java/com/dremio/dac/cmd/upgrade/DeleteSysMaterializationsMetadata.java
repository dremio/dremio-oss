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
package com.dremio.dac.cmd.upgrade;

import com.dremio.dac.cmd.AdminLogger;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.collect.ImmutableList;

/**
 * Upgrade task that deletes all system tables.
 */
public class DeleteSysMaterializationsMetadata extends UpgradeTask {

  static final String taskUUID = "0f5c698a-55c9-45e7-84fe-c5f737e30432";

  public DeleteSysMaterializationsMetadata() {
    super("Deleting System table metadata", ImmutableList.of(UpdateExternalReflectionHash.taskUUID));
  }

  @Override
  public String getTaskUUID() {
    return taskUUID;
  }

  @Override
  public void upgrade(UpgradeContext context) {
    final NamespaceService namespaceService = new NamespaceServiceImpl(context.getKVStoreProvider());
    try {
      final NamespaceKey key = new DatasetPath(ImmutableList.of("sys", "materializations")).toNamespaceKey();
      final DatasetConfig dataset = namespaceService.getDataset(key);

      namespaceService.deleteDataset(key, dataset.getTag());
    } catch (NamespaceNotFoundException e) {
      // no metadata was found for sys.materializations
      // most likely the table was never queried
      // nothing more to do
      AdminLogger.log("  'sys.materializations' metadata not found...skipping");
    } catch (NamespaceException e) {
      throw new RuntimeException("Failed to delete metadata for 'sys.materialization'", e);
    }
  }
}
