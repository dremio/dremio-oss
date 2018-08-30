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

import java.util.Map;

import com.dremio.dac.proto.model.dataset.NameDatasetRef;
import com.dremio.dac.proto.model.dataset.VirtualDatasetVersion;
import com.dremio.dac.service.datasets.DatasetVersionMutator.VersionDatasetKey;
import com.dremio.dac.service.datasets.DatasetVersionMutator.VersionStoreCreator;
import com.dremio.datastore.KVStore;
import com.google.common.collect.Maps;

/**
 * Deletes history of renamed datasets by setting the "previous version" link of the latest version of renamed datasets
 * to null.
 */
public class DeleteHistoryOfRenamedDatasets extends UpgradeTask {

  public DeleteHistoryOfRenamedDatasets() {
    super("Delete history of renamed datasets", VERSION_106, VERSION_210);
  }

  @Override
  public void upgrade(UpgradeContext context) throws Exception {
    final KVStore<VersionDatasetKey, VirtualDatasetVersion> datasetVersions =
        context.getKVStoreProvider().get()
            .getStore(VersionStoreCreator.class);

    final Map<VersionDatasetKey, VirtualDatasetVersion> renamedDatasets = Maps.newHashMap();
    for (final Map.Entry<VersionDatasetKey, VirtualDatasetVersion> datasetVersion : datasetVersions.find()) {
      final String currentEntryPath = datasetVersion.getKey().getPath().toPathString();
      final NameDatasetRef prevEntry = datasetVersion.getValue().getPreviousVersion();
      if (prevEntry != null && !currentEntryPath.equals(prevEntry.getDatasetPath())) {
        renamedDatasets.put(datasetVersion.getKey(), datasetVersion.getValue());
      }
    }

    System.out.printf("Found [%d] renamed entries. Remove their previous version links.\n", renamedDatasets.size());
    for (final Map.Entry<VersionDatasetKey, VirtualDatasetVersion> datasetVersion : renamedDatasets.entrySet()) {
      final VirtualDatasetVersion newValue = datasetVersion.getValue()
          .setPreviousVersion(null);
      datasetVersions.put(datasetVersion.getKey(), newValue);
    }
  }
}
