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
package com.dremio.dac.cmd.upgrade;

import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.AccelerationId;
import com.dremio.service.accelerator.store.AccelerationStore;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;

/**
 * Ensures accelerations use their dataset Id
 */
class FixAccelerationVersion extends UpgradeTask {

  FixAccelerationVersion() {
    super("Fixing acceleration version", VERSION_106, VERSION_107);
  }

  @Override
  public void upgrade(UpgradeContext context) {
    final AccelerationStore accelerationStore = new AccelerationStore(context.getKVStoreProvider());
    final NamespaceService namespaceService = new NamespaceServiceImpl(context.getKVStoreProvider().get());
    accelerationStore.start();

    Iterable<Acceleration> accelerations = accelerationStore.find();
    for (Acceleration acceleration : accelerations) {
      final AccelerationId id = acceleration.getId();
      final DatasetConfig accelerationDataset = acceleration.getContext().getDataset();

      DatasetConfig datasetConfig;
      try {
        datasetConfig = namespaceService.getDataset(new NamespaceKey(accelerationDataset.getFullPathList()));
      } catch (NamespaceException e) {
        // ignore this
        continue;
      }
      // check the dataset version here
      if (datasetConfig.getVersion().longValue() == accelerationDataset.getVersion().longValue() + 1) {
        System.out.printf("Updating version for acceleration with id = %s%n", id.getId());
        accelerationDataset.setVersion(datasetConfig.getVersion());
        accelerationStore.save(acceleration);
      }
    }
  }
}
