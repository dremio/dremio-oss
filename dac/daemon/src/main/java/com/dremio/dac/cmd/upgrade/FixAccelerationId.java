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
import com.dremio.service.accelerator.proto.AccelerationEntry;
import com.dremio.service.accelerator.proto.AccelerationId;
import com.dremio.service.accelerator.store.AccelerationEntryStore;
import com.dremio.service.accelerator.store.AccelerationStore;
import com.google.common.base.Optional;

/**
 * Ensures accelerations use their dataset Id
 */
class FixAccelerationId extends UpgradeTask {

  FixAccelerationId() {
    super("Fixing acceleration Ids", VERSION_106, VERSION_107);
  }

  @Override
  public void upgrade(UpgradeContext context) {
    final AccelerationStore accelerationStore = new AccelerationStore(context.getKVStoreProvider());
    final AccelerationEntryStore entryStore = new AccelerationEntryStore(context.getKVStoreProvider());
    accelerationStore.start();
    entryStore.start();

    Iterable<Acceleration> accelerations = accelerationStore.find();
    for (Acceleration acceleration : accelerations) {
      final AccelerationId id = acceleration.getId();
      final String datasetId = acceleration.getContext().getDataset().getId().getId();
      if (datasetId.equals(id.getId())) {
        continue;
      }

      final AccelerationId updatedId = new AccelerationId(datasetId);
      acceleration.setId(updatedId);
      acceleration.setVersion(0L); // OCC is disabled so make sure to set a version
      accelerationStore.save(acceleration);
      accelerationStore.remove(id);

      Optional<AccelerationEntry> entry = entryStore.get(id);
      if (entry.isPresent()) {
        entry.get().getDescriptor().setId(updatedId);
        entry.get().getDescriptor().setVersion(0L);
        entryStore.save(entry.get());
        entryStore.remove(id);
      }

      context.getUpgradeStats().accelerationUpdated();
    }
  }
}
