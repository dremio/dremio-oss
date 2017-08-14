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

import java.util.List;

import com.dremio.service.accelerator.AccelerationUtils;
import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.Layout;
import com.dremio.service.accelerator.proto.Materialization;
import com.dremio.service.accelerator.proto.MaterializedLayout;
import com.dremio.service.accelerator.store.AccelerationStore;
import com.dremio.service.accelerator.store.MaterializationStore;
import com.google.common.base.Optional;

/**
 * Ensures all layouts, materialized layouts, and tasks have a version
 * associated with them.
 */
class SetLayoutVersion extends UpgradeTask {

  SetLayoutVersion() {
    super("Setting layout versions", VERSION_106, VERSION_107);
  }

  private boolean update(MaterializedLayout materializedLayout, Integer layoutVersion, MaterializationStore materializationStore, UpgradeStats stats) {
    boolean updated = false;
    if (materializedLayout.getVersion() == null) {
      updated = true;
      materializedLayout.setVersion((long) layoutVersion);
      stats.materializedLayoutUpdated();
    }

    final List<Materialization> materializations = materializedLayout.getMaterializationList();
    for (Materialization materialization : materializations) {
      if (materialization.getLayoutVersion() == null) {
        updated = true;
        materialization.setLayoutVersion(layoutVersion);
        stats.materializationUpdated();
      }
    }

    if (updated) {
      materializationStore.save(materializedLayout);
    }

    return updated;
  }

  private boolean update(Layout layout, MaterializationStore materializationStore, UpgradeStats stats) {
    boolean updated = false;

    Integer layoutVersion = layout.getVersion();
    if (layoutVersion == null) {
      layout.setVersion(0);
      layoutVersion = 0;
      stats.layoutUpdated();
      updated = true;
    }

    Optional<MaterializedLayout> mlOptional = materializationStore.get(layout.getId());
    if (mlOptional.isPresent() && update(mlOptional.get(), layoutVersion, materializationStore, stats)) {
      updated = true;
    }

    return updated;
  }

  @Override
  public void upgrade(UpgradeContext context) {
    final AccelerationStore accelerationStore = new AccelerationStore(context.getKVStoreProvider());
    final MaterializationStore materializationStore = new MaterializationStore(context.getKVStoreProvider());
    accelerationStore.start();
    materializationStore.start();

    Iterable<Acceleration> accelerations = accelerationStore.find();
    for (Acceleration acceleration : accelerations) {
      boolean anyLayoutUpdated = false;
      final Iterable<Layout> layouts = AccelerationUtils.allLayouts(acceleration);
      for (Layout layout : layouts) {
        anyLayoutUpdated |= update(layout, materializationStore, context.getUpgradeStats());
      }

      if (anyLayoutUpdated) {
        accelerationStore.save(acceleration);
      }
    }
  }
}
