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

import com.dremio.service.accelerator.AccelerationUtils;
import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.Layout;
import com.dremio.service.accelerator.store.AccelerationStore;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.ParentDataset;

import io.protostuff.ByteString;

/**
 * Force re-planning of all home file or folder based acceleration layouts
 */
public class ReplanHomeBasedAccelerations extends UpgradeTask {

  ReplanHomeBasedAccelerations() {
    super("Forcing all home file or folder based acceleration to replan during next server startup", VERSION_107, VERSION_120);
  }

  @Override
  public void upgrade(UpgradeContext context) {
    final AccelerationStore accelerationStore = new AccelerationStore(context.getKVStoreProvider());
    accelerationStore.start();
    final NamespaceService namespace = new NamespaceServiceImpl(context.getKVStoreProvider().get());

    //iterate through all acceleraions
    Iterable<Acceleration> accelerations = accelerationStore.find();
    for (Acceleration acceleration : accelerations) {
      DatasetConfig dataset = namespace.findDatasetByUUID(acceleration.getId().getId());
      //check if we need to replan layouts based on dataset
      checkAndReplanIfNeeded(context, accelerationStore, namespace, acceleration, dataset);
    }
  }
  /**
   * checks if the dataset is based on a home pds. If so , sets up layouts of the acceleration to replan and returns immediately.
   * rturns true if replan is required and false otherwise.
   * @param context
   * @param accelerationStore
   * @param namespace
   * @param acceleration
   * @param dataset
   * @return
   */
  private boolean checkAndReplanIfNeeded(UpgradeContext context,
      final AccelerationStore accelerationStore,
      final NamespaceService namespace,
      Acceleration acceleration,
      DatasetConfig dataset) {
    //if its a virtual dataset, recursively check its parents
    if (dataset.getVirtualDataset() != null) {
      if (dataset.getVirtualDataset().getParentsList() == null) {
        System.out.printf("Null parent list for %s. Skipping the dataset.",
            AccelerationUtils.makePathString(dataset.getFullPathList()));
        return false;
      }
      for (ParentDataset parent : dataset.getVirtualDataset().getParentsList()) {
        try {
          DatasetConfig parentDataset = namespace.getDataset(new NamespaceKey(parent.getDatasetPathList()));
          if (checkAndReplanIfNeeded(context, accelerationStore, namespace, acceleration, parentDataset)) {
            return true;
          }
        } catch (NamespaceException e) {
          System.out.println("Error : " + e.getMessage());
        }
      }
    } else if (dataset.getType() == DatasetType.PHYSICAL_DATASET_HOME_FILE ||
        dataset.getType() == DatasetType.PHYSICAL_DATASET_HOME_FOLDER) {
      replanAllLayouts(context, accelerationStore, acceleration);
      return true;
    }
    return false;
  }

  private void replanAllLayouts(UpgradeContext context,
      final AccelerationStore accelerationStore,
      Acceleration acceleration) {
    final Iterable<Layout> layouts = AccelerationUtils.allLayouts(acceleration);
    for (Layout layout : layouts) {
      System.out.printf("  Updating layout %s.%s%n", acceleration.getId().getId(), layout.getId().getId());
      layout.setLogicalPlan(ByteString.EMPTY);
      context.getUpgradeStats().layoutUpdated();
    }
    accelerationStore.save(acceleration);
  }
}
