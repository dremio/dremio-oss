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

import io.protostuff.ByteString;

/**
 * Force re-planning all acceleration layouts
 */
public class ReplanAllAccelerations extends UpgradeTask {

  ReplanAllAccelerations() {
    super("Forcing all accelerations to replan during next server startup", VERSION_106, VERSION_107);
  }

  @Override
  public void upgrade(UpgradeContext context) {
    final AccelerationStore accelerationStore = new AccelerationStore(context.getKVStoreProvider());
    accelerationStore.start();

    Iterable<Acceleration> accelerations = accelerationStore.find();
    for (Acceleration acceleration : accelerations) {
      final Iterable<Layout> layouts = AccelerationUtils.allLayouts(acceleration);
      for (Layout layout : layouts) {
        System.out.printf("  updating layout %s.%s%n", acceleration.getId().getId(), layout.getId().getId());
        layout.setLogicalPlan(ByteString.EMPTY);
      }

      accelerationStore.save(acceleration);
    }
  }
}
