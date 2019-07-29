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
package com.dremio.service.coordinator.zk;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2;
import org.apache.curator.framework.recipes.locks.Lease;

import com.dremio.common.AutoCloseables;
import com.dremio.service.coordinator.DistributedSemaphore;

class ZkDistributedSemaphore implements DistributedSemaphore {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ZkDistributedSemaphore.class);

  private final InterProcessSemaphoreV2 semaphore;

  ZkDistributedSemaphore(CuratorFramework client, String path, int numberOfLeases) {
    this.semaphore = new InterProcessSemaphoreV2(client, path, numberOfLeases);
  }

  @Override
  public DistributedLease acquire(int permits, long time, TimeUnit unit) throws Exception {
    Collection<Lease> leases = semaphore.acquire(permits, time, unit);
    if (leases != null) {
      return new LeasesHolder(leases);
    }
    return null;
  }

  private class LeasesHolder implements DistributedLease {
    private Collection<Lease> leases;

    LeasesHolder(Collection<Lease> leases) {
      this.leases = leases;
    }

    @Override
    public void close() throws Exception {
      AutoCloseables.close(leases);
    }

  }
}
